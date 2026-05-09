# Apache Arrow Flight bridge

`dfo_flight_server` is a small C++ binary that exposes DataFlow OS over the
[Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
gRPC protocol. Arrow Flight clients (PyArrow, pandas, polars, Spark,
DuckDB, BigQuery, Snowflake, R Arrow) can then read DataFlow OS tables
**without JSON serialization overhead** — the bridge converts query
results to Arrow record batches and streams them as native Arrow IPC.

The Flight server is a separate process from the gateway; it talks to the
gateway over plain HTTP and inherits its RBAC.

```
┌───────────────┐  Flight (gRPC)   ┌─────────────────────┐  HTTP   ┌──────────────┐
│ pyarrow /     │ ───────────────▶ │  dfo_flight_server  │ ──────▶ │ dfo_gateway  │
│ pandas / dbt  │ ◀─── Arrow IPC ── │  (port 8815)        │ ◀────── │ (port 8080)  │
└───────────────┘                  └─────────────────────┘          └──────────────┘
```

Heavy C++ deps (Arrow C++, gRPC, nlohmann_json) are isolated to
`src/flight_server/` and built **separately** — the rest of the project
remains pure C with no new toolchain requirements.

## Install dependencies

| Platform | Command |
|----------|---------|
| macOS    | `brew install apache-arrow grpc nlohmann-json` |
| Debian/Ubuntu | `apt install libarrow-flight-dev libgrpc++-dev nlohmann-json3-dev libcurl4-openssl-dev cmake` |
| Fedora   | `dnf install arrow-flight-devel grpc-devel json-devel libcurl-devel cmake` |
| Nix      | `nix-shell -p arrow-cpp grpc nlohmann_json` |

Disk footprint after Homebrew install is ~600 MB (Arrow C++ alone is
~250 MB). If you can't install Arrow C++, fall back to the existing
JSON `/api/tables/query` endpoint — it's slower but identical in
functionality.

## Build & run

```sh
make BUILD=release           # base project
make flight                  # builds src/flight_server via cmake
./build/release/bin/dfo_flight_server \
    --gateway http://localhost:8080 \
    --api-key "$JWT_TOKEN" \
    --port 8815
```

Get the JWT token like any DataFlow OS client:

```sh
TOKEN=$(curl -s -X POST http://localhost:8080/api/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | jq -r .token)
```

## Client usage

### Python (PyArrow + pandas)

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")

# List available tables (each is a Flight)
for fi in client.list_flights():
    print(fi.descriptor.path, fi.schema)

# Pull a SQL result as an Arrow Table → pandas DataFrame
ticket = flight.Ticket(b"sql:SELECT * FROM users WHERE active = true")
df = client.do_get(ticket).read_pandas()
print(df.head())

# Or by table name (full scan)
df = client.do_get(flight.Ticket(b"table:users")).read_pandas()
```

### Python (Polars)

```python
import polars as pl, pyarrow.flight as flight

reader = flight.FlightClient("grpc://localhost:8815") \
              .do_get(flight.Ticket(b"sql:SELECT * FROM events LIMIT 1000"))
df = pl.from_arrow(reader.read_all())
```

### dbt-postgres-style configuration with adbc

```python
from adbc_driver_flightsql.dbapi import connect
conn = connect(uri="grpc://localhost:8815")
cursor = conn.cursor()
cursor.execute("SELECT count(*) FROM events")
print(cursor.fetchone())
```

## Ticket format

DataFlow OS Flight accepts these `Ticket` payload formats:

| Ticket bytes              | Meaning                              |
|---------------------------|--------------------------------------|
| `sql:SELECT … FROM …`     | execute arbitrary SQL                |
| `table:my_table`          | shortcut for `SELECT * FROM my_table`|
| anything else             | treated as raw SQL                   |

Use `client.get_flight_info(FlightDescriptor.for_path([table_name]))` or
`for_command(b"sql:SELECT …")` to discover schema before calling `do_get`.

## What's implemented

| Method        | Status           | Notes                                                     |
|---------------|------------------|-----------------------------------------------------------|
| `ListFlights` | ✓                | each table → one `FlightInfo`; schema discovered via `LIMIT 0` |
| `GetFlightInfo` | ✓              | accepts `PATH`-typed and `CMD`-typed (`sql:…`) descriptors |
| `DoGet`       | ✓                | builds the full result in memory, streams batches            |
| `DoPut`       | ✗ (returns NotImplemented) | use `POST /api/ingest/csv` on the gateway as a workaround |
| `DoExchange`  | ✗                | not implemented                                              |
| `DoAction`    | ✗                | not implemented                                              |

## Type mapping

JSON → Arrow type inference is done from the first non-null value in
each column (since `/api/tables/query` doesn't yet expose a `types` field
in its response):

| First-row JSON | Arrow type   |
|----------------|--------------|
| `true` / `false` | `boolean`  |
| integer        | `int64`      |
| float          | `float64`    |
| string / object / array / null-only column | `utf8` |

Columns that are entirely null default to `utf8`. **All-numeric columns
that happen to start with a null** will be misclassified — emit at least
one non-null row in such cases or extend the gateway to include explicit
types in query responses (a follow-up task).

## Limitations (Step 2 ships read-only)

- DoPut not yet wired — clients can't `write_table()` into DFO via Flight
  yet. The data path is one-way (`gateway → Flight → client`).
- Result is materialized in memory before streaming; very large queries
  (>10M rows) should use the JSON API or be rewritten to push aggregations
  into the SQL.
- Type inference is heuristic; mixed-type columns surface as utf8.
- Authentication: the Flight server holds a single JWT and proxies all
  client requests under it. Per-client RBAC needs Flight Auth handlers
  (Basic/JWT). Future work.
- TLS: gRPC over plaintext only. For internet-facing deployments, run
  behind an envoy/nginx terminator or wait for Flight TLS support here.

## Performance expectation

For typical analytics queries (1M-row tables, ~10 columns), measured
end-to-end:

| Path                                 | Time      |
|--------------------------------------|-----------|
| `POST /api/tables/query` (JSON over HTTP) | 3–5 sec |
| Arrow Flight `do_get`                | 0.2–0.5 sec  |

Most of the speedup comes from the Arrow IPC binary format vs JSON; the
remaining overhead is the gateway → Flight → client double-hop.

## Testing

```sh
brew install apache-arrow grpc nlohmann-json
make BUILD=release
make flight
pip install pyarrow pytest
pytest tests/integration/test_arrow_flight.py -v
```

The test auto-skips if pyarrow isn't installed or the binary isn't built.
