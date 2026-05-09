# PostgreSQL wire-protocol server

DataFlow OS speaks just enough of the [PostgreSQL v3 frontend/backend protocol](
https://www.postgresql.org/docs/current/protocol.html) to let standard
PostgreSQL clients connect — `psql`, JDBC, ODBC, psycopg2/3, asyncpg,
dbt-postgres, DBeaver, Tableau, Power BI, Metabase, Superset.

This doc describes **Step 3 Week 1** — handshake, auth, and a small set of
compatibility probes that satisfy psql's connection ritual. Real qengine
integration is Week 2.

## Enable

In your gateway config:

```json
{
  "port": 8080,
  "pgwire_enabled": 1,
  "pgwire_port": 5432
}
```

Gateway log on startup:

```
[INFO] PostgreSQL wire-protocol enabled on :5432 (Week 1: handshake + simple queries only)
[INFO] pgwire: listening on :5432
```

## Connect

```sh
PGPASSWORD=admin psql -h localhost -p 5432 -U admin -d dataflow

# or via psycopg
python -c "
import psycopg2
c = psycopg2.connect('host=localhost port=5432 user=admin password=admin dbname=dataflow')
print(c.cursor().execute('SELECT version()'))
"
```

DBeaver / Tableau / Power BI: pick "PostgreSQL" connector, use those same
credentials. SSL: pick "disable" (TLS termination is Week 4).

## Auth

Cleartext password over plaintext TCP. Two credential paths:

| Username | Password                                          | Verified against        |
|----------|---------------------------------------------------|-------------------------|
| `admin`  | the `admin_password` from your gateway config     | direct compare          |
| anything | a `dfo_<hex>` API key minted via `/api/auth/apikeys` | `auth_apikey_verify()` |

Cleartext is **fine over loopback / VPN / private networks**. If you're
exposing the port over the public internet, terminate TLS upstream
(stunnel / nginx stream) until Week 4 lands native pgwire SSL.

## What works in Week 1

| SQL                              | Behaviour                                        |
|----------------------------------|--------------------------------------------------|
| `SELECT 1` (any constant int)    | returns the integer                              |
| `SELECT version()`               | returns `DataFlow OS 0.1 (...)`                  |
| `SHOW server_version`            | same banner as `version()`                       |
| `SELECT current_database()`      | returns the database from startup                |
| `SELECT current_user` / `user`   | returns the username from startup                |
| `SELECT current_schema()`        | returns `public`                                 |
| `BEGIN` / `COMMIT` / `ROLLBACK`  | accepted as no-ops (no real txn over pgwire yet) |
| `SET key = value`                | accepted as no-op (returns `SET`)                |
| `DISCARD ALL`                    | accepted as no-op                                |
| empty statement                  | returns empty `CommandComplete`                  |
| anything else                    | **error** pointing at `POST /api/tables/query`   |

## What's coming

| Week  | Scope                                                                |
|-------|----------------------------------------------------------------------|
| 1 ✓   | TCP listener, startup, cleartext auth, simple-query subset above     |
| 2     | Full SQL execution via the existing qengine (SELECT/INSERT/UPDATE/DELETE) |
| 2     | Extended Query: Parse / Bind / Execute (psycopg parameter binding)   |
| 3     | `pg_catalog` + `information_schema` emulation → DBeaver shows tables |
| 4     | SCRAM-SHA-256 auth + TLS termination + dbt / BI tool polish          |

## Type mapping (Week 1)

The Week 1 query handler emits these PostgreSQL OIDs:

| DataFlow ColType | PostgreSQL OID  | Type name |
|------------------|-----------------|-----------|
| `COL_INT64`      | 20              | `int8` (bigint) |
| `COL_DOUBLE`     | 701             | `float8`        |
| `COL_TEXT`       | 25              | `text`          |
| `COL_BOOL`       | 16              | `bool`          |
| (timestamp via Week 2)| 1114       | `timestamp`     |

All values are sent in **text format** (format code 0). Binary format
support is not planned for Week 2; clients receive numbers as their
decimal representation.

## Operational notes

- **Per-connection thread.** The accept loop spawns a detached pthread
  for every socket. There is no connection limit yet — front it behind
  `pgbouncer` or similar if you need it.
- **No cancel support.** `Ctrl-C` in psql sends a `CancelRequest` on a
  separate socket; we accept the connection but ignore it. Long
  queries cannot yet be cancelled mid-flight.
- **No SSL.** SSLRequest is gracefully replied with `'N'` so clients
  fall back to cleartext.
- **Logs** go to gateway stderr like everything else. `pgwire: client
  connected fd=...`, `pgwire: auth ok user=...`, `pgwire: client
  disconnected`.

## Tests

```sh
make BUILD=release all
bash tests/integration/test_pgwire.sh
```

12 checks covering handshake, auth (good + bad password + sslmode
variants), every recognized statement, BEGIN/SET/COMMIT no-op flow,
and gateway log assertions. Skipped automatically if `psql` isn't in
`PATH`.

## Known limitations (Week 1 — don't file bugs for these)

- Real query execution is not wired yet. `SELECT * FROM users` returns
  a friendly error. Use `POST /api/tables/query` for full SQL.
- BEGIN/COMMIT are accepted but transactional semantics are NOT enforced
  on pgwire — they're just protocol no-ops to keep psql/DBeaver happy.
- `pg_catalog.pg_class` etc. don't exist yet, so DBeaver's left-pane
  table tree is empty until Week 3.
- Cancel, NOTIFY/LISTEN, COPY, prepared statements, two-phase commit:
  not implemented.
