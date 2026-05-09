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

## What works (Week 2 — real qengine integration)

After Week 2 the wire-protocol bridge runs SQL through the same `sql_parse`
+ `exec_stmt` path that powers `POST /api/tables/query`. Anything the JSON
API understands now works over pgwire too — including SELECT with FROM,
WHERE, ORDER BY, GROUP BY, aggregates, JOINs, subqueries, and window
functions.

| SQL                              | Behaviour                                        |
|----------------------------------|--------------------------------------------------|
| `SELECT * FROM users`            | full qengine execution; rows streamed as text    |
| `SELECT name FROM users WHERE …` | filters / projections                            |
| `SELECT COUNT(*), AVG(x) …`      | aggregates with optional GROUP BY                |
| `INSERT / UPDATE / DELETE`       | DML; CommandComplete tag includes affected count |
| `SELECT 1` / any constant        | works via the engine                             |
| `SELECT version()`               | returns `DataFlow OS 0.1 (...)` (canned probe)   |
| `SHOW server_version`            | same banner as `version()`                       |
| `SELECT current_database()`      | returns the database from startup                |
| `SELECT current_user` / `user`   | returns the username from startup                |
| `SELECT current_schema()`        | returns `public`                                 |
| `BEGIN` / `COMMIT` / `ROLLBACK`  | accepted as no-ops (no real txn over pgwire yet) |
| `SET key = value`                | accepted as no-op (returns `SET`)                |
| `DISCARD ALL`                    | accepted as no-op                                |
| empty statement                  | returns empty `CommandComplete`                  |
| parser error                     | proper `ErrorResponse` with SQLSTATE 42601       |

## What's coming

| Week  | Scope                                                                |
|-------|----------------------------------------------------------------------|
| 1 ✓   | TCP listener, startup, cleartext auth, compatibility probes          |
| 2 ✓   | Real qengine integration via `api_pg_execute` — Simple Query path    |
| 3     | Extended Query: Parse / Bind / Execute (psycopg parameter binding)   |
| 3     | `pg_catalog` + `information_schema` emulation → DBeaver shows tables |
| 3     | Type-OID polish — int8 / float8 / bool / timestamp instead of text   |
| 4     | SCRAM-SHA-256 auth + TLS termination + dbt / BI tool compat polish   |

## Type mapping (Week 2 — pragmatic: text for everything)

Week 2 sends every column as PostgreSQL `text` (OID 25) in text format
(format code 0). The internal qengine already serializes cells as
strings, so emitting them as `text` is a clean pass-through.

This works fine for psql, JDBC drivers, and most BI tools — they
coerce text values into the application-side type on the client. Tools
that probe metadata to plan operator dispatch (dbt's COALESCE-with-int,
some Tableau aggregations) may want precise OIDs; those land in Week 3
together with the `pg_catalog` emulation.

| DataFlow ColType | Wire OID (Week 2) | Final OID (Week 3+) |
|------------------|-------------------|----------------------|
| `COL_INT64`      | 25 (text)         | 20 (int8)            |
| `COL_DOUBLE`     | 25 (text)         | 701 (float8)         |
| `COL_TEXT`       | 25 (text)         | 25 (text)            |
| `COL_BOOL`       | 25 (text)         | 16 (bool)            |
| timestamp        | 25 (text)         | 1114 (timestamp)     |

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

## Known limitations (Week 2 — don't file bugs for these)

- **All columns reported as `text`.** Type-OID polish is Week 3.
- **No Extended Query protocol** (Parse/Bind/Execute). Parameter binding
  via psycopg's `cur.execute(sql, params)` falls back to in-driver
  string substitution and works for most cases. Server-side prepared
  statements and statement cache: Week 3.
- **BEGIN/COMMIT/ROLLBACK are no-ops at the engine level.** They emit
  the right protocol tags so psql / DBeaver are happy, but the
  transaction manager is not threaded through. Use the JSON
  `/api/tables/query` endpoint with `txn_id` for real transactions.
- **No `pg_catalog` / `information_schema` yet.** DBeaver and similar
  tools showing the left-pane table tree will be empty until Week 3.
- **RBAC bypassed for pgwire.** Auth happens once at connect time; per
  query RBAC checks (used by the JSON API) are not yet wired into the
  pgwire path. Users with API-key passwords get the same access their
  HTTP token would, scoped at connect.
- Cancel, NOTIFY/LISTEN, COPY, two-phase commit: not implemented.
