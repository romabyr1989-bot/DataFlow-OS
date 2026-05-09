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

## What works (Week 2 — real qengine; Week 3 — catalog + types)

After Week 2 the wire-protocol bridge runs SQL through the same `sql_parse`
+ `exec_stmt` path that powers `POST /api/tables/query`. Anything the JSON
API understands now works over pgwire too — including SELECT with FROM,
WHERE, ORDER BY, GROUP BY, aggregates, JOINs, subqueries, and window
functions.

Week 3 adds two things that make BI tools usable:

- **Per-column type inference at result time.** Numeric columns surface
  as `int8` / `float8`; booleans as `bool`. Falls back to `text` when a
  column is empty or contains mixed shapes. (`SELECT id FROM users`
  arrives in psql right-aligned now.)
- **`pg_catalog` and `information_schema` emulation.** Specific probes
  return synthesized rows from the live `g_app.tables` map — psql `\dt`
  works, DBeaver / DataGrip schema browsers populate.

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
| 3 ✓   | `pg_catalog` + `information_schema` emulation → DBeaver / `\dt` work |
| 3 ✓   | Type-OID inference — int8 / float8 / bool from cell values           |
| 4     | SCRAM-SHA-256 auth + TLS termination                                 |
| 4     | Extended Query: Parse / Bind / Execute (psycopg parameter binding)   |
| 4     | dbt-postgres / Tableau / Power BI compat polish                      |

## Type mapping (Week 3 — per-column inference)

The internal qengine already serializes cells to strings, so wire output
remains in **text format** (format code 0) — but the OID announced in
RowDescription is now picked per-column by sniffing the first non-null
cell:

| Cell shape                                 | Announced OID  |
|--------------------------------------------|----------------|
| `true` / `false` (case-insensitive)        | 16 (`bool`)    |
| `^[+-]?\d+$`                               | 20 (`int8`)    |
| `^[+-]?\d+\.\d+([eE][+-]?\d+)?$`           | 701 (`float8`) |
| anything else, or column entirely empty    | 25 (`text`)    |

This is heuristic but matches psql's right-alignment expectations and
satisfies dbt-postgres' "is this column numeric?" planner. A column with
mixed shapes (e.g. some integers, some words) collapses to text — the
qengine doesn't yet expose the underlying ColType per result column. A
follow-up will thread types through RS so we can use the schema directly.

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

## Catalog probes recognized

Matching is permissive: the SQL is lower-cased and we look for the
catalog table name as a substring. Filter / projection columns the
client tacked on are **ignored** — the synthesized row set is what they
get.

| Probe SQL contains                   | Returned columns                                          |
|--------------------------------------|-----------------------------------------------------------|
| `from information_schema.tables`     | table_catalog, table_schema, table_name, table_type        |
| `from information_schema.columns`    | table_catalog, schema, table_name, column_name, ordinal, data_type, is_nullable |
| `from pg_catalog.pg_namespace` / `from pg_namespace` | oid, nspname, nspowner                  |
| `from pg_catalog.pg_class` / `from pg_class`         | oid, relname, relnamespace, relkind     |

The schema is always `public`; the catalog is the database name from the
client's startup packet.

## Known limitations (Week 3 — don't file bugs for these)

- **No Extended Query protocol** (Parse/Bind/Execute). Parameter binding
  via psycopg's `cur.execute(sql, params)` falls back to in-driver
  string substitution and works for most cases. Server-side prepared
  statements: Week 4.
- **BEGIN/COMMIT/ROLLBACK are no-ops at the engine level.** They emit
  the right protocol tags so psql / DBeaver are happy, but the
  transaction manager is not threaded through. Use the JSON
  `/api/tables/query` endpoint with `txn_id` for real transactions.
- **Catalog probes ignore filters.** `SELECT * FROM information_schema
  .tables WHERE table_schema = 'public'` returns the same set as the
  unfiltered version. Tools that join multiple catalog tables together
  may need to fall back to plain SQL.
- **Type inference is heuristic.** A column where every value is
  numeric-looking gets `int8`/`float8`; a mixed column collapses to
  `text`. Schema-driven typing requires threading ColType through RS
  in the engine — follow-up work.
- **RBAC bypassed for pgwire.** Auth happens once at connect time; per
  query RBAC checks (used by the JSON API) are not yet wired into the
  pgwire path.
- Cancel, NOTIFY/LISTEN, COPY, two-phase commit: not implemented.
