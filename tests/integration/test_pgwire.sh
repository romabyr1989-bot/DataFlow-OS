#!/usr/bin/env bash
# Integration test for the PostgreSQL wire-protocol server (Step 3 Week 1).
#
# Uses the system `psql` client. Skips if it isn't installed.
# Covers: handshake, cleartext auth (good/bad/missing), Week 1 query subset.

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/dfo_gateway"
HTTP_PORT=18180
PG_PORT=15532
DATA="$(mktemp -d -t dfo_pgw_test_XXXX)"
SECRET="pgw-test-$$"
PASS=0; FAIL=0; SKIP=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }
skip()  { echo "SKIP: $1 — $2"; SKIP=$((SKIP+1)); }

[[ -x "$GW" ]] || { echo "missing $GW; run: make BUILD=release"; exit 1; }
command -v psql >/dev/null 2>&1 || { skip "all checks" "psql not installed"; echo "Pgwire test: $PASS passed, $FAIL failed, $SKIP skipped"; exit 0; }

# Spin up gateway with pgwire enabled
cat > "$DATA/cfg.json" <<EOF
{"port":$HTTP_PORT,"data_dir":"$DATA","auth_enabled":true,
 "jwt_secret":"$SECRET","admin_password":"admin",
 "pgwire_enabled":1,"pgwire_port":$PG_PORT}
EOF
"$GW" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do
  curl -sf "http://localhost:$HTTP_PORT/health" >/dev/null 2>&1 && break
  sleep 0.2
done

PSQL="psql -h localhost -p $PG_PORT -U admin -d dataflow -tAc"

# 1. Handshake + auth + SELECT 1
RESP=$(PGPASSWORD=admin $PSQL "SELECT 1" 2>&1)
check "SELECT 1 returns 1"                 "[[ '$RESP' == '1' ]]"

# 2. version()
RESP=$(PGPASSWORD=admin $PSQL "SELECT version()" 2>&1)
check "version() mentions DataFlow OS"     "[[ '$RESP' == *DataFlow* ]]"

# 3. current_database / current_user
RESP=$(PGPASSWORD=admin $PSQL "SELECT current_database()" 2>&1)
check "current_database() returns dataflow" "[[ '$RESP' == 'dataflow' ]]"
RESP=$(PGPASSWORD=admin $PSQL "SELECT current_user" 2>&1)
check "current_user returns admin"         "[[ '$RESP' == 'admin' ]]"

# 4. SHOW server_version
RESP=$(PGPASSWORD=admin $PSQL "SHOW server_version" 2>&1)
check "SHOW server_version works"          "[[ '$RESP' == *DataFlow* ]]"

# 5. Multiple constants
RESP=$(PGPASSWORD=admin $PSQL "SELECT 42" 2>&1)
check "SELECT 42 returns 42"               "[[ '$RESP' == '42' ]]"

# 6. BEGIN/SET/COMMIT no-op
RESP=$(PGPASSWORD=admin psql -h localhost -p $PG_PORT -U admin -d dataflow -tA <<'EOF' 2>&1
BEGIN;
SET TimeZone='UTC';
SELECT 7;
COMMIT;
EOF
)
check "BEGIN/SET/COMMIT around SELECT works" "echo '$RESP' | grep -q '^7\$'"

# 7. Bad password rejected with 28P01
RESP=$(PGPASSWORD=wrong $PSQL "SELECT 1" 2>&1)
check "bad password rejected (28P01-style err)" "[[ '$RESP' == *password* ]]"

# 8. Unknown SQL → clear error pointing at the JSON API
RESP=$(PGPASSWORD=admin $PSQL "SELECT * FROM no_such_table" 2>&1)
check "unknown SQL points at JSON API"     "[[ '$RESP' == *Week\ 1* || '$RESP' == *POST* ]]"

# 9. SSL request handled (psql probes SSL on connect by default — already
#    exercised above but verify by forcing sslmode=disable still works)
RESP=$(PGPASSWORD=admin psql "host=localhost port=$PG_PORT user=admin dbname=dataflow sslmode=disable" -tAc "SELECT 1" 2>&1)
check "sslmode=disable still works"        "[[ '$RESP' == '1' ]]"

# 10. Gateway log shows the listener and the connection event
check "gateway log: pgwire listening line"     "grep -q 'pgwire: listening on' '$DATA/gw.log'"
check "gateway log: at least one auth ok"      "grep -q 'pgwire: auth ok' '$DATA/gw.log'"

echo ""
echo "Pgwire test: $PASS passed, $FAIL failed, $SKIP skipped"
[[ $FAIL -eq 0 ]]
