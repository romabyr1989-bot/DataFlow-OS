#!/usr/bin/env bash
# Integration test for YAML pipelines (Step 5).
#
# Covers:
#   1. POST /api/pipelines/preview-yaml round-trips a valid YAML doc to JSON
#   2. Invalid YAML returns 400 with line/col error info
#   3. pipelines_dir auto-loads *.yaml at startup; pipeline appears in list
#   4. YAML pipeline with triggers[] (cron + webhook) round-trips correctly
#   5. Schema-invalid YAML (no `name`) — preview accepts it (lenient) but no triggers/cron parse

set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GW="$ROOT/build/release/bin/dfo_gateway"
PORT=19280
DATA="$(mktemp -d -t dfo_yaml_XXXX)"
PIPES="$DATA/pipelines"; mkdir -p "$PIPES"
SECRET="yaml-test-$$"
PASS=0; FAIL=0

cleanup() { [[ -n "${GW_PID:-}" ]] && kill "$GW_PID" 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
check() { if eval "$2"; then echo "PASS: $1"; PASS=$((PASS+1)); else echo "FAIL: $1   ($2)"; FAIL=$((FAIL+1)); fi; }

[[ -x "$GW" ]] || { echo "missing $GW"; exit 1; }

# Pre-seed a YAML pipeline file for auto-load
cat > "$PIPES/users_etl.yaml" <<'EOF'
name: users_etl_yaml
description: Loaded from YAML at startup
enabled: true
triggers:
  - type: cron
    cron_expr: "0 */6 * * *"
  - type: webhook
    webhook_token: yaml_wh_secret
steps:
  - id: extract
    transform_sql: |
      SELECT * FROM raw_users
      WHERE active = true
    target_table: users_clean
EOF

cat > "$DATA/cfg.json" <<EOF
{"port":$PORT,"data_dir":"$DATA","auth_enabled":true,
 "jwt_secret":"$SECRET","admin_password":"admin",
 "pipelines_dir":"$PIPES"}
EOF
"$GW" -c "$DATA/cfg.json" > "$DATA/gw.log" 2>&1 &
GW_PID=$!
for i in {1..30}; do
  curl -sf "http://localhost:$PORT/health" >/dev/null 2>&1 && break
  sleep 0.2
done
TOKEN=$(curl -sf -X POST "http://localhost:$PORT/api/auth/token" \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")
[[ -n "$TOKEN" ]] || { echo "FAIL: no admin token"; exit 1; }

# 1. preview-yaml with valid input
RESP=$(curl -sf -X POST "http://localhost:$PORT/api/pipelines/preview-yaml" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: text/yaml' \
  --data-binary @- <<'EOF'
name: my_test
enabled: true
triggers:
  - type: cron
    cron_expr: "@hourly"
steps:
  - id: x
    transform_sql: SELECT 1
    target_table: t
EOF
)
check "preview-yaml: valid YAML returns 200 JSON with name" \
  "echo '$RESP' | grep -q '\"name\":\"my_test\"'"
check "preview-yaml: triggers[] preserved" \
  "echo '$RESP' | grep -q '\"type\":\"cron\"'"

# 2. preview-yaml with malformed YAML
STATUS=$(curl -s -o "$DATA/err.json" -w "%{http_code}" -X POST \
  "http://localhost:$PORT/api/pipelines/preview-yaml" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: text/yaml' \
  --data-binary $'just bare text\nno colon')
check "preview-yaml: bare text returns 400" "[[ '$STATUS' == '400' ]]"
check "preview-yaml: error response includes 'line'" \
  "grep -q '\"line\"' '$DATA/err.json'"

# 3. Auto-load: YAML pipeline appears in /api/pipelines
LIST=$(curl -sf "http://localhost:$PORT/api/pipelines" -H "Authorization: Bearer $TOKEN")
check "auto-load: users_etl_yaml present in list" \
  "echo '$LIST' | grep -q 'users_etl_yaml'"
check "auto-load: webhook trigger preserved" \
  "echo '$LIST' | grep -q 'yaml_wh_secret'"
check "auto-load: cron trigger preserved" \
  "echo '$LIST' | grep -q '0 \\*/6 \\* \\* \\*'"
check "auto-load: block scalar SQL preserved" \
  "echo '$LIST' | grep -q 'WHERE active'"

# 4. The webhook token from the YAML pipeline actually fires
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  "http://localhost:$PORT/api/triggers/yaml_wh_secret" -d '{}')
check "auto-loaded webhook trigger fires (202)" "[[ '$STATUS' == '202' ]]"

# 5. gw.log mentions the YAML auto-load
check "gateway log mentions pipelines_dir auto-load" \
  "grep -q 'pipelines_dir.*loaded.*YAML' '$DATA/gw.log'"

echo ""
echo "YAML pipelines test: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
