#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "==> Cleaning stale processes"
pkill -f './node' 2>/dev/null || true
pkill -f './router' 2>/dev/null || true
rm -rf /tmp/shardkv

echo "==> Starting cluster"
./scripts/run-cluster.sh >/tmp/phase5_check.log 2>&1 &
CLUSTER_PID=$!

wait_for_router() {
  for i in $(seq 1 40); do
    if curl -fsS http://127.0.0.1:9000/cluster/shards >/tmp/cluster_shards.json 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "router did not become ready"
  cat /tmp/phase5_check.log || true
  return 1
}
wait_for_router

echo "==> Cluster topology"
cat /tmp/cluster_shards.json

A_LEADER=$(python3 - <<'PY'
import json
with open('/tmp/cluster_shards.json') as f:
    data = json.load(f)
for s in data['shards']:
    if s['id'] == 'A':
        print(s['leader_addr'])
        break
PY
)

B_LEADER=$(python3 - <<'PY'
import json
with open('/tmp/cluster_shards.json') as f:
    data = json.load(f)
for s in data['shards']:
    if s['id'] == 'B':
        print(s['leader_addr'])
        break
PY
)

KEY="phase5-runtime-check"
VALUE="phase5-value"

echo
echo "==> 1) Write key to A"
curl -sS -X PUT "http://$A_LEADER/kv/$KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"value\":\"$VALUE\"}"

echo
echo "==> 2) Confirm router sees it before migration"
curl -sS "http://127.0.0.1:9000/kv/$KEY?consistency=strong"

echo
echo "==> 3) Freeze A range"
curl -sS -X POST "http://$A_LEADER/admin/freeze" \
  -H 'Content-Type: application/json' \
  -d "{\"shard_id\":\"A\",\"key_range\":{\"start\":\"$KEY\",\"end\":\"$KEY\"},\"action\":\"freeze\"}"

echo
echo "==> 4) Attempt a write while frozen (should fail)"
WRITE_OUT=$(curl -sS -w '\nHTTP_STATUS:%{http_code}' -X PUT "http://$A_LEADER/kv/$KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"value\":\"should-be-rejected\"}")
echo "$WRITE_OUT"

echo
echo "==> 5) Read after failed write (should be unchanged or not found depending on state)"
curl -sS "http://127.0.0.1:9000/kv/$KEY?consistency=strong" || true

echo
echo "==> 6) Unfreeze A"
curl -sS -X POST "http://$A_LEADER/admin/freeze" \
  -H 'Content-Type: application/json' \
  -d "{\"shard_id\":\"A\",\"key_range\":{\"start\":\"$KEY\",\"end\":\"$KEY\"},\"action\":\"unfreeze\"}"

echo
echo "==> 7) Trigger migration A -> B"
curl -sS -X POST http://127.0.0.1:9000/admin/migrate \
  -H 'Content-Type: application/json' \
  -d "{\"from_shard\":\"A\",\"to_shard\":\"B\",\"key_range\":{\"start\":\"$KEY\",\"end\":\"$KEY\"}}"

echo
echo "==> 8) Strong read after migration"
curl -sS "http://127.0.0.1:9000/kv/$KEY?consistency=strong"

echo
echo "==> 9) Scan source and destination"
echo "-- A scan --"
curl -sS "http://$A_LEADER/admin/scan?start=$KEY&end=$KEY"
echo
echo "-- B scan --"
curl -sS "http://$B_LEADER/admin/scan?start=$KEY&end=$KEY"

echo
echo "==> 10) Cleanup"
kill "$CLUSTER_PID" 2>/dev/null || true
wait "$CLUSTER_PID" 2>/dev/null || true
pkill -f './node' 2>/dev/null || true
pkill -f './router' 2>/dev/null || true

echo "Runtime migration check completed."