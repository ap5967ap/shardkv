#!/bin/bash
set -euo pipefail

echo "Cleaning up..."
pkill -f 'node' 2>/dev/null || true
pkill -f 'router' 2>/dev/null || true
rm -rf /tmp/shardkv
mkdir -p /tmp/shardkv/{A1,A2,A3,B1,B2,B3,C1,C2,C3}
cd /home/ap/resume/kv

start_node() {
  local id=$1; shift
  ./node "$@" > /tmp/shardkv/${id}/node.log 2>&1 &
  local pid=$!
  echo $pid > /tmp/shardkv/${id}.pid
}

echo "Starting Shard A..."
PA="127.0.0.1:7000,127.0.0.1:7100,127.0.0.1:7200"
IA="A1,A2,A3"
HA="127.0.0.1:8001,127.0.0.1:8011,127.0.0.1:8021"
start_node A1 --node-id A1 --shard-id A --raft-addr 127.0.0.1:7000 --http-addr 127.0.0.1:8001 --data-dir /tmp/shardkv/A1 --bootstrap --peers "$PA" --peer-ids "$IA" --peers-http "$HA"
start_node A2 --node-id A2 --shard-id A --raft-addr 127.0.0.1:7100 --http-addr 127.0.0.1:8011 --data-dir /tmp/shardkv/A2 --peers "$PA" --peer-ids "$IA" --peers-http "$HA"
start_node A3 --node-id A3 --shard-id A --raft-addr 127.0.0.1:7200 --http-addr 127.0.0.1:8021 --data-dir /tmp/shardkv/A3 --peers "$PA" --peer-ids "$IA" --peers-http "$HA"

echo "Starting Shard B..."
PB="127.0.0.1:7300,127.0.0.1:7400,127.0.0.1:7500"
IB="B1,B2,B3"
HB="127.0.0.1:8031,127.0.0.1:8041,127.0.0.1:8051"
start_node B1 --node-id B1 --shard-id B --raft-addr 127.0.0.1:7300 --http-addr 127.0.0.1:8031 --data-dir /tmp/shardkv/B1 --bootstrap --peers "$PB" --peer-ids "$IB" --peers-http "$HB"
start_node B2 --node-id B2 --shard-id B --raft-addr 127.0.0.1:7400 --http-addr 127.0.0.1:8041 --data-dir /tmp/shardkv/B2 --peers "$PB" --peer-ids "$IB" --peers-http "$HB"
start_node B3 --node-id B3 --shard-id B --raft-addr 127.0.0.1:7500 --http-addr 127.0.0.1:8051 --data-dir /tmp/shardkv/B3 --peers "$PB" --peer-ids "$IB" --peers-http "$HB"

echo "Starting Shard C..."
PC="127.0.0.1:7600,127.0.0.1:7700,127.0.0.1:7800"
IC="C1,C2,C3"
HC="127.0.0.1:8061,127.0.0.1:8071,127.0.0.1:8091"
start_node C1 --node-id C1 --shard-id C --raft-addr 127.0.0.1:7600 --http-addr 127.0.0.1:8061 --data-dir /tmp/shardkv/C1 --bootstrap --peers "$PC" --peer-ids "$IC" --peers-http "$HC"
start_node C2 --node-id C2 --shard-id C --raft-addr 127.0.0.1:7700 --http-addr 127.0.0.1:8071 --data-dir /tmp/shardkv/C2 --peers "$PC" --peer-ids "$IC" --peers-http "$HC"
start_node C3 --node-id C3 --shard-id C --raft-addr 127.0.0.1:7800 --http-addr 127.0.0.1:8091 --data-dir /tmp/shardkv/C3 --peers "$PC" --peer-ids "$IC" --peers-http "$HC"

echo "Starting Router..."
./router --http-addr "127.0.0.1:9010" --shards "A:127.0.0.1:8001,127.0.0.1:8011,127.0.0.1:8021;B:127.0.0.1:8031,127.0.0.1:8041,127.0.0.1:8051;C:127.0.0.1:8061,127.0.0.1:8071,127.0.0.1:8091" > /tmp/shardkv/router.log 2>&1 &
router_pid=$!
echo $router_pid > /tmp/shardkv/router.pid

echo "Waiting 5s for elections..."
sleep 5

echo "Running Verification Script..."
bash verify-phase2.sh

echo "Done! Leaving cluster running in background (attached to this shell session)."
