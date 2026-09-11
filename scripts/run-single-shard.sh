#!/bin/bash

# Script to run a 3-node, single-shard Raft cluster for testing (single shard, 3 replicas)

set -e

SHARD_COUNT=1
NODES_PER_SHARD=3
BASE_RAFT_PORT=7000
BASE_HTTP_PORT=8100
DATA_DIR_BASE="/tmp/shardkv_single"

cleanup_stale_runtime() {
    echo "Cleaning stale shardkv processes and state..."
    pkill -f "./node" 2>/dev/null || true
    pkill -f "./router" 2>/dev/null || true
    rm -rf "$DATA_DIR_BASE"
}

cleanup_stale_runtime

echo "Starting single-shard (3-node) Raft cluster..."

# Build binaries
echo "Building node and router binaries..."
go build -o node ./cmd/node
go build -o router ./cmd/router

mkdir -p "/tmp/shardkv_single"

# Start nodes for the single shard
for node_idx in $(seq 1 $NODES_PER_SHARD); do
    SHARD_ID="A"
    NODE_ID="A${node_idx}"
    RAFT_PORT=$((BASE_RAFT_PORT + (node_idx - 1) * 100))
    HTTP_PORT=$((BASE_HTTP_PORT + (node_idx - 1) * 10))
    DATA_DIR="${DATA_DIR_BASE}/${NODE_ID}"
    mkdir -p "$DATA_DIR"

    PEERS=""
    PEER_IDS=""
    PEERS_HTTP=""
    for p in $(seq 1 $NODES_PER_SHARD); do
        PID="A${p}"
        RPORT=$((BASE_RAFT_PORT + (p - 1) * 100))
        HPORT=$((BASE_HTTP_PORT + (p - 1) * 10))
        if [ -z "$PEERS" ]; then
            PEERS="127.0.0.1:$RPORT"
            PEER_IDS="$PID"
            PEERS_HTTP="127.0.0.1:$HPORT"
        else
            PEERS="$PEERS,127.0.0.1:$RPORT"
            PEER_IDS="$PEER_IDS,$PID"
            PEERS_HTTP="$PEERS_HTTP,127.0.0.1:$HPORT"
        fi
    done

    BOOTSTRAP_FLAG=""
    PEERS_FLAG="--peers ${PEERS#,}"
    PEER_IDS_FLAG="--peer-ids ${PEER_IDS#,}"
    PEERS_HTTP_FLAG="--peers-http ${PEERS_HTTP#,}"
    if [ $node_idx -eq 1 ]; then
        BOOTSTRAP_FLAG="--bootstrap"
        echo "Bootstrapping node $NODE_ID for shard $SHARD_ID"
    fi

    ./node \
        --node-id "$NODE_ID" \
        --shard-id "$SHARD_ID" \
        --raft-addr "127.0.0.1:$RAFT_PORT" \
        --http-addr "127.0.0.1:$HTTP_PORT" \
        --data-dir "$DATA_DIR" \
        $BOOTSTRAP_FLAG \
        $PEERS_FLAG \
        $PEER_IDS_FLAG \
        $PEERS_HTTP_FLAG \
        > "${DATA_DIR}/node.log" 2>&1 &

    echo $! > "/tmp/shardkv_single/${NODE_ID}.pid"
    echo "Started $NODE_ID (Raft: 127.0.0.1:$RAFT_PORT, HTTP: 127.0.0.1:$HTTP_PORT)"
    sleep 1
done

# Build and write router config
CONFIG_FILE="${DATA_DIR_BASE}/shards.json"
cat > "$CONFIG_FILE" <<EOF
{
  "shards": [
    {"id": "A", "nodes": ["127.0.0.1:8100", "127.0.0.1:8110", "127.0.0.1:8120"]}
  ]
}
EOF

echo "Starting router with config $CONFIG_FILE"
./router --http-addr "127.0.0.1:9000" --config "$CONFIG_FILE" > "${DATA_DIR_BASE}/router.log" 2>&1 &
echo $! > "/tmp/shardkv_single/router.pid"

echo "Single-shard cluster started. Router: http://127.0.0.1:9000"
echo "Logs in ${DATA_DIR_BASE}"

wait
