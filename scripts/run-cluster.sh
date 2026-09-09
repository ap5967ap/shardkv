#!/bin/bash

# Script to run a 9-node, 3-shard Raft cluster for testing
# Each shard has 3 nodes, total 9 nodes across 3 shards
# Each node runs on different ports and uses its own data directory

set -e

# Configuration
SHARD_COUNT=3
NODES_PER_SHARD=3
BASE_RAFT_PORT=7000
BASE_HTTP_PORT=8100
DATA_DIR_BASE="/tmp/shardkv"

# Clean stale runtime state before starting the cluster.
cleanup_stale_runtime() {
    echo "Cleaning stale shardkv processes and state..."
    pkill -f "./node" 2>/dev/null || true
    pkill -f "./router" 2>/dev/null || true
    rm -rf "$DATA_DIR_BASE"
}

cleanup_stale_runtime

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting 9-node, 3-shard Raft cluster...${NC}"

# Function to check if a port is available
check_port_available() {
    local port=$1
    # Try multiple methods to check port availability
    if command -v ss >/dev/null 2>&1; then
        if ss -tuln 2>/dev/null | grep -q ":$port "; then
            return 1
        fi
    elif command -v lsof >/dev/null 2>&1; then
        if lsof -i :$port >/dev/null 2>&1; then
            return 1
        fi
    elif command -v netstat >/dev/null 2>&1; then
        if netstat -tuln 2>/dev/null | grep -q ":$port "; then
            return 1
        fi
    fi
    return 0
}

# Function to wait for a process to bind to a port
wait_for_port() {
    local pid=$1
    local port=$2
    local max_wait=10
    local count=0

    while [ $count -lt $max_wait ]; do
        if ! kill -0 $pid 2>/dev/null; then
            echo -e "${RED}Process $pid died before binding to port $port${NC}"
            return 1
        fi

        # Check if port is in use (assuming by our process)
        if command -v ss >/dev/null 2>&1; then
            if ss -tuln 2>/dev/null | grep -q ":$port "; then
                return 0
            fi
        elif command -v lsof >/dev/null 2>&1; then
            if lsof -i :$port >/dev/null 2>&1; then
                return 0
            fi
        elif command -v netstat >/dev/null 2>&1; then
            if netstat -tuln 2>/dev/null | grep -q ":$port "; then
                return 0
            fi
        fi

        sleep 1
        count=$((count + 1))
    done

    echo -e "${RED}Timeout waiting for process $pid to bind to port $port${NC}"
    return 1
}

# Clean up function
cleanup() {
    echo -e "${YELLOW}Shutting down cluster...${NC}"

    # Kill router
    if [ -f "/tmp/shardkv/router.pid" ]; then
        PID=$(cat "/tmp/shardkv/router.pid")
        if kill -0 $PID 2>/dev/null; then
            echo "Killing router (PID: $PID)"
            kill $PID
        fi
        rm -f "/tmp/shardkv/router.pid"
    fi

    # Kill all nodes
    for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
        SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
        for node_idx in $(seq 1 $NODES_PER_SHARD); do
            NODE_ID="${SHARD_ID}${node_idx}"
            PID_FILE="/tmp/shardkv/${NODE_ID}.pid"
            if [ -f "$PID_FILE" ]; then
                PID=$(cat "$PID_FILE")
                if kill -0 $PID 2>/dev/null; then
                    echo "Killing node $NODE_ID (PID: $PID)"
                    kill $PID
                fi
                rm -f "$PID_FILE"
            fi
        done
    done
    echo -e "${GREEN}Cluster shut down${NC}"
}

# Trap cleanup on exit
trap cleanup EXIT INT TERM

# Build binaries
echo "Building node and router binaries..."
go build -o node ./cmd/node
go build -o router ./cmd/router

# Start nodes for each shard
for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
    SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
    SHARD_BASE_RAFT=$((BASE_RAFT_PORT + shard_idx * NODES_PER_SHARD * 100))
    SHARD_BASE_HTTP=$((BASE_HTTP_PORT + shard_idx * NODES_PER_SHARD * 10))

    echo -e "${GREEN}Starting shard $SHARD_ID${NC}"

    # Build peer list for this shard (Raft addrs, IDs, and HTTP addrs in the same order)
    PEERS=""
    PEER_IDS=""
    PEERS_HTTP=""
    for node_idx in $(seq 1 $NODES_PER_SHARD); do
        NODE_ID="${SHARD_ID}${node_idx}"
        RAFT_PORT=$((SHARD_BASE_RAFT + (node_idx - 1) * 100))
        HTTP_PORT=$((SHARD_BASE_HTTP + (node_idx - 1) * 10))
        if [ -z "$PEERS" ]; then
            PEERS="127.0.0.1:$RAFT_PORT"
            PEER_IDS="$NODE_ID"
            PEERS_HTTP="127.0.0.1:$HTTP_PORT"
        else
            PEERS="$PEERS,127.0.0.1:$RAFT_PORT"
            PEER_IDS="$PEER_IDS,$NODE_ID"
            PEERS_HTTP="$PEERS_HTTP,127.0.0.1:$HTTP_PORT"
        fi
    done

    echo "Shard $SHARD_ID peer configuration: $PEERS"
    echo "Shard $SHARD_ID peer IDs: $PEER_IDS"
    echo "Shard $SHARD_ID peer HTTP: $PEERS_HTTP"

    # Start nodes for this shard
    for node_idx in $(seq 1 $NODES_PER_SHARD); do
        NODE_ID="${SHARD_ID}${node_idx}"
        RAFT_PORT=$((SHARD_BASE_RAFT + (node_idx - 1) * 100))
        HTTP_PORT=$((SHARD_BASE_HTTP + (node_idx - 1) * 10))
        DATA_DIR="${DATA_DIR_BASE}/${NODE_ID}"

        # Create data directory
        mkdir -p "$DATA_DIR"

        # Determine if this is the bootstrap node (first node in shard)
        BOOTSTRAP_FLAG=""
        PEERS_FLAG=""
        PEER_IDS_FLAG=""
        if [ $node_idx -eq 1 ]; then
            BOOTSTRAP_FLAG="--bootstrap"
            PEERS_FLAG="--peers $PEERS"
            PEER_IDS_FLAG="--peer-ids $PEER_IDS"
            echo -e "${GREEN}Bootstrapping shard $SHARD_ID with node $NODE_ID${NC}"
        else
            echo -e "${YELLOW}Starting node $NODE_ID (shard $SHARD_ID)${NC}"
        fi

        # Check if ports are available before starting
        if ! check_port_available $RAFT_PORT; then
            echo -e "${RED}Raft port $RAFT_PORT is already in use, cannot start $NODE_ID${NC}"
            exit 1
        fi
        if ! check_port_available $HTTP_PORT; then
            echo -e "${RED}HTTP port $HTTP_PORT is already in use, cannot start $NODE_ID${NC}"
            exit 1
        fi

        # Start the node in background
        ./node \
            --node-id "$NODE_ID" \
            --shard-id "$SHARD_ID" \
            --raft-addr "127.0.0.1:$RAFT_PORT" \
            --http-addr "127.0.0.1:$HTTP_PORT" \
            --data-dir "$DATA_DIR" \
            $BOOTSTRAP_FLAG \
            $PEERS_FLAG \
            $PEER_IDS_FLAG \
            > "${DATA_DIR}/node.log" 2>&1 &

        NODE_PID=$!
        echo $NODE_PID > "/tmp/shardkv/${NODE_ID}.pid"

        # Wait for HTTP port to be bound
        if ! wait_for_port $NODE_PID $HTTP_PORT; then
            echo -e "${RED}Failed to start $NODE_ID - HTTP port $HTTP_PORT not bound${NC}"
            cat "${DATA_DIR}/node.log"
            exit 1
        fi

        echo "Started $NODE_ID (PID: $NODE_PID, Raft: 127.0.0.1:$RAFT_PORT, HTTP: 127.0.0.1:$HTTP_PORT)"

        # Wait a bit between starting nodes
        sleep 1
    done
done

# Build shard config for router with all node addresses
# Format: shardID:httpAddr1,httpAddr2,httpAddr3;shardID:httpAddr1,httpAddr2,httpAddr3;...
SHARD_CONFIG=""
for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
    SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
    SHARD_BASE_HTTP=$((BASE_HTTP_PORT + shard_idx * NODES_PER_SHARD * 10))

    # Build list of all node HTTP addresses for this shard
    NODE_ADDRS=""
    for node_idx in $(seq 1 $NODES_PER_SHARD); do
        HTTP_PORT=$((SHARD_BASE_HTTP + (node_idx - 1) * 10))
        if [ -z "$NODE_ADDRS" ]; then
            NODE_ADDRS="127.0.0.1:$HTTP_PORT"
        else
            NODE_ADDRS="${NODE_ADDRS},127.0.0.1:$HTTP_PORT"
        fi
    done

    if [ -z "$SHARD_CONFIG" ]; then
        SHARD_CONFIG="${SHARD_ID}:${NODE_ADDRS}"
    else
        SHARD_CONFIG="${SHARD_CONFIG};${SHARD_ID}:${NODE_ADDRS}"
    fi
done

# Write shard config to file for router
CONFIG_FILE="${DATA_DIR_BASE}/shards.json"
cat > "$CONFIG_FILE" <<EOF
{
  "shards": [
EOF

for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
    SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
    SHARD_BASE_HTTP=$((BASE_HTTP_PORT + shard_idx * NODES_PER_SHARD * 10))

    # Build list of all node HTTP addresses for this shard
    NODE_ADDRS=""
    for node_idx in $(seq 1 $NODES_PER_SHARD); do
        HTTP_PORT=$((SHARD_BASE_HTTP + (node_idx - 1) * 10))
        if [ -z "$NODE_ADDRS" ]; then
            NODE_ADDRS="\"127.0.0.1:$HTTP_PORT\""
        else
            NODE_ADDRS="${NODE_ADDRS}, \"127.0.0.1:$HTTP_PORT\""
        fi
    done

    if [ $shard_idx -eq $((SHARD_COUNT - 1)) ]; then
        echo "    {\"id\": \"$SHARD_ID\", \"nodes\": [$NODE_ADDRS]}" >> "$CONFIG_FILE"
    else
        echo "    {\"id\": \"$SHARD_ID\", \"nodes\": [$NODE_ADDRS]}," >> "$CONFIG_FILE"
    fi
done

cat >> "$CONFIG_FILE" <<EOF
  ]
}
EOF

echo "Router shard config file: $CONFIG_FILE"
echo "Router will load configuration from $CONFIG_FILE"

# Check if router port is available
ROUTER_HTTP_PORT=9000
if ! check_port_available $ROUTER_HTTP_PORT; then
    echo -e "${RED}Router HTTP port $ROUTER_HTTP_PORT is already in use, cannot start router${NC}"
    exit 1
fi

# Start router
echo -e "${GREEN}Starting router${NC}"
./router \
    --http-addr "127.0.0.1:$ROUTER_HTTP_PORT" \
    --config "$CONFIG_FILE" \
    > "${DATA_DIR_BASE}/router.log" 2>&1 &

ROUTER_PID=$!
echo $ROUTER_PID > "/tmp/shardkv/router.pid"

# Wait for router HTTP port to be bound
if ! wait_for_port $ROUTER_PID $ROUTER_HTTP_PORT; then
    echo -e "${RED}Failed to start router - HTTP port $ROUTER_HTTP_PORT not bound${NC}"
    cat "${DATA_DIR_BASE}/router.log"
    exit 1
fi

echo "Started router (PID: $ROUTER_PID, HTTP: 127.0.0.1:$ROUTER_HTTP_PORT)"

echo -e "${GREEN}All nodes and router started and verified${NC}"
echo ""
echo "Cluster topology:"
for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
    SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
    SHARD_BASE_RAFT=$((BASE_RAFT_PORT + shard_idx * NODES_PER_SHARD * 100))
    SHARD_BASE_HTTP=$((BASE_HTTP_PORT + shard_idx * NODES_PER_SHARD * 10))
    echo "  Shard $SHARD_ID:"
    for node_idx in $(seq 1 $NODES_PER_SHARD); do
        NODE_ID="${SHARD_ID}${node_idx}"
        RAFT_PORT=$((SHARD_BASE_RAFT + (node_idx - 1) * 100))
        HTTP_PORT=$((SHARD_BASE_HTTP + (node_idx - 1) * 10))
        echo "    $NODE_ID: Raft=127.0.0.1:$RAFT_PORT, HTTP=127.0.0.1:$HTTP_PORT"
    done
done
echo "  Router: HTTP=127.0.0.1:$ROUTER_HTTP_PORT"
echo ""
echo "Log files:"
for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
    SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
    for node_idx in $(seq 1 $NODES_PER_SHARD); do
        NODE_ID="${SHARD_ID}${node_idx}"
        DATA_DIR="${DATA_DIR_BASE}/${NODE_ID}"
        echo "  $NODE_ID: $DATA_DIR/node.log"
    done
done
echo "  Router: ${DATA_DIR_BASE}/router.log"
echo ""
echo -e "${GREEN}Cluster successfully started and verified${NC}"
echo -e "Router available at: http://127.0.0.1:$ROUTER_HTTP_PORT"
echo -e "${YELLOW}Press Ctrl+C to stop the cluster${NC}"

# Wait for processes to run
wait