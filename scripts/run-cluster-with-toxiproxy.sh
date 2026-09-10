#!/bin/bash

# Start the 9-node Raft cluster with every directed peer connection routed through a
# Toxiproxy listener so network partitions actually pass through the proxy layer.

set -euo pipefail

SHARD_COUNT=3
NODES_PER_SHARD=3
BASE_RAFT_PORT=7000
BASE_HTTP_PORT=8100
DATA_DIR_BASE="/tmp/shardkv"
PROXY_BASE_PORT=9100

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

BIN_DIR="$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin"
TOXIPROXY_SERVER="$BIN_DIR/toxiproxy-server"
TOXIPROXY_CLI="$BIN_DIR/toxiproxy-cli"
export PATH="$BIN_DIR:$PATH"

if ! [ -x "$TOXIPROXY_SERVER" ] && command -v cmd >/dev/null 2>&1; then
    ln -sf "$(command -v cmd)" "$TOXIPROXY_SERVER"
fi
if ! [ -x "$TOXIPROXY_CLI" ] && command -v cli >/dev/null 2>&1; then
    ln -sf "$(command -v cli)" "$TOXIPROXY_CLI"
fi

check_port_available() {
    local port=$1
    if command -v ss >/dev/null 2>&1; then
        ss -tuln 2>/dev/null | grep -q ":$port " && return 1 || true
    elif command -v lsof >/dev/null 2>&1; then
        lsof -i :"$port" >/dev/null 2>&1 && return 1 || true
    elif command -v netstat >/dev/null 2>&1; then
        netstat -tuln 2>/dev/null | grep -q ":$port " && return 1 || true
    fi
    return 0
}

wait_for_port() {
    local pid=$1
    local port=$2
    local count=0
    while [ $count -lt 20 ]; do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo -e "${RED}Process $pid died before binding to port $port${NC}"
            return 1
        fi
        if command -v ss >/dev/null 2>&1; then
            ss -tuln 2>/dev/null | grep -q ":$port " && return 0 || true
        elif command -v lsof >/dev/null 2>&1; then
            lsof -i :"$port" >/dev/null 2>&1 && return 0 || true
        elif command -v netstat >/dev/null 2>&1; then
            netstat -tuln 2>/dev/null | grep -q ":$port " && return 0 || true
        fi
        sleep 1
        count=$((count + 1))
    done
    echo -e "${RED}Timeout waiting for process $pid to bind to port $port${NC}"
    return 1
}

cleanup_stale_runtime() {
    echo "Cleaning stale shardkv processes..."
    pkill -f "./node" 2>/dev/null || true
    pkill -f "./router" 2>/dev/null || true

    if [ -x "$TOXIPROXY_CLI" ]; then
        "$TOXIPROXY_CLI" delete all >/dev/null 2>&1 || true
        "$TOXIPROXY_CLI" list 2>/dev/null | awk 'NR > 2 && NF > 0 { print $1 }' | while read -r proxy_name; do
            [ -n "$proxy_name" ] && "$TOXIPROXY_CLI" delete "$proxy_name" >/dev/null 2>&1 || true
        done
    fi

    rm -rf "$DATA_DIR_BASE"
}

ensure_toxiproxy() {
        if ! [ -x "$TOXIPROXY_SERVER" ]; then
        echo -e "${YELLOW}Installing toxiproxy-server...${NC}"
        GOBIN="$BIN_DIR" GO111MODULE=on go install github.com/Shopify/toxiproxy/v2/cmd/toxiproxy-server@latest
        ln -sf "$BIN_DIR/toxiproxy-server" "$TOXIPROXY_SERVER"
    fi
    if ! [ -x "$TOXIPROXY_CLI" ]; then
        echo -e "${YELLOW}Installing toxiproxy-cli...${NC}"
        GOBIN="$BIN_DIR" GO111MODULE=on go install github.com/Shopify/toxiproxy/v2/cmd/cli@latest
        ln -sf "$BIN_DIR/cli" "$TOXIPROXY_CLI"
    fi
    if ! curl -fsS http://127.0.0.1:8474/proxies >/dev/null 2>&1; then
        if ! pgrep -f "toxiproxy-server.*8474" >/dev/null 2>&1; then
            echo -e "${YELLOW}Starting toxiproxy-server on 127.0.0.1:8474${NC}"
            nohup "$TOXIPROXY_SERVER" -port 8474 >/tmp/toxiproxy.log 2>&1 &
        fi
        sleep 2
    fi
    if ! curl -fsS http://127.0.0.1:8474/proxies >/dev/null 2>&1; then
        echo -e "${RED}Toxiproxy server is not running on port 8474${NC}"
        exit 1
    fi
}

proxy_port_for() {
    local shard_idx=$1
    local from_idx=$2
    local to_idx=$3
    echo $((PROXY_BASE_PORT + (shard_idx * 100) + ((from_idx - 1) * 10) + (to_idx - 1)))
}

create_proxy() {
    local from_node=$1
    local to_node=$2
    local target_port=$3
    local listen_port=$4
    local proxy_name="${from_node}-to-${to_node}"

    echo "Creating proxy $proxy_name -> 127.0.0.1:$target_port on 127.0.0.1:$listen_port"
    "$TOXIPROXY_CLI" delete "$proxy_name" 2>/dev/null || true
    "$TOXIPROXY_CLI" create "$proxy_name" --listen "127.0.0.1:$listen_port" --upstream "127.0.0.1:$target_port" >/dev/null
}

cleanup() {
    echo -e "${YELLOW}Shutting down cluster...${NC}"
    if [ -f "/tmp/shardkv/router.pid" ]; then
        PID=$(cat "/tmp/shardkv/router.pid")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID" 2>/dev/null || true
        fi
        rm -f "/tmp/shardkv/router.pid"
    fi
    for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
        SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
        for node_idx in $(seq 1 $NODES_PER_SHARD); do
            NODE_ID="${SHARD_ID}${node_idx}"
            PID_FILE="/tmp/shardkv/${NODE_ID}.pid"
            if [ -f "$PID_FILE" ]; then
                PID=$(cat "$PID_FILE")
                if kill -0 "$PID" 2>/dev/null; then
                    kill "$PID" 2>/dev/null || true
                fi
                rm -f "$PID_FILE"
            fi
        done
    done
    if [ -x "$TOXIPROXY_CLI" ]; then
        "$TOXIPROXY_CLI" delete all >/dev/null 2>&1 || true
        "$TOXIPROXY_CLI" list 2>/dev/null | awk 'NR > 2 && NF > 0 { print $1 }' | while read -r proxy_name; do
            [ -n "$proxy_name" ] && "$TOXIPROXY_CLI" delete "$proxy_name" >/dev/null 2>&1 || true
        done
    fi
    echo -e "${GREEN}Cluster shut down${NC}"
}
trap cleanup EXIT INT TERM

ensure_toxiproxy
cleanup_stale_runtime

echo -e "${GREEN}Starting 9-node, 3-shard Raft cluster with Toxiproxy...${NC}"

# Build proxy topology.
for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
    SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
    SHARD_BASE_RAFT=$((BASE_RAFT_PORT + shard_idx * NODES_PER_SHARD * 100))
    for from_idx in $(seq 1 $NODES_PER_SHARD); do
        for to_idx in $(seq 1 $NODES_PER_SHARD); do
            if [ "$from_idx" -ne "$to_idx" ]; then
                FROM_NODE="${SHARD_ID}${from_idx}"
                TO_NODE="${SHARD_ID}${to_idx}"
                TARGET_PORT=$((SHARD_BASE_RAFT + (to_idx - 1) * 100))
                LISTEN_PORT=$(proxy_port_for "$shard_idx" "$from_idx" "$to_idx")
                create_proxy "$FROM_NODE" "$TO_NODE" "$TARGET_PORT" "$LISTEN_PORT"
            fi
        done
    done
done

go build -o node ./cmd/node
go build -o router ./cmd/router

for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
    SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
    SHARD_BASE_RAFT=$((BASE_RAFT_PORT + shard_idx * NODES_PER_SHARD * 100))
    SHARD_BASE_HTTP=$((BASE_HTTP_PORT + shard_idx * NODES_PER_SHARD * 10))

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

    # Replace each remote peer address with its proxy listener so traffic traverses Toxiproxy.
    for node_idx in $(seq 1 $NODES_PER_SHARD); do
        NODE_ID="${SHARD_ID}${node_idx}"
        DATA_DIR="${DATA_DIR_BASE}/${NODE_ID}"
        mkdir -p "$DATA_DIR"

        PEERS_FOR_NODE=""
        PEER_IDS_FOR_NODE=""
        for peer_idx in $(seq 1 $NODES_PER_SHARD); do
            PEER_ID="${SHARD_ID}${peer_idx}"
            if [ "$peer_idx" -eq "$node_idx" ]; then
                peer_addr="127.0.0.1:$((SHARD_BASE_RAFT + (node_idx - 1) * 100))"
            else
                peer_addr="127.0.0.1:$(proxy_port_for "$shard_idx" "$node_idx" "$peer_idx")"
            fi
            if [ -z "$PEERS_FOR_NODE" ]; then
                PEERS_FOR_NODE="$peer_addr"
                PEER_IDS_FOR_NODE="$PEER_ID"
            else
                PEERS_FOR_NODE="$PEERS_FOR_NODE,$peer_addr"
                PEER_IDS_FOR_NODE="$PEER_IDS_FOR_NODE,$PEER_ID"
            fi
        done

        BOOTSTRAP_FLAG=""
        if [ "$node_idx" -eq 1 ]; then
            BOOTSTRAP_FLAG="--bootstrap"
        fi

        ./node \
            --node-id "$NODE_ID" \
            --shard-id "$SHARD_ID" \
            --raft-addr "127.0.0.1:$((SHARD_BASE_RAFT + (node_idx - 1) * 100))" \
            --http-addr "127.0.0.1:$((SHARD_BASE_HTTP + (node_idx - 1) * 10))" \
            --data-dir "$DATA_DIR" \
            $BOOTSTRAP_FLAG \
            --peers "$PEERS_FOR_NODE" \
            --peer-ids "$PEER_IDS_FOR_NODE" \
            --peers-http "$PEERS_HTTP" \
            > "${DATA_DIR}/node.log" 2>&1 &

        NODE_PID=$!
        echo "$NODE_PID" > "/tmp/shardkv/${NODE_ID}.pid"

        if ! wait_for_port "$NODE_PID" "$((SHARD_BASE_HTTP + (node_idx - 1) * 10))"; then
            echo -e "${RED}Failed to start $NODE_ID${NC}"
            cat "${DATA_DIR}/node.log"
            exit 1
        fi
        sleep 1
    done
done

CONFIG_FILE="${DATA_DIR_BASE}/shards.json"
cat > "$CONFIG_FILE" <<EOF
{
  "shards": [
EOF

for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
    SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
    SHARD_BASE_HTTP=$((BASE_HTTP_PORT + shard_idx * NODES_PER_SHARD * 10))
    NODE_ADDRS=""
    for node_idx in $(seq 1 $NODES_PER_SHARD); do
        HTTP_PORT=$((SHARD_BASE_HTTP + (node_idx - 1) * 10))
        if [ -z "$NODE_ADDRS" ]; then
            NODE_ADDRS="\"127.0.0.1:$HTTP_PORT\""
        else
            NODE_ADDRS="${NODE_ADDRS}, \"127.0.0.1:$HTTP_PORT\""
        fi
    done
    if [ "$shard_idx" -eq $((SHARD_COUNT - 1)) ]; then
        echo "    {\"id\": \"$SHARD_ID\", \"nodes\": [$NODE_ADDRS]}" >> "$CONFIG_FILE"
    else
        echo "    {\"id\": \"$SHARD_ID\", \"nodes\": [$NODE_ADDRS]}," >> "$CONFIG_FILE"
    fi
done

cat >> "$CONFIG_FILE" <<EOF
  ]
}
EOF

ROUTER_HTTP_PORT=9000
./router --http-addr "127.0.0.1:$ROUTER_HTTP_PORT" --config "$CONFIG_FILE" > "${DATA_DIR_BASE}/router.log" 2>&1 &
ROUTER_PID=$!
echo "$ROUTER_PID" > "/tmp/shardkv/router.pid"
if ! wait_for_port "$ROUTER_PID" "$ROUTER_HTTP_PORT"; then
    echo -e "${RED}Failed to start router${NC}"
    cat "${DATA_DIR_BASE}/router.log"
    exit 1
fi

echo -e "${GREEN}Cluster started successfully${NC}"
echo -e "Router available at: http://127.0.0.1:$ROUTER_HTTP_PORT"
echo -e "${YELLOW}Toxiproxy is active at http://127.0.0.1:8474${NC}"
wait