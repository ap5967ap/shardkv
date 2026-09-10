#!/bin/bash

# Create one Toxiproxy listener per directed Raft peer link so the cluster routes
# traffic through the proxy layer instead of directly connecting from node to node.

set -euo pipefail

SHARD_COUNT=3
NODES_PER_SHARD=3
BASE_RAFT_PORT=7000
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

proxy_port_for() {
    local shard_idx=$1
    local from_idx=$2
    local to_idx=$3
    echo $((PROXY_BASE_PORT + (shard_idx * 100) + ((from_idx - 1) * 10) + (to_idx - 1)))
}

SHARD_LABELS=(A B C)

for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
    SHARD_ID="${SHARD_LABELS[$shard_idx]}"
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

echo -e "${GREEN}Toxiproxy setup complete${NC}"
echo "Use ./scripts/partition-toxiproxy.sh to isolate or restore links."