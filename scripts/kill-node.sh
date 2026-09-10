#!/bin/bash

# Script to kill and optionally restart shard nodes for chaos testing
# This simulates node failures and tests recovery behavior

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

DATA_DIR_BASE="/tmp/shardkv"

# Function to show usage
show_usage() {
    echo "Usage: $0 <command> [args]"
    echo ""
    echo "Commands:"
    echo "  kill <node_id>          Kill a specific node (e.g., A1, B2, C3)"
    echo "  restart <node_id>       Restart a specific node"
    echo "  list                    List all running nodes"
    echo "  status <node_id>        Show status of a specific node"
    echo ""
    echo "Examples:"
    echo "  $0 kill A1              # Kill node A1"
    echo "  $0 restart A1           # Restart node A1"
    echo "  $0 list                # List all running nodes"
    echo "  $0 status A1           # Show status of node A1"
}

# Function to get node PID
get_node_pid() {
    local node_id=$1
    local pid_file="/tmp/shardkv/${node_id}.pid"
    
    if [ -f "$pid_file" ]; then
        cat "$pid_file"
    else
        echo ""
    fi
}

# Function to get node config from run-cluster.sh
get_node_config() {
    local node_id=$1
    local shard_id=${node_id:0:1}
    local node_num=${node_id:1}
    
    # These need to match the configuration in run-cluster.sh
    local shard_idx=$(printf "%d" "'$shard_id")  # A=65, B=66, C=67
    shard_idx=$((shard_idx - 65))
    
    local SHARD_COUNT=3
    local NODES_PER_SHARD=3
    local BASE_RAFT_PORT=7000
    local BASE_HTTP_PORT=8100
    
    local SHARD_BASE_RAFT=$((BASE_RAFT_PORT + shard_idx * NODES_PER_SHARD * 100))
    local SHARD_BASE_HTTP=$((BASE_HTTP_PORT + shard_idx * NODES_PER_SHARD * 10))
    
    local RAFT_PORT=$((SHARD_BASE_RAFT + (node_num - 1) * 100))
    local HTTP_PORT=$((SHARD_BASE_HTTP + (node_num - 1) * 10))
    local DATA_DIR="${DATA_DIR_BASE}/${node_id}"
    
    echo "$RAFT_PORT $HTTP_PORT $DATA_DIR"
}

# Function to kill a node
kill_node() {
    local node_id=$1
    
    if [ -z "$node_id" ]; then
        echo -e "${RED}Error: node_id required for kill command${NC}"
        show_usage
        exit 1
    fi
    
    local pid=$(get_node_pid "$node_id")
    
    if [ -z "$pid" ]; then
        echo -e "${YELLOW}Node $node_id is not running (no PID file found)${NC}"
        return
    fi
    
    if ! kill -0 "$pid" 2>/dev/null; then
        echo -e "${YELLOW}Node $node_id (PID: $pid) is not running${NC}"
        rm -f "/tmp/shardkv/${node_id}.pid"
        return
    fi
    
    echo -e "${YELLOW}Killing node $node_id (PID: $pid)${NC}"
    kill -9 "$pid"
    rm -f "/tmp/shardkv/${node_id}.pid"
    echo -e "${GREEN}Node $node_id killed${NC}"
}

# Function to restart a node
restart_node() {
    local node_id=$1
    
    if [ -z "$node_id" ]; then
        echo -e "${RED}Error: node_id required for restart command${NC}"
        show_usage
        exit 1
    fi
    
    # First kill if running
    local pid=$(get_node_pid "$node_id")
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        echo -e "${YELLOW}Node $node_id is already running, killing first...${NC}"
        kill -9 "$pid"
        rm -f "/tmp/shardkv/${node_id}.pid"
        sleep 1
    fi
    
    # Get node configuration
    local config=$(get_node_config "$node_id")
    local RAFT_PORT=$(echo "$config" | awk '{print $1}')
    local HTTP_PORT=$(echo "$config" | awk '{print $2}')
    local DATA_DIR=$(echo "$config" | awk '{print $3}')
    local shard_id=${node_id:0:1}
    
    echo -e "${GREEN}Restarting node $node_id${NC}"
    echo "  Raft: 127.0.0.1:$RAFT_PORT"
    echo "  HTTP: 127.0.0.1:$HTTP_PORT"
    echo "  Data: $DATA_DIR"
    
    # Check if node binary exists
    if [ ! -f "./node" ]; then
        echo -e "${RED}Error: node binary not found. Run from project root and build with: go build -o node ./cmd/node${NC}"
        exit 1
    fi
    
    # Check if data directory exists
    if [ ! -d "$DATA_DIR" ]; then
        echo -e "${RED}Error: data directory $DATA_DIR not found. Node may not have been started initially${NC}"
        exit 1
    fi
    
    # Start the node detached from this shell so it survives when the test script exits.
    # Include the full peer list and HTTP map so the restarted node can repopulate
    # its raftToHTTP map and route requests correctly after catch-up.
    local peers=""
    local peer_ids=""
    local peers_http=""
    for peer_idx in 1 2 3; do
        local peer_node="${shard_id}${peer_idx}"
        local peer_config=$(get_node_config "$peer_node")
        local peer_raft_port=$(echo "$peer_config" | awk '{print $1}')
        local peer_http_port=$(echo "$peer_config" | awk '{print $2}')
        local peer_raft="127.0.0.1:${peer_raft_port}"
        local peer_http="127.0.0.1:${peer_http_port}"
        if [ -z "$peers" ]; then
            peers="$peer_raft"
            peer_ids="$peer_node"
            peers_http="$peer_http"
        else
            peers="$peers,$peer_raft"
            peer_ids="$peer_ids,$peer_node"
            peers_http="$peers_http,$peer_http"
        fi
    done

    setsid ./node \
        --node-id "$node_id" \
        --shard-id "$shard_id" \
        --raft-addr "127.0.0.1:$RAFT_PORT" \
        --http-addr "127.0.0.1:$HTTP_PORT" \
        --data-dir "$DATA_DIR" \
        --peers "$peers" \
        --peer-ids "$peer_ids" \
        --peers-http "$peers_http" \
        > "${DATA_DIR}/node.log" 2>&1 < /dev/null &
    
    local new_pid=$!
    echo $new_pid > "/tmp/shardkv/${node_id}.pid"
    
    # Wait for HTTP port to be bound
    local max_wait=10
    local count=0
    while [ $count -lt $max_wait ]; do
        if ! kill -0 $new_pid 2>/dev/null; then
            echo -e "${RED}Node $node_id failed to start (PID: $new_pid died)${NC}"
            cat "${DATA_DIR}/node.log"
            exit 1
        fi
        
        # Check if port is in use
        if command -v ss >/dev/null 2>&1; then
            if ss -tuln 2>/dev/null | grep -q ":$HTTP_PORT "; then
                echo -e "${GREEN}Node $node_id restarted successfully (PID: $new_pid)${NC}"
                return
            fi
        elif command -v lsof >/dev/null 2>&1; then
            if lsof -i :$HTTP_PORT >/dev/null 2>&1; then
                echo -e "${GREEN}Node $node_id restarted successfully (PID: $new_pid)${NC}"
                return
            fi
        elif command -v netstat >/dev/null 2>&1; then
            if netstat -tuln 2>/dev/null | grep -q ":$HTTP_PORT "; then
                echo -e "${GREEN}Node $node_id restarted successfully (PID: $new_pid)${NC}"
                return
            fi
        fi
        
        sleep 1
        count=$((count + 1))
    done
    
    echo -e "${RED}Timeout waiting for node $node_id to bind to HTTP port $HTTP_PORT${NC}"
    cat "${DATA_DIR}/node.log"
    exit 1
}

# Function to list all running nodes
list_nodes() {
    echo -e "${GREEN}Running ShardKV nodes:${NC}"
    echo ""
    
    if [ ! -d "$DATA_DIR_BASE" ]; then
        echo "No shardkv data directory found"
        return
    fi
    
    local found=0
    for pid_file in "$DATA_DIR_BASE"/*.pid; do
        if [ -f "$pid_file" ]; then
            local node_id=$(basename "$pid_file" .pid)
            local pid=$(cat "$pid_file")
            
            if kill -0 "$pid" 2>/dev/null; then
                local config=$(get_node_config "$node_id")
                local RAFT_PORT=$(echo "$config" | awk '{print $1}')
                local HTTP_PORT=$(echo "$config" | awk '{print $2}')
                
                echo "  $node_id: PID=$pid, Raft=127.0.0.1:$RAFT_PORT, HTTP=127.0.0.1:$HTTP_PORT"
                found=$((found + 1))
            else
                echo "  $node_id: PID=$pid (not running)"
            fi
        fi
    done
    
    if [ $found -eq 0 ]; then
        echo "No running nodes found"
    fi
}

# Function to show node status
show_node_status() {
    local node_id=$1
    
    if [ -z "$node_id" ]; then
        echo -e "${RED}Error: node_id required for status command${NC}"
        show_usage
        exit 1
    fi
    
    local pid=$(get_node_pid "$node_id")
    local config=$(get_node_config "$node_id")
    local RAFT_PORT=$(echo "$config" | awk '{print $1}')
    local HTTP_PORT=$(echo "$config" | awk '{print $2}')
    local DATA_DIR=$(echo "$config" | awk '{print $3}')
    
    echo "Node $node_id status:"
    echo "  PID: $pid"
    echo "  Raft address: 127.0.0.1:$RAFT_PORT"
    echo "  HTTP address: 127.0.0.1:$HTTP_PORT"
    echo "  Data directory: $DATA_DIR"
    
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        echo -e "  Status: ${GREEN}Running${NC}"
        
        # Try to get health status
        if command -v curl >/dev/null 2>&1; then
            echo "  Health check:"
            curl -s "http://127.0.0.1:$HTTP_PORT/cluster/health" 2>/dev/null || echo "    Health endpoint unavailable"
        fi
    else
        echo -e "  Status: ${RED}Not running${NC}"
    fi
}

# Main command handling
case "$1" in
    kill)
        kill_node "$2"
        ;;
    restart)
        restart_node "$2"
        ;;
    list)
        list_nodes
        ;;
    status)
        show_node_status "$2"
        ;;
    *)
        show_usage
        exit 1
        ;;
esac