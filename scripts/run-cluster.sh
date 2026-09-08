#!/bin/bash

# Script to run a 3-node Raft cluster for testing
# Each node runs on different ports and uses its own data directory

set -e

# Configuration
NODE_COUNT=3
BASE_RAFT_PORT=7000
BASE_HTTP_PORT=8000
DATA_DIR_BASE="/tmp/shardkv"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting 3-node Raft cluster...${NC}"

# Clean up function
cleanup() {
    echo -e "${YELLOW}Shutting down cluster...${NC}"
    for i in $(seq 1 $NODE_COUNT); do
        NODE_ID="node$i"
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
    echo -e "${GREEN}Cluster shut down${NC}"
}

# Trap cleanup on exit
trap cleanup EXIT INT TERM

# Build the node binary unconditionally to ensure latest code is used
echo "Building node binary..."
go build -o node ./cmd/node

# Build peer list for all nodes
PEERS=""
PEER_IDS=""
for i in $(seq 1 $NODE_COUNT); do
    RAFT_PORT=$((BASE_RAFT_PORT + i - 1))
    NODE_ID="node$i"
    if [ -z "$PEERS" ]; then
        PEERS="127.0.0.1:$RAFT_PORT"
        PEER_IDS="$NODE_ID"
    else
        PEERS="$PEERS,127.0.0.1:$RAFT_PORT"
        PEER_IDS="$PEER_IDS,$NODE_ID"
    fi
done

echo "Peer configuration: $PEERS"
echo "Peer IDs: $PEER_IDS"

# Start nodes
for i in $(seq 1 $NODE_COUNT); do
    NODE_ID="node$i"
    RAFT_PORT=$((BASE_RAFT_PORT + i - 1))
    HTTP_PORT=$((BASE_HTTP_PORT + i - 1))
    DATA_DIR="${DATA_DIR_BASE}/${NODE_ID}"
    
    # Create data directory
    mkdir -p "$DATA_DIR"
    
    # Determine if this is the bootstrap node (first node)
    BOOTSTRAP_FLAG=""
    PEERS_FLAG=""
    PEER_IDS_FLAG=""
    if [ $i -eq 1 ]; then
        # Always bootstrap for first node - the node will check for existing state
        BOOTSTRAP_FLAG="--bootstrap"
        PEERS_FLAG="--peers $PEERS"
        PEER_IDS_FLAG="--peer-ids $PEER_IDS"
        echo -e "${GREEN}Bootstrapping cluster with node $NODE_ID${NC}"
    else
        echo -e "${YELLOW}Starting node $NODE_ID${NC}"
    fi
    
    # Start the node in background
    ./node \
        --node-id "$NODE_ID" \
        --raft-addr "127.0.0.1:$RAFT_PORT" \
        --http-addr "127.0.0.1:$HTTP_PORT" \
        --data-dir "$DATA_DIR" \
        $BOOTSTRAP_FLAG \
        $PEERS_FLAG \
        $PEER_IDS_FLAG \
        > "${DATA_DIR}/node.log" 2>&1 &
    
    NODE_PID=$!
    echo $NODE_PID > "/tmp/shardkv/${NODE_ID}.pid"
    
    echo "Started $NODE_ID (PID: $NODE_PID, Raft: 127.0.0.1:$RAFT_PORT, HTTP: 127.0.0.1:$HTTP_PORT)"
    
    # Wait a bit between starting nodes
    sleep 1
done

echo -e "${GREEN}All nodes started${NC}"
echo ""
echo "Node details:"
for i in $(seq 1 $NODE_COUNT); do
    NODE_ID="node$i"
    RAFT_PORT=$((BASE_RAFT_PORT + i - 1))
    HTTP_PORT=$((BASE_HTTP_PORT + i - 1))
    echo "  $NODE_ID: Raft=127.0.0.1:$RAFT_PORT, HTTP=127.0.0.1:$HTTP_PORT"
done
echo ""
echo "Log files:"
for i in $(seq 1 $NODE_COUNT); do
    NODE_ID="node$i"
    DATA_DIR="${DATA_DIR_BASE}/${NODE_ID}"
    echo "  $NODE_ID: $DATA_DIR/node.log"
done
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop the cluster${NC}"

# Wait for nodes to run
wait