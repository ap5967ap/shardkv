#!/bin/bash

# Script to stop all ShardKV cluster processes cleanly
# This script handles graceful shutdown and cleanup

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

DATA_DIR_BASE="/tmp/shardkv"

echo -e "${YELLOW}Stopping ShardKV cluster...${NC}"

# Kill router first
if [ -f "$DATA_DIR_BASE/router.pid" ]; then
    PID=$(cat "$DATA_DIR_BASE/router.pid")
    if kill -0 $PID 2>/dev/null; then
        echo -e "${YELLOW}Stopping router (PID: $PID)${NC}"
        kill $PID 2>/dev/null || true
        # Wait a bit for graceful shutdown
        sleep 1
        # Force kill if still running
        if kill -0 $PID 2>/dev/null; then
            echo -e "${RED}Force killing router (PID: $PID)${NC}"
            kill -9 $PID 2>/dev/null || true
        fi
    fi
    rm -f "$DATA_DIR_BASE/router.pid"
fi

# Kill all node processes
if [ -d "$DATA_DIR_BASE" ]; then
    for pid_file in "$DATA_DIR_BASE"/*.pid; do
        if [ -f "$pid_file" ]; then
            PID=$(cat "$pid_file")
            NODE_ID=$(basename "$pid_file" .pid)
            
            if kill -0 $PID 2>/dev/null; then
                echo -e "${YELLOW}Stopping node $NODE_ID (PID: $PID)${NC}"
                kill $PID 2>/dev/null || true
                # Wait a bit for graceful shutdown
                sleep 1
                # Force kill if still running
                if kill -0 $PID 2>/dev/null; then
                    echo -e "${RED}Force killing node $NODE_ID (PID: $PID)${NC}"
                    kill -9 $PID 2>/dev/null || true
                fi
            fi
            rm -f "$pid_file"
        fi
    done
fi

# Kill any remaining node or router processes by name
echo -e "${YELLOW}Cleaning up any remaining processes...${NC}"
pkill -f "./node" 2>/dev/null || true
pkill -f "./router" 2>/dev/null || true

# Wait for processes to terminate
sleep 2

# Verify no processes are still running
REMAINING_NODES=$(pgrep -f "./node" || true)
REMAINING_ROUTER=$(pgrep -f "./router" || true)
if [ -n "$REMAINING_NODES" ] || [ -n "$REMAINING_ROUTER" ]; then
    echo -e "${RED}Warning: Some processes may still be running${NC}"
    echo "Remaining node PIDs: $REMAINING_NODES"
    echo "Remaining router PIDs: $REMAINING_ROUTER"
    # Force kill remaining
    pkill -9 -f "./node" 2>/dev/null || true
    pkill -9 -f "./router" 2>/dev/null || true
fi

echo -e "${GREEN}Cluster stopped successfully${NC}"
