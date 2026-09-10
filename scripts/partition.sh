#!/bin/bash

# Script to simulate network partitions using iptables (Linux-specific)
# This allows isolating nodes, cutting specific links, or restoring all connections
# Alternative to Toxiproxy for systems where iptables is available

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SHARD_COUNT=3
NODES_PER_SHARD=3
BASE_RAFT_PORT=7000

# Check if running as root (required for iptables)
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}This script must be run as root for iptables commands${NC}"
    echo "Try: sudo $0 $@"
    exit 1
fi

# Function to get node Raft port
get_raft_port() {
    local node_id=$1
    local shard_id=${node_id:0:1}
    local node_num=${node_id:1}
    
    local shard_idx=$(printf "%d" "'$shard_id")
    shard_idx=$((shard_idx - 65))
    
    local SHARD_BASE_RAFT=$((BASE_RAFT_PORT + shard_idx * NODES_PER_SHARD * 100))
    local RAFT_PORT=$((SHARD_BASE_RAFT + (node_num - 1) * 100))
    
    echo $RAFT_PORT
}

# Function to show usage
show_usage() {
    echo "Usage: $0 <command> [args]"
    echo ""
    echo "Commands:"
    echo "  isolate <node_id>      Isolate a node from all its peers (e.g., A1, B2, C3)"
    echo "  link <from> <to>       Cut a specific directed link (e.g., A1 A2 cuts A1->A2)"
    echo "  restore                Restore all connections"
    echo "  status                 Show current iptables rules"
    echo ""
    echo "Examples:"
    echo "  $0 isolate A1          # Isolate node A1 from all peers"
    echo "  $0 link A1 A2          # Cut link from A1 to A2"
    echo "  $0 restore             # Restore all connections"
    echo "  $0 status              # Show current iptables rules"
}

# Function to isolate a node
isolate_node() {
    local node_id=$1
    
    if [ -z "$node_id" ]; then
        echo -e "${RED}Error: node_id required for isolate command${NC}"
        show_usage
        exit 1
    fi
    
    echo -e "${YELLOW}Isolating node $node_id from all peers...${NC}"
    
    local shard_id=${node_id:0:1}
    local node_num=${node_id:1}
    
    # Get all nodes in the same shard
    for peer_idx in $(seq 1 $NODES_PER_SHARD); do
        if [ $peer_idx -ne $node_num ]; then
            local peer_node="${shard_id}${peer_idx}"
            local peer_port=$(get_raft_port "$peer_node")
            
            echo -e "${RED}Blocking connections to $peer_node (port $peer_port)${NC}"
            iptables -A OUTPUT -p tcp --dport $peer_port -j DROP
            iptables -A INPUT -p tcp --sport $peer_port -j DROP
        fi
    done
    
    echo -e "${GREEN}Node $node_id isolated${NC}"
}

# Function to cut a specific link
cut_link() {
    local from=$1
    local to=$2
    
    if [ -z "$from" ] || [ -z "$to" ]; then
        echo -e "${RED}Error: both from and to node IDs required for link command${NC}"
        show_usage
        exit 1
    fi
    
    local to_port=$(get_raft_port "$to")
    
    echo -e "${YELLOW}Cutting link from $from to $to (port $to_port)${NC}"
    
    iptables -A OUTPUT -p tcp --dport $to_port -j DROP
    iptables -A INPUT -p tcp --sport $to_port -j DROP
    
    echo -e "${GREEN}Link $from->$to cut${NC}"
}

# Function to restore all connections
restore_all() {
    echo -e "${YELLOW}Restoring all connections...${NC}"
    
    # Remove all DROP rules related to our Raft ports
    iptables -D OUTPUT -p tcp --dport 7000:7200 -j DROP 2>/dev/null || true
    iptables -D INPUT -p tcp --sport 7000:7200 -j DROP 2>/dev/null || true
    
    # More specific cleanup
    for shard_idx in $(seq 0 $((SHARD_COUNT - 1))); do
        SHARD_ID=$(echo "$((65 + shard_idx))" | awk '{printf "%c", $1}')
        SHARD_BASE_RAFT=$((BASE_RAFT_PORT + shard_idx * NODES_PER_SHARD * 100))
        
        for node_idx in $(seq 1 $NODES_PER_SHARD); do
            RAFT_PORT=$((SHARD_BASE_RAFT + (node_idx - 1) * 100))
            iptables -D OUTPUT -p tcp --dport $RAFT_PORT -j DROP 2>/dev/null || true
            iptables -D INPUT -p tcp --sport $RAFT_PORT -j DROP 2>/dev/null || true
        done
    done
    
    echo -e "${GREEN}All connections restored${NC}"
}

# Function to show status
show_status() {
    echo -e "${GREEN}Current iptables rules for Raft ports:${NC}"
    echo ""
    
    iptables -L OUTPUT -n | grep -E "7000|DROP" || echo "No OUTPUT DROP rules"
    echo ""
    iptables -L INPUT -n | grep -E "7000|DROP" || echo "No INPUT DROP rules"
}

# Main command handling
case "$1" in
    isolate)
        isolate_node "$2"
        ;;
    link)
        cut_link "$2" "$3"
        ;;
    restore)
        restore_all
        ;;
    status)
        show_status
        ;;
    *)
        show_usage
        exit 1
        ;;
esac