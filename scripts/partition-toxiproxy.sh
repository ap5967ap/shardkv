#!/bin/bash

# Script to simulate network partitions using Toxiproxy
# This allows isolating nodes, cutting specific links, or restoring all connections

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SHARD_COUNT=3
NODES_PER_SHARD=3

BIN_DIR="$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin"
TOXIPROXY_CLI="$BIN_DIR/toxiproxy-cli"
export PATH="$BIN_DIR:$PATH"
if ! [ -x "$TOXIPROXY_CLI" ] && command -v cli >/dev/null 2>&1; then
    ln -sf "$(command -v cli)" "$TOXIPROXY_CLI"
fi

# Check if toxiproxy-cli is available
if ! [ -x "$TOXIPROXY_CLI" ]; then
    echo -e "${RED}toxiproxy-cli not found${NC}"
    echo -e "${YELLOW}Installing upstream toxiproxy CLI...${NC}"
    GOBIN="$BIN_DIR" GO111MODULE=on go install github.com/Shopify/toxiproxy/v2/cmd/cli@latest
    ln -sf "$BIN_DIR/cli" "$TOXIPROXY_CLI"
    export PATH="$BIN_DIR:$PATH"
fi

# Check if toxiproxy server is running
if ! curl -s http://localhost:8474/proxies &> /dev/null; then
    echo -e "${RED}Toxiproxy server not running on port 8474${NC}"
    exit 1
fi

# Function to show usage
show_usage() {
    echo "Usage: $0 <command> [args]"
    echo ""
    echo "Commands:"
    echo "  isolate <node_id>      Isolate a node from all its peers (e.g., A1, B2, C3)"
    echo "  link <from> <to>       Cut a specific directed link (e.g., A1 A2 cuts A1->A2)"
    echo "  restore                Restore all connections"
    echo "  status                 Show current proxy status"
    echo ""
    echo "Examples:"
    echo "  $0 isolate A1          # Isolate node A1 from all peers"
    echo "  $0 link A1 A2          # Cut link from A1 to A2"
    echo "  $0 restore             # Restore all connections"
    echo "  $0 status              # Show current proxy status"
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
    
    # Extract shard ID and node number
    shard_id=${node_id:0:1}
    node_num=${node_id:1}
    
    # Find all proxies from this node to other nodes.
    # `toxiproxy-cli list` prints a table with multiple columns, so we must parse
    # only the first field (the proxy name) to avoid passing the whole row as a URL.
    proxies=$(toxiproxy-cli list 2>/dev/null || echo "")
    
    count=0
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        proxy_name=${line%%[[:space:]]*}
        if [[ "$proxy_name" == "${node_id}-to-"* ]]; then
            echo -e "${RED}Cutting link: $proxy_name${NC}"
            "$TOXIPROXY_CLI" toggle "$proxy_name"
            count=$((count + 1))
        fi
    done <<< "$proxies"
    
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
    
    local proxy_name="${from}-to-${to}"
    
    echo -e "${YELLOW}Cutting link: $proxy_name${NC}"
    
    if "$TOXIPROXY_CLI" toggle "$proxy_name" 2>/dev/null; then
        echo -e "${GREEN}Link $proxy_name cut${NC}"
    else
        echo -e "${RED}Failed to cut link $proxy_name (may not exist)${NC}"
    fi
}

# Function to restore all connections
restore_all() {
    echo -e "${YELLOW}Restoring all connections...${NC}"
    
    proxies=$(toxiproxy-cli list 2>/dev/null || echo "")
    
    count=0
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        proxy_name=${line%%[[:space:]]*}
        if [ -n "$proxy_name" ]; then
            echo -e "${GREEN}Restoring: $proxy_name${NC}"
            "$TOXIPROXY_CLI" toggle "$proxy_name" 2>/dev/null || true
            count=$((count + 1))
        fi
    done <<< "$proxies"
    
    echo -e "${GREEN}All connections restored${NC}"
}

# Function to show status
show_status() {
    echo -e "${GREEN}Current Toxiproxy proxy status:${NC}"
    echo ""
    
    proxies=$(toxiproxy-cli list 2>/dev/null || echo "")
    
    if [ -z "$proxies" ]; then
        echo "No proxies configured"
        return
    fi
    
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        proxy_name=${line%%[[:space:]]*}
        if [ -n "$proxy_name" ]; then
            # Get proxy details
            details=$("$TOXIPROXY_CLI" inspect "$proxy_name" 2>/dev/null || echo "")
            if echo "$details" | grep -q "enabled: true"; then
                status="${GREEN}UP${NC}"
            else
                status="${RED}DOWN${NC}"
            fi
            echo -e "$proxy_name: $status"
        fi
    done <<< "$proxies"
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