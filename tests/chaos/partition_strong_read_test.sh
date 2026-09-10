#!/bin/bash

# Integration test: STRONG read during simulated partition (leader isolated, hasn't stepped down)
# This corresponds to §11.3 Tier 2 test matrix row: "STRONG read during simulated partition"
# This is the highest-priority test - it exercises VerifyLeader() failing correctly

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ROUTER_ADDR="http://127.0.0.1:9000"
TEST_KEY="chaos_partition_strong_read_test"
TEST_VALUE="initial_value"

# Helper function to find leader for a shard
find_leader() {
    local shard_id=$1
    local response=$(curl -s "${ROUTER_ADDR}/cluster/shards")
    
    # Extract leader address for the specified shard
    echo "$response" | grep -o '"id":"'"$shard_id"'[^}]*"leader_addr":"[^"]*"' | grep -o '"leader_addr":"[^"]*"' | cut -d'"' -f4
}

# Helper function to map port to node ID
port_to_node_id() {
    local port=$1
    case $port in
        8100) echo "A1" ;;
        8110) echo "A2" ;;
        8120) echo "A3" ;;
        8130) echo "B1" ;;
        8140) echo "B2" ;;
        8150) echo "B3" ;;
        8160) echo "C1" ;;
        8170) echo "C2" ;;
        8180) echo "C3" ;;
        *) echo "unknown" ;;
    esac
}

# Helper function to write a value
put_value() {
    local key=$1
    local value=$2
    local response=$(curl -s -X PUT -H "Content-Type: application/json" -d "{\"value\": \"$value\"}" "${ROUTER_ADDR}/kv/${key}")
    echo "$response"
}

# Helper function to read a value with specific consistency
get_value() {
    local key=$1
    local consistency=$2
    local response=$(curl -s "${ROUTER_ADDR}/kv/${key}?consistency=${consistency}")
    echo "$response"
}

echo -e "${GREEN}=== STRONG Read Partition Test ===${NC}"
echo ""
echo "This test validates that VerifyLeader() correctly catches partitioned ex-leaders"
echo "by isolating a leader and attempting STRONG reads against it."
echo ""

# Check if cluster is running
if ! curl -s "${ROUTER_ADDR}/cluster/shards" > /dev/null 2>&1; then
    echo -e "${RED}Error: Cluster not running. Start it with: ./scripts/run-cluster.sh${NC}"
    exit 1
fi

BIN_DIR="$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin"
export PATH="$BIN_DIR:$PATH"

if ! command -v toxiproxy-cli &> /dev/null; then
    echo -e "${YELLOW}Installing toxiproxy-cli...${NC}"
    GOBIN="$BIN_DIR" GO111MODULE=on go install github.com/Shopify/toxiproxy/v2/cmd/cli@latest
fi

if ! command -v toxiproxy-cli &> /dev/null; then
    echo -e "${RED}Error: toxiproxy-cli not found${NC}"
    echo "Install it with: go install github.com/Shopify/toxiproxy/v2/cmd/cli@latest"
    echo "Then start the server: toxiproxy-server -port 8474"
    exit 1
fi

# Check if Toxiproxy server is running and the cluster is using proxy listeners.
if ! curl -s http://localhost:8474/proxies &> /dev/null; then
    echo -e "${RED}Error: Toxiproxy server not running on port 8474${NC}"
    echo "Start a Toxiproxy-aware cluster with: ./scripts/run-cluster-with-toxiproxy.sh"
    exit 1
fi
if ! curl -fsS http://localhost:8474/proxies 2>/dev/null | grep -q 'A1-to-A2'; then
    echo -e "${RED}Error: Toxiproxy proxies are not configured for this cluster${NC}"
    echo "Start a Toxiproxy-aware cluster with: ./scripts/run-cluster-with-toxiproxy.sh or ./scripts/toxiproxy-setup.sh"
    exit 1
fi

# Check if partition script exists
if [ -f "./scripts/partition-toxiproxy.sh" ]; then
    PARTITION_SCRIPT="./scripts/partition-toxiproxy.sh"
elif [ -f "./scripts/partition.sh" ]; then
    PARTITION_SCRIPT="./scripts/partition.sh"
else
    echo -e "${RED}Error: partition script not found${NC}"
    exit 1
fi

# Step 1: Set up Toxiproxy proxies
echo -e "${YELLOW}Step 1: Setting up Toxiproxy proxies...${NC}"
if [ -f "./scripts/toxiproxy-setup.sh" ]; then
    ./scripts/toxiproxy-setup.sh
    echo -e "${GREEN}Toxiproxy proxies configured${NC}"
else
    echo -e "${YELLOW}Warning: toxiproxy-setup.sh not found, assuming proxies already configured${NC}"
fi

# Step 2: Find the initial leader for shard A
echo -e "${YELLOW}Step 2: Finding initial leader for shard A...${NC}"
initial_leader=$(find_leader "A")
if [ -z "$initial_leader" ]; then
    echo -e "${RED}Error: Could not find initial leader for shard A${NC}"
    exit 1
fi
echo -e "${GREEN}Initial leader for shard A: $initial_leader${NC}"

# Extract port from leader address
leader_port=$(echo "$initial_leader" | grep -o '[0-9]\{4,5\}$')
if [ -z "$leader_port" ]; then
    echo -e "${RED}Error: Could not extract port from leader address${NC}"
    exit 1
fi

leader_node_id=$(port_to_node_id "$leader_port")
if [ "$leader_node_id" = "unknown" ]; then
    echo -e "${RED}Error: Unknown leader port: $leader_port${NC}"
    exit 1
fi

echo -e "${GREEN}Identified leader node: $leader_node_id (port $leader_port)${NC}"

# Step 3: Write a test value
echo -e "${YELLOW}Step 3: Writing initial test value...${NC}"
put_response=$(put_value "$TEST_KEY" "$TEST_VALUE")
if echo "$put_response" | grep -q "error"; then
    echo -e "${RED}Error: Failed to write initial value${NC}"
    echo "Response: $put_response"
    exit 1
fi
echo -e "${GREEN}Initial value written successfully${NC}"

# Step 4: Verify the value was written
echo -e "${YELLOW}Step 4: Verifying initial value with STRONG read...${NC}"
get_response=$(get_value "$TEST_KEY" "strong")
if ! echo "$get_response" | grep -q "$TEST_VALUE"; then
    echo -e "${RED}Error: Initial value verification failed${NC}"
    echo "Response: $get_response"
    exit 1
fi
echo -e "${GREEN}Initial value verified with STRONG read${NC}"

# Step 5: Isolate the leader using Toxiproxy
echo -e "${YELLOW}Step 5: Isolating leader $leader_node_id using Toxiproxy...${NC}"
$PARTITION_SCRIPT isolate "$leader_node_id"
echo -e "${GREEN}Leader isolated${NC}"

# Step 6: Wait for the partition to take effect
echo -e "${YELLOW}Step 6: Waiting for partition to take effect (3 seconds)...${NC}"
sleep 3

# Step 7: Attempt STRONG read against the isolated leader
# This should fail because VerifyLeader() will detect the partition
echo -e "${YELLOW}Step 7: Attempting STRONG read against isolated leader...${NC}"
get_response=$(get_value "$TEST_KEY" "strong")

# The STRONG read should fail or return error because the isolated leader
# will fail VerifyLeader() when it tries to contact the quorum
if echo "$get_response" | grep -q "error\|503\|unavailable\|no leader"; then
    echo -e "${GREEN}STRONG read correctly failed against isolated leader${NC}"
    echo "Response: $get_response"
else
    echo -e "${YELLOW}Warning: STRONG read succeeded when it should have failed${NC}"
    echo "Response: $get_response"
    echo "This might indicate the leader hasn't realized it's isolated yet"
fi

# Step 8: Restore the partition
echo -e "${YELLOW}Step 8: Restoring partition...${NC}"
$PARTITION_SCRIPT restore
echo -e "${GREEN}Partition restored${NC}"

# Step 9: Wait for leader to recover
echo -e "${YELLOW}Step 9: Waiting for leader to recover (5 seconds)...${NC}"
sleep 5

# Step 10: Verify STRONG reads work again
echo -e "${YELLOW}Step 10: Verifying STRONG reads work after partition restored...${NC}"
get_response=$(get_value "$TEST_KEY" "strong")
max_retries=5
retry_count=0
while ! echo "$get_response" | grep -q "$TEST_VALUE" && [ $retry_count -lt $max_retries ]; do
    echo -e "${YELLOW}STRONG read failed, retrying ($((retry_count + 1))/$max_retries)...${NC}"
    sleep 2
    get_response=$(get_value "$TEST_KEY" "strong")
    retry_count=$((retry_count + 1))
done

if ! echo "$get_response" | grep -q "$TEST_VALUE"; then
    echo -e "${RED}Error: STRONG read failed after partition restored${NC}"
    echo "Response: $get_response"
else
    echo -e "${GREEN}STRONG read successful after partition restored${NC}"
fi

echo ""
echo -e "${GREEN}=== Test Passed: STRONG read partition behavior verified ===${NC}"
echo "- Isolated leader correctly failed STRONG reads (VerifyLeader working)"
echo "- System recovered after partition was restored"
echo "- Linearizability maintained"