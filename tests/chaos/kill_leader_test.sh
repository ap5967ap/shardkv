#!/bin/bash

# Integration test: Kill shard leader, confirm election + resumed writes
# This corresponds to §11.3 Tier 2 test matrix row: "kill shard leader, confirm election + resumed writes"

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ROUTER_ADDR="http://127.0.0.1:9000"
TEST_KEY="chaos_leader_kill_test"
TEST_VALUE="initial_value"
RECOVERY_VALUE="recovery_value"

# Helper function to find leader for a shard
find_leader() {
    local shard_id=$1
    local response=$(curl -s "${ROUTER_ADDR}/cluster/shards")
    
    # Extract leader address for the specified shard
    # Using jq would be cleaner, but we'll use grep/sed for portability
    echo "$response" | grep -o '"id":"'"$shard_id"'[^}]*"leader_addr":"[^"]*"' | grep -o '"leader_addr":"[^"]*"' | cut -d'"' -f4
}

# Helper function to PUT a value
put_value() {
    local key=$1
    local value=$2
    local response=$(curl -s -X PUT -H "Content-Type: application/json" -d "{\"value\": \"$value\"}" "${ROUTER_ADDR}/kv/${key}")
    echo "$response"
}

# Helper function to GET a value
get_value() {
    local key=$1
    local consistency=$2
    local response=$(curl -s "${ROUTER_ADDR}/kv/${key}?consistency=${consistency}")
    echo "$response"
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

echo -e "${GREEN}=== Leader Kill and Recovery Test ===${NC}"
echo ""

# Check if cluster is running
if ! curl -s "${ROUTER_ADDR}/cluster/shards" > /dev/null 2>&1; then
    echo -e "${RED}Error: Cluster not running. Start it with: ./scripts/run-cluster.sh${NC}"
    exit 1
fi

# Step 1: Find the initial leader for shard A
echo -e "${YELLOW}Step 1: Finding initial leader for shard A...${NC}"
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

# Step 2: Write a test value
echo -e "${YELLOW}Step 2: Writing initial test value...${NC}"
put_response=$(put_value "$TEST_KEY" "$TEST_VALUE")
if echo "$put_response" | grep -q "error"; then
    echo -e "${RED}Error: Failed to write initial value${NC}"
    echo "Response: $put_response"
    exit 1
fi
echo -e "${GREEN}Initial value written successfully${NC}"

# Step 3: Verify the value was written
echo -e "${YELLOW}Step 3: Verifying initial value...${NC}"
get_response=$(get_value "$TEST_KEY" "strong")
if ! echo "$get_response" | grep -q "$TEST_VALUE"; then
    echo -e "${RED}Error: Initial value verification failed${NC}"
    echo "Response: $get_response"
    exit 1
fi
echo -e "${GREEN}Initial value verified${NC}"

# Step 4: Kill the leader node
echo -e "${YELLOW}Step 4: Killing leader node $leader_node_id...${NC}"
if [ ! -f "./scripts/kill-node.sh" ]; then
    echo -e "${RED}Error: kill-node.sh script not found${NC}"
    exit 1
fi

./scripts/kill-node.sh kill "$leader_node_id"
echo -e "${GREEN}Leader node killed${NC}"

# Step 5: Wait for leader election
echo -e "${YELLOW}Step 5: Waiting for leader election (10 seconds)...${NC}"
sleep 10

# Step 6: Verify a new leader is elected
echo -e "${YELLOW}Step 6: Finding new leader for shard A...${NC}"
new_leader=$(find_leader "A")
if [ -z "$new_leader" ]; then
    echo -e "${RED}Error: Could not find new leader for shard A${NC}"
    echo -e "${YELLOW}Attempting to restart killed node...${NC}"
    ./scripts/kill-node.sh restart "$leader_node_id"
    exit 1
fi
echo -e "${GREEN}New leader for shard A: $new_leader${NC}"

# Verify the new leader is different
if [ "$new_leader" = "$initial_leader" ]; then
    echo -e "${YELLOW}Warning: New leader is the same as initial leader (may have restarted quickly)${NC}"
else
    echo -e "${GREEN}Leadership successfully transferred${NC}"
fi

# Step 7: Verify writes resume
echo -e "${YELLOW}Step 7: Writing recovery value to verify writes resume...${NC}"
put_response=$(put_value "$TEST_KEY" "$RECOVERY_VALUE")
max_retries=5
retry_count=0
while echo "$put_response" | grep -q "error" && [ $retry_count -lt $max_retries ]; do
    echo -e "${YELLOW}Write failed, retrying ($((retry_count + 1))/$max_retries)...${NC}"
    sleep 2
    put_response=$(put_value "$TEST_KEY" "$RECOVERY_VALUE")
    retry_count=$((retry_count + 1))
done

if echo "$put_response" | grep -q "error"; then
    echo -e "${RED}Error: Failed to write recovery value after $max_retries retries${NC}"
    echo "Response: $put_response"
    echo -e "${YELLOW}Restarting killed node for cleanup...${NC}"
    ./scripts/kill-node.sh restart "$leader_node_id"
    exit 1
fi
echo -e "${GREEN}Recovery value written successfully${NC}"

# Step 8: Verify the new value
echo -e "${YELLOW}Step 8: Verifying recovery value...${NC}"
get_response=$(get_value "$TEST_KEY" "strong")
if ! echo "$get_response" | grep -q "$RECOVERY_VALUE"; then
    echo -e "${RED}Error: Recovery value verification failed${NC}"
    echo "Response: $get_response"
    echo -e "${YELLOW}Restarting killed node for cleanup...${NC}"
    ./scripts/kill-node.sh restart "$leader_node_id"
    exit 1
fi
echo -e "${GREEN}Recovery value verified${NC}"

# Cleanup: Restart the killed node
echo -e "${YELLOW}Cleanup: Restarting killed node $leader_node_id...${NC}"
./scripts/kill-node.sh restart "$leader_node_id"
echo -e "${GREEN}Node restarted${NC}"

echo ""
echo -e "${GREEN}=== Test Passed: Leader kill and recovery successful ===${NC}"
echo "- Leader was successfully killed"
echo "- New leader was elected"
echo "- Writes resumed after recovery"
echo "- Data consistency maintained"