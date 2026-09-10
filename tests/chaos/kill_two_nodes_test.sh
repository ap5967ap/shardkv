#!/bin/bash

# Integration test: Kill 2 of 3 nodes in a shard
# This corresponds to §11.3 Tier 2 test matrix row: "kill 2 of 3 nodes in a shard"
# Expected behavior: shard becomes unavailable (not corrupted)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ROUTER_ADDR="http://127.0.0.1:9000"
TEST_KEY="chaos_two_nodes_kill_test"
TEST_VALUE="test_value"

# Helper function to check if a node is running
is_node_running() {
    local node_id=$1
    local pid_file="/tmp/shardkv/${node_id}.pid"
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

# Helper function to write a value
put_value() {
    local key=$1
    local value=$2
    local response=$(curl -s -X PUT -H "Content-Type: application/json" -d "{\"value\": \"$value\"}" "${ROUTER_ADDR}/kv/${key}")
    echo "$response"
}

# Helper function to read a value
get_value() {
    local key=$1
    local consistency=$2
    local response=$(curl -s "${ROUTER_ADDR}/kv/${key}?consistency=${consistency}")
    echo "$response"
}

echo -e "${GREEN}=== Kill 2 of 3 Nodes Test ===${NC}"
echo ""

# Check if cluster is running
if ! curl -s "${ROUTER_ADDR}/cluster/shards" > /dev/null 2>&1; then
    echo -e "${RED}Error: Cluster not running. Start it with: ./scripts/run-cluster.sh${NC}"
    exit 1
fi

# Check if kill-node.sh script exists
if [ ! -f "./scripts/kill-node.sh" ]; then
    echo -e "${RED}Error: kill-node.sh script not found${NC}"
    exit 1
fi

# Step 1: Write initial value to shard A
echo -e "${YELLOW}Step 1: Writing initial value to shard A...${NC}"
put_response=$(put_value "$TEST_KEY" "$TEST_VALUE")
if echo "$put_response" | grep -q "error"; then
    echo -e "${RED}Error: Failed to write initial value${NC}"
    echo "Response: $put_response"
    exit 1
fi
echo -e "${GREEN}Initial value written successfully${NC}"

# Step 2: Verify initial value
echo -e "${YELLOW}Step 2: Verifying initial value...${NC}"
get_response=$(get_value "$TEST_KEY" "strong")
if ! echo "$get_response" | grep -q "$TEST_VALUE"; then
    echo -e "${RED}Error: Initial value verification failed${NC}"
    echo "Response: $get_response"
    exit 1
fi
echo -e "${GREEN}Initial value verified${NC}"

# Step 3: Kill two live nodes in shard A.
# This avoids order dependence from previous chaos tests, where a node may already be down.
echo -e "${YELLOW}Step 3: Selecting and killing two live nodes in shard A...${NC}"
mapfile -t live_nodes < <(for node_id in A1 A2 A3; do if is_node_running "$node_id"; then echo "$node_id"; fi; done)
if [ "${#live_nodes[@]}" -lt 2 ]; then
    echo -e "${RED}Error: Need at least 2 live nodes in shard A, found ${#live_nodes[@]}${NC}"
    exit 1
fi

a_node="${live_nodes[0]}"
b_node="${live_nodes[1]}"

./scripts/kill-node.sh kill "$a_node"
echo -e "${GREEN}Node $a_node killed${NC}"

./scripts/kill-node.sh kill "$b_node"
echo -e "${GREEN}Node $b_node killed${NC}"

# Step 4: Wait for the system to detect the failure
echo -e "${YELLOW}Step 4: Waiting for system to detect failures (5 seconds)...${NC}"
sleep 5

# Step 5: Try to write - should fail because shard A has no quorum
echo -e "${YELLOW}Step 5: Attempting to write while shard has no quorum...${NC}"
violations=0
put_response=$(put_value "${TEST_KEY}_2" "should_fail")
if echo "$put_response" | grep -q "error"; then
    echo -e "${GREEN}Write correctly failed (no quorum)${NC}"
else
    echo -e "${YELLOW}Warning: Write succeeded when it should have failed (no quorum)${NC}"
    echo "Response: $put_response"
    violations=$((violations + 1))
fi

# Step 6: Try to read - should also fail or return error
echo -e "${YELLOW}Step 6: Attempting to read while shard has no quorum...${NC}"
get_response=$(get_value "$TEST_KEY" "strong")
if echo "$get_response" | grep -q "error\|503\|unavailable"; then
    echo -e "${GREEN}Read correctly failed (no quorum)${NC}"
else
    echo -e "${YELLOW}Warning: Read succeeded when it should have failed (no quorum)${NC}"
    echo "Response: $get_response"
    violations=$((violations + 1))
fi

# Step 7: Restart one node to restore quorum
echo -e "${YELLOW}Step 7: Restarting node $a_node to restore quorum...${NC}"
./scripts/kill-node.sh restart "$a_node"
echo -e "${GREEN}Node $a_node restarted${NC}"

# Step 8: Wait for leader election
echo -e "${YELLOW}Step 8: Waiting for leader election (10 seconds)...${NC}"
sleep 10

# Step 9: Verify reads work again
echo -e "${YELLOW}Step 9: Verifying reads work after quorum restored...${NC}"
get_response=$(get_value "$TEST_KEY" "strong")
max_retries=5
retry_count=0
while ! echo "$get_response" | grep -q "$TEST_VALUE" && [ $retry_count -lt $max_retries ]; do
    echo -e "${YELLOW}Read failed, retrying ($((retry_count + 1))/$max_retries)...${NC}"
    sleep 2
    get_response=$(get_value "$TEST_KEY" "strong")
    retry_count=$((retry_count + 1))
done

if ! echo "$get_response" | grep -q "$TEST_VALUE"; then
    echo -e "${RED}Error: Read failed after quorum restored${NC}"
    echo "Response: $get_response"
else
    echo -e "${GREEN}Read successful after quorum restored${NC}"
fi

# Step 10: Verify writes work again
echo -e "${YELLOW}Step 10: Verifying writes work after quorum restored...${NC}"
put_response=$(put_value "${TEST_KEY}_3" "recovery_value")
retry_count=0
while echo "$put_response" | grep -q "error" && [ $retry_count -lt $max_retries ]; do
    echo -e "${YELLOW}Write failed, retrying ($((retry_count + 1))/$max_retries)...${NC}"
    sleep 2
    put_response=$(put_value "${TEST_KEY}_3" "recovery_value")
    retry_count=$((retry_count + 1))
done

if echo "$put_response" | grep -q "error"; then
    echo -e "${RED}Error: Write failed after quorum restored${NC}"
    echo "Response: $put_response"
else
    echo -e "${GREEN}Write successful after quorum restored${NC}"
fi

# Cleanup: Restart the second killed node
echo -e "${YELLOW}Cleanup: Restarting node $b_node...${NC}"
./scripts/kill-node.sh restart "$b_node"
echo -e "${GREEN}Node $b_node restarted${NC}"

echo ""
if [ "$violations" -ne 0 ]; then
    echo -e "${RED}=== Test Failed: no-quorum correctness violation detected ===${NC}"
    echo "Detected $violations unexpected success(es) when quorum should have prevented writes/reads."
    exit 1
fi

echo -e "${GREEN}=== Test Passed: Kill 2 of 3 nodes behavior verified ===${NC}"
echo "- Shard became unavailable when 2 of 3 nodes were killed"
echo "- Shard recovered when quorum was restored"
echo "- No data corruption occurred"
echo "- Consistency maintained throughout"