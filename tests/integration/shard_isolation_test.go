package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardIsolation verifies that killing one shard's leader doesn't affect other shards
// This test validates Phase 2 exit criteria: "Killing one shard's leader doesn't affect the other two shards' availability"
func TestShardIsolation(t *testing.T) {
	// Skip if cluster is not running
	if !isClusterRunning() {
		t.Skip("Cluster not running, skipping isolation test")
	}

	// Get cluster topology to build address-to-node mappings
	shardsResp, err := httpGet("http://127.0.0.1:9000/cluster/shards")
	require.NoError(t, err, "Failed to get shard topology")
	require.Equal(t, http.StatusOK, shardsResp.StatusCode)

	var shardsData struct {
		Shards []struct {
			ID         string   `json:"id"`
			LeaderAddr string   `json:"leader_addr"`
			Nodes      []string `json:"nodes"`
		} `json:"shards"`
	}
	err = json.NewDecoder(shardsResp.Body).Decode(&shardsData)
	require.NoError(t, err, "Failed to decode shard topology")
	require.Equal(t, 3, len(shardsData.Shards), "Should have 3 shards")

	// Get current cluster health to identify a leader to kill
	healthResp, err := httpGet("http://127.0.0.1:9000/cluster/health")
	require.NoError(t, err, "Failed to get cluster health")
	defer healthResp.Body.Close()

	var healthData map[string]interface{}
	err = json.NewDecoder(healthResp.Body).Decode(&healthData)
	require.NoError(t, err, "Failed to decode health data")

	// Find a healthy shard to test isolation (preferably shard A)
	var targetShard string
	var targetLeader string
	for shardID, shardHealth := range healthData {
		if shardHealthMap, ok := shardHealth.(map[string]interface{}); ok {
			if healthy, ok := shardHealthMap["healthy"].(bool); ok && healthy {
				if leaderAddr, ok := shardHealthMap["leader_addr"].(string); ok && leaderAddr != "" {
					// Use shard A for isolation test
					if shardID == "A" {
						targetShard = shardID
						targetLeader = leaderAddr
						break
					}
					// Fall back to first healthy shard if A not available
					if targetShard == "" {
						targetShard = shardID
						targetLeader = leaderAddr
					}
				}
			}
		}
	}

	require.NotEmpty(t, targetShard, "Should find a healthy shard to test isolation")
	require.NotEmpty(t, targetLeader, "Should find a leader to kill")

	t.Logf("Testing isolation by killing leader of shard %s at %s", targetShard, targetLeader)

	// Find the PID file for the leader by checking all node PID files
	var pidFile string
	var foundPID int

	// List all possible node PID files
	for _, shardID := range []string{"A", "B", "C"} {
		for i := 1; i <= 3; i++ {
			nodeID := fmt.Sprintf("%s%d", shardID, i)
			candidatePIDFile := fmt.Sprintf("/tmp/shardkv/%s.pid", nodeID)

			pidBytes, err := os.ReadFile(candidatePIDFile)
			if err != nil {
				continue // PID file doesn't exist
			}

			pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
			if err != nil {
				continue
			}

			// Check if this node's HTTP address matches the leader address
			for _, shard := range shardsData.Shards {
				if shard.ID == shardID {
				// Nodes slice order matches the node index from run-cluster.sh
				// (node 1 → Nodes[0], node 2 → Nodes[1], etc.), which in turn
				// matches the PID file names (A1, A2, A3). This is an explicit
				// dependency on how run-cluster.sh builds the shard config —
				// it appends nodes in ascending node_idx order, so Nodes[i-1]
				// is always the HTTP address of node {SHARD_ID}{i}.
				if i-1 < len(shard.Nodes) {
					nodeAddr := shard.Nodes[i-1]
					if nodeAddr == targetLeader {
							pidFile = candidatePIDFile
							foundPID = pid
							t.Logf("Found leader process: node %s at %s has PID %d", nodeID, nodeAddr, pid)
							break
						}
					}
				}
			}

			if pidFile != "" {
				break
			}
		}
		if pidFile != "" {
			break
		}
	}

	require.NotEmpty(t, pidFile, "Could not find PID file for leader address %s", targetLeader)
	require.NotZero(t, foundPID, "PID should not be zero")

	t.Logf("Killing leader process (PID: %d) from file %s", foundPID, pidFile)

	// Verify the leader HTTP endpoint is actually responding before killing
	resp, err := httpGet(fmt.Sprintf("http://%s/cluster/health", targetLeader))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Leader at %s is not responding, cannot proceed with isolation test", targetLeader)
	}
	resp.Body.Close()

	// Try to kill the process
	err = killProcess(foundPID)
	if err != nil {
		t.Skipf("Failed to kill process %d: %v. Skipping isolation test (may need elevated permissions)", foundPID, err)
		return
	}

	t.Logf("Successfully killed leader process %d", foundPID)

	// Wait for leadership change
	time.Sleep(5 * time.Second)

	// Verify the killed process is no longer running
	proc, err := os.FindProcess(foundPID)
	if err == nil {
		// Check if process is still alive using signal 0
		err = proc.Signal(syscall.Signal(0))
		if err == nil {
			t.Fatalf("Process %d is still running after kill attempt", foundPID)
		}
	}

	// Wait a bit more for leader election to complete
	time.Sleep(3 * time.Second)

	// Verify a new leader was elected for the target shard
	healthResp2, err := httpGet("http://127.0.0.1:9000/cluster/health")
	require.NoError(t, err, "Failed to get cluster health after isolation")
	defer healthResp2.Body.Close()

	var healthData2 map[string]interface{}
	err = json.NewDecoder(healthResp2.Body).Decode(&healthData2)
	require.NoError(t, err, "Failed to decode health data after isolation")

	if targetShardHealth, ok := healthData2[targetShard].(map[string]interface{}); ok {
		if healthy, ok := targetShardHealth["healthy"].(bool); ok {
			assert.True(t, healthy, "Shard %s should be healthy after leader failover", targetShard)
		}
		if newLeaderAddr, ok := targetShardHealth["leader_addr"].(string); ok {
			t.Logf("Shard %s has leader at %s after failover", targetShard, newLeaderAddr)
			assert.NotEmpty(t, newLeaderAddr, "Shard %s should have a leader after failover", targetShard)
		}
	}

	// Verify other shards are still available by checking their health
	for shardID, shardHealth := range healthData2 {
		if shardID == targetShard {
			continue
		}
		if shardHealthMap, ok := shardHealth.(map[string]interface{}); ok {
			if healthy, ok := shardHealthMap["healthy"].(bool); ok {
				assert.True(t, healthy, "Shard %s should still be healthy after isolation", shardID)
			}
		}
	}

	t.Logf("Shard isolation test passed: killed leader of shard %s at %s, other shards remained available", targetShard, targetLeader)
}

func killProcess(pid int) error {
	// Try graceful shutdown first
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	err = proc.Signal(os.Interrupt)
	if err == nil {
		// Wait a bit for graceful shutdown
		time.Sleep(2 * time.Second)
	}

	// Force kill if still running
	err = proc.Kill()
	if err != nil && err.Error() == "os: process already finished" {
		// Process already died from graceful shutdown, that's fine
		return nil
	}
	return err
}
