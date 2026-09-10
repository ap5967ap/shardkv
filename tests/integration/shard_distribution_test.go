package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"shardkv/internal/routing"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardDistribution verifies that keys are distributed across all shards
// This test validates Phase 2 exit criteria: "Router correctly routes a batch of test keys across all 3 shards"
func TestShardDistribution(t *testing.T) {
	// Skip if cluster is not running
	if !isClusterRunning() {
		t.Skip("Cluster not running, skipping distribution test")
	}

	// Test a batch of keys that should distribute across shards
	testKeys := []string{
		"alpha", "beta", "gamma", "delta", "epsilon",
		"zeta", "eta", "theta", "iota", "kappa",
		"lambda", "mu", "nu", "xi", "omicron",
		"pi", "rho", "sigma", "tau", "upsilon",
		"phi", "chi", "psi", "omega",
	}

	// Store keys through router
	for i, key := range testKeys {
		value := fmt.Sprintf("value%d", i)
		resp, err := httpPut("http://127.0.0.1:9000/kv/"+key, value)
		require.NoError(t, err, "Failed to PUT key %s", key)
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "PUT should succeed for key %s", key)
	}

	// Give some time for replication
	time.Sleep(2 * time.Second)

	// Verify we can retrieve all keys through router
	for _, key := range testKeys {
		resp, err := httpGet("http://127.0.0.1:9000/kv/" + key)
		require.NoError(t, err, "Failed to GET key %s", key)
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "GET should succeed for key %s", key)
	}

	// Get shard information from router
	shardsResp, err := httpGet("http://127.0.0.1:9000/cluster/shards")
	require.NoError(t, err, "Failed to get shard information")
	require.Equal(t, http.StatusOK, shardsResp.StatusCode)
	defer shardsResp.Body.Close()

	var shardsData struct {
		Shards []struct {
			ID         string   `json:"id"`
			LeaderAddr string   `json:"leader_addr"`
			Nodes      []string `json:"nodes"`
		} `json:"shards"`
	}
	err = json.NewDecoder(shardsResp.Body).Decode(&shardsData)
	require.NoError(t, err, "Failed to decode shard information")
	require.Equal(t, 3, len(shardsData.Shards), "Should have 3 shards")

	// Build a map of shard ID to leader address
	shardLeaders := make(map[string]string)
	for _, shard := range shardsData.Shards {
		if shard.LeaderAddr != "" {
			shardLeaders[shard.ID] = shard.LeaderAddr
		}
	}

	// Create a ring with the same virtual node count as the router
	// Use the same default as router/main.go (100 virtual nodes)
	ring := routing.NewRing(100)
	for shardID := range shardLeaders {
		ring.AddShard(shardID)
	}

	// Track which keys were assigned to which shard
	keysByShard := make(map[string][]string)
	for _, key := range testKeys {
		expectedShard := ring.ShardFor(key)
		require.NotEmpty(t, expectedShard, "Key %s should map to a shard", key)
		keysByShard[expectedShard] = append(keysByShard[expectedShard], key)
	}

	// Verify each key is stored on its expected shard
	for shardID, keys := range keysByShard {
		leaderAddr, ok := shardLeaders[shardID]
		require.True(t, ok, "Shard %s should have a leader", shardID)

		// Verify the leader is actually responding
		healthResp, err := httpGet(fmt.Sprintf("http://%s/cluster/health", leaderAddr))
		require.NoError(t, err, "Leader %s for shard %s should be responding", leaderAddr, shardID)
		require.Equal(t, http.StatusOK, healthResp.StatusCode, "Leader %s for shard %s should return healthy", leaderAddr, shardID)
		defer healthResp.Body.Close()

		for _, key := range keys {
			// Verify the key exists on the shard's leader directly
			resp, err := httpGet(fmt.Sprintf("http://%s/kv/%s", leaderAddr, key))
			require.NoError(t, err, "Failed to GET key %s from shard %s leader at %s", key, shardID, leaderAddr)
			resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode, "Key %s should exist on shard %s", key, shardID)
		}
	}

	// Verify all three shards received keys
	assert.NotEmpty(t, keysByShard["A"], "Shard A should have received keys")
	assert.NotEmpty(t, keysByShard["B"], "Shard B should have received keys")
	assert.NotEmpty(t, keysByShard["C"], "Shard C should have received keys")

	t.Logf("Shard distribution test passed: %d keys distributed across 3 shards (A: %d, B: %d, C: %d)",
		len(testKeys), len(keysByShard["A"]), len(keysByShard["B"]), len(keysByShard["C"]))
}

// isClusterRunning checks that the router is up AND that every shard has an
// elected leader. The router's /cluster/health always returns 200 (it just
// reflects cached state), so we use /cluster/shards which exposes leader_addr.
func isClusterRunning() bool {
	resp, err := httpGet("http://127.0.0.1:9000/cluster/shards")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}

	var shardsData struct {
		Shards []struct {
			ID         string `json:"id"`
			LeaderAddr string `json:"leader_addr"`
		} `json:"shards"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&shardsData); err != nil {
		return false
	}

	// We expect at least 3 shards, each with a known leader.
	if len(shardsData.Shards) < 3 {
		return false
	}
	for _, shard := range shardsData.Shards {
		if shard.LeaderAddr == "" {
			return false
		}
	}
	return true
}

func httpPut(url, value string) (*http.Response, error) {
	body := fmt.Sprintf(`{"value":"%s"}`, value)
	req, err := http.NewRequest("PUT", url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	return client.Do(req)
}

func httpGet(url string) (*http.Response, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	return client.Get(url)
}
