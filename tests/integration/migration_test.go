package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMigrationFreeze tests that freeze/unfreeze works correctly
func resolveLeaderAddr(t *testing.T, shardID string) string {
	t.Helper()
	resp, err := http.Get("http://127.0.0.1:9000/cluster/shards")
	if err != nil {
		t.Skip("Cluster not running, skipping migration test")
	}
	defer resp.Body.Close()

	var payload struct {
		Shards []struct {
			ID         string `json:"id"`
			LeaderAddr string `json:"leader_addr"`
		} `json:"shards"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("failed to decode shard map: %v", err)
	}
	for _, shard := range payload.Shards {
		if shard.ID == shardID {
			if shard.LeaderAddr == "" {
				t.Fatalf("no leader discovered for shard %s", shardID)
			}
			return shard.LeaderAddr
		}
	}
	t.Fatalf("shard %s not found in router config", shardID)
	return ""
}

func TestMigrationFreeze(t *testing.T) {
	shardALeader := resolveLeaderAddr(t, "A")

	// Check if cluster is running
	healthResp, err := http.Get(fmt.Sprintf("http://%s/cluster/health", shardALeader))
	if err != nil {
		t.Skip("Cluster not running, skipping migration freeze test")
	}
	healthResp.Body.Close()

	// Test data
	testKey := "migration-test-key"
	testValue := "migration-test-value"

	// First, put a value to ensure the shard is working
	putReq := map[string]string{"value": testValue}
	putJSON, _ := json.Marshal(putReq)
	resp, err := http.Post(fmt.Sprintf("http://%s/kv/%s", shardALeader, testKey), "application/json", bytes.NewReader(putJSON))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Freeze a key range
	freezeReq := map[string]interface{}{
		"shard_id": "A",
		"key_range": map[string]string{
			"start": "migration-test-key",
			"end":   "migration-test-key-zzz",
		},
		"action": "freeze",
	}
	freezeJSON, _ := json.Marshal(freezeReq)
	resp, err = http.Post(fmt.Sprintf("http://%s/admin/freeze", shardALeader), "application/json", bytes.NewReader(freezeJSON))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Try to write to a frozen key - should fail with 503
	putReq2 := map[string]string{"value": "new-value"}
	putJSON2, _ := json.Marshal(putReq2)
	resp, err = http.Post(fmt.Sprintf("http://%s/kv/%s", shardALeader, testKey), "application/json", bytes.NewReader(putJSON2))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	// Verify the original value is still there
	resp, err = http.Get(fmt.Sprintf("http://%s/kv/%s?consistency=strong", shardALeader, testKey))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var getResp map[string]interface{}
	body, _ := io.ReadAll(resp.Body)
	json.Unmarshal(body, &getResp)
	assert.Equal(t, testValue, getResp["value"])

	// Unfreeze the range
	unfreezeReq := map[string]interface{}{
		"shard_id": "A",
		"key_range": map[string]string{
			"start": "migration-test-key",
			"end":   "migration-test-key-zzz",
		},
		"action": "unfreeze",
	}
	unfreezeJSON, _ := json.Marshal(unfreezeReq)
	resp, err = http.Post(fmt.Sprintf("http://%s/admin/freeze", shardALeader), "application/json", bytes.NewReader(unfreezeJSON))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Now the write should succeed
	putReq3 := map[string]string{"value": "new-value-after-unfreeze"}
	putJSON3, _ := json.Marshal(putReq3)
	resp, err = http.Post(fmt.Sprintf("http://%s/kv/%s", shardALeader, testKey), "application/json", bytes.NewReader(putJSON3))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Cleanup
	deleteReq, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/kv/%s", shardALeader, testKey), nil)
	resp, err = http.DefaultClient.Do(deleteReq)
	require.NoError(t, err)
	resp.Body.Close()
}

// TestMigrationScan tests the scan functionality
func TestMigrationScan(t *testing.T) {
	shardALeader := resolveLeaderAddr(t, "A")

	// Check if cluster is running
	healthResp, err := http.Get(fmt.Sprintf("http://%s/cluster/health", shardALeader))
	if err != nil {
		t.Skip("Cluster not running, skipping migration scan test")
	}
	healthResp.Body.Close()

	// Setup test data
	testKeys := []string{"scan-test-1", "scan-test-2", "scan-test-3"}

	for _, key := range testKeys {
		putReq := map[string]string{"value": fmt.Sprintf("value-%s", key)}
		putJSON, _ := json.Marshal(putReq)
		resp, err := http.Post(fmt.Sprintf("http://%s/kv/%s", shardALeader, key), "application/json", bytes.NewReader(putJSON))
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// Scan the range
	resp, err := http.Get(fmt.Sprintf("http://%s/admin/scan?start=scan-test-1&end=scan-test-3", shardALeader))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var scanResp map[string]interface{}
	body, _ := io.ReadAll(resp.Body)
	json.Unmarshal(body, &scanResp)

	keys, ok := scanResp["keys"].([]interface{})
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(keys), 3)

	// Cleanup
	for _, key := range testKeys {
		deleteReq, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/kv/%s", shardALeader, key), nil)
		resp, err := http.DefaultClient.Do(deleteReq)
		require.NoError(t, err)
		resp.Body.Close()
	}
}

// TestMigrationWorkflow tests the full migration workflow
func TestMigrationWorkflow(t *testing.T) {
	shardALeader := resolveLeaderAddr(t, "A")
	routerAddr := "127.0.0.1:9000"

	// Check if cluster is running
	healthResp, err := http.Get(fmt.Sprintf("http://%s/cluster/health", shardALeader))
	if err != nil {
		t.Skip("Cluster not running, skipping migration workflow test")
	}
	healthResp.Body.Close()

	// This is a simplified test that just tests the components
	// A full end-to-end migration test would require more complex setup

	// Test that we can freeze shard A
	freezeReq := map[string]interface{}{
		"shard_id": "A",
		"key_range": map[string]string{
			"start": "migration-workflow-test",
			"end":   "migration-workflow-test-zzz",
		},
		"action": "freeze",
	}
	freezeJSON, _ := json.Marshal(freezeReq)
	resp, err := http.Post(fmt.Sprintf("http://%s/admin/freeze", shardALeader), "application/json", bytes.NewReader(freezeJSON))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Test that we can scan shard A
	resp, err = http.Get(fmt.Sprintf("http://%s/admin/scan?start=migration-workflow-test&end=migration-workflow-test-zzz", shardALeader))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Test that we can update router configuration
	migrationReq := map[string]interface{}{
		"from_shard": "A",
		"to_shard":   "B",
		"key_range": map[string]string{
			"start": "migration-workflow-test",
			"end":   "migration-workflow-test-zzz",
		},
	}
	migrationJSON, _ := json.Marshal(migrationReq)
	resp, err = http.Post(fmt.Sprintf("http://%s/admin/migrate", routerAddr), "application/json", bytes.NewReader(migrationJSON))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Unfreeze shard A
	unfreezeReq := map[string]interface{}{
		"shard_id": "A",
		"key_range": map[string]string{
			"start": "migration-workflow-test",
			"end":   "migration-workflow-test-zzz",
		},
		"action": "unfreeze",
	}
	unfreezeJSON, _ := json.Marshal(unfreezeReq)
	resp, err = http.Post(fmt.Sprintf("http://%s/admin/freeze", shardALeader), "application/json", bytes.NewReader(unfreezeJSON))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestRaceConditionWriteDuringFreeze tests the race condition from §8.1
// This test ensures that a write attempted during the freeze window is rejected with 503
func TestRaceConditionWriteDuringFreeze(t *testing.T) {
	shardALeader := resolveLeaderAddr(t, "A")

	// Check if cluster is running
	healthResp, err := http.Get(fmt.Sprintf("http://%s/cluster/health", shardALeader))
	if err != nil {
		t.Skip("Cluster not running, skipping race condition test")
	}
	healthResp.Body.Close()

	testKey := "race-condition-test-key"

	// Setup: put initial value
	putReq := map[string]string{"value": "initial-value"}
	putJSON, _ := json.Marshal(putReq)
	resp, err := http.Post(fmt.Sprintf("http://%s/kv/%s", shardALeader, testKey), "application/json", bytes.NewReader(putJSON))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Freeze the key range
	freezeReq := map[string]interface{}{
		"shard_id": "A",
		"key_range": map[string]string{
			"start": testKey,
			"end":   testKey + "-zzz",
		},
		"action": "freeze",
	}
	freezeJSON, _ := json.Marshal(freezeReq)
	resp, err = http.Post(fmt.Sprintf("http://%s/admin/freeze", shardALeader), "application/json", bytes.NewReader(freezeJSON))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Simulate the race condition: a write lands during the freeze window
	// This should be rejected with 503, not silently lost
	go func() {
		time.Sleep(100 * time.Millisecond) // Simulate concurrent write
		putReq := map[string]string{"value": "concurrent-write-value"}
		putJSON, _ := json.Marshal(putReq)
		resp, err := http.Post(fmt.Sprintf("http://%s/kv/%s", shardALeader, testKey), "application/json", bytes.NewReader(putJSON))
		if err == nil {
			defer resp.Body.Close()
			// This should fail with 503
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Errorf("Expected 503 for write during freeze, got %d", resp.StatusCode)
			}
		}
	}()

	// Give the goroutine time to run
	time.Sleep(200 * time.Millisecond)

	// Verify the original value is still there (not silently lost or overwritten)
	resp, err = http.Get(fmt.Sprintf("http://%s/kv/%s?consistency=strong", shardALeader, testKey))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var getResp map[string]interface{}
	body, _ := io.ReadAll(resp.Body)
	json.Unmarshal(body, &getResp)
	assert.Equal(t, "initial-value", getResp["value"], "Original value should be preserved during freeze")

	// Unfreeze
	unfreezeReq := map[string]interface{}{
		"shard_id": "A",
		"key_range": map[string]string{
			"start": testKey,
			"end":   testKey + "-zzz",
		},
		"action": "unfreeze",
	}
	unfreezeJSON, _ := json.Marshal(unfreezeReq)
	resp, err = http.Post(fmt.Sprintf("http://%s/admin/freeze", shardALeader), "application/json", bytes.NewReader(unfreezeJSON))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Cleanup
	deleteReq, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/kv/%s", shardALeader, testKey), nil)
	resp, err = http.DefaultClient.Do(deleteReq)
	require.NoError(t, err)
	resp.Body.Close()
}
