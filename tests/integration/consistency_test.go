package integration

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsistencyLevels tests that STRONG and EVENTUAL reads work correctly
func TestConsistencyLevels(t *testing.T) {
	// This test requires the cluster to be running
	// Skip if router is not available
	routerAddr := "http://127.0.0.1:9000"
	if !isRouterAvailable(routerAddr) {
		t.Skip("Router not available - cluster not running")
	}

	testKey := "consistency_test_key"
	testValue := "consistency_test_value"

	// First, write a value using the existing httpPut function
	putResp, err := httpPut(routerAddr+"/kv/"+testKey, testValue)
	require.NoError(t, err, "PUT request should succeed")
	require.Equal(t, http.StatusOK, putResp.StatusCode, "PUT should return 200")

	var putResult map[string]string
	err = json.NewDecoder(putResp.Body).Decode(&putResult)
	putResp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, "committed", putResult["status"])

	// Test STRONG read
	strongResp, err := http.Get(routerAddr + "/kv/" + testKey + "?consistency=strong")
	require.NoError(t, err, "STRONG GET request should succeed")
	require.Equal(t, http.StatusOK, strongResp.StatusCode, "STRONG GET should return 200")

	var strongResult map[string]interface{}
	err = json.NewDecoder(strongResp.Body).Decode(&strongResult)
	strongResp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, "strong", strongResult["consistency"])
	assert.Equal(t, testValue, strongResult["value"])
	assert.Contains(t, strongResult, "applied_index")

	// Test EVENTUAL read
	eventualResp, err := http.Get(routerAddr + "/kv/" + testKey + "?consistency=eventual")
	require.NoError(t, err, "EVENTUAL GET request should succeed")
	require.Equal(t, http.StatusOK, eventualResp.StatusCode, "EVENTUAL GET should return 200")

	var eventualResult map[string]interface{}
	err = json.NewDecoder(eventualResp.Body).Decode(&eventualResult)
	eventualResp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, "eventual", eventualResult["consistency"])
	assert.Equal(t, testValue, eventualResult["value"])
	assert.Contains(t, eventualResult, "lag_index")
	assert.Contains(t, eventualResult, "applied_index")
	assert.NotContains(t, eventualResult, "lag_ms")

	// Test default (should be STRONG)
	defaultResp, err := http.Get(routerAddr + "/kv/" + testKey)
	require.NoError(t, err, "default GET request should succeed")
	require.Equal(t, http.StatusOK, defaultResp.StatusCode, "default GET should return 200")

	var defaultResult map[string]interface{}
	err = json.NewDecoder(defaultResp.Body).Decode(&defaultResult)
	defaultResp.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, "strong", defaultResult["consistency"], "default should be strong")
}

// TestInvalidConsistencyParameter tests that invalid consistency parameter is rejected
func TestInvalidConsistencyParameter(t *testing.T) {
	routerAddr := "http://127.0.0.1:9000"
	if !isRouterAvailable(routerAddr) {
		t.Skip("Router not available - cluster not running")
	}

	resp, err := http.Get(routerAddr + "/kv/test_key?consistency=invalid")
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()
}

// TestStrongReadLeadershipFailure tests that STRONG reads fail when node is not leader
func TestStrongReadLeadershipFailure(t *testing.T) {
	// This test would require manually killing the leader or simulating
	// a leadership failure. For now, we skip it as it requires complex setup.
	t.Skip("Requires manual leadership failure simulation")
}

// isRouterAvailable checks if the router is available
func isRouterAvailable(addr string) bool {
	client := &http.Client{Timeout: 1 * time.Second}
	resp, err := client.Get(addr + "/cluster/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
