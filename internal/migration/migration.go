package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Range represents a key range for migration
type Range struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// MigrationRequest represents a migration request
type MigrationRequest struct {
	FromShard string `json:"from_shard"`
	ToShard   string `json:"to_shard"`
	KeyRange  Range  `json:"key_range"`
}

// MigrationResult represents the result of a migration step
type MigrationResult struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	KeysCount int    `json:"keys_count,omitempty"`
}

// Client is an HTTP client for communicating with shard nodes
type Client struct {
	httpClient *http.Client
}

// NewClient creates a new migration client
func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Freeze sets a reject-writes flag on the shard leader for the given key range
func Freeze(shardLeaderAddr, shardID string, keyRange Range) error {
	client := NewClient()

	reqBody := map[string]interface{}{
		"shard_id":  shardID,
		"key_range": keyRange,
		"action":    "freeze",
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal freeze request: %w", err)
	}

	url := fmt.Sprintf("http://%s/admin/freeze", shardLeaderAddr)
	req, err := http.NewRequest("POST", url, bytes.NewReader(reqJSON))
	if err != nil {
		return fmt.Errorf("failed to create freeze request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send freeze request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("freeze request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Unfreeze removes the reject-writes flag on the shard leader for the given key range
func Unfreeze(shardLeaderAddr, shardID string, keyRange Range) error {
	client := NewClient()

	reqBody := map[string]interface{}{
		"shard_id":  shardID,
		"key_range": keyRange,
		"action":    "unfreeze",
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal unfreeze request: %w", err)
	}

	url := fmt.Sprintf("http://%s/admin/freeze", shardLeaderAddr)
	req, err := http.NewRequest("POST", url, bytes.NewReader(reqJSON))
	if err != nil {
		return fmt.Errorf("failed to create unfreeze request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send unfreeze request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unfreeze request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Copy reads keys from the source shard and applies them to the destination shard
func Copy(sourceShardLeaderAddr, destShardLeaderAddr, sourceShardID, destShardID string, keyRange Range) (int, error) {
	client := NewClient()

	// Step 1: Read all keys in the range from source shard
	url := fmt.Sprintf("http://%s/admin/scan?start=%s&end=%s", sourceShardLeaderAddr, keyRange.Start, keyRange.End)
	resp, err := client.httpClient.Get(url)
	if err != nil {
		return 0, fmt.Errorf("failed to scan source shard: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("scan request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var scanResult struct {
		Keys []struct {
			Key          string `json:"key"`
			Value        string `json:"value"`
			AppliedIndex uint64 `json:"applied_index"`
		} `json:"keys"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&scanResult); err != nil {
		return 0, fmt.Errorf("failed to decode scan result: %w", err)
	}

	// Step 2: Apply each key to the destination shard
	keysCopied := 0
	for _, keyData := range scanResult.Keys {
		// For each key, we need to apply it as a PUT operation to the destination shard
		// This goes through Raft consensus on the destination shard
		putURL := fmt.Sprintf("http://%s/kv/%s", destShardLeaderAddr, keyData.Key)
		putBody := map[string]string{
			"value": keyData.Value,
		}
		putJSON, err := json.Marshal(putBody)
		if err != nil {
			return keysCopied, fmt.Errorf("failed to marshal put request for key %s: %w", keyData.Key, err)
		}

		req, err := http.NewRequest("PUT", putURL, bytes.NewReader(putJSON))
		if err != nil {
			return keysCopied, fmt.Errorf("failed to create put request for key %s: %w", keyData.Key, err)
		}
		req.Header.Set("Content-Type", "application/json")

		putResp, err := client.httpClient.Do(req)
		if err != nil {
			return keysCopied, fmt.Errorf("failed to put key %s to destination shard: %w", keyData.Key, err)
		}
		putResp.Body.Close()

		if putResp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(putResp.Body)
			return keysCopied, fmt.Errorf("failed to put key %s to destination shard, status %d: %s", keyData.Key, putResp.StatusCode, string(body))
		}

		keysCopied++
	}

	return keysCopied, nil
}

// Verify compares data between source and destination shards for the given key range
func Verify(sourceShardLeaderAddr, destShardLeaderAddr string, keyRange Range) (bool, error) {
	client := NewClient()

	// Read keys from source shard
	sourceURL := fmt.Sprintf("http://%s/admin/scan?start=%s&end=%s", sourceShardLeaderAddr, keyRange.Start, keyRange.End)
	sourceResp, err := client.httpClient.Get(sourceURL)
	if err != nil {
		return false, fmt.Errorf("failed to scan source shard: %w", err)
	}
	defer sourceResp.Body.Close()

	if sourceResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(sourceResp.Body)
		return false, fmt.Errorf("source scan failed with status %d: %s", sourceResp.StatusCode, string(body))
	}

	var sourceResult struct {
		Keys []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"keys"`
	}

	if err := json.NewDecoder(sourceResp.Body).Decode(&sourceResult); err != nil {
		return false, fmt.Errorf("failed to decode source scan result: %w", err)
	}

	// Read keys from destination shard
	destURL := fmt.Sprintf("http://%s/admin/scan?start=%s&end=%s", destShardLeaderAddr, keyRange.Start, keyRange.End)
	destResp, err := client.httpClient.Get(destURL)
	if err != nil {
		return false, fmt.Errorf("failed to scan destination shard: %w", err)
	}
	defer destResp.Body.Close()

	if destResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(destResp.Body)
		return false, fmt.Errorf("destination scan failed with status %d: %s", destResp.StatusCode, string(body))
	}

	var destResult struct {
		Keys []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"keys"`
	}

	if err := json.NewDecoder(destResp.Body).Decode(&destResult); err != nil {
		return false, fmt.Errorf("failed to decode destination scan result: %w", err)
	}

	// Compare the key sets
	if len(sourceResult.Keys) != len(destResult.Keys) {
		return false, nil
	}

	// Create maps for comparison
	sourceMap := make(map[string]string)
	for _, key := range sourceResult.Keys {
		sourceMap[key.Key] = key.Value
	}

	destMap := make(map[string]string)
	for _, key := range destResult.Keys {
		destMap[key.Key] = key.Value
	}

	// Compare each key-value pair
	for key, value := range sourceMap {
		if destMap[key] != value {
			return false, nil
		}
	}

	return true, nil
}

// Switch updates the router's ring configuration to route the key range to the new shard
// This is a placeholder - the actual implementation will depend on the router's API
func Switch(routerAddr, fromShard, toShard string, keyRange Range) error {
	client := NewClient()

	reqBody := map[string]interface{}{
		"from_shard": fromShard,
		"to_shard":   toShard,
		"key_range":  keyRange,
		"mode":       "switch",
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal switch request: %w", err)
	}

	url := fmt.Sprintf("http://%s/admin/routing", routerAddr)
	req, err := http.NewRequest("POST", url, bytes.NewReader(reqJSON))
	if err != nil {
		return fmt.Errorf("failed to create switch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send switch request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("switch request failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// OrchestrateMigration performs the full migration workflow: freeze → copy → verify → switch → unfreeze
func OrchestrateMigration(ctx context.Context, routerAddr, sourceShardLeaderAddr, destShardLeaderAddr, sourceShardID, destShardID string, keyRange Range) error {
	// Step 1: Freeze the source shard
	if err := Freeze(sourceShardLeaderAddr, sourceShardID, keyRange); err != nil {
		return fmt.Errorf("freeze failed: %w", err)
	}

	// Ensure unfreeze happens even if later steps fail
	defer func() {
		Unfreeze(sourceShardLeaderAddr, sourceShardID, keyRange)
	}()

	// Step 2: Copy data from source to destination
	keysCopied, err := Copy(sourceShardLeaderAddr, destShardLeaderAddr, sourceShardID, destShardID, keyRange)
	if err != nil {
		return fmt.Errorf("copy failed: %w", err)
	}
	fmt.Printf("Copied %d keys from %s to %s\n", keysCopied, sourceShardID, destShardID)

	// Step 3: Verify data integrity
	verified, err := Verify(sourceShardLeaderAddr, destShardLeaderAddr, keyRange)
	if err != nil {
		return fmt.Errorf("verify failed: %w", err)
	}
	if !verified {
		return fmt.Errorf("verification failed: data mismatch between source and destination")
	}
	fmt.Println("Verification successful: data matches")

	// Step 4: Switch routing to the new shard
	if err := Switch(routerAddr, sourceShardID, destShardID, keyRange); err != nil {
		return fmt.Errorf("switch failed: %w", err)
	}
	fmt.Printf("Routing switched from %s to %s for key range\n", sourceShardID, destShardID)

	// Step 5: Unfreeze the source shard
	if err := Unfreeze(sourceShardLeaderAddr, sourceShardID, keyRange); err != nil {
		return fmt.Errorf("unfreeze failed: %w", err)
	}
	fmt.Println("Unfreeze successful")

	return nil
}
