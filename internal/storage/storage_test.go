package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStorage(t *testing.T) *Storage {
	tmpFile, err := os.CreateTemp("", "kv-test-*.db")
	require.NoError(t, err)
	tmpFile.Close()

	store, err := New(tmpFile.Name())
	require.NoError(t, err)

	t.Cleanup(func() {
		store.Close()
		os.Remove(tmpFile.Name())
	})

	return store
}

func TestStorage_ScanRange(t *testing.T) {
	store := setupTestStorage(t)

	// Add test data
	testData := []struct {
		key   string
		value string
	}{
		{"a", "value-a"},
		{"b", "value-b"},
		{"c", "value-c"},
		{"d", "value-d"},
		{"e", "value-e"},
	}

	for i, data := range testData {
		err := store.Put(data.key, data.value, uint64(i+1))
		require.NoError(t, err)
	}

	// Test full range scan
	results, err := store.ScanRange("", "")
	require.NoError(t, err)
	assert.Len(t, results, 5)

	// Test range scan with start
	results, err = store.ScanRange("b", "")
	require.NoError(t, err)
	assert.Len(t, results, 4) // b, c, d, e

	// Test range scan with end
	results, err = store.ScanRange("", "c")
	require.NoError(t, err)
	assert.Len(t, results, 3) // a, b, c

	// Test range scan with both start and end
	results, err = store.ScanRange("b", "d")
	require.NoError(t, err)
	assert.Len(t, results, 3) // b, c, d

	// Verify the values
	keys := make([]string, len(results))
	for i, result := range results {
		keys[i] = result.Key
	}
	assert.Equal(t, []string{"b", "c", "d"}, keys)
}

func TestStorage_ScanRangeEmpty(t *testing.T) {
	store := setupTestStorage(t)

	// Add test data
	err := store.Put("test-key", "test-value", 1)
	require.NoError(t, err)

	// Test scan with non-existent range
	results, err := store.ScanRange("x", "z")
	require.NoError(t, err)
	assert.Len(t, results, 0)
}