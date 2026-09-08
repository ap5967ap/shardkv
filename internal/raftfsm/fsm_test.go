package raftfsm

import (
	"encoding/json"
	"io"
	"path/filepath"
	"testing"

	"shardkv/internal/storage"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStorage(t *testing.T) *storage.Storage {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	store, err := storage.New(dbPath)
	require.NoError(t, err)
	return store
}

func TestFSM_Apply_PUT(t *testing.T) {
	store := setupTestStorage(t)
	defer store.Close()

	fsm := NewKVFSM(store)

	op := Operation{
		Op:    "PUT",
		Key:   "test-key",
		Value: "test-value",
	}

	data, err := json.Marshal(op)
	require.NoError(t, err)

	log := &raft.Log{
		Index: 1,
		Data:  data,
	}

	result := fsm.Apply(log)
	require.NotNil(t, result)

	applyResult, ok := result.(*ApplyResult)
	require.True(t, ok)
	assert.NoError(t, applyResult.Error)

	// Verify the value was stored
	value, appliedIndex, err := store.Get("test-key")
	require.NoError(t, err)
	assert.Equal(t, "test-value", value)
	assert.Equal(t, uint64(1), appliedIndex)
}

func TestFSM_Apply_DELETE(t *testing.T) {
	store := setupTestStorage(t)
	defer store.Close()

	fsm := NewKVFSM(store)

	// First put a value
	putOp := Operation{
		Op:    "PUT",
		Key:   "test-key",
		Value: "test-value",
	}
	putData, _ := json.Marshal(putOp)
	putLog := &raft.Log{Index: 1, Data: putData}
	fsm.Apply(putLog)

	// Then delete it
	deleteOp := Operation{
		Op:  "DELETE",
		Key: "test-key",
	}
	deleteData, _ := json.Marshal(deleteOp)
	deleteLog := &raft.Log{Index: 2, Data: deleteData}
	result := fsm.Apply(deleteLog)

	applyResult, ok := result.(*ApplyResult)
	require.True(t, ok)
	assert.NoError(t, applyResult.Error)

	// Verify the key was deleted
	_, _, err := store.Get("test-key")
	assert.Error(t, err)
}

func TestFSM_Apply_UnknownOperation(t *testing.T) {
	store := setupTestStorage(t)
	defer store.Close()

	fsm := NewKVFSM(store)

	op := Operation{
		Op:  "UNKNOWN",
		Key: "test-key",
	}

	data, err := json.Marshal(op)
	require.NoError(t, err)

	log := &raft.Log{
		Index: 1,
		Data:  data,
	}

	result := fsm.Apply(log)
	require.NotNil(t, result)

	applyResult, ok := result.(*ApplyResult)
	require.True(t, ok)
	assert.Error(t, applyResult.Error)
	assert.Contains(t, applyResult.Error.Error(), "unknown operation")
}

// testSnapshotSink implements a simple in-memory sink for testing
type testSnapshotSink struct {
	data []byte
}

// snapshotSinkWrapper implements raft.SnapshotSink for testing
type snapshotSinkWrapper struct {
	sink *testSnapshotSink
}

func (w *snapshotSinkWrapper) Write(p []byte) (n int, err error) {
	w.sink.data = append(w.sink.data, p...)
	return len(p), nil
}

func (w *snapshotSinkWrapper) Close() error {
	return nil
}

func (w *snapshotSinkWrapper) ID() string {
	return "test-snapshot"
}

func (w *snapshotSinkWrapper) Cancel() error {
	return nil
}

func TestFSM_Snapshot(t *testing.T) {
	store := setupTestStorage(t)
	defer store.Close()

	fsm := NewKVFSM(store)

	// Put some data
	putOp := Operation{Op: "PUT", Key: "key1", Value: "value1"}
	putData, _ := json.Marshal(putOp)
	putLog := &raft.Log{Index: 1, Data: putData}
	fsm.Apply(putLog)

	putOp2 := Operation{Op: "PUT", Key: "key2", Value: "value2"}
	putData2, _ := json.Marshal(putOp2)
	putLog2 := &raft.Log{Index: 2, Data: putData2}
	fsm.Apply(putLog2)

	// Create snapshot
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snapshot)

	// Test Persist with our test sink
	testSink := &testSnapshotSink{}
	err = snapshot.Persist(&snapshotSinkWrapper{sink: testSink})
	require.NoError(t, err)
	require.NotEmpty(t, testSink.data, "Snapshot data should not be empty")

	// CRITICAL: Release the snapshot to free the Bolt transaction
	snapshot.Release()

	// Verify the snapshot contains our data
	var snapshotData map[string]storage.Record
	err = json.Unmarshal(testSink.data, &snapshotData)
	require.NoError(t, err)
	assert.Len(t, snapshotData, 2)
	assert.Equal(t, "value1", snapshotData["key1"].Value)
	assert.Equal(t, "value2", snapshotData["key2"].Value)
}

func TestFSM_Restore(t *testing.T) {
	// Create source storage with data
	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "source.db")
	sourceStore, err := storage.New(sourcePath)
	require.NoError(t, err)

	// Put some data
	err = sourceStore.Put("key1", "value1", 1)
	require.NoError(t, err)
	err = sourceStore.Put("key2", "value2", 2)
	require.NoError(t, err)
	// Put a key with empty value to test that it's not treated as deletion
	err = sourceStore.Put("key3", "", 3)
	require.NoError(t, err)

	// Create snapshot from source
	sourceFSM := NewKVFSM(sourceStore)
	snapshot, err := sourceFSM.Snapshot()
	require.NoError(t, err)

	// Persist snapshot
	testSink := &testSnapshotSink{}
	err = snapshot.Persist(&snapshotSinkWrapper{sink: testSink})
	require.NoError(t, err)

	// CRITICAL: Release the snapshot to free the Bolt transaction
	snapshot.Release()

	// Create destination storage with some existing data
	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "dest.db")
	destStore, err := storage.New(destPath)
	require.NoError(t, err)

	// Add some conflicting data to destination
	err = destStore.Put("old_key", "old_value", 100)
	require.NoError(t, err)

	// Restore snapshot to destination
	destFSM := NewKVFSM(destStore)
	snapshotReader := &snapshotReadCloser{data: testSink.data}

	err = destFSM.Restore(snapshotReader)
	require.NoError(t, err)

	// Verify restored data
	value, appliedIndex, err := destStore.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, "value1", value)
	assert.Equal(t, uint64(1), appliedIndex)

	value, appliedIndex, err = destStore.Get("key2")
	require.NoError(t, err)
	assert.Equal(t, "value2", value)
	assert.Equal(t, uint64(2), appliedIndex)

	// Verify empty value is preserved correctly
	value, appliedIndex, err = destStore.Get("key3")
	require.NoError(t, err)
	assert.Equal(t, "", value)
	assert.Equal(t, uint64(3), appliedIndex)

	// Verify old data was cleared
	_, _, err = destStore.Get("old_key")
	assert.Error(t, err, "Old data should be cleared after restore")

	sourceStore.Close()
	destStore.Close()
}

// snapshotReadCloser implements io.ReadCloser for testing
type snapshotReadCloser struct {
	data []byte
	pos  int
}

func (r *snapshotReadCloser) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *snapshotReadCloser) Close() error {
	return nil
}

func TestFSM_Apply_InvalidJSON(t *testing.T) {
	store := setupTestStorage(t)
	defer store.Close()

	fsm := NewKVFSM(store)

	log := &raft.Log{
		Index: 1,
		Data:  []byte("invalid json"),
	}

	result := fsm.Apply(log)
	require.NotNil(t, result)

	applyResult, ok := result.(*ApplyResult)
	require.True(t, ok)
	assert.Error(t, applyResult.Error)
	assert.Contains(t, applyResult.Error.Error(), "failed to unmarshal")
}
