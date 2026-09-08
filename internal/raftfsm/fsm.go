package raftfsm

import (
	"encoding/json"
	"fmt"
	"io"

	"shardkv/internal/storage"

	"github.com/hashicorp/raft"
	"go.etcd.io/bbolt"
)

// Operation represents a log entry operation
type Operation struct {
	Op    string `json:"op"` // "PUT" or "DELETE"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// ApplyResult is returned by Apply() to indicate success/failure
type ApplyResult struct {
	Error error
}

// KVFSM implements the raft.FSM interface
type KVFSM struct {
	storage *storage.Storage
}

// NewKVFSM creates a new KVFSM instance
func NewKVFSM(store *storage.Storage) *KVFSM {
	return &KVFSM{
		storage: store,
	}
}

// Apply applies a Raft log entry to the FSM
func (f *KVFSM) Apply(log *raft.Log) interface{} {
	var op Operation
	if err := json.Unmarshal(log.Data, &op); err != nil {
		return &ApplyResult{Error: fmt.Errorf("failed to unmarshal operation: %w", err)}
	}

	switch op.Op {
	case "PUT":
		if err := f.storage.Put(op.Key, op.Value, log.Index); err != nil {
			return &ApplyResult{Error: fmt.Errorf("failed to put key: %w", err)}
		}
	case "DELETE":
		if err := f.storage.Delete(op.Key, log.Index); err != nil {
			return &ApplyResult{Error: fmt.Errorf("failed to delete key: %w", err)}
		}
	default:
		return &ApplyResult{Error: fmt.Errorf("unknown operation: %s", op.Op)}
	}

	return &ApplyResult{Error: nil}
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM.
func (f *KVFSM) Snapshot() (raft.FSMSnapshot, error) {
	// CRITICAL: Hold a Bolt read transaction at snapshot time to ensure
	// point-in-time consistency. This transaction will be released in Release().
	tx, err := f.storage.GetDB().Begin(false)
	if err != nil {
		return nil, fmt.Errorf("failed to begin read transaction: %w", err)
	}

	return &KVFSMSnapshot{storage: f.storage, tx: tx}, nil
}

// Restore is used to restore an FSM from a snapshot. It is called
// with a ReadCloser to a snapshot. Restore must close the ReadCloser
// when it is done.
func (f *KVFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// CRITICAL: Decode snapshot data FIRST before clearing any state
	// This prevents destroying valid data on invalid snapshot input
	var snapshotData map[string]storage.Record
	decoder := json.NewDecoder(rc)
	if err := decoder.Decode(&snapshotData); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	// Snapshot data decoded successfully, now atomically replace the bucket
	// This ensures we don't destroy valid data on invalid input
	err := f.storage.GetDB().Update(func(tx *bbolt.Tx) error {
		// Delete the existing bucket if it exists
		if err := tx.DeleteBucket([]byte(storage.KVBucket)); err != nil && err != bbolt.ErrBucketNotFound {
			return fmt.Errorf("failed to delete existing bucket: %w", err)
		}

		// Create a fresh bucket
		bucket, err := tx.CreateBucket([]byte(storage.KVBucket))
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}

		// Write all snapshot data in this single transaction
		for key, record := range snapshotData {
			data, err := json.Marshal(record)
			if err != nil {
				return fmt.Errorf("failed to marshal record for key %s: %w", key, err)
			}
			if err := bucket.Put([]byte(key), data); err != nil {
				return fmt.Errorf("failed to write key %s: %w", key, err)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to atomically restore snapshot: %w", err)
	}

	return nil
}

// KVFSMSnapshot implements raft.FSMSnapshot
type KVFSMSnapshot struct {
	storage *storage.Storage
	tx      *bbolt.Tx
}

// Persist should write the snapshot to the given sink.
func (s *KVFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	// For simplicity, we'll serialize all data to JSON
	// In production, you'd want a more efficient format
	snapshotData := make(map[string]storage.Record)

	// CRITICAL: Use the read transaction held from Snapshot() time
	// This ensures point-in-time consistency - no writes after Raft selected the snapshot index
	bucket := s.tx.Bucket([]byte(storage.KVBucket))
	if bucket != nil {
		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var record storage.Record
			if err := json.Unmarshal(v, &record); err != nil {
				return err
			}
			snapshotData[string(k)] = record
		}
	}

	data, err := json.Marshal(snapshotData)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	_, err = sink.Write(data)
	return err
}

// Release is called when we are done with the snapshot.
// CRITICAL: Release the Bolt read transaction held from Snapshot() time.
func (s *KVFSMSnapshot) Release() {
	if s.tx != nil {
		s.tx.Rollback()
	}
}
