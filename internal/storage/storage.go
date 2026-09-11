package storage

import (
	"encoding/json"
	"fmt"

	"shardkv/internal/migration"

	"go.etcd.io/bbolt"
)

const (
	KVBucket       = "kv"
	MigrationBucket = "migration"
	FrozenRangeKey = "frozen_range"
)

// Record represents a stored key-value pair with metadata
type Record struct {
	Value        string `json:"value"`
	AppliedIndex uint64 `json:"applied_index"`
}

// Storage wraps BoltDB for KV operations
type Storage struct {
	db *bbolt.DB
}

// New creates a new Storage instance
func New(dbPath string) (*Storage, error) {
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bolt db: %w", err)
	}

	// Create bucket if it doesn't exist
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(KVBucket))
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	return &Storage{db: db}, nil
}

// Get retrieves a value and its applied index for a key
func (s *Storage) Get(key string) (string, uint64, error) {
	var value string
	var appliedIndex uint64

	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(KVBucket))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		data := bucket.Get([]byte(key))
		if data == nil {
			return fmt.Errorf("key not found")
		}

		var record Record
		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("failed to unmarshal record: %w", err)
		}

		value = record.Value
		appliedIndex = record.AppliedIndex
		return nil
	})

	if err != nil {
		return "", 0, err
	}

	return value, appliedIndex, nil
}

// Put stores a key-value pair with its applied index
func (s *Storage) Put(key string, value string, appliedIndex uint64) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(KVBucket))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		record := Record{
			Value:        value,
			AppliedIndex: appliedIndex,
		}

		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to marshal record: %w", err)
		}

		return bucket.Put([]byte(key), data)
	})
}

// Delete removes a key with its applied index
func (s *Storage) Delete(key string, appliedIndex uint64) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(KVBucket))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		return bucket.Delete([]byte(key))
	})
}

// Close closes the database
func (s *Storage) Close() error {
	return s.db.Close()
}

// GetDB returns the underlying BoltDB instance (needed for snapshots)
func (s *Storage) GetDB() *bbolt.DB {
	return s.db
}

// Clear removes all data from the storage
func (s *Storage) Clear() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(KVBucket))
		if bucket != nil {
			return tx.DeleteBucket([]byte(KVBucket))
		}
		return nil
	})
}

// SetFrozenRange persists the active migration freeze range to BoltDB so it survives
// leader failover and can be reconstructed after a restart.
func (s *Storage) SetFrozenRange(keyRange migration.Range) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(MigrationBucket))
		if err != nil {
			return fmt.Errorf("failed to create migration bucket: %w", err)
		}
		data, err := json.Marshal(keyRange)
		if err != nil {
			return fmt.Errorf("failed to marshal frozen range: %w", err)
		}
		return bucket.Put([]byte(FrozenRangeKey), data)
	})
}

// GetFrozenRange reads the persisted freeze range, if any.
func (s *Storage) GetFrozenRange() (migration.Range, bool, error) {
	var result migration.Range
	var found bool

	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(MigrationBucket))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte(FrozenRangeKey))
		if data == nil {
			return nil
		}
		if err := json.Unmarshal(data, &result); err != nil {
			return fmt.Errorf("failed to unmarshal frozen range: %w", err)
		}
		found = true
		return nil
	})
	if err != nil {
		return migration.Range{}, false, err
	}
	return result, found, nil
}

// ClearFrozenRange removes the persisted freeze range after migration completion.
func (s *Storage) ClearFrozenRange() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(MigrationBucket))
		if bucket == nil {
			return nil
		}
		return bucket.Delete([]byte(FrozenRangeKey))
	})
}

// ScanResult represents a key-value pair returned from a scan operation
type ScanResult struct {
	Key          string `json:"key"`
	Value        string `json:"value"`
	AppliedIndex uint64 `json:"applied_index"`
}

// ScanRange returns all keys in the given range [start, end]
// If start is empty, scans from the beginning
// If end is empty, scans to the end
func (s *Storage) ScanRange(start, end string) ([]ScanResult, error) {
	var results []ScanResult

	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(KVBucket))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}

		cursor := bucket.Cursor()

		// Determine the starting key for the cursor
		var startKey []byte
		if start != "" {
			startKey = []byte(start)
		}

		// Iterate through the range
		for k, v := cursor.Seek(startKey); k != nil; k, v = cursor.Next() {
			keyStr := string(k)

			// Check if we've passed the end key
			if end != "" && keyStr > end {
				break
			}

			var record Record
			if err := json.Unmarshal(v, &record); err != nil {
				return fmt.Errorf("failed to unmarshal record for key %s: %w", keyStr, err)
			}

			results = append(results, ScanResult{
				Key:          keyStr,
				Value:        record.Value,
				AppliedIndex: record.AppliedIndex,
			})
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}
