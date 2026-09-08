package storage

import (
	"encoding/json"
	"fmt"

	"go.etcd.io/bbolt"
)

const (
	KVBucket = "kv"
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
