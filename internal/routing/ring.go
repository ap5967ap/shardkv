package routing

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// Ring implements consistent hashing for shard distribution
type Ring struct {
	mu             sync.RWMutex
	virtualNodes   int // virtual nodes per physical shard
	ring           []uint32
	shardMap       map[uint32]string // hash -> shard ID
	virtualNodeMap map[string]string // virtual node ID -> shard ID
	shards         map[string]bool   // set of active shard IDs
}

// NewRing creates a new consistent hashing ring
func NewRing(virtualNodes int) *Ring {
	return &Ring{
		virtualNodes:   virtualNodes,
		ring:           make([]uint32, 0),
		shardMap:       make(map[uint32]string),
		virtualNodeMap: make(map[string]string),
		shards:         make(map[string]bool),
	}
}

// AddShard adds a shard to the ring
func (r *Ring) AddShard(shardID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.shards[shardID] {
		return // already exists
	}

	r.shards[shardID] = true

	// Add virtual nodes for this shard
	for i := 0; i < r.virtualNodes; i++ {
		virtualNodeID := virtualNodeName(shardID, i)
		hash := hashString(virtualNodeID)
		r.ring = append(r.ring, hash)
		r.shardMap[hash] = shardID
		r.virtualNodeMap[virtualNodeID] = shardID
	}

	// Sort the ring for binary search
	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i] < r.ring[j]
	})
}

// RemoveShard removes a shard from the ring
func (r *Ring) RemoveShard(shardID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.shards[shardID] {
		return // doesn't exist
	}

	delete(r.shards, shardID)

	// Remove all virtual nodes for this shard
	for i := 0; i < r.virtualNodes; i++ {
		virtualNodeID := virtualNodeName(shardID, i)
		hash := hashString(virtualNodeID)
		delete(r.shardMap, hash)
		delete(r.virtualNodeMap, virtualNodeID)
	}

	// Rebuild the ring
	r.ring = make([]uint32, 0, len(r.shardMap))
	for hash := range r.shardMap {
		r.ring = append(r.ring, hash)
	}
	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i] < r.ring[j]
	})
}

// ShardFor returns the shard ID for a given key
func (r *Ring) ShardFor(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return ""
	}

	hash := hashString(key)

	// Find the first virtual node with hash >= key hash
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i] >= hash
	})

	// Wrap around to the first node if we reach the end
	if idx == len(r.ring) {
		idx = 0
	}

	hashValue := r.ring[idx]
	return r.shardMap[hashValue]
}

// GetShards returns all active shard IDs
func (r *Ring) GetShards() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	shards := make([]string, 0, len(r.shards))
	for shardID := range r.shards {
		shards = append(shards, shardID)
	}
	sort.Strings(shards)
	return shards
}

// virtualNodeName generates a virtual node name
func virtualNodeName(shardID string, index int) string {
	return fmt.Sprintf("%s#%d", shardID, index)
}

// hashString hashes a string using FNV-1a
func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
