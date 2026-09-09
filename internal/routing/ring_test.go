package routing

import (
	"fmt"
	"testing"
)

func TestNewRing(t *testing.T) {
	ring := NewRing(100)
	if ring == nil {
		t.Fatal("NewRing returned nil")
	}
	if ring.virtualNodes != 100 {
		t.Errorf("Expected virtualNodes=100, got %d", ring.virtualNodes)
	}
}

func TestAddShard(t *testing.T) {
	ring := NewRing(10)
	ring.AddShard("A")
	ring.AddShard("B")
	ring.AddShard("C")

	shards := ring.GetShards()
	if len(shards) != 3 {
		t.Errorf("Expected 3 shards, got %d", len(shards))
	}

	// Test idempotency - adding same shard again should not duplicate
	ring.AddShard("A")
	shards = ring.GetShards()
	if len(shards) != 3 {
		t.Errorf("Expected 3 shards after duplicate add, got %d", len(shards))
	}
}

func TestRemoveShard(t *testing.T) {
	ring := NewRing(10)
	ring.AddShard("A")
	ring.AddShard("B")
	ring.AddShard("C")

	ring.RemoveShard("B")
	shards := ring.GetShards()
	if len(shards) != 2 {
		t.Errorf("Expected 2 shards after removal, got %d", len(shards))
	}

	// Test idempotency - removing non-existent shard should be safe
	ring.RemoveShard("B")
	shards = ring.GetShards()
	if len(shards) != 2 {
		t.Errorf("Expected 2 shards after duplicate removal, got %d", len(shards))
	}
}

func TestShardFor(t *testing.T) {
	ring := NewRing(100)
	ring.AddShard("A")
	ring.AddShard("B")
	ring.AddShard("C")

	// Test that the same key always routes to the same shard
	key := "test-key"
	shard1 := ring.ShardFor(key)
	shard2 := ring.ShardFor(key)
	if shard1 != shard2 {
		t.Errorf("Expected same shard for same key, got %s and %s", shard1, shard2)
	}

	// Test that we get valid shard IDs
	validShards := map[string]bool{"A": true, "B": true, "C": true}
	if !validShards[shard1] {
		t.Errorf("Expected valid shard ID, got %s", shard1)
	}
}

func TestShardForEmptyRing(t *testing.T) {
	ring := NewRing(100)
	shard := ring.ShardFor("test-key")
	if shard != "" {
		t.Errorf("Expected empty string for empty ring, got %s", shard)
	}
}

func TestDistribution(t *testing.T) {
	ring := NewRing(100)
	shards := []string{"A", "B", "C"}
	for _, shard := range shards {
		ring.AddShard(shard)
	}

	// Test distribution with many keys
	counts := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		shard := ring.ShardFor(key)
		counts[shard]++
	}

	// Each shard should get roughly 1/3 of the keys (with some tolerance)
	// With 10000 keys and 3 shards, we expect ~3333 keys per shard
	// Allow for broader tolerance due to hash distribution variance
	minExpected := 1000
	maxExpected := 6000

	for _, shard := range shards {
		count := counts[shard]
		if count < minExpected || count > maxExpected {
			t.Errorf("Shard %s got %d keys, expected between %d and %d", shard, count, minExpected, maxExpected)
		}
	}

	t.Logf("Distribution: A=%d, B=%d, C=%d", counts["A"], counts["B"], counts["C"])
}

func TestShardForAfterRemoval(t *testing.T) {
	ring := NewRing(100)
	ring.AddShard("A")
	ring.AddShard("B")
	ring.AddShard("C")

	key := "test-key"
	shardBefore := ring.ShardFor(key)

	ring.RemoveShard("B")
	shardAfter := ring.ShardFor(key)

	// After removing B, the key should either stay on A or move to C
	// but should never be B
	if shardAfter == "B" {
		t.Errorf("Key should not route to removed shard B, got %s", shardAfter)
	}

	t.Logf("Key %s routed to %s before removal, %s after removal", key, shardBefore, shardAfter)
}

func TestGetShardsSorted(t *testing.T) {
	ring := NewRing(10)
	ring.AddShard("C")
	ring.AddShard("A")
	ring.AddShard("B")

	shards := ring.GetShards()
	if len(shards) != 3 {
		t.Errorf("Expected 3 shards, got %d", len(shards))
	}

	// Check that shards are sorted
	for i := 1; i < len(shards); i++ {
		if shards[i] < shards[i-1] {
			t.Errorf("Shards not sorted: %v", shards)
		}
	}

	expected := []string{"A", "B", "C"}
	for i, shard := range shards {
		if shard != expected[i] {
			t.Errorf("Expected shard %s at position %d, got %s", expected[i], i, shard)
		}
	}
}
