package main

import (
	"fmt"
	"shardkv/internal/routing"
)

func main() {
	ring := routing.NewRing(100)
	ring.AddShard("A")
	ring.AddShard("B")

	aCount := 0
	bCount := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		shard := ring.ShardFor(key)
		if shard == "A" && aCount < 3 {
			fmt.Printf("A: %s\n", key)
			aCount++
		}
		if shard == "B" && bCount < 3 {
			fmt.Printf("B: %s\n", key)
			bCount++
		}
	}
}
