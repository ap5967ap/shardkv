package main

import "fmt"

type rangeOverride struct {
	start   string
	end     string
	shardID string
}

func matchesRange(key, start, end string) bool {
	if start != "" && key < start {
		return false
	}
	if end != "" && key > end {
		return false
	}
	return true
}

func main() {
	rangeRouting := []rangeOverride{
		{start: "a", end: "z", shardID: "B"},
		{start: "m", end: "n", shardID: "C"},
	}

	key := "m"
	for _, override := range rangeRouting {
		if matchesRange(key, override.start, override.end) {
			fmt.Println("Matched:", override.shardID)
			return
		}
	}
}
