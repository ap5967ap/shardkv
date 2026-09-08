package main

import (
	"shardkv/internal/raftfsm"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNode_NewNode(t *testing.T) {
	dataDir := t.TempDir()

	node, err := NewNode("test-node", "127.0.0.1:7001", "127.0.0.1:8001", dataDir)
	require.NoError(t, err)
	require.NotNil(t, node)
	defer node.Shutdown()

	assert.Equal(t, "test-node", node.nodeID)
	assert.Equal(t, "127.0.0.1:7001", node.raftAddr)
	assert.Equal(t, "127.0.0.1:8001", node.httpAddr)
	assert.NotNil(t, node.raft)
	assert.NotNil(t, node.fsm)
	assert.NotNil(t, node.storage)
}

func TestSingleNodeCluster(t *testing.T) {
	dataDir := t.TempDir()

	node, err := NewNode("node1", "127.0.0.1:7000", "127.0.0.1:8000", dataDir)
	require.NoError(t, err)
	defer node.Shutdown()

	// Bootstrap the cluster
	err = node.BootstrapCluster()
	require.NoError(t, err)

	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Verify node is leader
	assert.True(t, node.IsLeader(), "Node should be leader after bootstrap")
	assert.Equal(t, "127.0.0.1:7000", node.LeaderAddr())

	// Test Apply operation
	op := raftfsm.Operation{
		Op:    "PUT",
		Key:   "test-key",
		Value: "test-value",
	}

	err = node.ApplyOperation(op)
	require.NoError(t, err)

	// Wait for operation to be applied
	time.Sleep(500 * time.Millisecond)

	// Verify the value was stored
	value, appliedIndex, err := node.Get("test-key")
	require.NoError(t, err)
	assert.Equal(t, "test-value", value)
	assert.Greater(t, appliedIndex, uint64(0))
}

func TestNodeRestart(t *testing.T) {
	dataDir := t.TempDir()

	// Step 1: Create and bootstrap a node
	node1, err := NewNode("node1", "127.0.0.1:7000", "127.0.0.1:8000", dataDir)
	require.NoError(t, err)
	defer node1.Shutdown()

	err = node1.BootstrapCluster()
	require.NoError(t, err)

	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Write data
	op := raftfsm.Operation{
		Op:    "PUT",
		Key:   "test-key",
		Value: "test-value",
	}
	err = node1.ApplyOperation(op)
	require.NoError(t, err)

	// Wait for operation to be applied
	time.Sleep(500 * time.Millisecond)

	// Verify the value was stored
	value, _, err := node1.Get("test-key")
	require.NoError(t, err)
	assert.Equal(t, "test-value", value)

	// Step 2: Shutdown the node
	node1.Shutdown()

	// Step 3: Create a new node from the same data directory (simulates restart)
	node2, err := NewNode("node1", "127.0.0.1:7000", "127.0.0.1:8000", dataDir)
	require.NoError(t, err)
	defer node2.Shutdown()

	// Step 4: Attempt bootstrap - should skip due to existing state
	err = node2.BootstrapClusterIfNew()
	require.NoError(t, err, "BootstrapClusterIfNew should succeed without bootstrapping")

	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Step 5: Verify the original value is still present
	value, _, err = node2.Get("test-key")
	require.NoError(t, err, "Should be able to read data after restart")
	assert.Equal(t, "test-value", value, "Original value should be retained after restart")
}
