package integration

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"shardkv/internal/raftfsm"
	"shardkv/internal/storage"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVerifyLeaderPartitionScenario tests the critical scenario from §6.2:
// A node that believes it's leader but has been partitioned should fail VerifyLeader()
func TestVerifyLeaderPartitionScenario(t *testing.T) {
	// Create 3 nodes with in-memory transport
	transports := make(map[string]*raft.InmemTransport)
	storageInstances := make(map[string]*storage.Storage)
	fsms := make(map[string]*raftfsm.KVFSM)
	raftInstances := make(map[string]*raft.Raft)

	nodeIDs := []string{"A1", "A2", "A3"}

	// Setup each node
	for _, id := range nodeIDs {
		// Create storage
		dir := t.TempDir()
		store, err := storage.New(filepath.Join(dir, "kv.db"))
		require.NoError(t, err)
		storageInstances[id] = store

		// Create FSM
		fsm := raftfsm.NewKVFSM(store)
		fsms[id] = fsm

		// Create in-memory transport
		_, trans := raft.NewInmemTransport(raft.ServerAddress(id))
		transports[id] = trans

		// Raft configuration
		config := raft.DefaultConfig()
		config.LocalID = raft.ServerID(id)
		config.HeartbeatTimeout = 500 * time.Millisecond // Increased to allow isolation test
		config.ElectionTimeout = 1 * time.Second
		config.LeaderLeaseTimeout = 250 * time.Millisecond

		// Create in-memory stores
		logStore := raft.NewInmemStore()
		stableStore := raft.NewInmemStore()
		snapshotStore := raft.NewInmemSnapshotStore()

		// Create Raft instance
		raftInstance, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, trans)
		require.NoError(t, err)
		raftInstances[id] = raftInstance
	}

	// Wire full connectivity initially
	for id, trans := range transports {
		for peerID, peerTrans := range transports {
			if id != peerID {
				trans.Connect(peerTrans.LocalAddr(), peerTrans)
			}
		}
	}

	// Bootstrap the cluster with all nodes
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{ID: raft.ServerID("A1"), Address: raft.ServerAddress("A1")},
			{ID: raft.ServerID("A2"), Address: raft.ServerAddress("A2")},
			{ID: raft.ServerID("A3"), Address: raft.ServerAddress("A3")},
		},
	}

	future := raftInstances["A1"].BootstrapCluster(configuration)
	require.NoError(t, future.Error())

	// Wait for leader election
	time.Sleep(1 * time.Second)

	// Find the leader
	var leaderID string
	for _, id := range nodeIDs {
		if raftInstances[id].State() == raft.Leader {
			leaderID = id
			break
		}
	}
	require.NotEmpty(t, leaderID, "A leader should be elected")

	t.Logf("Leader elected: %s", leaderID)

	// Verify leader can use VerifyLeader successfully
	leaderRaft := raftInstances[leaderID]
	err := leaderRaft.VerifyLeader().Error()
	assert.NoError(t, err, "Leader should pass VerifyLeader()")

	// Now simulate the partition scenario: isolate the leader
	t.Logf("Isolating leader %s from the cluster", leaderID)

	// CRITICAL: Disconnect bidirectional links to truly isolate the leader
	// Disconnect leader from all peers
	transports[leaderID].DisconnectAll()

	// Also disconnect all peers from the leader (simulate bidirectional partition)
	for _, peerID := range nodeIDs {
		if peerID != leaderID {
			transports[peerID].Disconnect(transports[leaderID].LocalAddr())
		}
	}

	// CRITICAL: Require that the node is still in Leader state immediately after isolation
	// This is the exact scenario from §6.2 - a node that believes it's leader but has been partitioned
	stateBefore := leaderRaft.State()
	require.Equal(t, raft.Leader, stateBefore, "Node must still be leader immediately after isolation to test the critical scenario")
	t.Logf("Leader state immediately after isolation: %s", stateBefore)

	// The critical test: the isolated leader should fail VerifyLeader()
	// This is the key behavior that prevents linearizability violations
	err = leaderRaft.VerifyLeader().Error()
	require.Error(t, err, "Isolated leader must fail VerifyLeader() while still believing it's leader")
	t.Logf("VerifyLeader() correctly failed for isolated leader: %v", err)

	// Verify the isolated leader can't commit operations
	op := raftfsm.Operation{
		Op:    "PUT",
		Key:   "test-key",
		Value: "test-value",
	}
	data, _ := json.Marshal(op)

	applyFuture := leaderRaft.Apply(data, 1*time.Second)
	err = applyFuture.Error()
	assert.Error(t, err, "Isolated leader should fail to apply operations")
	t.Logf("Apply operation correctly failed for isolated leader: %v", err)

	// Restore connectivity (bidirectional)
	t.Logf("Restoring connectivity for %s", leaderID)
	for peerID, peerTrans := range transports {
		if peerID != leaderID {
			transports[leaderID].Connect(peerTrans.LocalAddr(), peerTrans)
			transports[peerID].Connect(transports[leaderID].LocalAddr(), transports[leaderID])
		}
	}

	// Wait for cluster to stabilize
	time.Sleep(1 * time.Second)

	// Clean up
	for _, id := range nodeIDs {
		raftInstances[id].Shutdown()
		storageInstances[id].Close()
	}
}

// TestVerifyLeader正常Scenario tests that VerifyLeader works correctly for a healthy leader
func TestVerifyLeaderNormalScenario(t *testing.T) {
	// Create a simple 3-node cluster
	transports := make(map[string]*raft.InmemTransport)
	storageInstances := make(map[string]*storage.Storage)
	fsms := make(map[string]*raftfsm.KVFSM)
	raftInstances := make(map[string]*raft.Raft)

	nodeIDs := []string{"B1", "B2", "B3"}

	// Setup each node (similar to above test)
	for _, id := range nodeIDs {
		dir := t.TempDir()
		store, err := storage.New(filepath.Join(dir, "kv.db"))
		require.NoError(t, err)
		storageInstances[id] = store

		fsm := raftfsm.NewKVFSM(store)
		fsms[id] = fsm

		_, trans := raft.NewInmemTransport(raft.ServerAddress(id))
		transports[id] = trans

		config := raft.DefaultConfig()
		config.LocalID = raft.ServerID(id)
		config.HeartbeatTimeout = 500 * time.Millisecond // Increased to allow isolation test
		config.ElectionTimeout = 1 * time.Second
		config.LeaderLeaseTimeout = 250 * time.Millisecond

		logStore := raft.NewInmemStore()
		stableStore := raft.NewInmemStore()
		snapshotStore := raft.NewInmemSnapshotStore()

		raftInstance, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, trans)
		require.NoError(t, err)
		raftInstances[id] = raftInstance
	}

	// Wire connectivity
	for id, trans := range transports {
		for peerID, peerTrans := range transports {
			if id != peerID {
				trans.Connect(peerTrans.LocalAddr(), peerTrans)
			}
		}
	}

	// Bootstrap
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{ID: raft.ServerID("B1"), Address: raft.ServerAddress("B1")},
			{ID: raft.ServerID("B2"), Address: raft.ServerAddress("B2")},
			{ID: raft.ServerID("B3"), Address: raft.ServerAddress("B3")},
		},
	}

	future := raftInstances["B1"].BootstrapCluster(configuration)
	require.NoError(t, future.Error())

	// Wait for leader
	time.Sleep(1 * time.Second)

	// Find leader and test VerifyLeader
	var leaderID string
	for _, id := range nodeIDs {
		if raftInstances[id].State() == raft.Leader {
			leaderID = id
			break
		}
	}
	require.NotEmpty(t, leaderID)

	// Leader should pass VerifyLeader
	err := raftInstances[leaderID].VerifyLeader().Error()
	assert.NoError(t, err, "Healthy leader should pass VerifyLeader()")

	// Followers should fail VerifyLeader
	for _, id := range nodeIDs {
		if id != leaderID {
			err := raftInstances[id].VerifyLeader().Error()
			assert.Error(t, err, "Follower should fail VerifyLeader()")
		}
	}

	// Clean up
	for _, id := range nodeIDs {
		raftInstances[id].Shutdown()
		storageInstances[id].Close()
	}
}
