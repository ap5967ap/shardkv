package integration

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"shardkv/internal/raftfsm"
	"shardkv/internal/storage"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBootstrapRestartFix proves the restart/bootstrap fix works end-to-end
// using real BoltDB stores (not in-memory). It verifies:
//  1. A cluster bootstraps cleanly on first start.
//  2. After shutdown and restart with the same data directory,
//     HasExistingState returns true and bootstrap is skipped.
//  3. The restarted node re-elects leader and can read data written before shutdown.
func TestBootstrapRestartFix(t *testing.T) {
	dir := t.TempDir()

	openStores := func() (*raftboltdb.BoltStore, *raftboltdb.BoltStore, raft.SnapshotStore, *storage.Storage) {
		logStore, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
		require.NoError(t, err)
		stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dir, "stable.db"))
		require.NoError(t, err)
		snapshotStore, err := raft.NewFileSnapshotStore(dir, 3, nil)
		require.NoError(t, err)
		store, err := storage.New(filepath.Join(dir, "kv.db"))
		require.NoError(t, err)
		return logStore, stableStore, snapshotStore, store
	}

	newRaft := func(logStore raft.LogStore, stableStore raft.StableStore, snapshotStore raft.SnapshotStore, fsm raft.FSM) *raft.Raft {
		_, trans := raft.NewInmemTransport(raft.ServerAddress("N1"))
		cfg := raft.DefaultConfig()
		cfg.LocalID = "N1"
		cfg.HeartbeatTimeout = 200 * time.Millisecond
		cfg.ElectionTimeout = 400 * time.Millisecond
		cfg.LeaderLeaseTimeout = 200 * time.Millisecond
		r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapshotStore, trans)
		require.NoError(t, err)
		return r
	}

	// --- First boot ---
	logStore1, stableStore1, snapshotStore1, kvStore1 := openStores()
	fsm1 := raftfsm.NewKVFSM(kvStore1)
	r1 := newRaft(logStore1, stableStore1, snapshotStore1, fsm1)

	// Bootstrap a single-node cluster.
	hasState, err := raft.HasExistingState(logStore1, stableStore1, snapshotStore1)
	require.NoError(t, err)
	require.False(t, hasState, "fresh data dir should have no existing state")

	future := r1.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{ID: "N1", Address: "N1"}},
	})
	require.NoError(t, future.Error())

	// Wait for leadership.
	require.Eventually(t, func() bool { return r1.State() == raft.Leader }, 3*time.Second, 50*time.Millisecond)

	// Apply a write.
	op := raftfsm.Operation{Op: "PUT", Key: "restart-key", Value: "hello"}
	data, _ := json.Marshal(op)
	applyFuture := r1.Apply(data, 2*time.Second)
	require.NoError(t, applyFuture.Error())

	// Verify the value is readable.
	val, _, err := kvStore1.Get("restart-key")
	require.NoError(t, err)
	assert.Equal(t, "hello", val)

	// Shut down cleanly.
	require.NoError(t, r1.Shutdown().Error())
	kvStore1.Close()
	logStore1.Close()
	stableStore1.Close()

	// --- Second boot (same data directory) ---
	logStore2, stableStore2, snapshotStore2, kvStore2 := openStores()
	defer kvStore2.Close()
	defer logStore2.Close()
	defer stableStore2.Close()

	// HasExistingState must be true — this is what prevents double-bootstrap.
	hasState2, err := raft.HasExistingState(logStore2, stableStore2, snapshotStore2)
	require.NoError(t, err)
	assert.True(t, hasState2, "restarted node must detect existing Raft state and skip bootstrap")

	fsm2 := raftfsm.NewKVFSM(kvStore2)
	r2 := newRaft(logStore2, stableStore2, snapshotStore2, fsm2)
	defer r2.Shutdown()

	// Wait for re-election (single node, should self-elect).
	require.Eventually(t, func() bool { return r2.State() == raft.Leader }, 3*time.Second, 50*time.Millisecond)

	// The value written before shutdown must survive the restart.
	val2, _, err := kvStore2.Get("restart-key")
	require.NoError(t, err)
	assert.Equal(t, "hello", val2, "value written before shutdown must be readable after restart")
}

