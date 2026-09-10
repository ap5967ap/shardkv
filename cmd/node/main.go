package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"shardkv/internal/api"
	"shardkv/internal/raftfsm"
	"shardkv/internal/storage"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	nodeID    = flag.String("node-id", "", "Unique node ID (required)")
	shardID   = flag.String("shard-id", "", "Shard ID for this node (e.g., A, B, C)")
	raftAddr  = flag.String("raft-addr", "", "Raft address (e.g., 127.0.0.1:7000) (required)")
	httpAddr  = flag.String("http-addr", "", "HTTP address (e.g., 127.0.0.1:8000) (required)")
	dataDir   = flag.String("data-dir", "", "Data directory for Raft storage (required)")
	bootstrap = flag.Bool("bootstrap", false, "Bootstrap the cluster (only for first node)")
	peers     = flag.String("peers", "", "Comma-separated list of peer Raft addresses (e.g., 127.0.0.1:7000,127.0.0.1:7100)")
	peerIDs   = flag.String("peer-ids", "", "Comma-separated list of peer IDs corresponding to peers (e.g., A1,A2,A3)")
	peersHTTP = flag.String("peers-http", "", "Comma-separated list of peer HTTP addresses, same order as --peers (e.g., 127.0.0.1:8000,127.0.0.1:8001)")
)


type Node struct {
	nodeID        string
	shardID       string
	raftAddr      string
	httpAddr      string
	dataDir       string
	raft          *raft.Raft
	fsm           *raftfsm.KVFSM
	storage       *storage.Storage
	transport     raft.Transport
	apiServer     *api.Server
	httpSrv       *http.Server
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
	// raftToHTTP maps each peer's Raft address to its HTTP address.
	// Populated at bootstrap time so followers can expose the leader's HTTP
	// address to the router even when they are not the leader.
	raftToHTTP map[string]string
}

func main() {
	flag.Parse()

	if *nodeID == "" || *shardID == "" || *raftAddr == "" || *httpAddr == "" || *dataDir == "" {
		log.Fatal("node-id, shard-id, raft-addr, http-addr, and data-dir are required")
	}

	// Build the Raft-addr → HTTP-addr mapping from --peers / --peers-http.
	// This lets any node (leader or follower) resolve the leader's HTTP address
	// without polling, because n.raft.Leader() always returns the Raft address.
	raftToHTTPMap := make(map[string]string)
	if *peers != "" && *peersHTTP != "" {
		raftAddrs := strings.Split(*peers, ",")
		httpAddrs := strings.Split(*peersHTTP, ",")
		if len(raftAddrs) != len(httpAddrs) {
			log.Fatalf("--peers (%d entries) and --peers-http (%d entries) must have the same number of entries",
				len(raftAddrs), len(httpAddrs))
		}
		for i, ra := range raftAddrs {
			raftToHTTPMap[strings.TrimSpace(ra)] = strings.TrimSpace(httpAddrs[i])
		}
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	node, err := NewNode(*nodeID, *shardID, *raftAddr, *httpAddr, *dataDir, raftToHTTPMap)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}
	defer node.Shutdown()
	
	// Bootstrap if requested
	if *bootstrap {
		if *peers != "" {
			// Bootstrap with full peer configuration
			if err := node.ConfigurePeers(*peers, *peerIDs); err != nil {
				log.Fatalf("Failed to bootstrap cluster with peers: %v", err)
			}
		} else {
			// Bootstrap single node, but only if not already bootstrapped
			if err := node.BootstrapClusterIfNew(); err != nil {
				log.Fatalf("Failed to bootstrap cluster: %v", err)
			}
		}
	} else {
		// Non-bootstrap nodes do not need to "join" explicitly.
		// Known limitation: this cluster topology requires all nodes to be
		// started together in a single launch. Node 1 (--bootstrap) calls
		// BootstrapCluster with the full peer list (A1+A2+A3), which writes
		// the initial Raft configuration for the whole group before nodes 2
		// and 3 start. Nodes 2 and 3 simply open their empty stores and wait;
		// they receive the configuration from node 1 via the first AppendEntries
		// RPC once connectivity is established.
		//
		// Consequence: you cannot add a node to a running cluster dynamically
		// via this path — raft.AddVoter() would be needed for that (Phase 5+).
		// The --peers flag on non-bootstrap nodes is currently unused but kept
		// for future use.
		if *peers != "" {
			log.Printf("Node %s: peer list provided but this node is not bootstrapping. "+
				"Peers are registered by the bootstrap node (node 1). "+
				"Dynamic join via raft.AddVoter() is not yet implemented.", *nodeID)
		}
	}

	log.Printf("Node %s started successfully (shard: %s)", *nodeID, *shardID)
	log.Printf("Raft address: %s", *raftAddr)
	log.Printf("HTTP address: %s", *httpAddr)

	// Start HTTP server
	if err := node.StartHTTPServer(); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}

	// Wait for shutdown signal
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	<-signalCh
	log.Println("Shutting down node...")
}


func NewNode(nodeID, shardID, raftAddr, httpAddr, dataDir string, raftToHTTP map[string]string) (*Node, error) {
	// Create storage
	storagePath := filepath.Join(dataDir, "kv.db")
	store, err := storage.New(storagePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	// Create FSM
	fsm := raftfsm.NewKVFSM(store)

	// Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.SnapshotInterval = 30 * time.Second
	config.HeartbeatTimeout = 500 * time.Millisecond
	config.ElectionTimeout = 1 * time.Second
	config.LeaderLeaseTimeout = 500 * time.Millisecond

	// Create transport
	addr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve raft address: %w", err)
	}

	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	// Create BoltDB stores
	raftDBPath := filepath.Join(dataDir, "raft.db")
	logStore, err := raftboltdb.NewBoltStore(raftDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "stable.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Create Raft instance
	raftInstance, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	// Always include this node's own raft→http mapping.
	if raftToHTTP == nil {
		raftToHTTP = make(map[string]string)
	}
	raftToHTTP[raftAddr] = httpAddr

	return &Node{
		nodeID:        nodeID,
		shardID:       shardID,
		raftAddr:      raftAddr,
		httpAddr:      httpAddr,
		dataDir:       dataDir,
		raft:          raftInstance,
		fsm:           fsm,
		storage:       store,
		transport:     transport,
		logStore:      logStore,
		stableStore:   stableStore,
		snapshotStore: snapshotStore,
		raftToHTTP:    raftToHTTP,
	}, nil
}


// StartHTTPServer starts the HTTP API server
func (n *Node) StartHTTPServer() error {
	n.apiServer = api.NewServer(n)

	mux := http.NewServeMux()
	n.apiServer.RegisterRoutes(mux)
	mux.Handle("/metrics", promhttp.Handler())

	n.httpSrv = &http.Server{
		Addr:    n.httpAddr,
		Handler: mux,
	}

	go func() {
		// Start metrics collection goroutine
		go n.collectMetricsPeriodically()
		log.Printf("Starting HTTP server on %s", n.httpAddr)
		if err := n.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	return nil
}

func (n *Node) BootstrapCluster() error {
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(n.nodeID),
				Address: raft.ServerAddress(n.raftAddr),
			},
		},
	}

	future := n.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to bootstrap cluster: %w", err)
	}

	log.Printf("Cluster bootstrapped with node %s", n.nodeID)
	return nil
}

func (n *Node) BootstrapClusterIfNew() error {
	// CRITICAL: Check for existing state before attempting bootstrap
	// Use the already-open Raft stores to avoid Bolt file lock deadlock
	hasState, err := raft.HasExistingState(n.logStore, n.stableStore, n.snapshotStore)
	if err != nil {
		return fmt.Errorf("failed to check for existing state: %w", err)
	}

	if hasState {
		log.Printf("Existing Raft state detected for node %s, skipping bootstrap", n.nodeID)
		return nil
	}

	// No existing state, safe to bootstrap
	return n.BootstrapCluster()
}

func (n *Node) ConfigurePeers(peersStr string, peerIDsStr string) error {
	// Parse peer addresses
	peerAddrs := strings.Split(peersStr, ",")

	// Parse peer IDs if provided, otherwise fall back to hardcoded format
	var peerIDs []string
	if peerIDsStr != "" {
		peerIDs = strings.Split(peerIDsStr, ",")
	} else {
		// Fallback to hardcoded format for compatibility
		for i := range peerAddrs {
			peerIDs = append(peerIDs, fmt.Sprintf("node%d", i+1))
		}
	}

	// Build configuration with all peers using actual node IDs
	var servers []raft.Server
	for i, addr := range peerAddrs {
		if i >= len(peerIDs) {
			return fmt.Errorf("not enough peer IDs provided for peer addresses")
		}
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(peerIDs[i]),
			Address: raft.ServerAddress(addr),
		})
	}

	configuration := raft.Configuration{
		Servers: servers,
	}

	// CRITICAL: Check for existing state before attempting bootstrap
	// Use the already-open Raft stores to avoid Bolt file lock deadlock
	hasState, err := raft.HasExistingState(n.logStore, n.stableStore, n.snapshotStore)
	if err != nil {
		return fmt.Errorf("failed to check for existing state: %w", err)
	}

	if hasState {
		log.Printf("Existing Raft state detected for node %s, skipping bootstrap", n.nodeID)
		return nil
	}

	// No existing state, safe to bootstrap
	future := n.raft.BootstrapCluster(configuration)
	err = future.Error()
	if err != nil {
		return fmt.Errorf("failed to bootstrap cluster: %w", err)
	}

	log.Printf("Cluster bootstrapped with %d nodes using peer IDs: %v", len(servers), peerIDs)
	return nil
}

func (n *Node) Shutdown() {
	if n.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		n.httpSrv.Shutdown(ctx)
	}
	if n.raft != nil {
		n.raft.Shutdown().Error()
	}
	if n.storage != nil {
		n.storage.Close()
	}
	// CRITICAL: Close Raft stores to prevent resource leaks
	if n.logStore != nil {
		if closer, ok := n.logStore.(interface{ Close() error }); ok {
			closer.Close()
		}
	}
	if n.stableStore != nil {
		if closer, ok := n.stableStore.(interface{ Close() error }); ok {
			closer.Close()
		}
	}
	// Note: raft.FileSnapshotStore doesn't have a Close() method
	// It's managed by the Raft library
}

// ApplyOperation applies a Raft operation
func (n *Node) ApplyOperation(op raftfsm.Operation) error {
	data, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("failed to marshal operation: %w", err)
	}

	future := n.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to apply operation: %w", err)
	}

	result := future.Response()
	if result == nil {
		return nil
	}

	applyResult, ok := result.(*raftfsm.ApplyResult)
	if !ok {
		return fmt.Errorf("unexpected result type")
	}

	return applyResult.Error
}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) LeaderAddr() string {
	return string(n.raft.Leader())
}

// LeaderHTTPAddr returns the HTTP address of the current Raft leader.
// Works on both the leader itself and followers — the leader's Raft address
// is always known via n.raft.Leader(), and we translate it to HTTP via the
// raftToHTTP map populated at bootstrap time.
// Returns "" only if no leader has been elected yet.
func (n *Node) LeaderHTTPAddr() string {
	leaderRaftAddr := string(n.raft.Leader())
	if leaderRaftAddr == "" {
		return ""
	}
	if httpAddr, ok := n.raftToHTTP[leaderRaftAddr]; ok {
		return httpAddr
	}
	// Fallback: if we somehow don't have the mapping (e.g., older peer added
	// dynamically), return empty string so the router falls back to polling.
	return ""
}


func (n *Node) Get(key string) (string, uint64, error) {
	return n.storage.Get(key)
}

// GetRaft returns the Raft instance (for testing)
func (n *Node) GetRaft() *raft.Raft {
	return n.raft
}

// GetShardID returns the shard ID for this node
func (n *Node) GetShardID() string {
	return n.shardID
}

// GetAppliedIndex returns the highest index applied to this node's FSM
func (n *Node) GetAppliedIndex() uint64 {
	// Get the last applied index from Raft stats
	stats := n.raft.Stats()
	if appliedIndexStr, ok := stats["applied_index"]; ok {
		var index uint64
		fmt.Sscanf(appliedIndexStr, "%d", &index)
		return index
	}
	return 0
}

// GetCommitIndex returns the commit index known to this node
func (n *Node) GetCommitIndex() uint64 {
	// Get the commit index from Raft stats
	stats := n.raft.Stats()
	if commitIndexStr, ok := stats["commit_index"]; ok {
		var index uint64
		fmt.Sscanf(commitIndexStr, "%d", &index)
		return index
	}
	return 0
}

// collectMetricsPeriodically collects and updates Prometheus metrics periodically
func (n *Node) collectMetricsPeriodically() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
	}
}
