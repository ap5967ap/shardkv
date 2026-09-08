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
)

var (
	nodeID    = flag.String("node-id", "", "Unique node ID (required)")
	raftAddr  = flag.String("raft-addr", "", "Raft address (e.g., 127.0.0.1:7000) (required)")
	httpAddr  = flag.String("http-addr", "", "HTTP address (e.g., 127.0.0.1:8000) (required)")
	dataDir   = flag.String("data-dir", "", "Data directory for Raft storage (required)")
	bootstrap = flag.Bool("bootstrap", false, "Bootstrap the cluster (only for first node)")
	peers     = flag.String("peers", "", "Comma-separated list of peer addresses (e.g., 127.0.0.1:7000,127.0.0.1:7001)")
	peerIDs   = flag.String("peer-ids", "", "Comma-separated list of peer IDs corresponding to peers (e.g., node1,node2,node3)")
)

type Node struct {
	nodeID        string
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
}

func main() {
	flag.Parse()

	if *nodeID == "" || *raftAddr == "" || *httpAddr == "" || *dataDir == "" {
		log.Fatal("node-id, raft-addr, http-addr, and data-dir are required")
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	node, err := NewNode(*nodeID, *raftAddr, *httpAddr, *dataDir)
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
		// Not bootstrapping, but check if we need to join existing cluster
		if *peers != "" {
			log.Printf("Node %s joining existing cluster with peers: %s", *nodeID, *peers)
			// For now, just log - actual joining logic would be implemented here
		}
	}

	log.Printf("Node %s started successfully", *nodeID)
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

func NewNode(nodeID, raftAddr, httpAddr, dataDir string) (*Node, error) {
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

	return &Node{
		nodeID:        nodeID,
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
	}, nil
}

// StartHTTPServer starts the HTTP API server
func (n *Node) StartHTTPServer() error {
	n.apiServer = api.NewServer(n)

	mux := http.NewServeMux()
	n.apiServer.RegisterRoutes(mux)

	n.httpSrv = &http.Server{
		Addr:    n.httpAddr,
		Handler: mux,
	}

	go func() {
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

func (n *Node) Get(key string) (string, uint64, error) {
	return n.storage.Get(key)
}

// GetRaft returns the Raft instance (for testing)
func (n *Node) GetRaft() *raft.Raft {
	return n.raft
}
