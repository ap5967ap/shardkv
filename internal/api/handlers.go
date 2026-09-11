package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"shardkv/internal/raftfsm"

	"github.com/hashicorp/raft"
)

// Node represents the interface that the API handlers need from the node
type Node interface {
	ApplyOperation(op raftfsm.Operation) error
	Get(key string) (string, uint64, error)
	IsLeader() bool
	LeaderAddr() string
	LeaderHTTPAddr() string
	GetShardID() string
	GetRaft() *raft.Raft
	GetAppliedIndex() uint64
	GetCommitIndex() uint64
}

// Server wraps the HTTP server and node reference
type Server struct {
	node Node
}

// NewServer creates a new API server
func NewServer(node Node) *Server {
	return &Server{
		node: node,
	}
}

// RegisterRoutes registers all HTTP handlers
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/cluster/health", s.handleClusterHealth)
}

// handleKV handles PUT, GET, DELETE requests for keys
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	// Extract key from path
	key := r.URL.Path[len("/kv/"):]
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// GET can be served by any node (leader or follower) from its local FSM.
		// Phase 2 reads are simple local FSM reads with no consistency guarantee.
		// Phase 3 will add the ?consistency=strong|eventual parameter and route
		// strong reads through VerifyLeader+Barrier; eventual reads stay here.
		s.handleGet(w, r, key)
	case http.MethodPut, http.MethodPost:
		// Compatibility: the project and older scripts sometimes issue a POST
		// for writes, but the underlying semantics are still leader-only Raft writes.
		if !s.node.IsLeader() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "not leader",
			})
			return
		}
		s.handlePut(w, r, key)
	case http.MethodDelete:
		// Same leadership requirement as PUT.
		if !s.node.IsLeader() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "not leader",
			})
			return
		}
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePut handles PUT requests
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	var req struct {
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	op := raftfsm.Operation{
		Op:    "PUT",
		Key:   key,
		Value: req.Value,
	}

	if err := s.node.ApplyOperation(op); err != nil {
		// Check if this is a leadership loss error - return 503 so router can retry
		if !s.node.IsLeader() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "leadership lost during operation",
			})
			return
		}
		// Check if this is a frozen key error - return 503 with retry-after
		if err.Error() == "key is frozen for migration" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "key is frozen for migration",
			})
			return
		}
		http.Error(w, fmt.Sprintf("failed to apply operation: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "committed",
	})
}

// handleGet handles GET requests
// Supports consistency parameter: ?consistency=strong|eventual (default: strong)
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	// Parse consistency parameter (default: strong)
	consistency := r.URL.Query().Get("consistency")
	if consistency == "" {
		consistency = "strong"
	}

	switch consistency {
	case "strong":
		s.handleStrongGet(w, r, key)
	case "eventual":
		s.handleEventualGet(w, r, key)
	default:
		http.Error(w, "invalid consistency parameter, must be 'strong' or 'eventual'", http.StatusBadRequest)
	}
}

// handleStrongGet implements STRONG reads with VerifyLeader() + Barrier()
// This is the correct linearizable read protocol per design.md §6.2
func (s *Server) handleStrongGet(w http.ResponseWriter, r *http.Request, key string) {
	raftNode := s.node.GetRaft()

	// Step 1: VerifyLeader() - confirms this node is still the leader
	// This catches the partition scenario where a node believes it's leader but isn't
	if err := raftNode.VerifyLeader().Error(); err != nil {
		// Not actually leader - return 503 so router can retry against new leader
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "not leader",
		})
		return
	}

	// Step 2: Barrier() - ensures all committed entries are applied to local FSM
	// This closes the gap between "committed" and "applied locally"
	if err := raftNode.Barrier(5 * time.Second).Error(); err != nil {
		http.Error(w, fmt.Sprintf("barrier failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Step 3: Now safe to read from local FSM - this is linearizable
	value, appliedIndex, err := s.node.Get(key)
	if err != nil {
		http.Error(w, fmt.Sprintf("key not found: %v", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"value":         value,
		"consistency":   "strong",
		"applied_index": appliedIndex,
	})
}

// handleEventualGet implements EVENTUAL reads from local FSM
// No VerifyLeader/Barrier - accepts potential staleness
func (s *Server) handleEventualGet(w http.ResponseWriter, r *http.Request, key string) {
	// Read from local FSM without any consistency checks
	value, appliedIndex, err := s.node.Get(key)
	if err != nil {
		http.Error(w, fmt.Sprintf("key not found: %v", err), http.StatusNotFound)
		return
	}

	lagIndex := s.calculateLagIndex()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"value":         value,
		"consistency":   "eventual",
		"applied_index": appliedIndex,
		"lag_index":     lagIndex,
	})
}

// calculateLagIndex reports the actual local replica lag in committed entries.
// This is the truthful, observable metric. We do not fabricate a wall-clock
// lag_ms from an index delta because no real timings are captured.
func (s *Server) calculateLagIndex() uint64 {
	appliedIndex := s.node.GetAppliedIndex()
	commitIndex := s.node.GetCommitIndex()
	if commitIndex <= appliedIndex {
		return 0
	}
	return commitIndex - appliedIndex
}

// handleDelete handles DELETE requests
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	op := raftfsm.Operation{
		Op:  "DELETE",
		Key: key,
	}

	if err := s.node.ApplyOperation(op); err != nil {
		// Check if this is a leadership loss error - return 503 so router can retry
		if !s.node.IsLeader() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "leadership lost during operation",
			})
			return
		}
		// Check if this is a frozen key error - return 503 with retry-after
		if err.Error() == "key is frozen for migration" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "key is frozen for migration",
			})
			return
		}
		http.Error(w, fmt.Sprintf("failed to apply operation: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "deleted",
	})
}

// handleClusterHealth returns cluster health information
func (s *Server) handleClusterHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	raftNode := s.node.GetRaft()
	stats := raftNode.Stats()

	// Return HTTP address if this node is leader, otherwise empty
	// Router will discover actual leader via health checks
	leaderHTTPAddr := ""
	if s.node.IsLeader() {
		leaderHTTPAddr = s.node.LeaderHTTPAddr()
	}

	health := map[string]interface{}{
		"shard_id":    s.node.GetShardID(),
		"is_leader":   s.node.IsLeader(),
		"leader_addr": s.node.LeaderAddr(), // Raft address for debugging
		"leader_http": leaderHTTPAddr,      // HTTP address for client connections
		"raft_state":  raftNode.State().String(),
		"raft_stats":  stats,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(health)
}
