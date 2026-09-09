package api

import (
	"encoding/json"
	"fmt"
	"net/http"

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
	case http.MethodPut:
		// Writes must go to the leader — only the leader can call raft.Apply().
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
// For Phase 2, we do simple reads from local FSM
// Strong linearizable reads (VerifyLeader + Barrier) are Phase 3
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	// Simple read from local FSM for Phase 2
	value, appliedIndex, err := s.node.Get(key)
	if err != nil {
		http.Error(w, fmt.Sprintf("key not found: %v", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"value":         value,
		"applied_index": appliedIndex,
	})
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
