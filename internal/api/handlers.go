package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"shardkv/internal/raftfsm"
)

// Node represents the interface that the API handlers need from the node
type Node interface {
	ApplyOperation(op raftfsm.Operation) error
	Get(key string) (string, uint64, error)
	IsLeader() bool
	LeaderAddr() string
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
}

// handleKV handles PUT, GET, DELETE requests for keys
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	// Extract key from path
	key := r.URL.Path[len("/kv/"):]
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	// Check if this node is the leader
	if !s.node.IsLeader() {
		leaderAddr := s.node.LeaderAddr()
		if leaderAddr == "" {
			http.Error(w, "no leader available", http.StatusServiceUnavailable)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"error":  "not leader",
			"leader": leaderAddr,
		})
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodDelete:
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
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
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
		http.Error(w, fmt.Sprintf("failed to apply operation: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "deleted",
	})
}
