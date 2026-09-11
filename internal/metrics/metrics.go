package metrics

import (
	"fmt"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Raft metrics
	// Use labeled GaugeVecs for term/commit/applied so multiple nodes/shards
	// do not overwrite the same metric series.
	RaftTerm = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "raft_term",
		Help: "Current Raft term",
	}, []string{"node_id", "shard_id"})

	RaftCommitIndex = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "raft_commit_index",
		Help: "Current Raft commit index",
	}, []string{"node_id", "shard_id"})

	RaftAppliedIndex = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "raft_applied_index",
		Help: "Current Raft applied index",
	}, []string{"node_id", "shard_id"})

	RaftState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "raft_state",
		Help: "Current Raft state (0=follower, 1=candidate, 2=leader)",
	}, []string{"node_id", "shard_id"})

	RaftLeaderChangesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "raft_leader_changes_total",
		Help: "Total number of Raft leader changes",
	}, []string{"node_id", "shard_id"})

	// KV operation metrics
	KVReadsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kv_reads_total",
		Help: "Total number of KV read operations",
	}, []string{"node_id", "shard_id", "consistency"})

	KVWritesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kv_writes_total",
		Help: "Total number of KV write operations",
	}, []string{"node_id", "shard_id", "operation"})

	RequestLatencyMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "request_latency_ms",
		Help:    "Request latency in milliseconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"node_id", "shard_id", "operation", "consistency"})

	ReplicationLagMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "replication_lag_ms",
		Help: "Replication lag in milliseconds (estimate based on index gap)",
	}, []string{"node_id", "shard_id"})
)

// MetricsCollector wraps a Raft instance and collects metrics
type MetricsCollector struct {
	raft      *raft.Raft
	nodeID    string
	shardID   string
	lastTerm  uint64
	lastState raft.RaftState
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(raftInstance *raft.Raft, nodeID, shardID string) *MetricsCollector {
	return &MetricsCollector{
		raft:      raftInstance,
		nodeID:    nodeID,
		shardID:   shardID,
		lastTerm:  0,
		lastState: raft.Follower,
	}
}

// Collect updates all Prometheus metrics from the Raft instance
func (m *MetricsCollector) Collect() {
	if m.raft == nil {
		return
	}

	// Get Raft stats
	stats := m.raft.Stats()

	// Update term
	if termStr, ok := stats["term"]; ok {
		var term uint64
		fmt.Sscanf(termStr, "%d", &term)
		RaftTerm.WithLabelValues(m.nodeID, m.shardID).Set(float64(term))

		// Track leader changes
		if term > m.lastTerm {
			RaftLeaderChangesTotal.WithLabelValues(m.nodeID, m.shardID).Inc()
			m.lastTerm = term
		}
	}

	// Update commit index
	if commitIndexStr, ok := stats["commit_index"]; ok {
		var commitIndex uint64
		fmt.Sscanf(commitIndexStr, "%d", &commitIndex)
		RaftCommitIndex.WithLabelValues(m.nodeID, m.shardID).Set(float64(commitIndex))
	}

	// Update applied index
	if appliedIndexStr, ok := stats["applied_index"]; ok {
		var appliedIndex uint64
		fmt.Sscanf(appliedIndexStr, "%d", &appliedIndex)
		RaftAppliedIndex.WithLabelValues(m.nodeID, m.shardID).Set(float64(appliedIndex))
	}

	// Update state
	currentState := m.raft.State()
	stateValue := 0.0
	switch currentState {
	case raft.Follower:
		stateValue = 0.0
	case raft.Candidate:
		stateValue = 1.0
	case raft.Leader:
		stateValue = 2.0
	}
	RaftState.WithLabelValues(m.nodeID, m.shardID).Set(stateValue)

	// Track state changes (especially for leader changes)
	if currentState != m.lastState {
		if currentState == raft.Leader && m.lastState != raft.Leader {
			// Became leader
			RaftLeaderChangesTotal.WithLabelValues(m.nodeID, m.shardID).Inc()
		}
		m.lastState = currentState
	}

	// Estimate replication lag based on index gap
	commitIndexStr, commitOk := stats["commit_index"]
	appliedIndexStr, appliedOk := stats["applied_index"]
	if commitOk && appliedOk {
		var commitIndex, appliedIndex uint64
		fmt.Sscanf(commitIndexStr, "%d", &commitIndex)
		fmt.Sscanf(appliedIndexStr, "%d", &appliedIndex)

		if commitIndex > appliedIndex {
			// This is a rough estimate - true lag would require timestamp tracking
			// For now, we report the index gap as a proxy for lag
			ReplicationLagMs.WithLabelValues(m.nodeID, m.shardID).Set(float64(commitIndex - appliedIndex))
		} else {
			ReplicationLagMs.WithLabelValues(m.nodeID, m.shardID).Set(0)
		}
	}
}

// RecordRead records a read operation
func RecordRead(nodeID, shardID, consistency string) {
	KVReadsTotal.WithLabelValues(nodeID, shardID, consistency).Inc()
}

// RecordWrite records a write operation
func RecordWrite(nodeID, shardID, operation string) {
	KVWritesTotal.WithLabelValues(nodeID, shardID, operation).Inc()
}

// RecordLatency records request latency
func RecordLatency(nodeID, shardID, operation, consistency string, latencyMs float64) {
	RequestLatencyMs.WithLabelValues(nodeID, shardID, operation, consistency).Observe(latencyMs)
}
