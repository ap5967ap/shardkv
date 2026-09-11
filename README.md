# ShardKV

> **Sharded, Raft-replicated distributed key-value store with linearizable and stale-replica reads.**

Built in Go using [`hashicorp/raft`](https://github.com/hashicorp/raft) — the same consensus library that powers Consul and Vault. This project sits *on top* of the library: the interesting parts are the system design, the correct linearizable-read protocol, the race-free shard migration procedure, and the two-tier partition-testing harness.

---

## What This Is

A horizontally-scalable KV store where:

- Data is **partitioned across multiple shards** via consistent hashing
- Each shard is a **3-node Raft group** — writes are replicated before being acknowledged
- Clients choose **`STRONG` (linearizable) or `EVENTUAL` (stale-tolerant)** reads
- A **stateless router** handles key routing, leader discovery, and consistency-level dispatch

### Architecture

```
Client
  │
  ▼
┌────────────────────────────────────────┐
│  Router  (consistent hashing, :9000)   │
│  — stateless, forwards to shard leader │
└─────────────────┬──────────────────────┘
        ┌─────────┼─────────┐
        ▼         ▼         ▼
   [Shard A]  [Shard B]  [Shard C]
   A1 A2 A3   B1 B2 B3   C1 C2 C3
   (Raft)     (Raft)     (Raft)
   (BoltDB)   (BoltDB)   (BoltDB)
```

| Component | Role |
|-----------|------|
| **Router** | Stateless. Computes target shard via consistent hashing, forwards writes and `STRONG` reads to the shard's current leader, forwards `EVENTUAL` reads to any replica. |
| **Shard node** | Independent Raft group of 3 nodes, each running the KV state machine (FSM) on top of the replicated log. |
| **FSM** | Implements `raft.FSM` — `Apply()`, `Snapshot()`, `Restore()` — backed by BoltDB. |

---

## Project Structure

```
shardkv/
├── cmd/
│   ├── node/               # Shard-node binary: boots hashicorp/raft + FSM
│   └── router/             # Router binary: consistent hashing + HTTP proxy
├── internal/
│   ├── raftfsm/            # FSM implementation: Apply(), Snapshot(), Restore()
│   ├── shard/              # Shard membership and leader tracking
│   ├── routing/            # Consistent hashing ring
│   ├── storage/            # BoltDB-backed KV storage for the FSM
│   ├── migration/          # Freeze → copy → verify → switch → unfreeze
│   ├── metrics/            # Prometheus metrics (raft stats, latency histograms)
│   └── api/                # REST handlers (net/http)
├── tests/
│   ├── unit/               # FSM apply/snapshot/restore
│   ├── integration/        # InmemTransport partition tests, consistency tests
│   └── chaos/              # Toxiproxy-driven real-process partition tests (shell)
├── scripts/
│   ├── run-cluster.sh          # Boots full 9-node cluster locally
│   ├── run-single-shard.sh     # Single 3-node shard for quick testing
│   ├── stop-cluster.sh         # Graceful cluster teardown
│   ├── kill-node.sh            # Kill specific node(s) by ID
│   ├── partition.sh            # Simulate network partitions via Toxiproxy
│   ├── toxiproxy-setup.sh      # Declare per-peer-link Toxiproxy proxies
│   └── run-cluster-with-toxiproxy.sh  # Full cluster wired through Toxiproxy
├── benchmarks/
│   ├── load_harness.go     # Go load generator (throughput + latency)
│   ├── BENCHMARKS.md       # Results and analysis
│   └── *.json              # Benchmark configs (strong-only, eventual-only, mixed)
├── docs/
│   ├── architecture.md
│   └── prometheus.yml      # Prometheus scrape config for local setup
├── start-cluster.py        # Quick-start Python script (alternative to shell)
├── runner.py               # Test runner script
└── design.md               # Full system design document
```

---

## How to Run

### Prerequisites

- Go 1.22+
- (Optional) [Toxiproxy](https://github.com/Shopify/toxiproxy) for real network partition testing
- (Optional) Prometheus + Grafana for metrics

### 1. Build

```bash
# Build both binaries
go build -o node ./cmd/node
go build -o router ./cmd/router
```

### 2. Start a Full 3-Shard Cluster (9 nodes)

**Option A — Shell script:**
```bash
./scripts/run-cluster.sh
```

**Option B — Python quick-start:**
```bash
python3 start-cluster.py
```

This starts:
- 9 node processes (A1–A3, B1–B3, C1–C3) on ports `7000–7800` (Raft) and `8100–8180` (HTTP)
- 1 router on port `9000`
- All data written to `/tmp/shardkv/`

**Stop the cluster:**
```bash
./scripts/stop-cluster.sh
```

### 3. Start a Single Shard (faster for development)

```bash
./scripts/run-single-shard.sh
# Or manually:
./node --node-id A1 --shard-id A --raft-addr 127.0.0.1:7000 \
       --http-addr 127.0.0.1:8100 --data-dir /tmp/shardkv/A1 \
       --bootstrap --peers 127.0.0.1:7000,127.0.0.1:7100,127.0.0.1:7200 \
       --peer-ids A1,A2,A3 \
       --peers-http 127.0.0.1:8100,127.0.0.1:8110,127.0.0.1:8120
```

---

## API Reference

All requests go through the **router** at `localhost:9000`.

### Write

```bash
# PUT
curl -X PUT http://localhost:9000/kv/mykey \
     -H 'Content-Type: application/json' \
     -d '{"value": "hello"}'
# → {"status": "committed", "shard": "B", "leader": "B1"}

# DELETE
curl -X DELETE http://localhost:9000/kv/mykey
```

### Read

```bash
# STRONG (linearizable) — default
curl http://localhost:9000/kv/mykey?consistency=strong
# → {"value": "hello", "consistency": "strong", "served_by": "B1"}

# EVENTUAL (stale-tolerant, served from any replica)
curl http://localhost:9000/kv/mykey?consistency=eventual
# → {"value": "hello", "consistency": "eventual", "served_by": "B2", "lag_index": 3}
```

### Cluster Status

```bash
# Shard topology + current leaders
curl http://localhost:9000/cluster/shards

# Per-node health: raft state, term, commit index, applied index
curl http://localhost:9000/cluster/health

# Prometheus metrics (per node)
curl http://localhost:8100/metrics
```

---

## Consistency Model

### The Core Design Decision: Linearizable Reads

Naively reading from "the leader" is **not** linearizable. Consider:

```
T1: Node A1 is elected leader
T2: A1 commits WRITE X=1
T3: Network partition isolates A1
T4: A2 is elected as new leader — commits X=2
T5: Client sends STRONG GET to A1 (A1 still believes it's leader)
    A1 returns X=1  ← STALE, not linearizable
```

**The fix** (`internal/raftfsm`, `internal/api`):

```go
// STRONG read path — not just "read from leader"
if err := raftNode.VerifyLeader().Error(); err != nil {
    return nil, err // partitioned ex-leader correctly rejects
}
if err := raftNode.Barrier(timeout).Error(); err != nil {
    return nil, err
}
return fsm.Get(key) // safe: linearizable
```

- **`VerifyLeader()`** — heartbeats a quorum of voters; fails immediately on a partitioned node.
- **`Barrier()`** — no-op log entry that blocks until all committed entries are applied locally. Closes the gap between "committed" and "applied on FSM."

### EVENTUAL Reads

Every write still goes through full Raft consensus — there is no weaker write path. `EVENTUAL` just skips `VerifyLeader + Barrier` and reads directly from the local FSM. The response includes `lag_index` (unapplied entry count relative to the leader's commit index), reported honestly as an index gap rather than a wall-clock ms estimate.

---

## Testing

### Unit Tests

```bash
go test ./internal/raftfsm/...    # FSM Apply/Snapshot/Restore
go test ./internal/routing/...   # Consistent hash ring
go test ./internal/migration/...  # Migration logic
```

### Integration Tests (in-process with `InmemTransport`)

```bash
go test ./tests/integration/... -v -timeout 120s
```

Key integration tests:

| Test file | What it verifies |
|-----------|-----------------|
| `verifyleader_test.go` | `STRONG` read on a partitioned (isolated) leader correctly returns an error — the core linearizability correctness test |
| `consistency_test.go` | `EVENTUAL` reads may be stale; `STRONG` reads never are |
| `shard_isolation_test.go` | A failure in one shard doesn't affect others |
| `migration_test.go` | Freeze→copy→verify→switch→unfreeze prevents stale reads during rebalancing |
| `shard_distribution_test.go` | Keys distribute across shards via consistent hashing |
| `restart_test.go` | Node restart recovers state from BoltDB log/snapshot |

### Chaos Tests (real processes + Toxiproxy)

```bash
# Kill leader, verify election and recovery
./tests/chaos/kill_leader_test.sh

# Kill 2 of 3 nodes (majority loss — shard should become unavailable, not corrupt)
./tests/chaos/kill_two_nodes_test.sh

# Partition leader mid-STRONG-read — must return error, not stale data
./tests/chaos/partition_strong_read_test.sh
```

**Setup Toxiproxy for real network partitions:**
```bash
# Start toxiproxy-server (download from https://github.com/Shopify/toxiproxy)
toxiproxy-server &

# Wire all inter-node links through Toxiproxy proxies
./scripts/toxiproxy-setup.sh

# Start cluster with Toxiproxy-routed transport
./scripts/run-cluster-with-toxiproxy.sh

# Simulate a partition (cut A1 from its peers)
./scripts/partition.sh A1
```

---

## Shard Migration (Rebalancing)

The naive approach has a race:

```
10:00  migration starts — key X copied to new shard
10:01  client updates X on the OLD shard (before router switches)
10:02  router switches to new shard
       → new shard silently serves the stale pre-update value
```

**Implemented fix — stop-the-world for the migrating key range:**

```
FREEZE → COPY → VERIFY → SWITCH → UNFREEZE
```

1. **Freeze**: old shard rejects writes to the key range (`503`), via a `FREEZE` log entry applied through Raft so all replicas see it atomically
2. **Copy**: read the frozen range from the old shard leader; `raft.Apply()` the same entries on the new shard
3. **Verify**: read back from the new shard and confirm it matches the source
4. **Switch**: update the router's ring config
5. **Unfreeze**: an `UNFREEZE` Raft entry resumes writes, now routed to the new shard

The freeze/unfreeze ops go through the Raft log (not out-of-band), so they are durable and seen by all replicas.

---

## Observability

Each node exposes Prometheus metrics at `/metrics`:

```
raft_term
raft_commit_index
raft_applied_index
raft_state                 # leader / follower / candidate
raft_leader_changes_total
kv_reads_total             # tagged by consistency level
kv_writes_total
request_latency_ms         # histogram, tagged by op type + consistency
replication_lag_index      # unapplied entries on follower
```

**Start Prometheus locally:**
```bash
./scripts/start-prometheus.sh
# Config: docs/prometheus.yml
```

---

## Benchmark Results

Benchmarked on a single host (9 nodes + router, all localhost), 30-second runs, 100 req/s, 10 concurrent workers.

### STRONG vs EVENTUAL Read Latency

| Metric | STRONG (linearizable) | EVENTUAL (stale-tolerant) | Improvement |
|--------|-----------------------|---------------------------|-------------|
| P50    | 2.90 ms               | 1.96 ms                   | **32% faster** |
| P95    | 4.28 ms               | 2.86 ms                   | **33% faster** |
| P99    | 4.92 ms               | 3.36 ms                   | 32% faster |
| Mean   | 2.95 ms               | 1.99 ms                   | 33% faster |

The `VerifyLeader() + Barrier()` round-trip is the measurable cost of true linearizability.

### Write Latency (3-shard mixed workload — 30% PUT, 70% GET)

| Operation | P50     | P95     | P99     | Success rate |
|-----------|---------|---------|---------|-------------|
| PUT       | 3.43 ms | 4.56 ms | 5.30 ms | **100%** |
| GET       | 2.76 ms | 4.42 ms | 5.12 ms | 65.7% |

PUT ops hit 100% — writes go through Raft and are acknowledged only after majority persistence.

> Full results and analysis: [`benchmarks/BENCHMARKS.md`](benchmarks/BENCHMARKS.md)

**Run benchmarks yourself:**
```bash
go build -o benchmarks/load_harness ./benchmarks/load_harness.go

# Strong reads only
./benchmarks/load_harness -config benchmarks/config_strong_only.json

# Eventual reads only
./benchmarks/load_harness -config benchmarks/config_eventual_only.json

# 3-shard mixed (30% PUT, 70% GET)
./benchmarks/load_harness -config benchmarks/config_three_shards.json

# Analyze results
python3 benchmarks/analyze_results.py benchmarks/results_strong_only.jsonl
```

---

## Things I Tried / Design Decisions

### What worked

**`VerifyLeader` + `Barrier` for linearizable reads**  
The most important correctness choice. Early versions just read from the node that self-reported as leader — which is wrong under a partition. `VerifyLeader()` actively heartbeats a quorum, and `Barrier()` ensures the FSM has caught up to the commit index before reading. Both are built into `hashicorp/raft`.

**Two-tier partition testing**  
- *Tier 1 (in-process)*: `raft.NewInmemTransport()` — same fake transport hashicorp's own test suite uses. Deterministic, fast, no real sockets. Used for the partition-during-STRONG-read correctness test that runs on every `go test`.
- *Tier 2 (real processes)*: Toxiproxy — a Go binary that proxies real TCP connections between node processes. Lets you toggle per-link partitions live without root or iptables. Used for chaos demos and measuring actual recovery times.

**Freeze/unfreeze through the Raft log**  
Rather than using an out-of-band flag to freeze a key range during migration, `FREEZE`/`UNFREEZE` are log entries that go through consensus. This means all 3 replicas in a shard see the freeze atomically — no race where some replicas are frozen and others aren't.

**BoltDB read transaction held through snapshot**  
The FSM's `Snapshot()` opens a BoltDB read transaction and holds it open through `Persist()`, releasing it in `Release()`. This is critical for point-in-time consistency — without it, a write could be applied between `Snapshot()` and `Persist()`, producing a snapshot that doesn't correspond to any single committed Raft index.

**No Docker/Compose anywhere**  
Local multi-node dev is shell scripts + OS processes on different ports. Partition testing is Toxiproxy, not container networking. This is simpler, faster to iterate on, and actually more precise: traffic must route through the Toxiproxy link to reach a peer, so there's no accidental loopback bypass.

### Future work

**Online (non-stop-the-world) migration**  
The current migration freezes the key range for a brief window. A correct online migration would dual-write during migration with versioning to resolve ordering. Scoped out in favor of getting the core system right first.

**Lag reported in wall-clock ms**  
The API returns `lag_index` (unapplied entry count) rather than `lag_ms`. A proper `lag_ms` metric would require sampling leader/follower timestamps — it's intentional to not report a number that sounds precise but isn't.

**Cross-shard transactions**  
Intentionally not in scope — would require 2PC or Percolator-style MVCC.

**Kubernetes / container orchestration**  
Explicitly cut. The time is better spent finishing the core system correctly (correct linearizable reads, real partition tests, real benchmarks) than adding a deployment layer on top.

---

## Tech Stack

| Layer | Choice | Why |
|-------|--------|-----|
| Language | Go | Goroutines map naturally onto per-peer connections and FSM apply loops |
| Consensus | `hashicorp/raft` | Production-proven (Consul, Vault); exposes `VerifyLeader()` + `Barrier()` |
| Log store | `raft-boltdb` (BoltDB) | Standard pairing with hashicorp/raft |
| FSM storage | BoltDB (`go.etcd.io/bbolt`) | Embedded KV; reuses the dependency already in the tree |
| HTTP layer | Go stdlib `net/http` | No framework needed at this scope |
| Metrics | `prometheus/client_golang` | `raft.Stats()` maps directly onto promauto gauges |
| Partition testing | `InmemTransport` + Toxiproxy | Two tiers: deterministic in-process + real TCP-level |
| Load testing | Custom Go harness | `benchmarks/load_harness.go` |

---
