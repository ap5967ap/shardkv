# Architecture notes

## Prometheus dashboard setup

This repo already exposes Prometheus metrics from each node at `/metrics`, so a dashboard can be attached without changing the Go nodes themselves.

### Start Prometheus

```bash
./scripts/start-prometheus.sh
```

Then open:

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (if you also run Grafana)

### Scrape targets

The config in [docs/prometheus.yml](prometheus.yml) scrapes all nine shard replicas:

- 127.0.0.1:8100
- 127.0.0.1:8110
- 127.0.0.1:8120
- 127.0.0.1:8130
- 127.0.0.1:8140
- 127.0.0.1:8150
- 127.0.0.1:8160
- 127.0.0.1:8170
- 127.0.0.1:8180

### Useful Prometheus queries

```promql
raft_leader_changes_total
```

```promql
raft_state{node_id="A1"}
```

```promql
rate(kv_writes_total[1m])
```

```promql
histogram_quantile(0.95, sum by (le) (rate(request_latency_ms_bucket[1m])))
```

```promql
raft_commit_index - raft_applied_index
```

These are the queries to use when validating leader failover, write throughput, replication lag, and latency differences under load.

### Demo flow

1. Start the cluster.
2. Start Prometheus via the helper script.
3. Run a kill-node or partition scenario.
4. Capture screenshots of the leader-change and replication-lag panels.

Suggested screenshot paths:

- /tmp/prometheus-leader-change.png
- /tmp/prometheus-replication-lag.png
- /tmp/prometheus-write-throughput.png
