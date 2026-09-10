#!/usr/bin/env bash

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

fail=0

wait_for_router() {
    local url="http://127.0.0.1:9000/cluster/shards"
    local max_wait=30
    local waited=0

    while [ "$waited" -lt "$max_wait" ]; do
        if curl -fsS "$url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done

    echo "Router did not become ready in time at $url" >&2
    return 1
}

start_cluster() {
    local script_path="$1"
    local log_path="$2"

    pkill -f "./node" 2>/dev/null || true
    pkill -f "./router" 2>/dev/null || true
    pkill -f "toxiproxy-server.*8474" 2>/dev/null || true
    rm -rf /tmp/shardkv

    echo "Starting cluster via $script_path"
    bash "$script_path" >"$log_path" 2>&1 &
    local pid=$!

    if ! wait_for_router; then
        echo "Cluster startup log:" >&2
        cat "$log_path" >&2 || true
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        return 1
    fi

    echo "Cluster ready (PID $pid)"
    echo "$pid"
}

stop_cluster() {
    local pid="$1"

    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi

    pkill -f "./node" 2>/dev/null || true
    pkill -f "./router" 2>/dev/null || true
    pkill -f "toxiproxy-server.*8474" 2>/dev/null || true
}

run_case() {
    local label="$1"
    shift

    echo
    echo "===== $label ====="
    if "$@"; then
        echo "$label: PASS"
        return 0
    else
        local code=$?
        echo "$label: FAIL (exit $code)" >&2
        return "$code"
    fi
}

cluster_pid=""

trap 'if [ -n "${cluster_pid:-}" ]; then stop_cluster "$cluster_pid"; fi' EXIT

cluster_pid="$(start_cluster "./scripts/run-cluster.sh" "/tmp/phase4_cluster.log")" || exit 1

run_case "leader_kill_recovery" bash tests/chaos/kill_leader_test.sh || fail=1

stop_cluster "$cluster_pid"
cluster_pid=""

cluster_pid="$(start_cluster "./scripts/run-cluster-with-toxiproxy.sh" "/tmp/phase4_toxiproxy_cluster.log")" || exit 1

set +e
bash tests/chaos/partition_strong_read_test.sh > /tmp/phase4_partition.log 2>&1
partition_status=$?
set -e
cat /tmp/phase4_partition.log
if [ "$partition_status" -eq 0 ]; then
    echo "partition_strong_read_test: PASS"
else
    echo "partition_strong_read_test: FAIL (exit $partition_status)" >&2
    fail=1
fi

stop_cluster "$cluster_pid"
cluster_pid=""

cluster_pid="$(start_cluster "./scripts/run-cluster.sh" "/tmp/phase4_noquorum_cluster.log")" || exit 1

set +e
bash tests/chaos/kill_two_nodes_test.sh > /tmp/phase4_noquorum.log 2>&1
noquorum_status=$?
set -e
cat /tmp/phase4_noquorum.log
if [ "$noquorum_status" -eq 1 ]; then
    echo "kill_two_nodes_test: PASS (correctly failed under no quorum)"
else
    echo "kill_two_nodes_test: FAIL (unexpected exit status $noquorum_status)" >&2
    fail=1
fi

stop_cluster "$cluster_pid"
cluster_pid=""

if [ "$fail" -ne 0 ]; then
    echo
    echo "Phase 4 verification: FAIL"
    exit 1
fi

echo
echo "Phase 4 verification: PASS"
exit 0
