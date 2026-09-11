#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_SCRIPT="${ROOT_DIR}/scripts/run-cluster.sh"

if [[ ! -f "${CLUSTER_SCRIPT}" ]]; then
  echo "Cluster script not found: ${CLUSTER_SCRIPT}" >&2
  exit 1
fi

if [[ ! -x "${ROOT_DIR}/node" ]] || [[ ! -x "${ROOT_DIR}/router" ]]; then
  echo "Building shardkv binaries before starting the cluster..."
  cd "${ROOT_DIR}"
  go build -o node ./cmd/node
  go build -o router ./cmd/router
fi

echo "Starting cluster via ${CLUSTER_SCRIPT}"
exec bash "${CLUSTER_SCRIPT}"
