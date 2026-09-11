#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROMETHEUS_BIN="${PROMETHEUS_BIN:-prometheus}"
PROMETHEUS_DATA_DIR="${PROMETHEUS_DATA_DIR:-/tmp/prometheus-data}"

mkdir -p "${PROMETHEUS_DATA_DIR}"

exec "${PROMETHEUS_BIN}" \
  --config.file="${ROOT_DIR}/docs/prometheus.yml" \
  --storage.tsdb.path="${PROMETHEUS_DATA_DIR}"
