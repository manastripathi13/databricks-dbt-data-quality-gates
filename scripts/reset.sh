#!/usr/bin/env bash
# Wipe all local state (landing files, tables, metastore, checkpoints) for a fresh run.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
for d in landing warehouse metastore_db pipeline-storage dq_results dbt_target; do
  rm -rf "$ROOT/data/$d"
done
rm -f "$ROOT/derby.log" "$ROOT/pipelines/derby.log"
