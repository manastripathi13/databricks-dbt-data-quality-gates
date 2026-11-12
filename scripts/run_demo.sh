#!/usr/bin/env bash
# Full local run: 10 business days through land -> pipeline -> quality gate -> dbt Gold, then the report.
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/reset.sh
uv run python scripts/render_spark_conf.py >/dev/null
uv run python scripts/init_metastore.py >/dev/null 2>&1
uv run python -u scripts/orchestrate.py "${1:-10}"
uv run python scripts/report.py
