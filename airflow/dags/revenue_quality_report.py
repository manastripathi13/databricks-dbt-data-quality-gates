"""Airflow DAG (manual): measure the data quality SLA and write docs/RESULTS.md.

    baseline_on_raw_drops ─► dbt_build_scorecard ─► write_report

Runs the same Great Expectations checks on each day's *raw* drop (what dashboards got
before the gates existed), rebuilds the data quality scorecard, and writes the report.
Run after revenue_quality_daily has processed the days you want to compare.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

REPO = Path(__file__).resolve().parents[2]
POOL = "spark_metastore"


def sh(cmd: str) -> str:
    return (f"set -euo pipefail; cd {REPO}; unset VIRTUAL_ENV; "
            f"export SPARK_CONF_DIR={REPO}/conf GX_ANALYTICS_ENABLED=False; {cmd} ")  # trailing space: stops Airflow treating "*.sh" as a template file


with DAG(
    dag_id="revenue_quality_report",
    description="Baseline checks on raw drops, data quality scorecard, results report",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 0},
    tags=["project-3", "data-quality", "report"],
    doc_md=__doc__,
) as dag:
    baseline = BashOperator(
        task_id="baseline_on_raw_drops",
        pool=POOL,
        bash_command=sh(
            "DATES=$(uv run python -c \"import json; print(' '.join(json.loads(l)['run_date'] for l in open('data/run_log.jsonl')))\"); "
            "uv run python quality/run_gate.py --raw --run-date $DATES 2>&1 | tr '\\r' '\\n' | grep -E '^\\[gate\\]|CRITICAL' || true"
        ),
    )
    scorecard = BashOperator(
        task_id="dbt_build_scorecard",
        pool=POOL,
        bash_command=sh("cd dbt && uv run dbt build --profiles-dir . --no-use-colors 2>&1 | grep -vE 'HiveAlterHandler|^\\s+at '; exit ${PIPESTATUS[0]}"),
    )
    report = BashOperator(
        task_id="write_report",
        pool=POOL,
        bash_command=sh("uv run python scripts/report.py 2>&1 | grep -vE 'WARN|ivy|::|found |artifact|jars|confs|log4j|setLogLevel'"),
    )
    baseline >> scorecard >> report
