"""Airflow DAG (manual): wipe all local state for a fresh simulation.

Deletes landing files, Delta tables, the metastore, pipeline checkpoints and the run log
(everything under data/). After this, clear and re-run revenue_quality_daily.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

REPO = Path(__file__).resolve().parents[2]

with DAG(
    dag_id="revenue_quality_reset",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["project-3", "maintenance"],
    doc_md=__doc__,
) as dag:
    BashOperator(task_id="reset_local_state", pool="spark_metastore",
                 bash_command=f"cd {REPO} && ./scripts/reset.sh && rm -f data/run_log.jsonl && echo 'local state wiped'")
