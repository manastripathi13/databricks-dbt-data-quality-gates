"""Airflow DAG: one business day of the revenue data platform with an automated quality gate.

    prepare ─► land_daily_drop ─► run_lakeflow_pipeline ─► quality_gate ─► dbt_build_gold ─┐
                                                               └─(fails)─► alert_gold_blocked ├─► record_run
Steps
  land_daily_drop        the day's CRM, billing CDC and usage files arrive in the landing zone
  run_lakeflow_pipeline  Bronze -> Silver with quarantine tables and SCD2 history (Spark Declarative Pipelines)
  quality_gate           Great Expectations on Silver. A critical failure fails this task, so
                         dbt_build_gold does NOT run and dashboards keep yesterday's correct numbers
  alert_gold_blocked     runs only when the gate fails (ADF sends a Teams message in production)
  record_run             always runs; writes the day's outcome for the SLA report

The schedule covers 2026-06-01 to 2026-06-10 with catch-up, so unpausing the DAG plays the
10 simulated business days in order. Spark tasks share the `spark_metastore` pool (1 slot),
because the local metastore allows one Spark process at a time.
Mirrors adf/pl_revenue_daily.json (Azure Data Factory) and databricks/databricks.yml.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

REPO = Path(__file__).resolve().parents[2]
START = datetime(2026, 6, 1)
POOL = "spark_metastore"


def sh(cmd: str) -> str:
    return (f"set -euo pipefail; cd {REPO}; unset VIRTUAL_ENV; "
            f"export SPARK_CONF_DIR={REPO}/conf GX_ANALYTICS_ENABLED=False; {cmd} ")  # trailing space: stops Airflow treating "*.sh" as a template file


RUN_DATE = "{{ params.run_date or data_interval_start.strftime('%Y-%m-%d') }}"
DAY_NO = ("{{ ((macros.datetime.strptime(params.run_date, '%Y-%m-%d').date() if params.run_date "
          "else data_interval_start.date()) - dag.start_date.date()).days + 1 }}")

with DAG(
    dag_id="revenue_quality_daily",
    description="Land -> Lakeflow-style pipeline -> Great Expectations gate -> dbt Gold (only if the gate passes)",
    start_date=START,
    end_date=datetime(2026, 6, 10),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    params={"run_date": Param(None, type=["null", "string"], description="YYYY-MM-DD between 2026-06-01 and 2026-06-10")},
    default_args={"owner": "data-engineering", "retries": 0},
    tags=["project-3", "data-quality", "great-expectations", "delta", "dbt", "revenue"],
    doc_md=__doc__,
) as dag:
    prepare = BashOperator(
        task_id="prepare",
        pool=POOL,
        bash_command=sh("uv run python scripts/render_spark_conf.py && "
                        "uv run python scripts/init_metastore.py 2>&1 | tail -1"),
    )
    land = BashOperator(
        task_id="land_daily_drop",
        bash_command=sh(f"uv run python generator/generate_daily_drops.py --days 10 --only {DAY_NO}"),
    )
    pipeline = BashOperator(
        task_id="run_lakeflow_pipeline",
        pool=POOL,
        bash_command=sh("./scripts/run_pipeline.sh 2>&1 | grep -E '(COMPLETED|FAILED)|Run is' | cut -c1-160; exit ${PIPESTATUS[0]}"),
    )
    gate = BashOperator(
        task_id="quality_gate",
        pool=POOL,
        bash_command=sh(f"uv run python quality/run_gate.py --run-date {RUN_DATE} 2>&1 | tr '\\r' '\\n' "
                        "| grep -E '^\\[gate\\]|CRITICAL'; exit ${PIPESTATUS[0]}"),
    )
    gold = BashOperator(
        task_id="dbt_build_gold",
        pool=POOL,
        bash_command=sh("cd dbt && uv run dbt build --profiles-dir . --no-use-colors 2>&1 | grep -vE 'HiveAlterHandler|^\\s+at '; exit ${PIPESTATUS[0]}"),
    )
    alert = BashOperator(
        task_id="alert_gold_blocked",
        trigger_rule="one_failed",
        bash_command=f"echo 'ALERT: Gold NOT refreshed for {RUN_DATE}: critical data quality failure. "
                     "Dashboards keep the last good data. See dq.validation_results and Great Expectations Data Docs.'",
    )
    record = BashOperator(
        task_id="record_run",
        trigger_rule="all_done",
        bash_command=sh(
            f"uv run python scripts/record_run.py {RUN_DATE} "
            # a task that succeeded has pushed its last output line to XCom; failed/skipped ones have not
            + " ".join("{{ 'success' if ti.xcom_pull(task_ids='%s') is not none else 'failed' }}" % t
                       for t in ["run_lakeflow_pipeline", "quality_gate", "dbt_build_gold"])
        ),
    )
    prepare >> land >> pipeline >> gate >> gold >> record
    gate >> alert >> record
    # record_run always runs, but as a teardown it does not decide the run's colour:
    # a failed Gold build or a blocked gate still shows the run as failed in Airflow.
    record.as_teardown()
