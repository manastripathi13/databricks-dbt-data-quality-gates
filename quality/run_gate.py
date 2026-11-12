"""Run the Great Expectations quality gate for one business day.

    python quality/run_gate.py --run-date 2026-06-05            # gate the Silver layer (exit 1 = block Gold)
    python quality/run_gate.py --run-date 2026-06-05 --raw      # same checks on the raw drop (baseline only)

Results go to the Delta table `dq.validation_results` (one row per expectation per run)
and to Great Expectations Data Docs (HTML) in data/gx/.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as gx
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from quality.expectations import build_suites, suite_from  # noqa: E402
from scripts.spark import get_spark  # noqa: E402


def silver_frames(spark, run_date: str) -> dict[str, DataFrame]:
    return {
        # usage is gated per business day: the rows that will feed today's Gold refresh
        "silver_usage": spark.table("revenue.silver_usage").where(F.col("received_at").cast("date") == run_date),
        "silver_subscriptions": spark.table("revenue.silver_subscriptions"),
        "silver_opportunities": spark.table("revenue.silver_opportunities"),
        "silver_accounts": spark.table("revenue.silver_accounts"),
    }


def raw_frames(spark, run_date: str) -> dict[str, DataFrame]:
    """The "before" baseline: what the old batch jobs delivered to dashboards.

    They kept the latest version of each CRM/billing record, but had no validation and no
    quarantine, and appended usage events as received.
    """
    from pyspark.sql import Window

    land = ROOT / "data" / "landing"
    day = f"day={run_date}"

    def latest(df: DataFrame, key: str) -> DataFrame:
        w = Window.partitionBy(key).orderBy(F.col("updated_at").desc())
        return df.select("*", F.row_number().over(w).alias("_rn")).where("_rn = 1")

    usage = spark.read.json(str(land / "usage" / day)).select(
        "event_id", "account_id", "product", F.to_date("usage_date").alias("usage_date"), "api_units")
    subs = latest(spark.read.json(str(land / "subscriptions" / day)), "subscription_id")
    opps = latest(spark.read.option("header", True).option("inferSchema", True).csv(str(land / "opportunities" / day)), "opportunity_id")
    accounts = spark.read.option("header", True).csv(str(land / "accounts" / day))
    return {"silver_usage": usage,
            "silver_subscriptions": subs.select("subscription_id", "account_id", "mrr", "currency", "status", "seats"),
            "silver_opportunities": opps.select("opportunity_id", "account_id", "amount_usd", "stage"),
            "silver_accounts": accounts}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True, nargs="+", help="one or more business dates")
    ap.add_argument("--raw", action="store_true", help="validate the raw drop instead of Silver (baseline)")
    a = ap.parse_args()

    spark = get_spark("dq-gate")
    context = gx.get_context(mode="file", project_root_dir=str(ROOT / "data" / "gx"))
    source = context.data_sources.add_or_update_spark(name="raw" if a.raw else "silver")
    exit_code = 0
    for run_date in a.run_date:
        exit_code |= gate_one_day(spark, context, source, run_date, a.raw)
    return exit_code


def gate_one_day(spark, context, source, run_date: str, raw: bool) -> int:
    frames = raw_frames(spark, run_date) if raw else silver_frames(spark, run_date)
    known_accounts = [r[0] for r in spark.table("revenue.silver_accounts").select("account_id").collect()]
    hist = spark.table("revenue.silver_usage").groupBy(F.col("received_at").cast("date").alias("d")).count().collect()
    counts = [r["count"] for r in hist if str(r["d"]) < run_date]
    avg = sum(counts) / len(counts) if counts else 100
    suites = build_suites(run_date, known_accounts, (int(avg * 0.7), int(avg * 1.3)))
    run_ts = datetime.now(timezone.utc)
    rows, critical_failures = [], []

    for table, expectations in suites.items():
        suite = context.suites.add_or_update(suite_from(f"{table}_suite", expectations))
        try:
            asset = source.get_asset(table)
        except LookupError:
            asset = source.add_dataframe_asset(name=table)
        try:
            batch_def = asset.get_batch_definition("whole")
        except KeyError:
            batch_def = asset.add_batch_definition_whole_dataframe("whole")
        vd = context.validation_definitions.add_or_update(
            gx.ValidationDefinition(name=f"{source.name}_{table}", data=batch_def, suite=suite))
        checkpoint = context.checkpoints.add_or_update(gx.Checkpoint(
            name=f"{source.name}_{table}_gate", validation_definitions=[vd],
            actions=[gx.checkpoint.UpdateDataDocsAction(name="docs")],
            result_format={"result_format": "SUMMARY"}))
        result = checkpoint.run(batch_parameters={"dataframe": frames[table]})

        for vr in result.run_results.values():
            for r in vr.results:
                cfg = r.expectation_config
                sev = (cfg.meta or {}).get("severity", "critical")
                observed = r.result.get("unexpected_count", r.result.get("observed_value"))
                rows.append((run_date, "raw" if raw else "silver", table, cfg.type, json.dumps(cfg.kwargs.get("column")),
                             sev, bool(r.success), str(observed)[:200], run_ts))
                if not r.success and sev == "critical":
                    critical_failures.append(f"{table}: {cfg.type}({cfg.kwargs.get('column', '')}) observed={observed}")

    schema = ("run_date string, layer string, table_name string, expectation string, column_name string, "
              "severity string, success boolean, observed string, validated_at timestamp")
    spark.createDataFrame(rows, schema).write.format("delta").mode("append").saveAsTable("dq.validation_results")

    passed = sum(r[6] for r in rows)
    print(f"[gate] {run_date} {'raw' if raw else 'silver'}: {passed}/{len(rows)} expectations passed, "
          f"{len(critical_failures)} critical failure(s)")
    for f in critical_failures:
        print("       CRITICAL", f)
    return 1 if critical_failures else 0


if __name__ == "__main__":
    sys.exit(main())
