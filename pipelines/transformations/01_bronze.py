"""BRONZE: ingest daily landing files as-is into Delta streaming tables.

A streaming table processes each new file exactly once (checkpointed), so re-running
the pipeline only picks up files that arrived since the last run, like Databricks Auto Loader.
"""

import os

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LANDING = os.environ["LANDING_PATH"]


def _with_load_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_file", F.col("_metadata.file_path"))


@dp.table(comment="CRM accounts, daily full snapshot, raw.")
def bronze_accounts() -> DataFrame:
    schema = "account_id string, account_name string, segment string, region string, csm_owner string"
    return _with_load_metadata(
        spark.readStream.format("csv").option("header", True).schema(schema).load(f"{LANDING}/accounts")  # noqa: F821
    )


@dp.table(comment="CRM opportunities created or changed each day, raw.")
def bronze_opportunities() -> DataFrame:
    schema = ("opportunity_id string, account_id string, product string, type string, stage string, "
              "amount_usd double, close_date string, updated_at string")
    return _with_load_metadata(
        spark.readStream.format("csv").option("header", True).schema(schema).load(f"{LANDING}/opportunities")  # noqa: F821
    )


@dp.table(comment="Billing CDC feed: every change to a subscription, raw.")
def bronze_subscriptions_cdc() -> DataFrame:
    schema = ("subscription_id string, account_id string, product string, seats int, mrr double, currency string, "
              "included_units long, status string, start_date string, end_date string, updated_at string, op string")
    return _with_load_metadata(
        spark.readStream.format("json").schema(schema).load(f"{LANDING}/subscriptions")  # noqa: F821
    )


@dp.table(comment="Product metering: API units per account per day, raw.")
def bronze_usage() -> DataFrame:
    schema = ("event_id string, account_id string, subscription_id string, product string, "
              "usage_date string, api_units long, received_at string")
    return _with_load_metadata(
        spark.readStream.format("json").schema(schema).load(f"{LANDING}/usage")  # noqa: F821
    )
