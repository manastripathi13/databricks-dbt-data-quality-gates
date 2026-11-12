"""Databricks-native version of the pipeline (Lakeflow Declarative Pipelines).

Same Bronze/Silver design as pipelines/transformations/, but using features that only
exist on Databricks:
  - Auto Loader (cloudFiles) for incremental file ingestion from ADLS
  - expectations (@dp.expect_all / expect_all_or_drop) recorded in the pipeline event log
  - create_auto_cdc_flow with stored_as_scd_type=2 for subscription history

Deployed by databricks/databricks.yml. This file is NOT executed by the local demo;
the local demo runs pipelines/transformations/ on open-source Spark.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("revenue.landing_path")  # noqa: F821  e.g. abfss://landing@<storage>.dfs.core.windows.net

USAGE_RULES = {
    "account_id_present": "account_id IS NOT NULL",
    "units_non_negative": "api_units >= 0",
    "usage_date_not_future": "usage_date <= current_date()",
}
SUB_RULES = {
    "mrr_non_negative": "mrr >= 0",
    "currency_supported": "currency IN ('USD','EUR','GBP','INR','AED','SGD')",
    "account_id_present": "account_id IS NOT NULL",
}


def autoloader(entity: str, fmt: str):
    return (spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"{LANDING}/{entity}")
            .select("*", F.current_timestamp().alias("_ingested_at"), F.col("_metadata.file_path").alias("_source_file")))


@dp.table(comment="Raw usage events (Auto Loader).")
def bronze_usage():
    return autoloader("usage", "json")


@dp.table(comment="Raw subscription change feed (Auto Loader).")
def bronze_subscriptions_cdc():
    return autoloader("subscriptions", "json")


@dp.table(comment="Validated usage; rows breaking a rule are dropped here and counted in the event log.")
@dp.expect_all_or_drop(USAGE_RULES)
def silver_usage():
    return (spark.readStream.table("bronze_usage")  # noqa: F821
            .withWatermark("_ingested_at", "1 day")
            .dropDuplicatesWithinWatermark(["event_id"])
            .withColumn("usage_date", F.to_date("usage_date")))


@dp.table(comment="Usage rows that broke at least one rule (quarantine).")
def quarantine_usage():
    broken = " OR ".join(f"NOT ({c})" for c in USAGE_RULES.values())
    return spark.readStream.table("bronze_usage").where(broken)  # noqa: F821


@dp.view
@dp.expect_all_or_drop(SUB_RULES)
def subscriptions_changes_valid():
    return spark.readStream.table("bronze_subscriptions_cdc").withColumn("updated_at", F.to_timestamp("updated_at"))  # noqa: F821


dp.create_streaming_table("silver_subscriptions_history")
dp.create_auto_cdc_flow(
    target="silver_subscriptions_history",
    source="subscriptions_changes_valid",
    keys=["subscription_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=2,
    except_column_list=["op", "_ingested_at", "_source_file"],
)

dp.create_streaming_table("silver_subscriptions")
dp.create_auto_cdc_flow(
    target="silver_subscriptions",
    source="subscriptions_changes_valid",
    keys=["subscription_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=1,
    except_column_list=["op", "_ingested_at", "_source_file"],
)
