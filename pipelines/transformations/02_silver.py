"""SILVER: typed, de-duplicated, validated data, with bad rows quarantined instead of dropped.

Quarantine pattern: each Silver dataset has a set of rules. Rows that pass every rule
go to the Silver table. Rows that fail go to a `quarantine_*` table with the list of
rules they broke, so an engineer sees exactly what was wrong and which file it came from.
Good data keeps flowing and bad data never reaches Gold.

Subscriptions arrive as a change feed. A window function turns the changes into full
history (SCD Type 2: one row per version with valid_from / valid_to / is_current), and the
current state (SCD Type 1) is the latest version. On Databricks both are one call each to
`create_auto_cdc_flow` (see databricks/lakeflow_pipeline.py).

Note: transformations use `select` with explicit columns (not withColumn/drop), which keeps
every dataset definition lazy so the pipeline can resolve dependencies in any order.
"""

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

SUPPORTED_CURRENCIES = ["USD", "EUR", "GBP", "INR", "AED", "SGD"]

RULES = {
    "usage": {
        "account_id_present": "account_id IS NOT NULL",
        "units_non_negative": "api_units >= 0",
        "usage_date_not_future": "usage_date <= current_date()",
    },
    "subscriptions": {
        "mrr_non_negative": "mrr >= 0",
        "currency_supported": f"currency IN ({', '.join(repr(c) for c in SUPPORTED_CURRENCIES)})",
        "account_id_present": "account_id IS NOT NULL",
    },
    "opportunities": {
        "amount_positive": "amount_usd > 0",
        "account_exists": "account_known",
    },
}

USAGE_COLS = ["event_id", "account_id", "subscription_id", "product", "usage_date", "api_units", "received_at", "_source_file"]
OPP_COLS = ["opportunity_id", "account_id", "product", "type", "stage", "amount_usd", "close_date", "updated_at", "_source_file"]
SUB_COLS = ["subscription_id", "account_id", "product", "seats", "mrr", "currency", "included_units", "status",
            "start_date", "end_date", "updated_at"]


def failed_rules(rules: dict[str, str]) -> Column:
    """Array with the names of the rules a row breaks (empty array = valid row)."""
    checks = [F.when(~F.expr(cond), F.lit(name)) for name, cond in rules.items()]
    return F.filter(F.array(*checks), lambda x: x.isNotNull()).alias("_failed_rules")


# ---------- usage ----------
@dp.temporary_view()
def usage_checked() -> DataFrame:
    latest_first = Window.partitionBy("event_id").orderBy(F.col("_ingested_at").desc())
    typed = spark.read.table("bronze_usage").select(  # noqa: F821
        "event_id", "account_id", "subscription_id", "product",
        F.to_date("usage_date").alias("usage_date"),
        "api_units",
        F.to_timestamp("received_at").alias("received_at"),
        "_source_file",
        F.row_number().over(latest_first).alias("_copy"),  # the same event can arrive twice
    )
    return typed.where("_copy = 1").select(*USAGE_COLS, failed_rules(RULES["usage"]))


@dp.materialized_view(comment="Validated daily product usage, one row per event.")
def silver_usage() -> DataFrame:
    return spark.read.table("usage_checked").where(F.size("_failed_rules") == 0).select(*USAGE_COLS)  # noqa: F821


@dp.materialized_view(comment="Usage rows that failed validation, with the rules they broke.")
def quarantine_usage() -> DataFrame:
    return spark.read.table("usage_checked").where(F.size("_failed_rules") > 0)  # noqa: F821


# ---------- accounts ----------
@dp.materialized_view(comment="Latest CRM account record per account_id.")
def silver_accounts() -> DataFrame:
    w = Window.partitionBy("account_id").orderBy(F.col("_ingested_at").desc())
    ranked = spark.read.table("bronze_accounts").select(  # noqa: F821
        "account_id", "account_name", "segment", "region", "csm_owner", "_ingested_at",
        F.row_number().over(w).alias("_rn"),
    )
    return ranked.where("_rn = 1").select("account_id", "account_name", "segment", "region", "csm_owner", "_ingested_at")


# ---------- opportunities ----------
@dp.temporary_view()
def opportunities_checked() -> DataFrame:
    w = Window.partitionBy("opportunity_id").orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
    latest = spark.read.table("bronze_opportunities").select(  # noqa: F821
        "opportunity_id", "account_id", "product", "type", "stage", "amount_usd",
        F.to_date("close_date").alias("close_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
        "_source_file",
        F.row_number().over(w).alias("_rn"),
    ).where("_rn = 1")
    known = spark.read.table("silver_accounts").select("account_id", F.lit(True).alias("account_known"))  # noqa: F821
    joined = latest.join(known, "account_id", "left").select(
        *OPP_COLS, F.coalesce("account_known", F.lit(False)).alias("account_known")
    )
    return joined.select(*OPP_COLS, failed_rules(RULES["opportunities"]))


@dp.materialized_view(comment="Latest version of each valid opportunity.")
def silver_opportunities() -> DataFrame:
    return spark.read.table("opportunities_checked").where(F.size("_failed_rules") == 0).select(*OPP_COLS)  # noqa: F821


@dp.materialized_view(comment="Opportunities that failed validation.")
def quarantine_opportunities() -> DataFrame:
    return spark.read.table("opportunities_checked").where(F.size("_failed_rules") > 0)  # noqa: F821


# ---------- subscriptions (change data capture) ----------
@dp.temporary_view()
def subscriptions_changes_checked() -> DataFrame:
    typed = spark.readStream.table("bronze_subscriptions_cdc").select(  # noqa: F821
        "subscription_id", "account_id", "product", "seats", "mrr", "currency", "included_units", "status",
        F.to_date("start_date").alias("start_date"),
        F.to_date("end_date").alias("end_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
        "_source_file",
    )
    return typed.select(*SUB_COLS, "_source_file", failed_rules(RULES["subscriptions"]))


@dp.table(comment="Subscription changes that failed validation.")
def quarantine_subscriptions() -> DataFrame:
    return spark.readStream.table("subscriptions_changes_checked").where(F.size("_failed_rules") > 0)  # noqa: F821


@dp.materialized_view(comment="Every version of every subscription (SCD Type 2): valid_from, valid_to, is_current.")
def silver_subscriptions_history() -> DataFrame:
    """SCD Type 2 built explicitly: each change becomes a row valid until the next change.

    On Databricks this is one call: create_auto_cdc_flow(..., stored_as_scd_type=2).
    Open-source Spark 4.2 auto CDC needs a target type local Delta does not expose, so here it is by hand.
    """
    changes = spark.read.table("bronze_subscriptions_cdc").select(  # noqa: F821
        "subscription_id", "account_id", "product", "seats", "mrr", "currency", "included_units", "status",
        F.to_date("start_date").alias("start_date"),
        F.to_date("end_date").alias("end_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
    )
    same_change = Window.partitionBy("subscription_id", "updated_at").orderBy("mrr")
    valid = (changes.where(F.size(failed_rules(RULES["subscriptions"])) == 0)
             .select(*SUB_COLS, F.row_number().over(same_change).alias("_copy"))
             .where("_copy = 1"))  # a re-sent change counts once
    by_sub = Window.partitionBy("subscription_id").orderBy("updated_at")
    return valid.select(
        *SUB_COLS,
        F.col("updated_at").alias("valid_from"),
        F.lead("updated_at").over(by_sub).alias("valid_to"),
        F.lead("updated_at").over(by_sub).isNull().alias("is_current"),
    )


@dp.materialized_view(comment="Current state of every subscription (SCD Type 1): the latest valid version.")
def silver_subscriptions() -> DataFrame:
    # On Databricks: create_auto_cdc_flow(..., stored_as_scd_type=1) (see databricks/lakeflow_pipeline.py).
    return spark.read.table("silver_subscriptions_history").where("is_current").select(*SUB_COLS)  # noqa: F821
