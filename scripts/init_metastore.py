"""Create the schemas used by the pipeline (revenue), the quality gate (dq) and dbt (gold)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.spark import get_spark  # noqa: E402

spark = get_spark("init")
for schema in ["revenue", "dq", "gold"]:
    spark.sql(f"create schema if not exists {schema}")
print([r[0] for r in spark.sql("show schemas").collect()])
