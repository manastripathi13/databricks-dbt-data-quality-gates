"""Plain SparkSession with the same config the pipeline uses (Delta + shared metastore)."""
import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("SPARK_CONF_DIR", str(ROOT / "conf"))


def get_spark(app: str = "revenue-quality") -> SparkSession:
    spark = SparkSession.builder.appName(app).master("local[4]").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
