"""Write conf/spark-defaults.conf with absolute paths for this checkout.

Local stand-in for a Databricks workspace:
  - Delta Lake as the table format
  - an embedded Hive metastore (Derby) so the pipeline, Great Expectations and dbt
    all see the same tables (on Databricks this is Unity Catalog)
"""
from pathlib import Path

root = Path(__file__).resolve().parents[1]
data = root / "data"
(root / "conf").mkdir(exist_ok=True)
(root / "conf" / "spark-defaults.conf").write_text(f"""\
spark.jars.packages                         io.delta:delta-spark_2.13:4.4.1
spark.sql.extensions                        io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog             org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.sql.catalogImplementation             hive
spark.sql.warehouse.dir                     {data}/warehouse
spark.hadoop.javax.jdo.option.ConnectionURL jdbc:derby:;databaseName={data}/metastore_db;create=true
spark.sql.session.timeZone                  UTC
spark.driver.memory                         2g
spark.sql.shuffle.partitions                4
spark.ui.showConsoleProgress                false
""")
print("wrote conf/spark-defaults.conf")
