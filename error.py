from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from datetime import date

# -------------------------
# Parameters
# -------------------------
ICEBERG_CATALOG = "cacib-lsdh-dev-df"   # project id, also used as Spark catalog alias
ICEBERG_DB = "blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb"
ICEBERG_TABLE_NAME = "store_sales_denorm_insert_medium_test"

PROJECT = "cacib-lsdh-dev-df"
LOCATION = "europe-west9"
BUCKET = "gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb"
INPUT_BUCKET = "gs://bkt-lsdh-dev-ew9-bench-bl-raw-data-f72a/convert/10G/store_sales_denorm_start/*.parquet"

BQ_CONNECTION = "cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl"
connection_id_short = BQ_CONNECTION.split('.')[2]

# -------------------------
# Spark session
# -------------------------
spark = (
    SparkSession.builder
    .appName("spark-bigquery-demo")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "com.google.cloud.bigquery.connector.common.IcebergCatalog")
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.gcpProject", PROJECT)
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.gcpLocation", LOCATION)
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.connectionId",
            f"projects/{PROJECT}/locations/{LOCATION}/connections/{connection_id_short}")
    .getOrCreate()
)

spark.conf.set("viewsEnabled", "true")

# -------------------------
# Diagnostics
# -------------------------
print("Affichage des catalogues disponibles:")
spark.sql("SHOW CATALOGS").show()

print("Affichage des bases de données dans le catalogue BigLake:")
spark.sql(f"SHOW DATABASES IN `{ICEBERG_CATALOG}`").show()

print("Affichage des tables dans la base de données cible:")
spark.sql(f"SHOW TABLES IN `{ICEBERG_CATALOG}`.`{ICEBERG_DB}`").show()

# -------------------------
# Load input parquet
# -------------------------
input_df = spark.read.parquet(INPUT_BUCKET)
input_df.createOrReplaceTempView("src_data")

# -------------------------
# Insert into BigLake/Iceberg table
# -------------------------
spark.sql(f"""
    INSERT INTO `{ICEBERG_CATALOG}`.`{ICEBERG_DB}`.`{ICEBERG_TABLE_NAME}`
    SELECT * FROM src_data
""")

print("✅ Données écrites avec succès dans la table BigQuery/Iceberg gérée.")

spark.stop()
