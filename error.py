from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
import os
import yaml
from yaml.loader import SafeLoader
from pathlib import Path
from datetime import date


ICEBERG_CATALOG = 'cacib-lsdh-dev-df'
ICEBERG_DB = 'blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb'
ICEBERG_TABLE_NAME = 'store_sales_denorm_bench_test'
BUCKET = "gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb"
PROJECT = "cacib-lsdh-dev-df"
LOCATION = "europe-west9"
INPUT_BUCKET = "gs://bkt-lsdh-dev-ew9-bench-bl-raw-data-f72a/convert/50G/store_sales_denorm_start/*.parquet"
BQ_DATASET = f"{ICEBERG_CATALOG}.{ICEBERG_DB}"
BQ_CONNECTION = "cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl"
connection_id_short = BQ_CONNECTION.split('.')[2]

options = [
    ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
    (f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog"),
    (f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl", "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"),
    (f"spark.sql.catalog.{ICEBERG_CATALOG}.gcp_project", PROJECT),
    (f"spark.sql.catalog.{ICEBERG_CATALOG}.gcp_location", LOCATION),
    # CORRECTION ICI : Ajout de '/connections/'
    (f"spark.sql.catalog.{ICEBERG_CATALOG}.connection-id", f"projects/{PROJECT}/locations/{LOCATION}/connections/{connection_id_short}"),
    (f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", BUCKET)
]


spark_conf = SparkConf().setAppName(value="setup_iceberg").setAll(pairs=options)

spark = SparkSession \
  .builder \
  .appName('spark-bigquery-demo') \
  .config(conf=spark_conf) \
  .getOrCreate()
    
spark.conf.set("viewsEnabled", "true")

create_sql_queries = f"""
  CREATE TABLE IF NOT EXISTS `{ICEBERG_CATALOG}`.`{ICEBERG_DB}`.`{ICEBERG_TABLE_NAME}`
  (
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL,
    order_date DATE
  )
  USING iceberg
  TBLPROPERTIES(
    'format-version'='2',
    'location'='{BUCKET}/{ICEBERG_TABLE_NAME}'
  )
"""

df = spark.sql(create_sql_queries)
df.show()
