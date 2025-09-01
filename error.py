from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
import os
import yaml
from yaml.loader import SafeLoader
from  pathlib import Path
from datetime import date


ICEBERG_CATALOG='cacib-lsdh-dev-df'
ICEBERG_DB='blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb'
ICEBERG_TABLE_NAME='store_sales_denorm_bench_test'
BUCKET = "gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb"
PROJECT = "cacib-lsdh-dev-df"
LOCATION = "europe-west9"
INPUT_BUCKET="gs://bkt-lsdh-dev-ew9-bench-bl-raw-data-f72a/convert/10G/store_sales_denorm_start/*.parquet"
BQ_DATASET = f"{ICEBERG_CATALOG}.{ICEBERG_DB}"
BQ_CONNECTION = "cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl"
connection_id_short=BQ_CONNECTION.split('.')[2]
options: list[tuple[str]] = [
        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog"),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl","org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.gcp_project", PROJECT),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.gcp_location", LOCATION),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.connection-id", f"projects/{PROJECT}/locations/{LOCATION}{connection_id_short}"),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", BUCKET)
    ]

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.


spark_conf = SparkConf() \
                    .setAppName(value="setup_iceberg") \
                    .setAll(pairs=options)

spark = SparkSession \
  .builder\
  .appName('spark-bigquery-demo') \
  .config(conf=spark_conf) \
  .getOrCreate() \
      
spark.conf.set("viewsEnabled","true")


#table_name=f"{ICEBERG_CATALOG}.{ICEBERG_DB}.{ICEBERG_TABLE_NAME}"
#print(f"table_name: {table_name}")
#df = spark.read.format("bigquery").option("table",table_name).load()
# WITH CONNECTION `{BQ_CONNECTION}` 
create_sql_queries=f"""
  CREATE TABLE IF NOT EXISTS `{ICEBERG_CATALOG}`.{ICEBERG_DB}.demo_test
 (
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL,
  order_date DATE
)
USING iceberg
TBLPROPERTIES(
  'format-version'='2'
) ;
"""


# TBLPROPERTIES(
#             'connection_id' = '{BQ_CONNECTION}',
#             'bq_table' = '{BQ_DATASET}.{ICEBERG_TABLE_NAME}',
#             'file_format' = 'PARQUET',
#             'table_format' = 'ICEBERG',
#             'table_type' = 'MANAGED'
#             )
# OPTIONS(
# 'file_format' = 'PARQUET',
# 'table_format' = 'ICEBERG',

#  )
# 'metadata_storage_format' = 'BIGQUERY'
input_df=spark.read.parquet(INPUT_BUCKET)
input_df.writeTo(f"`{ICEBERG_CATALOG}`.{ICEBERG_DB}.store_sales_denorm_delete_medium_test").append()


# df=spark.sql(create_sql_queries)
# df.show()
#df=spark.read.format("bigquery").load(f"select * from `{ICEBERG_CATALOG}`.`{ICEBERG_DB}`.store_sale_denorm_bench_10G")
#df.write.format("bigquery").option("writeMethod","direct").option("writeAtLeastOnce","true").save(f"{ICEBERG_DB}.store_sale_denorm_bench_11G")
