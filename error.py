from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import yaml
from yaml.loader import SafeLoader
from pathlib import Path
from datetime import date


ICEBERG_CATALOG = 'cacib-lsdh-dev-df'
ICEBERG_DB = 'blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb'
ICEBERG_TABLE_NAME = 'store_sales_denorm_insert_medium_test' # Le nom de la table doit correspondre à la table créée dans BigQuery
BUCKET = "gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00//blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb"
PROJECT = "cacib-lsdh-dev-df"
LOCATION = "europe-west9"
INPUT_BUCKET = "gs://bkt-lsdh-dev-ew9-bench-bl-raw-data-f72a/convert/10G/store_sales_denorm_start/*.parquet"
BQ_DATASET = f"{ICEBERG_CATALOG}.{ICEBERG_DB}"
BQ_CONNECTION = "cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl"
connection_id_short = BQ_CONNECTION.split('.')[2]

spark = SparkSession.builder \
    .appName('spark-bigquery-demo') \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl", "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.gcp_project", PROJECT) \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.gcp_location", LOCATION) \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.connection-id", f"projects/{PROJECT}/locations/{LOCATION}/connections/{connection_id_short}") \
    .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", BUCKET) \
    .getOrCreate()
      
spark.conf.set("viewsEnabled", "true")

# Ajoutez cette ligne pour vérifier la configuration du catalogue
print("Vérification de la configuration du catalogue :")
print(f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl -> {spark.conf.get(f'spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl', 'NOT FOUND')}")

# Les commandes de SHOW pour le diagnostic
print("Affichage des catalogues disponibles:")
spark.sql("SHOW CATALOGS").show()
print("Affichage des bases de données dans le catalogue BigLake:")
spark.sql(f"SHOW DATABASES IN `{ICEBERG_CATALOG}`").show()
spark.sql(f"USE `{ICEBERG_CATALOG}`")
spark.sql(f"USE `{ICEBERG_DB}`")

input_df = spark.read.parquet(INPUT_BUCKET)
input_df.createOrReplaceTempView("src_data")



spark.sql(f"""
    INSERT INTO `{ICEBERG_DB}`.`{ICEBERG_TABLE_NAME}`
    SELECT * from src_data
""")

print("Données écrites avec succès dans la table BigQuery/Iceberg gérée.")
