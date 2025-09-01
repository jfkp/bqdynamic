ICEBERG_CATALOG='cacib-lsdh-dev-df'
ICEBERG_DB='blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb'
ICEBERG_TABLE_NAME='store_sales_denorm_bench_test'
BUCKET = "gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb"
PROJECT = "cacib-lsdh-dev-df"
LOCATION = "europe-west9"
INPUT_BUCKET="gs://bkt-lsdh-dev-ew9-bench-bl-raw-data-f72a/convert/50G/store_sales_denorm_start/*.parquet"
BQ_DATASET = f"{ICEBERG_CATALOG}.{ICEBERG_DB}"
BQ_CONNECTION = "cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl"
options: list[tuple[str]] = [
        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog"),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl","org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.gcp_project", PROJECT),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.gcp_location", LOCATION),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.connection_id", BQ_CONNECTION),
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
  .enableHiveSupport().config(conf=spark_conf) \
  .enableHiveSupport()  \
  .getOrCreate() \
     



Table info
Table ID
cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_100G
Created
Aug 29, 2025, 3:37:29 PM UTC+2
Last modified
Aug 29, 2025, 3:47:53 PM UTC+2
Table expiration
NEVER
Data location
europe-west9
Default collation
Default rounding mode
ROUNDING_MODE_UNSPECIFIED
Case insensitive
false
Description
Labels
Primary key(s)
Tags
Clustered by
ss_sold_date_sk
BigQuery table for Apache Iceberg configuration
Connection ID
cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl
Storage URI
gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/100G/store_sale_denorm_bench/
File format
PARQUET
Table format
ICEBERG
Storage info
Number of rows
253,860,188
Current physical bytes
60.17 GB

ensuite je crée avec  CREATE TABLE IF NOT EXISTS demo_test
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

Dans la description de la table j'ai cette description

Table info
Table ID
cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.demo_test
Created
Sep 1, 2025, 6:34:44 PM UTC+2
Last modified
Sep 1, 2025, 6:34:44 PM UTC+2
Table expiration
NEVER
Data location
europe-west9
Case insensitive
false
Description
Labels
Primary key(s)
Tags
Open Catalog Table Configuration
Location URI
gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.db/demo_test
Input Format
org.apache.hadoop.mapred.FileInputFormat
Output Format
org.apache.hadoop.mapred.FileOutputFormat
SerDe Parameters
Parameters
owner : rootmetadata_location : gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.db/demo_test/metadata/00000-552ab908-ed69-4e44-a43b-d922777bf856.metadata.jsonEXTERNAL : TRUEuuid : 4edf056a-83f6-4ee1-a051-595b36bf159dwrite.parquet.compression-codec : zstdtable_type : iceberg


