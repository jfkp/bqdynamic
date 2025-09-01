  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/errors/exceptions/captured.py", line 185, in deco
pyspark.errors.exceptions.captured.AnalysisException: [SCHEMA_NOT_FOUND] The schema `cacib-lsdh-dev-df`.`blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a catalog, verify the current_schema() output, or qualify the name with the correct catalog.
To tolerate the error on drop use DROP SCHEMA IF EXISTS.
25/08/31 10:06:28 INFO DataprocSparkPlugin: Shutting down dr



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


