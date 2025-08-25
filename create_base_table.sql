-- Declare the scale variable
DECLARE SCALE STRING DEFAULT '10G';

-- Drop tables
EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_start_%s", SCALE);
EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_medium_%s", SCALE);
EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_small_%s", SCALE);
EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_xsmall_%s", SCALE);
EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_insert_medium_%s", SCALE);
EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_upsert_%s", SCALE);

-- Create tables
EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_start_%s
CLUSTER BY ss_sold_date_sk
WITH CONNECTION `cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/%s/store_sales_denorm_start'
) AS
SELECT * FROM cacib-lsdh-dev-df.bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl.store_sales_denorm_start_%s
""", SCALE, SCALE, SCALE);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_upsert_%s
WITH CONNECTION `cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/%s/store_sales_denorm_upsert'
) AS
SELECT * FROM cacib-lsdh-dev-df.bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl.store_sales_denorm_upsert_%s
""", SCALE, SCALE, SCALE);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_insert_medium_%s
WITH CONNECTION `cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/%s/store_sales_denorm_insert_medium'
) AS
SELECT * FROM cacib-lsdh-dev-df.bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl.store_sales_denorm_insert_medium_%s
""", SCALE, SCALE, SCALE);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_medium_%s
WITH CONNECTION `cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/%s/store_sales_denorm_delete_medium'
) AS
SELECT * FROM cacib-lsdh-dev-df.bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl.store_sales_denorm_delete_medium_%s
""", SCALE, SCALE, SCALE);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_small_%s
WITH CONNECTION `cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/%s/store_sales_denorm_delete_small'
) AS
SELECT * FROM cacib-lsdh-dev-df.bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl.store_sales_denorm_delete_small_%s
""", SCALE, SCALE, SCALE);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_xsmall_%s
WITH CONNECTION `cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/%s/store_sales_denorm_delete_xsmall'
) AS
SELECT * FROM cacib-lsdh-dev-df.bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl.store_sales_denorm_delete_xsmall_%s
""", SCALE, SCALE, SCALE);
