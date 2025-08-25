DECLARE SCALE STRING DEFAULT "10G";  -- change once, reused everywhere

-- List of table suffixes to drop and recreate
DECLARE table_suffixes ARRAY<STRING>;
SET table_suffixes = [
  "store_sales_denorm_start",
  "store_sales_denorm_delete_medium",
  "store_sales_denorm_delete_small",
  "store_sales_denorm_delete_xsmall",
  "store_sales_denorm_insert_medium",
  "store_sales_denorm_upsert"
];

-- Drop loop
FOR suffix IN (SELECT * FROM UNNEST(table_suffixes)) DO
  EXECUTE IMMEDIATE FORMAT("""
    DROP TABLE IF EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.%s_%s
  """, suffix, SCALE);
END FOR;

-- Create loop
FOR suffix IN (SELECT * FROM UNNEST(table_suffixes)) DO
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.%s_%s
    %s
    WITH CONNECTION `cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl`
    OPTIONS (
      file_format = 'PARQUET',
      table_format = 'ICEBERG',
      storage_uri ='gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/%s/%s'
    ) AS
    SELECT * FROM cacib-lsdh-dev-df.bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl.%s_%s
  """,
  suffix, SCALE,
  IF(suffix = "store_sales_denorm_start", "CLUSTER BY ss_sold_date_sk", ""),  -- only start table has CLUSTER
  SCALE, suffix, suffix, SCALE);
END FOR;
