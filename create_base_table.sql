DECLARE SCALE STRING DEFAULT "10G";  
DECLARE DATASET STRING DEFAULT "blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb";  
DECLARE EXT_DATASET STRING DEFAULT "bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl";  
DECLARE PROJECT STRING DEFAULT "cacib-lsdh-dev-df";  
DECLARE BUCKET STRING DEFAULT "bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00";

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
    DROP TABLE IF EXISTS %s.%s_%s
  """, DATASET, suffix, SCALE);
END FOR;

-- Create loop
FOR suffix IN (SELECT * FROM UNNEST(table_suffixes)) DO
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS %s.%s_%s
    %s
    WITH CONNECTION `%s.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl`
    OPTIONS (
      file_format = 'PARQUET',
      table_format = 'ICEBERG',
      storage_uri ='gs://%s/%s/%s'
    ) AS
    SELECT * FROM %s.%s.%s_%s
  """,
  DATASET, suffix, SCALE,
  IF(suffix = "store_sales_denorm_start", "CLUSTER BY ss_sold_date_sk", ""),
  PROJECT,
  BUCKET, SCALE, suffix,
  PROJECT, EXT_DATASET, suffix, SCALE);
END FOR;
