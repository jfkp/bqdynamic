DECLARE PROJECT  STRING DEFAULT "cacib-lsdh-dev-df";
DECLARE DATASET  STRING DEFAULT "blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb";
DECLARE EXT_DATASET STRING DEFAULT "bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl";
DECLARE CONNECTION STRING DEFAULT "cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl";
DECLARE BUCKET STRING DEFAULT "gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00";
DECLARE SCALE   STRING DEFAULT "10G";

-- Example tables you want to drop/recreate
DECLARE tables ARRAY<STRING> DEFAULT [
  "store_sales_denorm_start",
  "store_sales_denorm_upsert",
  "store_sales_denorm_insert_medium",
  "store_sales_denorm_delete_medium",
  "store_sales_denorm_delete_small",
  "store_sales_denorm_delete_xsmall"
];

-- Loop over each table
FOR t IN (SELECT * FROM UNNEST(tables) AS table_name) DO
  
  -- Drop if exists
  EXECUTE IMMEDIATE FORMAT("""
    DROP TABLE IF EXISTS `%s.%s.%s_%s`
  """, PROJECT, DATASET, t.table_name, SCALE);

  -- Recreate
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS `%s.%s.%s_%s`
    WITH CONNECTION `%s`
    OPTIONS (
      file_format = 'PARQUET',
      table_format = 'ICEBERG',
      storage_uri = '%s/%s/%s_%s'
    )
    AS SELECT * FROM `%s.%s.%s_%s`
  """,
    PROJECT, DATASET, t.table_name, SCALE,         -- target table
    CONNECTION, BUCKET, DATASET, t.table_name, SCALE, -- Iceberg storage
    PROJECT, EXT_DATASET, t.table_name, SCALE      -- source external table
  );

END FOR;
