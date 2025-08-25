BEGIN

-- ===============================
-- Variables
-- ===============================
DECLARE SCALE STRING DEFAULT "10G";
DECLARE DATASET STRING DEFAULT "blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb";  -- Dataset ID (must be valid)
DECLARE EXT_DATASET STRING DEFAULT "bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl";
DECLARE CONNECTION STRING DEFAULT "cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl";
DECLARE BUCKET STRING DEFAULT "gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00";

-- List of tables to drop/recreate
DECLARE tables ARRAY<STRING> DEFAULT [
  "store_sales_denorm_start",
  "store_sales_denorm_upsert",
  "store_sales_denorm_insert_medium",
  "store_sales_denorm_delete_medium",
  "store_sales_denorm_delete_small",
  "store_sales_denorm_delete_xsmall",
  "store_sales_denorm_delete_insert_medium",
  "store_sales_denorm_delete_upsert"
];

-- ===============================
-- Loop over tables
-- ===============================
FOR t IN (SELECT * FROM UNNEST(tables) AS table_name) DO

  -- Drop table if exists
  EXECUTE IMMEDIATE FORMAT("""
    DROP TABLE IF EXISTS `region-europe-west9.%s.%s_%s`
  """, DATASET, t.table_name, SCALE);

  -- Create Iceberg table from external source
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS `region-europe-west9.%s.%s_%s`
    WITH CONNECTION `%s`
    OPTIONS (
      file_format = 'PARQUET',
      table_format = 'ICEBERG',
      storage_uri = '%s/%s/%s_%s'
    )
    AS SELECT * FROM `region-europe-west9.%s.%s_%s`
  """,
    DATASET, t.table_name, SCALE,       -- target table
    CONNECTION, BUCKET, DATASET, t.table_name, SCALE,        -- Iceberg storage URI
    EXT_DATASET, t.table_name, SCALE     -- source external table
  );

END FOR;

END;
