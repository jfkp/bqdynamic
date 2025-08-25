-- ===============================
-- Managed Iceberg Tables Setup
-- ===============================
DECLARE SCALE STRING DEFAULT '10G';
DECLARE DATASET STRING DEFAULT 'blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb';
DECLARE REGION STRING DEFAULT 'europe-west9';

-- List of tables to create
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

  -- Drop table if it exists
  EXECUTE IMMEDIATE FORMAT("""
    DROP TABLE IF EXISTS `region-%s.%s.%s_%s`
  """, REGION, DATASET, t.table_name, SCALE);

  -- Create managed Iceberg table
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS `region-%s.%s.%s_%s`
    OPTIONS (
      table_format='ICEBERG'
      %s
    ) AS
    SELECT *
    FROM `region-%s.%s.%s_%s`
    WHERE 1=0
  """,
    REGION, DATASET, t.table_name, SCALE,                   -- target table
    CASE WHEN t.table_name = "store_sales_denorm_start" THEN "clustering=[ss_sold_date_sk]" ELSE "" END,  -- cluster start table
    REGION, "bqms_ds_lsdh_dev_ew9_bench_bq_bl_ib_ext_tbl", t.table_name, SCALE  -- source table
  );

END FOR;
