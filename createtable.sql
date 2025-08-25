
-- 1️⃣ Set the table scale
DECLARE SCALE STRING DEFAULT '10G';
DECLARE columns_list STRING;
DECLARE create_query STRING;
DECLARE insert_query STRING;

-- 2️⃣ Generate comma-separated column list from the source table
SET columns_list = (
  SELECT STRING_AGG('`' || column_name || '`', ', ' ORDER BY ordinal_position)
  FROM `cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'store_sales_denorm_start_' || SCALE
);

-- Optional: check generated columns
SELECT columns_list;

-- 3️⃣ Create Iceberg external table with schema only (no data)
SET create_query = '''
CREATE TABLE IF NOT EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
CLUSTER BY ss_sold_date_sk
WITH CONNECTION `cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/''' || SCALE || '''/store_sale_denorm_bench'
)
AS
SELECT ' || columns_list || '
FROM cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_start_''' || SCALE || '
WHERE 1=0
''';

-- Execute table creation
EXECUTE IMMEDIATE create_query;

-- 4️⃣ Insert data into the newly created table
SET insert_query = '''
INSERT INTO blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
(' || columns_list || ')
SELECT ' || columns_list || '
FROM cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_start_''' || SCALE || '''
''';

-- Execute data insert
EXECUTE IMMEDIATE insert_query;
