
declare SCALE STRING DEFAULT '10G';
declare q1 ,q2 ,q3 ,q4, q5,q6,q7,q8,q9,merge_insert, merge_update , merge_xdelete,merge_sdelete,merge_mdelete, d_delete,update_cols,columns_list,load_query,create_query STRING;
DECLARE r_queries ARRAY<STRUCT<name STRING, sql STRING>>;
DECLARE w_queries ARRAY<STRUCT<name STRING, sql STRING>>;
DECLARE cycle INT64 DEFAULT 1;
DECLARE  start_time,end_time TIMESTAMP;
DECLARE TABLE_FORMAT STRING;


SET TABLE_FORMAT="iceberg";
--  Generate comma-separated column list from the source table
SET columns_list = (
  SELECT STRING_AGG('`' || column_name || '`', ', ' ORDER BY ordinal_position)
  FROM `cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'store_sales_denorm_start_' || SCALE
);

--  generate the update statment column  part
SET update_cols = (
  SELECT STRING_AGG('a.' || column_name || ' = b.' || column_name)
  FROM `cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = CONCAT('store_sale_denorm_bench_', SCALE)
);

/*
Q1 description :
- For December sales of manufacturer 436, which brands sold the most each year?
Computes the total December sales revenue (ss_ext_sales_price) per brand for manufacturer 436,
grouped by year, and orders the results by year, descending sales, and brand ID.
*/

-- q1
set q1 = """
 SELECT d_year
            , i_brand_id              brand_id
            , i_brand                 brand
            , SUM(ss_ext_sales_price) sum_agg
        FROM `blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_""" || SCALE ||
       """`
        WHERE i_manufact_id = 436
          AND d_moy=12
        GROUP BY d_year, i_brand, i_brand_id
        ORDER BY d_year, sum_agg DESC, brand_id     
""";

/*
Q2 description :
high-priced sales concentration by state
Which states had at least 10 February 2000 sales where the item was sold significantly above its category average price (≥20% higher), and how many such sales per state?
*/

-- q2 
SET q2 = '''
    SELECT s.ca_state AS state, COUNT(*) AS cnt
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''  s
    WHERE s.d_month_seq = (
            SELECT DISTINCT d_month_seq
            FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
            WHERE d_year = 2000 AND d_moy = 2
    )
    AND s.i_current_price > 1.2 * (
            SELECT AVG(j.i_current_price)
            FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' j
            WHERE j.i_category = s.i_category
    )
    GROUP BY s.ca_state
    HAVING COUNT(*) >= 10
    ORDER BY cnt, s.ca_state
''';

/*
Q3 description :
- Demographic-specific item-level sales analysis
For widowed female customers with primary education who were not targeted by email or event marketing in 1998, 
what are the average quantity, list price, coupon amount, and actual sales price for each item?”
*/

-- q3



SET q3 = '''
    SELECT i_item_id,
           AVG(ss_quantity)    AS agg1,
           AVG(ss_list_price)  AS agg2,
           AVG(ss_coupon_amt)  AS agg3,
           AVG(ss_sales_price) AS agg4
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE cd_gender = 'F'
      AND cd_marital_status = 'W'
      AND cd_education_status = 'Primary'
      AND (p_channel_email = 'N'
           OR p_channel_event = 'N')
      AND d_year = 1998
    GROUP BY i_item_id
    ORDER BY i_item_id
''';



/*
Q4 description :
Conditional aggregation with dynamic metrics for different sales quantity ranges
Creates 5 summary buckets based on ss_quantity ranges.
Each bucket’s value depends on whether the number of sales in that range exceeds a threshold: if yes → average discount, if no → average net profit.
*/

-- q4

SET q4 = '''
SELECT 
    CASE WHEN (SELECT COUNT(*) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 1 AND 20) > 48409437
         THEN (SELECT AVG(ss_ext_discount_amt) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 1 AND 20)
         ELSE (SELECT AVG(ss_net_profit) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 1 AND 20)
    END AS bucket1,
    
    CASE WHEN (SELECT COUNT(*) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 21 AND 40) > 24804257
         THEN (SELECT AVG(ss_ext_discount_amt) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 21 AND 40)
         ELSE (SELECT AVG(ss_net_profit) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 21 AND 40)
    END AS bucket2,
    
    CASE WHEN (SELECT COUNT(*) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 41 AND 60) > 128048939
         THEN (SELECT AVG(ss_ext_discount_amt) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 41 AND 60)
         ELSE (SELECT AVG(ss_net_profit) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 41 AND 60)
    END AS bucket3,
    
    CASE WHEN (SELECT COUNT(*) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 61 AND 80) > 56503968
         THEN (SELECT AVG(ss_ext_discount_amt) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 61 AND 80)
         ELSE (SELECT AVG(ss_net_profit) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 61 AND 80)
    END AS bucket4,
    
    CASE WHEN (SELECT COUNT(*) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 81 AND 100) > 43571537
         THEN (SELECT AVG(ss_ext_discount_amt) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 81 AND 100)
         ELSE (SELECT AVG(ss_net_profit) 
               FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' 
               WHERE ss_quantity BETWEEN 81 AND 100)
    END AS bucket5
''';

/*
Q5 description :
targeted profitability and sales analysis across different customer segments and regions
Computes average quantity, sales price, and wholesale cost, plus total wholesale cost for 2001 sales 
that satisfy specific demographic criteria (marital status, education, household size, price range) 
and specific location × profit constraints (different thresholds for different states).
*/

-- q5
SET q5 = '''
    SELECT 
        AVG(ss_quantity) AS avg_quantity,
        AVG(ss_ext_sales_price) AS avg_sales_price,
        AVG(ss_ext_wholesale_cost) AS avg_wholesale_cost,
        SUM(ss_ext_wholesale_cost) AS total_wholesale_cost
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE d_year = 2001
      AND (
            (cd_marital_status = 'D'
             AND cd_education_status = '2 yr Degree'
             AND ss_sales_price BETWEEN 100.00 AND 150.00
             AND hd_dep_count = 3)
          OR
            (cd_marital_status = 'S'
             AND cd_education_status = 'Secondary'
             AND ss_sales_price BETWEEN 50.00 AND 100.00
             AND hd_dep_count = 1)
          OR
            (cd_marital_status = 'W'
             AND cd_education_status = 'Advanced Degree'
             AND ss_sales_price BETWEEN 150.00 AND 200.00
             AND hd_dep_count = 1)
      )
      AND (
            (ca_country = 'United States'
             AND ca_state IN ('CO', 'IL', 'MN')
             AND ss_net_profit BETWEEN 100 AND 200)
          OR
            (ca_country = 'United States'
             AND ca_state IN ('OH', 'MT', 'NM')
             AND ss_net_profit BETWEEN 150 AND 300)
          OR
            (ca_country = 'United States'
             AND ca_state IN ('TX', 'MO', 'MI')
             AND ss_net_profit BETWEEN 50 AND 250)
      )
''';

/*
Q6 description :
ranked by total sales descending to see the most lucrative brand/manufacturer combos first
Computes total November 1999 sales for manager 7’s products, per brand and manufacturer, but only for sales where the customer’s ZIP differs from the store’s ZIP

*/

SET q6 = '''
    SELECT 
        i_brand_id AS brand_id,
        i_brand AS brand,
        i_manufact_id,
        i_manufact,
        SUM(ss_ext_sales_price) AS ext_price
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE i_manager_id = 7
      AND d_moy = 11
      AND d_year = 1999
      AND SUBSTR(ca_zip, 1, 5) <> SUBSTR(s_zip, 1, 5)
    GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
    ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
''';


/*
Q7 description :
For married female customers with a 4-year degree in 2002, computes item-level and item + state-level average sales metrics.
*/

SET q7 = '''
    SELECT 
         i_item_id,
        s_state,
        GROUPING(s_state) AS g_state,
        AVG(ss_quantity) AS agg1,
        AVG(ss_list_price) AS agg2,
        AVG(ss_coupon_amt) AS agg3,
        AVG(ss_sales_price) AS agg4
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE cd_gender = 'F'
      AND cd_marital_status = 'M'
      AND cd_education_status = '4 yr Degree'
      AND d_year = 2002
      AND s_state IN ('NE', 'IN', 'SD', 'MN', 'TX', 'MN')
    GROUP BY ROLLUP(i_item_id, s_state)
    ORDER BY i_item_id, s_state
''';


/*
Q8 description :
quick overview comparisons across multiple sales ranges in a single row
Produces one row summarizing six distinct quantity-based buckets of sales metrics, 
using both quantity ranges and optional numeric filters on list price, coupon, or wholesale cost.
*/

-- q8

SET q8 = '''
SELECT *
FROM (
    SELECT AVG(ss_list_price) AS B1_LP,
           COUNT(ss_list_price) AS B1_CNT,
           COUNT(DISTINCT ss_list_price) AS B1_CNTD
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE ss_quantity BETWEEN 0 AND 5
      AND (ss_list_price BETWEEN 11 AND 21
           OR ss_coupon_amt BETWEEN 460 AND 1460
           OR ss_wholesale_cost BETWEEN 14 AND 34)
) B1,
(
    SELECT AVG(ss_list_price) AS B2_LP,
           COUNT(ss_list_price) AS B2_CNT,
           COUNT(DISTINCT ss_list_price) AS B2_CNTD
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE ss_quantity BETWEEN 6 AND 10
      AND (ss_list_price BETWEEN 91 AND 101
           OR ss_coupon_amt BETWEEN 1430 AND 2430
           OR ss_wholesale_cost BETWEEN 32 AND 52)
) B2,
(
    SELECT AVG(ss_list_price) AS B3_LP,
           COUNT(ss_list_price) AS B3_CNT,
           COUNT(DISTINCT ss_list_price) AS B3_CNTD
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE ss_quantity BETWEEN 11 AND 15
      AND (ss_list_price BETWEEN 66 AND 76
           OR ss_coupon_amt BETWEEN 920 AND 1920
           OR ss_wholesale_cost BETWEEN 4 AND 24)
) B3,
(
    SELECT AVG(ss_list_price) AS B4_LP,
           COUNT(ss_list_price) AS B4_CNT,
           COUNT(DISTINCT ss_list_price) AS B4_CNTD
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE ss_quantity BETWEEN 16 AND 20
      AND (ss_list_price BETWEEN 142 AND 152
           OR ss_coupon_amt BETWEEN 3054 AND 4054
           OR ss_wholesale_cost BETWEEN 80 AND 100)
) B4,
(
    SELECT AVG(ss_list_price) AS B5_LP,
           COUNT(ss_list_price) AS B5_CNT,
           COUNT(DISTINCT ss_list_price) AS B5_CNTD
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE ss_quantity BETWEEN 21 AND 25
      AND (ss_list_price BETWEEN 135 AND 145
           OR ss_coupon_amt BETWEEN 14180 AND 15180
           OR ss_wholesale_cost BETWEEN 38 AND 58)
) B5,
(
    SELECT AVG(ss_list_price) AS B6_LP,
           COUNT(ss_list_price) AS B6_CNT,
           COUNT(DISTINCT ss_list_price) AS B6_CNTD
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE ss_quantity BETWEEN 26 AND 30
      AND (ss_list_price BETWEEN 28 AND 38
           OR ss_coupon_amt BETWEEN 2513 AND 3513
           OR ss_wholesale_cost BETWEEN 42 AND 62)
) B6
''';

/*
Q9 description :
Combines hierarchical aggregation, gross margin calculation, and ranking.
Computes gross margin for product categories and classes in 1999 across specific states.
Uses ROLLUP to get multi-level totals (class, category, overall).
Calculates a rank within each parent grouping to identify which classes or categories have higher/lower gross margins.
The lochierarchy column identifies the aggregation level, and the RANK() helps compare performance within the hierarchy.

*/

-- q9

SET q9 = '''
    SELECT 
        SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin,
        i_category,
        i_class,
        GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
        RANK() OVER (
            PARTITION BY GROUPING(i_category) + GROUPING(i_class),
                         CASE WHEN GROUPING(i_class) = 0 THEN i_category END
            ORDER BY SUM(ss_net_profit)/SUM(ss_ext_sales_price) ASC
        ) AS rank_within_parent
    FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
    WHERE d_year = 1999
      AND s_state IN ('NE', 'IN', 'SD', 'MN', 'TX', 'MN', 'MI', 'LA')
    GROUP BY ROLLUP(i_category, i_class)
    ORDER BY lochierarchy DESC,
             CASE WHEN lochierarchy = 0 THEN i_category END,
             rank_within_parent
''';

SET create_query = (
  'CREATE TABLE IF NOT EXISTS blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_' || SCALE || ' ' ||
  'CLUSTER BY ss_sold_date_sk ' ||
  'WITH CONNECTION `cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl` ' ||
  'OPTIONS ( ' ||
  '  file_format = "PARQUET", ' ||
  '  table_format = "ICEBERG", ' ||
  '  storage_uri = "gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/' || SCALE || '/store_sale_denorm_bench" ' ||
  ') AS ' ||
  'SELECT ' || columns_list || ' ' ||
  'FROM cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_start_' || SCALE || ' ' ||
  'WHERE 1=0'
);



/*
load query description :
Bulk Insert in append mode into benchmark table

*/
--  Insert data into the newly created table
SET load_query = (
  'INSERT INTO blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_' || SCALE || ' (' ||
    columns_list || ') ' ||
  'SELECT ' || columns_list || ' ' ||
  'FROM cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_start_' || SCALE
);


/*
merge_insert description :
update bench table with merge insert  into benchmark table multiple partitions included

*/
-- merge_insert

SET merge_insert = '''
    MERGE INTO blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' AS a
    USING blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_insert_medium_''' || SCALE || ''' AS b
    ON a.ss_sold_date_sk = b.ss_sold_date_sk
       AND a.ss_item_sk = b.ss_item_sk
       AND a.ss_ticket_number = b.ss_ticket_number
    WHEN NOT MATCHED THEN
      INSERT ROW
''';

/*
merge_update description :
update bench table with merge update benchmark table multiple partitions included

*/
-- merge_update

SET merge_update = '''
MERGE INTO blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' AS a
USING blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_upsert_''' || SCALE || ''' AS b
ON a.ss_sold_date_sk = b.ss_sold_date_sk
   AND a.ss_item_sk = b.ss_item_sk
   AND a.ss_ticket_number = b.ss_ticket_number
WHEN MATCHED THEN UPDATE SET ''' || update_cols || ' ';

/*
merge_*delete description :
update bench table with merge delete with differente size medium,small xsmall and direct delete benchmark table multiple partitions included

*/

SET merge_xdelete = '''
MERGE INTO blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' AS a
USING blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_xsmall_''' || SCALE || ''' AS b
ON a.ss_sold_date_sk = b.ss_sold_date_sk
   AND a.ss_item_sk = b.ss_item_sk
   AND a.ss_ticket_number = b.ss_ticket_number
WHEN MATCHED THEN DELETE
''';

SET merge_sdelete = '''
MERGE INTO blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' AS a
USING blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_small_''' || SCALE || ''' AS b
ON a.ss_sold_date_sk = b.ss_sold_date_sk
   AND a.ss_item_sk = b.ss_item_sk
   AND a.ss_ticket_number = b.ss_ticket_number
WHEN MATCHED THEN DELETE
''';

SET merge_mdelete = '''
MERGE INTO blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || ''' AS a
USING blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_delete_medium_''' || SCALE || ''' AS b
ON a.ss_sold_date_sk = b.ss_sold_date_sk
   AND a.ss_item_sk = b.ss_item_sk
   AND a.ss_ticket_number = b.ss_ticket_number
WHEN MATCHED THEN DELETE
''';

SET d_delete = '''
DELETE FROM blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_''' || SCALE || '''
WHERE c_customer_sk = 221580
''';



SET w_queries = [
  STRUCT("load_query"     AS name, load_query     AS sql),
  STRUCT("merge_insert"   AS name, merge_insert   AS sql),
  STRUCT("merge_update"   AS name, merge_update   AS sql),
  STRUCT("merge_sdelete"  AS name, merge_sdelete  AS sql),
  STRUCT("merge_xdelete"  AS name, merge_xdelete  AS sql),
  STRUCT("merge_mdelete"  AS name, merge_mdelete  AS sql),
  STRUCT("d_delete"       AS name, d_delete       AS sql)
];



SET r_queries = [
  STRUCT("q1" AS name, q1 AS sql),
  STRUCT("q2" AS name, q2 AS sql),
  STRUCT("q3" AS name, q3 AS sql),
  STRUCT("q4" AS name, q4 AS sql),
  STRUCT("q5" AS name, q5 AS sql),
  STRUCT("q6" AS name, q6 AS sql),
  STRUCT("q7" AS name, q7 AS sql),
  STRUCT("q8" AS name, q8 AS sql),
  STRUCT("q9" AS name, q9 AS sql)
];

-- initialization process
-- DROP TABLE cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.rw_query_log;

CREATE TABLE IF NOT EXISTS cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.rw_query_log(
scale STRING,
query_type STRING,
query_name STRING,
start_time TIMESTAMP,
end_time TIMESTAMP,
elapsed_millisecond FLOAT64,
execute_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
w_query_name STRING DEFAULT "",
run_id INT64,
table_type STRING
);

-- DELETE FROM cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.rw_query_log
-- WHERE scale = SCALE and table_type = TABLE_FORMAT;

-- create main bench fact table

execute immediate create_query;

-- start of bench Loop for w_queries and r_queries

FOR wq IN (SELECT * FROM UNNEST(w_queries))
DO
  -- Print the query name
  SELECT CONCAT("Executing query name: ", wq.name) AS log_message;
  SELECT CONCAT("Executing query sql: ", wq.sql) AS log_message;
  -- Run the query
  SET start_time= CURRENT_TIMESTAMP();
  EXECUTE IMMEDIATE wq.sql;
  SET end_time= CURRENT_TIMESTAMP();
  INSERT INTO cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.rw_query_log
  (scale,query_type,query_name,start_time,end_time,elapsed_millisecond,table_type)
  VALUES(SCALE,"wquery",wq.name,start_time,end_time,TIMESTAMP_DIFF(end_time,start_time,MILLISECOND),'iceberg');
     WHILE cycle <= 3 DO

        -- Inner loop: execute each query sequentially
        FOR q IN (SELECT * FROM UNNEST(r_queries)) DO
          -- Print query name and cycle number
          SELECT CONCAT("Cycle ", CAST(cycle AS STRING), ": Executing query ", q.name) AS log_message;
          SELECT CONCAT("Cycle ", CAST(cycle AS STRING), ": Executing query ", q.sql) AS log_message;
          -- Execute query
          SET start_time= CURRENT_TIMESTAMP();
          EXECUTE IMMEDIATE q.sql;
          SET end_time= CURRENT_TIMESTAMP();
          INSERT INTO cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.rw_query_log
          (scale,query_type,query_name,start_time,end_time,elapsed_millisecond,w_query_name,run_id,table_type)
          VALUES(SCALE,"rquery",q.name,start_time,end_time,TIMESTAMP_DIFF(end_time,start_time,MILLISECOND),wq.name,cycle,TABLE_FORMAT);
        END FOR;

        SET cycle = cycle + 1;
      END WHILE;
     SET cycle = 1;

END FOR;

--SELECT count(*) FROM cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.rw_query_log
--where table_type = TABLE_FORMAT and scale = SCALE;

-- SELECT * from cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.rw_query_log where table_type='iceberg' and scale="50G" and query_type='rquery';
-- SELECT * from cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.rw_query_log where table_type='iceberg' and scale="50G" and query_type='wquery';
--SELECT AVG(elapsed_millisecond/1000) as exec_time from cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.rw_query_log where table_type='iceberg' and --scale="50G" and query_type='wquery';

DELETE FROM cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.rw_query_log where table_type='iceberg'  and scale="10G";
