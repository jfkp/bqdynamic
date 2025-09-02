25/09/02 09:26:57 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 09:26:57 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 09:26:57 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 09:26:59 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 09:27:00 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 09:27:01 INFO SparkEnv: Registering MapOutputTracker
25/09/02 09:27:01 INFO SparkEnv: Registering BlockManagerMaster
25/09/02 09:27:01 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/09/02 09:27:01 INFO SparkEnv: Registering OutputCommitCoordinator
25/09/02 09:27:02 INFO MetricsConfig: Loaded properties from hadoop-metrics2.properties
25/09/02 09:27:02 INFO MetricsSystemImpl: Scheduled Metric snapshot period at 10 second(s).
25/09/02 09:27:02 INFO MetricsSystemImpl: google-hadoop-file-system metrics system started
25/09/02 09:27:02 INFO DataprocSparkPlugin: Registered 188 driver metrics
25/09/02 09:27:03 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at dpr-cl-ce-lsdh-dev-ew9-bench-bl-m.europe-west9-b.c.cacib-lsdh-dev-df.internal./172.18.5.6:8032
25/09/02 09:27:04 INFO AHSProxy: Connecting to Application History server at dpr-cl-ce-lsdh-dev-ew9-bench-bl-m.europe-west9-b.c.cacib-lsdh-dev-df.internal./172.18.5.6:10200
25/09/02 09:27:04 INFO Configuration: resource-types.xml not found
25/09/02 09:27:04 INFO ResourceUtils: Unable to find 'resource-types.xml'.
25/09/02 09:27:06 INFO YarnClientImpl: Submitted application application_1755756127970_0149
25/09/02 09:27:07 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 09:27:07 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at dpr-cl-ce-lsdh-dev-ew9-bench-bl-m.europe-west9-b.c.cacib-lsdh-dev-df.internal./172.18.5.6:8030
25/09/02 09:27:09 INFO GoogleCloudStorageImpl: Ignoring exception of type GoogleJsonResponseException; verified object already exists with desired state.
25/09/02 09:27:10 INFO GoogleHadoopOutputStream: hflush(): No-op due to rate limit (RateLimiter[stableRate=0.2qps]): readers will *not* yet see flushed data for gs://dataproc-temp-europe-west9-527974666444-ijcootxu/974c06b6-6981-4054-9fb6-a7e372b4cbbf/spark-job-history/application_1755756127970_0149.inprogress [CONTEXT ratelimit_period="1 MINUTES" ]
Vérification de la configuration du catalogue :
spark.sql.catalog.cacib-lsdh-dev-df.catalog-impl -> org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog
Affichage des catalogues disponibles:
+-------------+
|      catalog|
+-------------+
|spark_catalog|
+-------------+

Affichage des bases de données dans le catalogue BigLake:
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
25/09/02 09:27:18 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.hadoop.HadoopFileIO
25/09/02 09:27:18 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
+--------------------+
|           namespace|
+--------------------+
|UT31L3_PowerBI_Tests|
|blms_ds_lsdh_dev_...|
|blmt_ds_lsdh_dev_...|
|bq_ds_lsdh_dev_ew...|
|bq_ds_lsdh_dev_ew...|
|bq_ds_lsdh_dev_ew...|
|bqmn_ds_lsdh_dev_...|
|bqms_ds_lsdh_dev_...|
|                 gfd|
|                 ldt|
|   pbi_connexion_xls|
+--------------------+

25/09/02 09:27:31 INFO GhfsGlobalStorageStatistics: periodic connector metrics: {action_http_delete_request=3, action_http_delete_request_duration=97, action_http_delete_request_max=35, action_http_delete_request_mean=32, action_http_delete_request_min=28, action_http_post_request=8, action_http_post_request_duration=407, action_http_post_request_max=92, action_http_post_request_mean=50, action_http_post_request_min=33, action_http_put_request=4, action_http_put_request_duration=333, action_http_put_request_max=108, action_http_put_request_mean=83, action_http_put_request_min=65, directories_created=1, files_created=1, gcs_api_client_non_found_response_count=20, gcs_api_client_precondition_failed_response_count=1, gcs_api_client_side_error_count=21, gcs_api_time=2185, gcs_api_total_request_count=46, gcs_connector_time=2786, gcs_get_other_request=3, gcs_list_dir_request=2, gcs_list_dir_request_duration=170, gcs_list_dir_request_max=95, gcs_list_dir_request_mean=85, gcs_list_dir_request_min=75, gcs_list_file_request=3, gcs_list_file_request_duration=337, gcs_list_file_request_max=282, gcs_list_file_request_mean=112, gcs_list_file_request_min=18, gcs_metadata_request=22, gcs_metadata_request_duration=785, gcs_metadata_request_max=282, gcs_metadata_request_mean=35, gcs_metadata_request_min=15, gs_filesystem_create=4, gs_filesystem_initialize=3, op_create=1, op_create_duration=102, op_create_max=102, op_create_mean=102, op_create_min=102, op_get_file_status=2, op_get_file_status_duration=780, op_get_file_status_max=732, op_get_file_status_mean=390, op_get_file_status_min=48, op_get_list_status_result_size=1832, op_glob_status=1, op_hflush=21, op_hflush_duration=922, op_hflush_max=269, op_hflush_mean=43, op_list_status=1, op_list_status_duration=789, op_list_status_max=789, op_list_status_mean=789, op_list_status_min=789, op_mkdirs=1, op_mkdirs_duration=193, op_mkdirs_max=193, op_mkdirs_mean=193, op_mkdirs_min=193, stream_write_bytes=2324587, uptimeSeconds=32}
[CONTEXT ratelimit_period="5 MINUTES" ]
25/09/02 09:27:40 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
Traceback (most recent call last):
  File "/tmp/job-bc0af568/testbqstd.py", line 47, in <module>
    spark.sql(f"""
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/session.py", line 1631, in sql
  File "/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py", line 1322, in __call__
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/errors/exceptions/captured.py", line 185, in deco
pyspark.errors.exceptions.captured.AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `cacib-lsdh-dev-df`.`blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb`.`store_sales_denorm_insert_medium_test` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.
To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS.; line 2 pos 16;
'InsertIntoStatement 'UnresolvedRelation [cacib-lsdh-dev-df, blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb, store_sales_denorm_insert_medium_test], [__required_write_privileges__=INSERT], false, false, false, false
+- Project [ss_sold_time_sk#21L, ss_item_sk#22L, ss_customer_sk#23L, ss_cdemo_sk#24L, ss_hdemo_sk#25L, ss_addr_sk#26L, ss_store_sk#27L, ss_promo_sk#28L, ss_ticket_number#29L, ss_quantity#30L, ss_wholesale_cost#31, ss_list_price#32, ss_sales_price#33, ss_ext_discount_amt#34, ss_ext_sales_price#35, ss_ext_wholesale_cost#36, ss_ext_list_price#37, ss_ext_tax#38, ss_coupon_amt#39, ss_net_paid#40, ss_net_paid_inc_tax#41, ss_net_profit#42, ss_sold_date_sk#43L, d_date_sk#44L, ... 152 more fields]
   +- SubqueryAlias src_data
      +- View (`src_data`, [ss_sold_time_sk#21L,ss_item_sk#22L,ss_customer_sk#23L,ss_cdemo_sk#24L,ss_hdemo_sk#25L,ss_addr_sk#26L,ss_store_sk#27L,ss_promo_sk#28L,ss_ticket_number#29L,ss_quantity#30L,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42,ss_sold_date_sk#43L,d_date_sk#44L,d_date_id#45,d_date#46,d_month_seq#47L,d_week_seq#48L,d_quarter_seq#49L,d_year#50L,d_dow#51L,d_moy#52L,d_dom#53L,d_qoy#54L,d_fy_year#55L,d_fy_quarter_seq#56L,d_fy_week_seq#57L,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63L,d_last_dom#64L,d_same_day_ly#65L,d_same_day_lq#66L,d_current_day#67,d_current_week#68,d_current_month#69,d_current_quarter#70,d_current_year#71,t_time_sk#72L,t_time_id#73,t_time#74L,t_hour#75L,t_minute#76L,t_second#77L,t_am_pm#78,t_shift#79,t_sub_shift#80,t_meal_time#81,c_customer_sk#82L,c_customer_id#83,c_current_cdemo_sk#84L,c_current_hdemo_sk#85L,c_current_addr_sk#86L,c_first_shipto_date_sk#87L,c_first_sales_date_sk#88L,c_salutation#89,c_first_name#90,c_last_name#91,c_preferred_cust_flag#92,c_birth_day#93L,c_birth_month#94L,c_birth_year#95L,c_birth_country#96,c_login#97,c_email_address#98,c_last_review_date#99L,cd_demo_sk#100L,cd_gender#101,cd_marital_status#102,cd_education_status#103,cd_purchase_estimate#104L,cd_credit_rating#105,cd_dep_count#106L,cd_dep_employed_count#107L,cd_dep_college_count#108L,hd_demo_sk#109L,hd_income_band_sk#110L,hd_buy_potential#111,hd_dep_count#112L,hd_vehicle_count#113L,ca_address_sk#114L,ca_address_id#115,ca_street_number#116L,ca_street_name#117,ca_street_type#118,ca_suite_number#119,ca_city#120,ca_county#121,ca_state#122,ca_zip#123L,ca_country#124,ca_gmt_offset#125L,ca_location_type#126,s_store_sk#127L,s_store_id#128,s_rec_start_date#129,s_rec_end_date#130,s_closed_date_sk#131L,s_store_name#132,s_number_employees#133L,s_floor_space#134L,s_hours#135,s_manager#136,s_market_id#137L,s_geography_class#138,s_market_desc#139,s_market_manager#140,s_division_id#141L,s_division_name#142,s_company_id#143L,s_company_name#144,s_street_number#145L,s_street_name#146,s_street_type#147,s_suite_number#148,s_city#149,s_county#150,s_state#151,s_zip#152L,s_country#153,s_gmt_offset#154L,s_tax_precentage#155,p_promo_sk#156L,p_promo_id#157,p_start_date_sk#158L,p_end_date_sk#159L,p_item_sk#160L,p_cost#161,p_response_target#162L,p_promo_name#163,p_channel_dmail#164,p_channel_email#165,p_channel_catalog#166,p_channel_tv#167,p_channel_radio#168,p_channel_press#169,p_channel_event#170,p_channel_demo#171,p_channel_details#172,p_purpose#173,p_discount_active#174,i_item_sk#175L,i_item_id#176,i_rec_start_date#177,i_rec_end_date#178,i_item_desc#179,i_current_price#180,i_wholesale_cost#181,i_brand_id#182L,i_brand#183,i_class_id#184L,i_class#185,i_category_id#186L,i_category#187,i_manufact_id#188L,i_manufact#189,i_size#190,i_formulation#191,i_color#192,i_units#193,i_container#194,i_manager_id#195L,i_product_name#196])
         +- Relation [ss_sold_time_sk#21L,ss_item_sk#22L,ss_customer_sk#23L,ss_cdemo_sk#24L,ss_hdemo_sk#25L,ss_addr_sk#26L,ss_store_sk#27L,ss_promo_sk#28L,ss_ticket_number#29L,ss_quantity#30L,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42,ss_sold_date_sk#43L,d_date_sk#44L,... 152 more fields] parquet

25/09/02 09:27:41 INFO DataprocSparkPlugin: Shutti

Table info
Table ID
cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sales_denorm_insert_medium_test
Created
Sep 2, 2025, 11:26:33 AM UTC+2
Last modified
Sep 2, 2025, 11:26:33 AM UTC+2
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
BigQuery table for Apache Iceberg configuration
Connection ID
cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl
Storage URI
gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/store_sales_denorm_insert_medium_test/
File format
PARQUET
Table format
ICEBERG
Storage info
Number of rows
74,704
Current physical bytes
0 B
pyspark.errors.exceptions.captured.AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `cacib-lsdh-dev-df`.`blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb`.`store_sales_denorm_insert_medium_test` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.
To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS.; line 2 pos 16;
'InsertIntoStatement 'UnresolvedRelation [cacib-lsdh-dev-df, blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb, store_sales_denorm_insert_medium_test], [__required_write_privileges__=INSERT], false, false, false, false
+- Project [ss_sold_time_sk#21L, ss_item_sk#22L, ss_customer_sk#23L, ss_cdemo_sk#24L, ss_hdemo_sk#25L, ss_addr_sk#26L, ss_store_sk#27L, ss_promo_sk#28L, ss_ticket_number#29L, ss_quantity#30L, ss_wholesale_cost#31, ss_list_price#32, ss_sales_price#33, ss_ext_discount_amt#34, ss_ext_sales_price#35, ss_ext_wholesale_cost#36, ss_ext_list_price#37, ss_ext_tax#38, ss_coupon_amt#39, ss_net_paid#40, ss_net_paid_inc_tax#41, ss_net_profit#42, ss_sold_date_sk#43L, d_date_sk#44L, ... 152 more fields]
   +- SubqueryAlias src_data
      +- View (`src_data`, [ss_sold_time_sk#21L,ss_item_sk#22L,ss_customer_sk#23L,ss_cdemo_sk#24L,ss_hdemo_sk#25L,ss_addr_sk#26L,ss_store_sk#27L,ss_promo_sk#28L,ss_ticket_number#29L,ss_quantity#30L,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42,ss_sold_date_sk#43L,d_date_sk#44L,d_date_id#45,d_date#46,d_month_seq#47L,d_week_seq#48L,d_quarter_seq#49L,d_year#50L,d_dow#51L,d_moy#52L,d_dom#53L,d_qoy#54L,d_fy_year#55L,d_fy_quarter_seq#56L,d_fy_week_seq#57L,d_day_name#58,d_quarter_name#59,d_holiday#60,d_weekend#61,d_following_holiday#62,d_first_dom#63L,d_last_dom#64L,d_same_day_ly#65L,d_same_day_lq#66L,d_current_day#67,d_current_week#68,d_current_month#69,d_current_quarter#70,d_current_year#71,t_time_sk#72L,t_time_id#73,t_time#74L,t_hour#75L,t_minute#76L,t_second#77L,t_am_pm#78,t_shift#79,t_sub_shift#80,t_meal_time#81,c_customer_sk#82L,c_customer_id#83,c_current_cdemo_sk#84L,c_current_hdemo_sk#85L,c_current_addr_sk#86L,c_first_shipto_date_sk#87L,c_first_sales_date_sk#88L,c_salutation#89,c_first_name#90,c_last_name#91,c_preferred_cust_flag#92,c_birth_day#93L,c_birth_month#94L,c_birth_year#95L,c_birth_country#96,c_login#97,c_email_address#98,c_last_review_date#99L,cd_demo_sk#100L,cd_gender#101,cd_marital_status#102,cd_education_status#103,cd_purchase_estimate#104L,cd_credit_rating#105,cd_dep_count#106L,cd_dep_employed_count#107L,cd_dep_college_count#108L,hd_demo_sk#109L,hd_income_band_sk#110L,hd_buy_potential#111,hd_dep_count#112L,hd_vehicle_count#113L,ca_address_sk#114L,ca_address_id#115,ca_street_number#116L,ca_street_name#117,ca_street_type#118,ca_suite_number#119,ca_city#120,ca_county#121,ca_state#122,ca_zip#123L,ca_country#124,ca_gmt_offset#125L,ca_location_type#126,s_store_sk#127L,s_store_id#128,s_rec_start_date#129,s_rec_end_date#130,s_closed_date_sk#131L,s_store_name#132,s_number_employees#133L,s_floor_space#134L,s_hours#135,s_manager#136,s_market_id#137L,s_geography_class#138,s_market_desc#139,s_market_manager#140,s_division_id#141L,s_division_name#142,s_company_id#143L,s_company_name#144,s_street_number#145L,s_street_name#146,s_street_type#147,s_suite_number#148,s_city#149,s_county#150,s_state#151,s_zip#152L,s_country#153,s_gmt_offset#154L,s_tax_precentage#155,p_promo_sk#156L,p_promo_id#157,p_start_date_sk#158L,p_end_date_sk#159L,p_item_sk#160L,p_cost#161,p_response_target#162L,p_promo_name#163,p_channel_dmail#164,p_channel_email#165,p_channel_catalog#166,p_channel_tv#167,p_channel_radio#168,p_channel_press#169,p_channel_event#170,p_channel_demo#171,p_channel_details#172,p_purpose#173,p_discount_active#174,i_item_sk#175L,i_item_id#176,i_rec_start_date#177,i_rec_end_date#178,i_item_desc#179,i_current_price#180,i_wholesale_cost#181,i_brand_id#182L,i_brand#183,i_class_id#184L,i_class#185,i_category_id#186L,i_category#187,i_manufact_id#188L,i_manufact#189,i_size#190,i_formulation#191,i_color#192,i_units#193,i_container#194,i_manager_id#195L,i_product_name#196])
         +- Relation [ss_sold_time_sk#21L,ss_item_sk#22L,ss_customer_sk#23L,ss_cdemo_sk#24L,ss_hdemo_sk#25L,ss_addr_sk#26L,ss_store_sk#27L,ss_promo_sk#28L,ss_ticket_number#29L,ss_quantity#30L,ss_wholesale_cost#31,ss_list_price#32,ss_sales_price#33,ss_ext_discount_amt#34,ss_ext_sales_price#35,ss_ext_wholesale_cost#36,ss_ext_list_price#37,ss_ext_tax#38,ss_coupon_amt#39,ss_net_paid#40,ss_net_paid_inc_tax#41,ss_net_profit#42,ss_sold_date_sk#43L,d_date_sk#44L,... 152 more fields] parquet


gs://spark-lib/biglake/spark-biglake-iceberg-catalog_2.13-0.1.0-with-dependencies.jar

py4j.protocol.Py4JJavaError: An error occurred while calling o103.parquet.
: java.util.ServiceConfigurationError: org.apache.spark.sql.sources.DataSourceRegister: Provider com.google.cloud.spark.bigquery.BigQueryRelationProvider could not be instantiated
	at java.base/java.util.ServiceLoader.fail(ServiceLoader.java:582)

aused by: java.lang.IllegalStateException:  This connector was made for Scala null, it was not meant to run on Scala 2.12
	at com.google.cloud.spark.bigquery.BigQueryUtilScala$.validateScalaVersionCompatibility(BigQueryUtil.scala:37)
	at com.google.cloud.spark.bigquery.BigQueryRelationProvider.<init>(BigQueryRelationProvider.scala:42)
	at com.google.cloud.spark.bigquery.BigQueryRelationProvider.<init>(BigQueryRelationProvider.scala:49)


Jar files for job
gs://bkt-spark-runtime-121f/iceberg-spark-runtime-3.5_2.12-1.9.2.jar
gs://spark-lib/biglake/biglake-catalog-iceberg1.9.1-0.1.3-with-dependencies.jar
gs://spark-lib/bigquery/iceberg-bigquery-catalog-1.6.1-1.0.1.jar
gs://bkt-spark-runtime-121f/spark-measure_2.12-0.25.jar
gs://bkt-spark-runtime-121f/spark-avro_2.12-3.5.0.jar
gs://bkt-spark-runtime-121f/spark-bigquery-with-dependencies_2.12-0.42.2.jar


dataporc config
Region
europe-west9
Zone
europe-west9-b
Image version 
2.2.61-debian12
Autoscaling
On - dpr-cl-scl-dev-bench-bl
Performance Enhancements
Advanced optimizations
Off
Advanced execution layer
Off
Google Cloud Storage caching
Off
Dataproc Metastore
None
Scheduled deletion
Off
Confidential computing enabled?
Disabled
Master node
Standard (1 master, N workers)
Machine type
e2-highmem-4
Number of GPUs
0
Primary disk type
pd-standard
Primary disk size
1000GB
Local SSDs
0
Worker nodes
2
Machine type
e2-highmem-4
Number of GPUs
0
Primary disk type
pd-standard
Primary disk size
1000GB
Local SSDs
0
Secondary worker nodes
0
Secure Boot
Enabled
VTPM
Enabled
Integrity Monitoring
Enabled
Cloud Storage staging bucket
dataproc-staging-europe-west9-527974666444-7jvvho4h
Subnetwork
sb-base-euw9-cloud-composer
Network tags
None
Internal IP only
Yes
Project access
Allow API access to all Google Cloud services in the same project
Created
Aug 12, 2025, 11:40:43 AM
Properties
Metadata
SPARK_BQ_CONNECTOR_URL
gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.1.jar
Advanced security
Disabled




25/09/02 08:47:06 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 08:47:06 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 08:47:06 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 08:47:08 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 08:47:09 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 08:47:10 INFO SparkEnv: Registering MapOutputTracker
25/09/02 08:47:10 INFO SparkEnv: Registering BlockManagerMaster
25/09/02 08:47:10 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/09/02 08:47:10 INFO SparkEnv: Registering OutputCommitCoordinator
25/09/02 08:47:11 INFO MetricsConfig: Loaded properties from hadoop-metrics2.properties
25/09/02 08:47:11 INFO MetricsSystemImpl: Scheduled Metric snapshot period at 10 second(s).
25/09/02 08:47:11 INFO MetricsSystemImpl: google-hadoop-file-system metrics system started
25/09/02 08:47:11 INFO DataprocSparkPlugin: Registered 188 driver metrics
25/09/02 08:47:12 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at dpr-cl-ce-lsdh-dev-ew9-bench-bl-m.europe-west9-b.c.cacib-lsdh-dev-df.internal./172.18.5.6:8032
25/09/02 08:47:12 INFO AHSProxy: Connecting to Application History server at dpr-cl-ce-lsdh-dev-ew9-bench-bl-m.europe-west9-b.c.cacib-lsdh-dev-df.internal./172.18.5.6:10200
25/09/02 08:47:13 INFO Configuration: resource-types.xml not found
25/09/02 08:47:13 INFO ResourceUtils: Unable to find 'resource-types.xml'.
25/09/02 08:47:15 INFO YarnClientImpl: Submitted application application_1755756127970_0144
25/09/02 08:47:16 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/02 08:47:16 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at dpr-cl-ce-lsdh-dev-ew9-bench-bl-m.europe-west9-b.c.cacib-lsdh-dev-df.internal./172.18.5.6:8030
25/09/02 08:47:18 INFO GoogleCloudStorageImpl: Ignoring exception of type GoogleJsonResponseException; verified object already exists with desired state.
25/09/02 08:47:19 INFO GoogleHadoopOutputStream: hflush(): No-op due to rate limit (RateLimiter[stableRate=0.2qps]): readers will *not* yet see flushed data for gs://dataproc-temp-europe-west9-527974666444-ijcootxu/974c06b6-6981-4054-9fb6-a7e372b4cbbf/spark-job-history/application_1755756127970_0144.inprogress [CONTEXT ratelimit_period="1 MINUTES" ]
Vérification de la configuration du catalogue :
spark.sql.catalog.cacib-lsdh-dev-df.catalog-impl -> org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog
Affichage des catalogues disponibles:
+-------------+
|      catalog|
+-------------+
|spark_catalog|
+-------------+

Affichage des bases de données dans le catalogue BigLake:
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
25/09/02 08:47:26 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.hadoop.HadoopFileIO
25/09/02 08:47:27 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
+--------------------+
|           namespace|
+--------------------+
|UT31L3_PowerBI_Tests|
|blms_ds_lsdh_dev_...|
|blmt_ds_lsdh_dev_...|
|bq_ds_lsdh_dev_ew...|
|bq_ds_lsdh_dev_ew...|
|bq_ds_lsdh_dev_ew...|
|bqmn_ds_lsdh_dev_...|
|bqms_ds_lsdh_dev_...|
|                 gfd|
|                 ldt|
|   pbi_connexion_xls|
+--------------------+

Traceback (most recent call last):
  File "/tmp/job-ecfc73ab/testbqstd.py", line 44, in <module>
    input_df = spark.read.parquet(INPUT_BUCKET)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/readwriter.py", line 544, in parquet
  File "/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py", line 1322, in __call__
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/errors/exceptions/captured.py", line 179, in deco
  File "/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/protocol.py", line 326, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling o103.parquet.
: java.util.ServiceConfigurationError: org.apache.spark.sql.sources.DataSourceRegister: Provider com.google.cloud.spark.bigquery.BigQueryRelationProvider could not be instantiated
	at java.base/java.util.ServiceLoader.fail(ServiceLoader.java:582)
	at java.base/java.util.ServiceLoader$ProviderImpl.newInstance(ServiceLoader.java:804)
	at java.base/java.util.ServiceLoader$ProviderImpl.get(ServiceLoader.java:722)
	at java.base/java.util.ServiceLoader$3.next(ServiceLoader.java:1395)
	at scala.collection.convert.Wrappers$JIteratorWrapper.next(Wrappers.scala:46)
	at scala.collection.Iterator.foreach(Iterator.scala:943)
	at scala.collection.Iterator.foreach$(Iterator.scala:943)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
	at scala.collection.IterableLike.foreach(IterableLike.scala:74)
	at scala.collection.IterableLike.foreach$(IterableLike.scala:73)
	at scala.collection.AbstractIterable.foreach(Iterable.scala:56)
	at scala.collection.TraversableLike.filterImpl(TraversableLike.scala:303)
	at scala.collection.TraversableLike.filterImpl$(TraversableLike.scala:297)
	at scala.collection.AbstractTraversable.filterImpl(Traversable.scala:108)
	at scala.collection.TraversableLike.filter(TraversableLike.scala:395)
	at scala.collection.TraversableLike.filter$(TraversableLike.scala:395)
	at scala.collection.AbstractTraversable.filter(Traversable.scala:108)
	at org.apache.spark.sql.execution.datasources.DataSource$.lookupDataSource(DataSource.scala:629)
	at org.apache.spark.sql.execution.datasources.DataSource$.lookupDataSourceV2(DataSource.scala:697)
	at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:208)
	at org.apache.spark.sql.DataFrameReader.parquet(DataFrameReader.scala:563)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.base/java.lang.reflect.Method.invoke(Method.java:566)
	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
	at py4j.Gateway.invoke(Gateway.java:282)
	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
	at py4j.commands.CallCommand.execute(CallCommand.java:79)
	at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
	at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
	at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.lang.IllegalStateException:  This connector was made for Scala null, it was not meant to run on Scala 2.12
	at com.google.cloud.spark.bigquery.BigQueryUtilScala$.validateScalaVersionCompatibility(BigQueryUtil.scala:37)
	at com.google.cloud.spark.bigquery.BigQueryRelationProvider.<init>(BigQueryRelationProvider.scala:42)
	at com.google.cloud.spark.bigquery.BigQueryRelationProvider.<init>(BigQueryRelationProvider.scala:49)
	at java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
	at java.base/jdk.internal.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:490)
	at java.base/java.util.ServiceLoader$ProviderImpl.newInstance(ServiceLoader.java:780)
	... 31 more

25/09/02 08:47:28 INFO DataprocSparkPlugin: Shutting down driver plugin. metrics=[action_http_patch_request=0, files_created=1, gcs_api_server_timeout_count=0, op_get_list_status_result_size=0, op_open=0, action_http_delete_request=1, gcs_api_time=1181, gcs_backoff_count=0, gcs_api_client_unauthorized_response_count=0, stream_read_close_operations=0, stream_read_bytes_backwards_on_seek=0, gs_filesystem_create=3, exception_count=0, gcs_exception_count=0, gcs_api_total_request_count=25, op_create=1, stream_read_vectored_operations=0, gcs_metadata_request=14, gcs_api_client_bad_request_count=0, action_http_put_request=2, op_create_non_recursive=0, gcs_api_client_gone_response_count=0, gs_filesystem_initialize=2, stream_read_vectored_incoming_ranges=0, stream_write_operations=0, gcs_list_dir_request=0, stream_read_operations=0, gcs_api_client_request_timeout_count=0, op_rename=0, op_get_file_status=1, op_glob_status=0, op_exists=0, stream_write_bytes=107559, op_xattr_list=0, op_get_delegation_token=0, gcs_api_server_unavailable_count=0, directories_created=1, files_delete_rejected=0, stream_read_vectored_combined_ranges=0, op_xattr_get_named=0, gcs_list_file_request=2, op_hsync=0, action_http_get_request=0, stream_read_operations_incomplete=0, op_delete=0, stream_read_bytes=0, gcs_api_client_non_found_response_count=12, op_list_located_status=0, gcs_api_client_requested_range_not_statisfiable_count=0, op_hflush=20, op_list_status=0, stream_read_vectored_read_bytes_discarded=0, op_xattr_get_named_map=0, gcs_api_client_side_error_count=13, op_get_file_checksum=0, gcs_api_server_internal_error_count=0, stream_read_seek_bytes_skipped=0, stream_write_close_operations=0, gcs_get_media_request=0, gcs_connector_time=1320, files_deleted=0, action_http_post_request=5, op_mkdirs=1, gcs_api_client_rate_limit_error_count=0, op_copy_from_local_file=0, gcs_api_server_bad_gateway_count=0, stream_readVectored_range_duration=0, stream_read_seek_backward_operations=0, gcs_api_server_side_error_count=0, stream_read_seek_operations=0, gcs_get_other_request=1, stream_read_seek_forward_operations=0, gcs_api_client_precondition_failed_response_count=1, op_xattr_get_map=0, delegation_tokens_issued=0, gcs_backoff_time=0, gcs_list_dir_request_min=0, gcs_metadata_request_min=11, op_delete_min=0, op_glob_status_min=0, op_create_non_recursive_min=0, op_hsync_min=0, op_xattr_get_named_min=0, op_xattr_get_named_map_min=0, op_hflush_min=0, op_xattr_list_min=0, action_http_put_request_min=59, op_open_min=0, gcs_list_file_request_min=17, stream_write_close_operations_min=0, op_create_min=93, action_http_delete_request_min=30, op_mkdirs_min=202, op_list_status_min=0, gcs_get_media_request_min=0, stream_readVectored_range_duration_min=0, stream_read_vectored_operations_min=0, stream_read_close_operations_min=0, stream_read_operations_min=0, stream_read_seek_operations_min=0, op_xattr_get_map_min=0, stream_write_operations_min=0, action_http_patch_request_min=0, op_get_file_status_min=639, op_rename_min=0, delegation_tokens_issued_min=0, action_http_post_request_min=30, stream_read_close_operations_max=0, stream_read_seek_operations_max=0, op_hflush_max=243, op_xattr_list_max=0, op_xattr_get_map_max=0, action_http_put_request_max=62, action_http_patch_request_max=0, action_http_post_request_max=78, stream_write_close_operations_max=0, action_http_delete_request_max=30, op_mkdirs_max=202, gcs_get_media_request_max=0, op_rename_max=0, stream_read_vectored_operations_max=0, stream_readVectored_range_duration_max=0, op_xattr_get_named_map_max=0, stream_write_operations_max=0, stream_read_operations_max=0, op_xattr_get_named_max=0, op_glob_status_max=0, op_create_non_recursive_max=0, op_get_file_status_max=639, op_open_max=0, delegation_tokens_issued_max=0, gcs_list_file_request_max=243, gcs_metadata_request_max=239, op_create_max=93, op_delete_max=0, op_list_status_max=0, op_hsync_max=0, gcs_list_dir_request_max=0, op_open_mean=0, op_xattr_list_mean=0, op_rename_mean=0, op_xattr_get_map_mean=0, gcs_list_dir_request_mean=0, op_glob_status_mean=0, stream_read_seek_operations_mean=0, gcs_list_file_request_mean=130, stream_write_operations_mean=0, op_hflush_mean=19, gcs_metadata_request_mean=36, op_list_status_mean=0, stream_read_close_operations_mean=0, op_xattr_get_named_map_mean=0, stream_read_vectored_operations_mean=0, op_mkdirs_mean=202, action_http_post_request_mean=44, stream_write_close_operations_mean=0, action_http_put_request_mean=60, action_http_patch_request_mean=0, op_hsync_mean=0, delegation_tokens_issued_mean=0, action_http_delete_request_mean=30, stream_read_operations_mean=0, op_create_mean=93, op_delete_mean=0, op_create_non_recursive_mean=0, stream_readVectored_range_duration_mean=0, op_xattr_get_named_mean=0, gcs_get_media_request_mean=0, op_get_file_status_mean=639, op_delete_duration=0, op_get_file_status_duration=639, action_http_put_request_duration=121, stream_write_operations_duration=0, op_hsync_duration=0, gcs_metadata_request_duration=514, gcs_get_media_request_duration=0, gcs_list_file_request_duration=260, op_list_status_duration=0, op_mkdirs_duration=202, op_open_duration=0, op_create_duration=93, op_hflush_duration=386, gcs_list_dir_request_duration=0, op_glob_status_duration=0, stream_read_operations_duration=0, action_http_delete_request_duration=30, action_http_post_request_duration=220, op_rename_duration=0]
