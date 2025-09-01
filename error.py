5/09/01 17:30:09 INFO SparkEnv: Registering MapOutputTracker
25/09/01 17:30:09 INFO SparkEnv: Registering BlockManagerMaster
25/09/01 17:30:09 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/09/01 17:30:10 INFO SparkEnv: Registering OutputCommitCoordinator
25/09/01 17:30:10 INFO MetricsConfig: Loaded properties from hadoop-metrics2.properties
25/09/01 17:30:10 INFO MetricsSystemImpl: Scheduled Metric snapshot period at 10 second(s).
25/09/01 17:30:10 INFO MetricsSystemImpl: google-hadoop-file-system metrics system started
25/09/01 17:30:11 INFO DataprocSparkPlugin: Registered 188 driver metrics
25/09/01 17:30:11 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at dpr-cl-ce-lsdh-dev-ew9-bench-bl-m.europe-west9-b.c.cacib-lsdh-dev-df.internal./172.18.5.6:8032
25/09/01 17:30:12 INFO AHSProxy: Connecting to Application History server at dpr-cl-ce-lsdh-dev-ew9-bench-bl-m.europe-west9-b.c.cacib-lsdh-dev-df.internal./172.18.5.6:10200
25/09/01 17:30:12 INFO Configuration: resource-types.xml not found
25/09/01 17:30:12 INFO ResourceUtils: Unable to find 'resource-types.xml'.
25/09/01 17:30:14 INFO YarnClientImpl: Submitted application application_1755756127970_0121
25/09/01 17:30:15 WARN SparkConf: The configuration key 'spark.yarn.executor.failuresValidityInterval' has been deprecated as of Spark 3.5 and may be removed in the future. Please use the new key 'spark.executor.failuresValidityInterval' instead.
25/09/01 17:30:15 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at dpr-cl-ce-lsdh-dev-ew9-bench-bl-m.europe-west9-b.c.cacib-lsdh-dev-df.internal./172.18.5.6:8030
25/09/01 17:30:17 INFO GoogleCloudStorageImpl: Ignoring exception of type GoogleJsonResponseException; verified object already exists with desired state.
25/09/01 17:30:17 INFO GoogleHadoopOutputStream: hflush(): No-op due to rate limit (RateLimiter[stableRate=0.2qps]): readers will *not* yet see flushed data for gs://dataproc-temp-europe-west9-527974666444-ijcootxu/974c06b6-6981-4054-9fb6-a7e372b4cbbf/spark-job-history/application_1755756127970_0121.inprogress [CONTEXT ratelimit_period="1 MINUTES" ]
SLF4J(W): No SLF4J providers were found.
SLF4J(W): Defaulting to no-operation (NOP) logger implementation
SLF4J(W): See https://www.slf4j.org/codes.html#noProviders for further details.
25/09/01 17:30:20 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.hadoop.HadoopFileIO
25/09/01 17:30:22 INFO BaseMetastoreCatalog: Table properties set at catalog level through catalog properties: {}
25/09/01 17:30:23 INFO BaseMetastoreCatalog: Table properties enforced at catalog level through catalog properties: {}
25/09/01 17:30:24 INFO BaseMetastoreTableOperations: Successfully committed to table blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.demo_test in 1004 ms
25/09/01 17:30:24 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.db/demo_test/metadata/00000-53d1c1e2-14df-4db3-9a7f-f37bbd057b56.metadata.json
++
||
++
++

25/09/01 17:30:24 INFO DataprocSparkPlugin: Shutting down driver plugin. metrics=[action_http_patch_request=0, files_created=2, gcs_api_server_timeout_count=0, op_get_list_status_result_size=0, op_open=1, action_http_delete_request=1, gcs_api_time=1580, gcs_backoff_count=0, gcs_api_client_unauthorized_response_count=0, stream_read_close_operations=2, stream_read_bytes_backwards_on_seek=0, gs_filesystem_create=4, exception_count=0, gcs_exception_count=0, gcs_api_total_request_count=35, op_create=2, stream_read_vectored_operations=0, gcs_metadata_request=20, gcs_api_client_bad_request_count=0, action_http_put_request=3, op_create_non_recursive=0, gcs_api_client_gone_response_count=0, gs_filesystem_initialize=3, stream_read_vectored_incoming_ranges=0, stream_write_operations=1, gcs_list_dir_request=0, stream_read_operations=1, gcs_api_client_request_timeout_count=0, op_rename=0, op_get_file_status=1, op_glob_status=0, op_exists=0, stream_write_bytes=105461, op_xattr_list=0, op_get_delegation_token=0, gcs_api_server_unavailable_count=0, directories_created=1, files_delete_rejected=0, stream_read_vectored_combined_ranges=0, op_xattr_get_named=0, gcs_list_file_request=3, op_hsync=0, action_http_get_request=0, stream_read_operations_incomplete=1, op_delete=0, stream_read_bytes=983, gcs_api_client_non_found_response_count=17, op_list_located_status=0, gcs_api_client_requested_range_not_statisfiable_count=0, op_hflush=15, op_list_status=0, stream_read_vectored_read_bytes_discarded=0, op_xattr_get_named_map=0, gcs_api_client_side_error_count=18, op_get_file_checksum=0, gcs_api_server_internal_error_count=0, stream_read_seek_bytes_skipped=0, stream_write_close_operations=2, gcs_get_media_request=1, gcs_connector_time=1540, files_deleted=0, action_http_post_request=6, op_mkdirs=1, gcs_api_client_rate_limit_error_count=0, op_copy_from_local_file=0, gcs_api_server_bad_gateway_count=0, stream_readVectored_range_duration=0, stream_read_seek_backward_operations=0, gcs_api_server_side_error_count=0, stream_read_seek_operations=0, gcs_get_other_request=1, stream_read_seek_forward_operations=0, gcs_api_client_precondition_failed_response_count=1, op_xattr_get_map=0, delegation_tokens_issued=0, gcs_backoff_time=0, gcs_list_dir_request_min=0, gcs_metadata_request_min=14, op_delete_min=0, op_glob_status_min=0, op_create_non_recursive_min=0, op_hsync_min=0, op_xattr_get_named_min=0, op_xattr_get_named_map_min=0, op_hflush_min=0, op_xattr_list_min=0, action_http_put_request_min=67, op_open_min=36, gcs_list_file_request_min=20, stream_write_close_operations_min=0, op_create_min=84, action_http_delete_request_min=30, op_mkdirs_min=185, op_list_status_min=0, gcs_get_media_request_min=32, stream_readVectored_range_duration_min=0, stream_read_vectored_operations_min=0, stream_read_close_operations_min=0, stream_read_operations_min=42, stream_read_seek_operations_min=0, op_xattr_get_map_min=0, stream_write_operations_min=0, action_http_patch_request_min=0, op_get_file_status_min=630, op_rename_min=0, delegation_tokens_issued_min=0, action_http_post_request_min=28, stream_read_close_operations_max=0, stream_read_seek_operations_max=0, op_hflush_max=249, op_xattr_list_max=0, op_xattr_get_map_max=0, action_http_put_request_max=82, action_http_patch_request_max=0, action_http_post_request_max=70, stream_write_close_operations_max=69, action_http_delete_request_max=30, op_mkdirs_max=185, gcs_get_media_request_max=32, op_rename_max=0, stream_read_vectored_operations_max=0, stream_readVectored_range_duration_max=0, op_xattr_get_named_map_max=0, stream_write_operations_max=0, stream_read_operations_max=42, op_xattr_get_named_max=0, op_glob_status_max=0, op_create_non_recursive_max=0, op_get_file_status_max=630, op_open_max=36, delegation_tokens_issued_max=0, gcs_list_file_request_max=235, gcs_metadata_request_max=235, op_create_max=101, op_delete_max=0, op_list_status_max=0, op_hsync_max=0, gcs_list_dir_request_max=0, op_open_mean=36, op_xattr_list_mean=0, op_rename_mean=0, op_xattr_get_map_mean=0, gcs_list_dir_request_mean=0, op_glob_status_mean=0, stream_read_seek_operations_mean=0, gcs_list_file_request_mean=99, stream_write_operations_mean=0, op_hflush_mean=26, gcs_metadata_request_mean=35, op_list_status_mean=0, stream_read_close_operations_mean=0, op_xattr_get_named_map_mean=0, stream_read_vectored_operations_mean=0, op_mkdirs_mean=185, action_http_post_request_mean=41, stream_write_close_operations_mean=34, action_http_put_request_mean=72, action_http_patch_request_mean=0, op_hsync_mean=0, delegation_tokens_issued_mean=0, action_http_delete_request_mean=30, stream_read_operations_mean=42, op_create_mean=92, op_delete_mean=0, op_create_non_recursive_mean=0, stream_readVectored_range_duration_mean=0, op_xattr_get_named_mean=0, gcs_get_media_request_mean=32, op_get_file_status_mean=630, op_delete_duration=0, op_get_file_status_duration=630, action_http_put_request_duration=216, stream_write_operations_duration=0, op_hsync_duration=0, gcs_metadata_request_duration=718, gcs_get_media_request_duration=32, gcs_list_file_request_duration=299, op_list_status_duration=0, op_mkdirs_duration=185, op_open_duration=36, op_create_duration=185, op_hflush_duration=393, gcs_list_dir_request_duration=0, op_glob_status_duration=0, stream_read_operations_duration=42, action_http_delete_request_duration=30, action_http_post_request_duration=251, op_rename_duration=0]


BQ_CONNECTION = "cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl"

ICEBERG_CATALOG='cacib-lsdh-dev-df'
ICEBERG_DB='blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb'
ICEBERG_TABLE_NAME='store_sales_denorm_bench_test'
BUCKET = "gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb"
PROJECT = "cacib-lsdh-dev-df"
LOCATION = "europe-west9"
INPUT_BUCKET="gs://bkt-lsdh-dev-ew9-bench-bl-raw-data-f72a/convert/50G/store_sales_denorm_start/*.parquet"
BQ_DATASET = f"{ICEBERG_CATALOG}.{ICEBERG_DB}"
BQ_CONNECTION = "cacib-lsdh-dev-df.europe-west9.bq-co-lsdh-dev-ew9-vai-bench-bl"
options: list[tuple[str]] = [
        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog"),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl","org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.gcp_project", PROJECT),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.gcp_location", LOCATION),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.connection_id", BQ_CONNECTION),
        (f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", BUCKET)
    ]

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.


spark_conf = SparkConf() \
                    .setAppName(value="setup_iceberg") \
                    .setAll(pairs=options)

spark = SparkSession \
  .builder\
  .appName('spark-bigquery-demo') \
  .enableHiveSupport().config(conf=spark_conf) \
  .enableHiveSupport()  \
  .getOrCreate() \
     



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


