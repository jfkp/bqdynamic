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
