Traceback (most recent call last):
  File "/tmp/job-0d849214/testbqstd.py", line 44, in <module>
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

25/09/02 08:30:45 INFO DataprocSparkPlugin: Shutting down driver plugin. metrics=[action_http_patch_request=0, files_created=1, gcs_api_server_timeout_count=0, op_get_list_status_result_size=0, op_open=0, action_http_delete_request=1, gcs_api_time=1368, gcs_backoff_count=0, gcs_api_client_unauthorized_response_count=0, stream_read_close_operations=0, stream_read_bytes_backwards_on_seek=0, gs_filesystem_create=3, exception_count=0, gcs_exception_count=0, gcs_api_total_request_count=25, op_create=1, stream_read_vectored_operations=0, gcs_metadata_request=14, gcs_api_client_bad_request_count=0, action_http_put_request=2, op_create_non_recursive=0, gcs_api_client_gone_response_count=0, gs_filesystem_initialize=2, stream_read_vectored_incoming_ranges=0, stream_write_operations=0, gcs_list_dir_request=0, stream_read_operations=0, gcs_api_client_request_timeout_count=0, op_rename=0, op_get_file_status=1, op_glob_status=0, op_exists=0, stream_write_bytes=107559, op_xattr_list=0, op_get_delegation_token=0, gcs_api_server_unavailable_count=0, directories_created=1, files_delete_rejected=0, stream_read_vectored_combined_ranges=0, op_xattr_get_named=0, gcs_list_file_request=2, op_hsync=0, action_http_get_request=0, stream_read_operations_incomplete=0, op_delete=0, stream_read_bytes=0, gcs_api_client_non_found_response_count=12, op_list_located_status=0, gcs_api_client_requested_range_not_statisfiable_count=0, op_hflush=20, op_list_status=0, stream_read_vectored_read_bytes_discarded=0, op_xattr_get_named_map=0, gcs_api_client_side_error_count=13, op_get_file_checksum=0, gcs_api_server_internal_error_count=0, stream_read_seek_bytes_skipped=0, stream_write_close_operations=0, gcs_get_media_request=0, gcs_connector_time=1446, files_deleted=0, action_http_post_request=5, op_mkdirs=1, gcs_api_client_rate_limit_error_count=0, op_copy_from_local_file=0, gcs_api_server_bad_gateway_count=0, stream_readVectored_range_duration=0, stream_read_seek_backward_operations=0, gcs_api_server_side_error_count=0, stream_read_seek_operations=0, gcs_get_other_request=1, stream_read_seek_forward_operations=0, gcs_api_client_precondition_failed_response_count=1, op_xattr_get_map=0, delegation_tokens_issued=0, gcs_backoff_time=0, gcs_list_dir_request_min=0, gcs_metadata_request_min=17, op_delete_min=0, op_glob_status_min=0, op_create_non_recursive_min=0, op_hsync_min=0, op_xattr_get_named_min=0, op_xattr_get_named_map_min=0, op_hflush_min=0, op_xattr_list_min=0, action_http_put_request_min=61, op_open_min=0, gcs_list_file_request_min=24, stream_write_close_operations_min=0, op_create_min=100, action_http_delete_request_min=36, op_mkdirs_min=220, op_list_status_min=0, gcs_get_media_request_min=0, stream_readVectored_range_duration_min=0, stream_read_vectored_operations_min=0, stream_read_close_operations_min=0, stream_read_operations_min=0, stream_read_seek_operations_min=0, op_xattr_get_map_min=0, stream_write_operations_min=0, action_http_patch_request_min=0, op_get_file_status_min=700, op_rename_min=0, delegation_tokens_issued_min=0, action_http_post_request_min=33, stream_read_close_operations_max=0, stream_read_seek_operations_max=0, op_hflush_max=277, op_xattr_list_max=0, op_xattr_get_map_max=0, action_http_put_request_max=65, action_http_patch_request_max=0, action_http_post_request_max=73, stream_write_close_operations_max=0, action_http_delete_request_max=36, op_mkdirs_max=220, gcs_get_media_request_max=0, op_rename_max=0, stream_read_vectored_operations_max=0, stream_readVectored_range_duration_max=0, op_xattr_get_named_map_max=0, stream_write_operations_max=0, stream_read_operations_max=0, op_xattr_get_named_max=0, op_glob_status_max=0, op_create_non_recursive_max=0, op_get_file_status_max=700, op_open_max=0, delegation_tokens_issued_max=0, gcs_list_file_request_max=273, gcs_metadata_request_max=272, op_create_max=100, op_delete_max=0, op_list_status_max=0, op_hsync_max=0, gcs_list_dir_request_max=0, op_open_mean=0, op_xattr_list_mean=0, op_rename_mean=0, op_xattr_get_map_mean=0, gcs_list_dir_request_mean=0, op_glob_status_mean=0, stream_read_seek_operations_mean=0, gcs_list_file_request_mean=148, stream_write_operations_mean=0, op_hflush_mean=21, gcs_metadata_request_mean=42, op_list_status_mean=0, stream_read_close_operations_mean=0, op_xattr_get_named_map_mean=0, stream_read_vectored_operations_mean=0, op_mkdirs_mean=220, action_http_post_request_mean=48, stream_write_close_operations_mean=0, action_http_put_request_mean=63, action_http_patch_request_mean=0, op_hsync_mean=0, delegation_tokens_issued_mean=0, action_http_delete_request_mean=36, stream_read_operations_mean=0, op_create_mean=100, op_delete_mean=0, op_create_non_recursive_mean=0, stream_readVectored_range_duration_mean=0, op_xattr_get_named_mean=0, gcs_get_media_request_mean=0, op_get_file_status_mean=700, op_delete_duration=0, op_get_file_status_duration=700, action_http_put_request_duration=126, stream_write_operations_duration=0, op_hsync_duration=0, gcs_metadata_request_duration=588, gcs_get_media_request_duration=0, gcs_list_file_request_duration=297, op_list_status_duration=0, op_mkdirs_duration=220, op_open_duration=0, op_create_duration=100, op_hflush_duration=426, gcs_list_dir_request_duration=0, op_glob_status_duration=0, stream_read_operations_duration=0, action_http_delete_request_duration=36, action_http_post_request_duration=244, op_rename_duration=0]
