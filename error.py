 spark.sql(f"""CREATE EXTERNAL TABLE  `{ICEBERG_CATALOG}`.`{ICEBERG_DB}`.`{ICEBERG_TABLE_NAME}`
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/session.py", line 1631, in sql
  File "/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/java_gateway.py", line 1322, in __call__
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/errors/exceptions/captured.py", line 179, in deco
  File "/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip/py4j/protocol.py", line 326, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling o96.sql.
: java.lang.NoSuchMethodError: 'org.apache.iceberg.BaseMetastoreTableOperations$CommitStatus org.apache.iceberg.gcp.bigquery.BigQueryTableOperations.checkCommitStatus(java.lang.String, org.apache.iceberg.TableMetadata)'
	at org.apache.iceberg.gcp.bigquery.BigQueryTableOperations.doCommit(BigQueryTableOperations.java:98)
