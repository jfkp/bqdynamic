CREATE EXTERNAL TABLE cacib-lsdh-dev-df.blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb.store_sale_denorm_bench_test
(
    order_id BIGINT,
    customer_id BIGINT,
    amount DECIMAL,
    order_date DATE
)
WITH EXTERNAL_TABLE_OPTIONS (
    uri_patterns = ['gs://bkt-lsdh-dev-ew9-bench-bl-lakehouse-ext-tb-00/blmt_ds_lsdh_dev_ew9_bench_bl_ib_mg_tb/store_sales_denorm_bench_test/*'],
    connection = 'projects/cacib-lsdh-dev-df/locations/europe-west9/connections/bq-co-lsdh-dev-ew9-vai-bench-bl',
    data_format = 'ICEBERG'
);



# --- Lecture des données source (par exemple, un DataFrame) ---
# Vous avez déjà le code pour lire les données d'un autre bucket
input_df = spark.read.parquet(INPUT_BUCKET)

# --- Écriture des données dans la table Iceberg ---
# Utilisez la syntaxe d'écriture pour appendre les données à la table existante
# qui a été créée dans BigQuery à l'étape 1.
input_df.writeTo(f"`{ICEBERG_CATALOG}`.`{ICEBERG_DB}`.`{ICEBERG_TABLE_NAME}`").append()

print("Données écrites avec succès dans la table BigQuery/Iceberg gérée.")

# Fermez la session Spark
spark.stop()
