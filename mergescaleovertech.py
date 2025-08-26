import pandas as pd
from functools import reduce

# --- Define CSV files and technologies ---
files = ["iceberg_10G.csv", "external_10G.csv", "bigquery_10G.csv"]
technologies = ["Iceberg", "External Table", "BigQuery"]

# --- Define metrics to extract ---
metrics_to_compare = [
    'query',  # merge key
    'exec_time',
    's_before_added-records',
    's_before_removed-files-size',
    's_after_added-records',
    's_after_removed-files-size',
    's_after_total-files-size',
    's_after_total-data-files'
]

# --- Load CSVs and extract metrics ---
df_list = []

for file, tech in zip(files, technologies):
    temp = pd.read_csv(file)
    temp_metrics = temp[[col for col in metrics_to_compare if col in temp.columns]].copy()
    
    # Rename metrics to include technology (except 'query')
    rename_dict = {col: f"{col}_{tech}" for col in temp_metrics.columns if col != 'query'}
    temp_metrics = temp_metrics.rename(columns=rename_dict)
    
    df_list.append(temp_metrics)

# --- Merge all technologies on 'query' ---
df_merged = reduce(lambda left, right: pd.merge(left, right, on='query', how='outer'), df_list)

# Optional: save merged metrics to CSV
df_merged.to_csv("merged_technologies_10G.csv", index=False)
