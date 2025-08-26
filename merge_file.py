import pandas as pd
from functools import reduce
import matplotlib.pyplot as plt
import numpy as np

# --- 1️⃣ Define CSV files and scales ---
files = ["bench_10G.csv", "bench_100G.csv", "bench_1T.csv"]
scales = ["10G", "100G", "1T"]

# --- 2️⃣ Define metrics to extract and compare ---
metrics_to_compare = [
    'query',  # always keep the key for merging
    'exec_time',
    's_before_added-records',
    's_before_removed-files-size',
    's_after_added-records',
    's_after_removed-files-size',
    's_after_total-files-size',
    's_after_total-data-files'
]

# --- 3️⃣ Load CSVs and extract only metrics ---
df_list = []

for file, scale in zip(files, scales):
    temp = pd.read_csv(file)
    # Keep only metrics of interest that exist in this CSV
    temp_metrics = temp[[col for col in metrics_to_compare if col in temp.columns]].copy()
    
    # Rename metric columns to include scale (except 'query')
    rename_dict = {col: f"{col}_{scale}" for col in temp_metrics.columns if col != 'query'}
    temp_metrics = temp_metrics.rename(columns=rename_dict)
    
    df_list.append(temp_metrics)

# --- 4️⃣ Merge all metrics DataFrames on 'query' ---
df_merged = reduce(lambda left, right: pd.merge(left, right, on='query', how='outer'), df_list)
