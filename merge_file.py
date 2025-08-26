import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import glob

# List your CSV files
files = ["bench_10G.csv", "bench_100G.csv", "bench_1T.csv"]
scales = ["10G", "100G", "1T"]

# Read and merge CSVs on 'query'
df_list = []
for file, scale in zip(files, scales):
    temp = pd.read_csv(file)
    # Rename execution time and other metrics to include scale
    temp = temp.rename(columns={
        'exec_time': f'exec_time_{scale}',
        's_after_added-records': f'added_records_{scale}',
        's_before_deleted-records': f'deleted_records_{scale}',
        's_after_added-files-size': f'added_size_{scale}',
        's_before_removed-files-size': f'removed_size_{scale}',
        's_after_total-data-files': f'file_count_{scale}',
        's_after_total-files-size': f'total_size_{scale}'
    })
    df_list.append(temp)

# Merge all CSVs on 'query'
from functools import reduce
df_merged = reduce(lambda left, right: pd.merge(left, right, on='query', how='outer'), df_list)
