import pandas as pd

# ----------------------------
# Inputs
# ----------------------------
files = {
    "10G": {
        "Iceberg": "iceberg_10G.csv",
        "External": "external_10G.csv",
        "BigQuery": "bigquery_10G.csv"
    },
    "100G": {
        "Iceberg": "iceberg_100G.csv",
        "External": "external_100G.csv",
        "BigQuery": "bigquery_100G.csv"
    }
}

metrics_to_compare = [
    "query", "exec_time",
    "s_after_added-records", "s_before_added-records",
    "s_after_removed-files-size", "s_before_removed-files-size",
    "s_after_total-data-files", "s_after_total-files-size"
]

# ----------------------------
# Build long DataFrame
# ----------------------------
long_data = []

for scale, tech_files in files.items():
    for tech, file in tech_files.items():
        df = pd.read_csv(file)
        df = df[[c for c in metrics_to_compare if c in df.columns]].copy()

        # Derived metrics
        rows = df["s_after_added-records"].fillna(0) + df["s_before_added-records"].fillna(0)
        size_mb = (df["s_after_removed-files-size"].fillna(0) + df["s_before_removed-files-size"].fillna(0)) / (1024*1024)
        file_count = df["s_after_total-data-files"].fillna(0).astype(int)
        total_size_gb = df["s_after_total-files-size"].fillna(0) / (1024*1024*1024)

        # Append long format
        for q, et, r, s, fc, ts in zip(df["query"], df["exec_time"], rows, size_mb, file_count, total_size_gb):
            long_data.append({
                "query": q,
                "scale": scale,
                "technology": tech,
                "exec_time": et,
                "rows": r,
                "processed_size_MB": s,
                "file_count": fc,
                "total_size_GB": ts
            })

df_long = pd.DataFrame(long_data)
df_long.head()
