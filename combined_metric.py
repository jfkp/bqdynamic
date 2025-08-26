import pandas as pd
import plotly.express as px

def load_combined_metrics(files, metric_type="update"):
    """
    Load multiple CSVs (scale × technology) and return a long-format DataFrame.
    Adds run_type column automatically for combined plotting.
    """
    if metric_type == "update":
        metrics_map = {
            "exec_time": "exec_time",
            "s_before_added-records": "rows_before_added",
            "s_after_added-records": "rows_after_added",
            "s_before_removed-files-size": "size_before_removed_MB",
            "s_after_removed-files-size": "size_after_removed_MB",
            "s_after_total-files-size": "total_size_GB",
            "s_after_total-data-files": "file_count"
        }
    elif metric_type == "query":
        metrics_map = {
            "exec_time": "exec_time",
            "numStages": "num_stages",
            "numTasks": "num_tasks",
            "executorRunTime": "executor_runtime",
            "executorCpuTime": "executor_cpu_time",
            "memoryBytesSpilled": "mem_spilled",
            "diskBytesSpilled": "disk_spilled",
            "bytesRead": "bytes_read",
            "recordsRead": "records_read",
            "bytesWritten": "bytes_written",
            "recordsWritten": "records_written",
            "shuffleTotalBytesRead": "shuffle_bytes_read",
            "shuffleBytesWritten": "shuffle_bytes_written",
            "run_id": "run_id"   # optional, for hover
        }
    else:
        raise ValueError("metric_type must be 'update' or 'query'")

    long_data = []

    for scale, tech_files in files.items():
        for tech, file in tech_files.items():
            df = pd.read_csv(file)

            # Extract relevant columns
            cols_to_keep = [c for c in metrics_map.keys() if c in df.columns] + ["query"]
            df = df[cols_to_keep].copy()

            # Derived metrics for update type
            if metric_type == "update":
                rows = df.get("s_after_added-records", 0).fillna(0) + df.get("s_before_added-records", 0).fillna(0)
                size_mb = (df.get("s_after_removed-files-size", 0).fillna(0) + df.get("s_before_removed-files-size", 0).fillna(0)) / (1024*1024)
                file_count = df.get("s_after_total-data-files", 0).fillna(0).astype(int)
                total_size_gb = df.get("s_after_total-files-size", 0).fillna(0) / (1024*1024*1024)

            for idx, row_data in df.iterrows():
                row_dict = {
                    "query": row_data["query"],
                    "scale": scale,
                    "technology": tech,
                    "run_type": metric_type,          # <-- mark type
                    "exec_time": row_data.get("exec_time", None)
                }
                if metric_type == "update":
                    row_dict.update({
                        "rows": rows[idx],
                        "processed_size_MB": size_mb[idx],
                        "file_count": file_count[idx],
                        "total_size_GB": total_size_gb[idx]
                    })
                else:  # query metrics
                    for orig, new in metrics_map.items():
                        if orig in row_data and orig != "run_id":  # run_id is already optional
                            row_dict[new] = row_data[orig]
                    if "run_id" in row_data:
                        row_dict["run_id"] = row_data["run_id"]

                long_data.append(row_dict)

    return pd.DataFrame(long_data)


def plot_combined_metrics(df_long, metric="exec_time"):
    """
    Plot grouped bar chart for combined update and query metrics,
    showing run_type (update/query) as pattern.
    """
    hover_cols = [c for c in df_long.columns if c not in ["query", "scale", "technology", metric]]

    fig = px.bar(
        df_long,
        x="query",
        y=metric,
        color="technology",
        barmode="group",
        facet_col="scale",
        pattern_shape="run_type",    # differentiate update vs query
        hover_data=hover_cols
    )

    fig.update_layout(
        title=f"Update vs Query {metric} per Query Across Technologies and Scales",
        xaxis_title="Query",
        yaxis_title=metric,
        height=700,
        template="plotly_white"
    )
    fig.update_xaxes(tickangle=-45)
    fig.show()


# ----------------------------
# Usage Example
# ----------------------------
files = {
    "10G": {"Iceberg": "iceberg_10G.csv", "External": "external_10G.csv", "BigQuery": "bigquery_10G.csv"},
    "100G": {"Iceberg": "iceberg_100G.csv", "External": "external_100G.csv", "BigQuery": "bigquery_100G.csv"}
}

# Load both update and query metrics
df_update = load_combined_metrics(files, metric_type="update")
df_query = load_combined_metrics(files, metric_type="query")

# Combine
df_combined = pd.concat([df_update, df_query], ignore_index=True)

# Plot combined execution time
plot_combined_metrics(df_combined, metric="exec_time")
