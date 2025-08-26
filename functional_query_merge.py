import pandas as pd
import plotly.express as px

def load_metrics(files, metric_type="update"):
    """
    Load multiple CSVs (scale × technology) and return a long-format DataFrame.
    
    files: dict[scale][technology] = path_to_csv
    metric_type: "update" or "query"
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
            "shuffleBytesWritten": "shuffle_bytes_written"
        }
    else:
        raise ValueError("metric_type must be 'update' or 'query'")

    long_data = []

    for scale, tech_files in files.items():
        for tech, file in tech_files.items():
            df = pd.read_csv(file)
            
            # Extract only relevant metrics
            df = df[[c for c in metrics_map.keys() if c in df.columns] + ["query"]].copy()

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
                        if orig in row_data:
                            row_dict[new] = row_data[orig]

                long_data.append(row_dict)

    return pd.DataFrame(long_data)


def plot_metrics(df_long, metric="exec_time"):
    """
    Plot interactive grouped bar chart of a metric across scales and technologies.
    """
    fig = px.bar(
        df_long,
        x="query",
        y=metric,
        color="technology",
        barmode="group",
        facet_col="scale",
        hover_data=[c for c in df_long.columns if c not in ["query", "scale", "technology", metric]]
    )

    fig.update_layout(
        title=f"{metric} Comparison Across Technologies and Scales",
        xaxis_title="Query",
        yaxis_title=metric,
        height=700,
        template="plotly_white"
    )
    fig.update_xaxes(tickangle=-45)
    fig.show()

# Load update metrics
df_update = load_metrics(files, metric_type="update")

# Plot update execution time
plot_metrics(df_update, metric="exec_time")

# Load query metrics
df_query = load_metrics(files, metric_type="query")

# Plot query execution time
plot_metrics(df_query, metric="exec_time")
