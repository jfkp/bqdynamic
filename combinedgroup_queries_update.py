import pandas as pd
import plotly.express as px

def load_combined_w_r_metrics(files, metric_type="update"):
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
            "run_id": "run_id",
            "wquery": "associated_update_query"  # link to update query
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
                    "run_type": metric_type,
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
                        if orig in row_data and orig not in ["wquery"]:
                            row_dict[new] = row_data[orig]
                    # assign the associated update query for merging
                    row_dict["query"] = row_data.get("wquery", None)
                    row_dict["associated_update_query"] = row_data.get("wquery", None)
                    if "run_id" in row_data:
                        row_dict["run_id"] = row_data["run_id"]

                long_data.append(row_dict)

    return pd.DataFrame(long_data)

def plot_combined_w_r_metrics(df_long, metric="exec_time"):
    """
    Plot grouped bar chart for update queries vs their associated query runs.
    """
    hover_cols = [c for c in df_long.columns if c not in ["query", "scale", "technology", metric]]

    fig = px.bar(
        df_long,
        x="query",                     # this is the update query
        y=metric,
        color="technology",
        barmode="group",
        facet_col="scale",
        pattern_shape="run_type",      # differentiate update vs query
        hover_data=hover_cols
    )

    fig.update_layout(
        title=f"Update Queries vs Associated Query Runs Execution Time",
        xaxis_title="Update Query",
        yaxis_title=metric,
        height=700,
        template="plotly_white"
    )
    fig.update_xaxes(tickangle=-45)
    fig.show()

def plot_combined_metrics_with_annotations(df_long, metric="exec_time"):
    """
    Plot grouped bar chart for update queries vs their associated query runs,
    with annotations showing the associated read query on top of query bars.
    """
    fig = px.bar(
        df_long,
        x="query",                     # update query
        y=metric,
        color="technology",
        barmode="group",
        facet_col="scale",
        pattern_shape="run_type",
        hover_data=[c for c in df_long.columns if c not in ["query", "scale", "technology", metric]]
    )

    # Add annotations for query runs (read queries)
    for i, row in df_long.iterrows():
        if row["run_type"] == "query" and pd.notna(row.get("associated_update_query")):
            fig.add_annotation(
                x=row["query"],
                y=row[metric],
                text=row.get("query", ""),  # you can show the query name or wquery
                showarrow=False,
                yshift=10,
                font=dict(size=9, color="black"),
                xanchor="center"
            )

    fig.update_layout(
        title=f"Update Queries vs Associated Query Runs Execution Time",
        xaxis_title="Update Query",
        yaxis_title=metric,
        height=700,
        template="plotly_white"
    )
    fig.update_xaxes(tickangle=-45)
    fig.show()



def plot_update_and_reads(df_update, df_query, metric="exec_time"):
    """
    Plot update queries along with their associated read queries.
    Uses 'query' in df_update as update query name,
    and 'wquery' in df_query to associate read queries.
    """
    # Step 1: merge queries with updates ON (wquery == update.query) + scale + technology
    df_merged = df_query.merge(
        df_update[["query", "scale", "technology"]],
        left_on=["wquery", "scale", "technology"],
        right_on=["query", "scale", "technology"],
        suffixes=("_read", "_update")
    )

    # Step 2: Concatenate update and its reads
    update_df = df_update.copy()
    update_df["query_type"] = "update"
    update_df["query_display"] = update_df["query"]

    read_df = df_merged.copy()
    read_df["query_type"] = "read"
    read_df["query_display"] = read_df["query_read"]

    combined = pd.concat([
        update_df[["scale", "technology", "query_display", "query_type", metric]],
        read_df[["scale", "technology", "query_display", "query_type", metric]]
    ])

    # Step 3: Plot
    fig = px.bar(
        combined,
        x="query_display",
        y=metric,
        color="technology",
        barmode="group",
        facet_col="scale",
        hover_data=["query_type"]
    )

    # Annotate read queries with their name
    annotations = []
    for i, row in combined.iterrows():
        if row["query_type"] == "read":
            annotations.append(
                dict(
                    x=row["query_display"],
                    y=row[metric],
                    text=row["query_display"],
                    showarrow=False,
                    font=dict(size=10, color="black")
                )
            )
    fig.update_layout(annotations=annotations)

    fig.update_layout(
        title=f"{metric} Update + Read Queries",
        xaxis_title="Query",
        yaxis_title=metric,
        height=700,
        template="plotly_white"
    )
    fig.update_xaxes(tickangle=-45)
    fig.show()


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
        extra_fields = ["query", "timestamp", "run_id"]  # keep update query name and IDs
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
        # keep query name, its reference to update query, and order fields
        extra_fields = ["query", "wquery", "timestamp", "run_id"]
    else:
        raise ValueError("metric_type must be 'update' or 'query'")

    long_data = []

    for scale, tech_files in files.items():
        for tech, file in tech_files.items():
            df = pd.read_csv(file)
            
            # Ensure we only keep relevant + extra fields
            cols_to_keep = [c for c in metrics_map.keys() if c in df.columns]
            cols_to_keep += [c for c in extra_fields if c in df.columns]
            df = df[cols_to_keep].copy()

            # Derived metrics for update type
            if metric_type == "update":
                rows = (
                    df.get("s_after_added-records", 0).fillna(0) +
                    df.get("s_before_added-records", 0).fillna(0)
                )
                size_mb = (
                    df.get("s_after_removed-files-size", 0).fillna(0) +
                    df.get("s_before_removed-files-size", 0).fillna(0)
                ) / (1024*1024)
                file_count = df.get("s_after_total-data-files", 0).fillna(0).astype(int)
                total_size_gb = df.get("s_after_total-files-size", 0).fillna(0) / (1024*1024*1024)

            for idx, row_data in df.iterrows():
                row_dict = {
                    "scale": scale,
                    "technology": tech
                }
                # Always keep extra fields if present
                for f in extra_fields:
                    if f in row_data:
                        row_dict[f] = row_data[f]

                # Common metrics
                if metric_type == "update":
                    row_dict.update({
                        "exec_time": row_data.get("exec_time", None),
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




# ----------------------------
# Example usage
# ----------------------------
files = {
    "10G": {"Iceberg": "iceberg_10G.csv", "External": "external_10G.csv", "BigQuery": "bigquery_10G.csv"},
    "100G": {"Iceberg": "iceberg_100G.csv", "External": "external_100G.csv", "BigQuery": "bigquery_100G.csv"}
}

# Load update and query metrics
df_update = load_combined_w_r_metrics(files, metric_type="update")
df_query = load_combined_w_r_metrics(files, metric_type="query")

# Combine into one DataFrame
df_combined = pd.concat([df_update, df_query], ignore_index=True)

# Plot combined execution time
plot_combined_w_r_metrics(df_combined, metric="exec_time")


# Load metrics
df_update = load_metrics(files, metric_type="update")
df_query = load_metrics(files, metric_type="query")

# Plot
plot_update_and_reads(df_update, df_query, metric="exec_time")


# Load metrics
df_update = load_metrics(files, metric_type="update")
df_query = load_metrics(files, metric_type="query")

# Plot
plot_update_and_reads(df_update, df_query, metric="exec_time")


