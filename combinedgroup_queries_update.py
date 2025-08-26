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


import pandas as pd
import plotly.express as px

def prepare_update_read_timeline(df_update, df_query):
    # Step 1: aggregate query metrics (mean per wquery + query)
    df_query_mean = (
        df_query.groupby(["wquery", "query", "scale", "technology"], as_index=False)
        .agg({"exec_time": "mean"})
    )

    # Step 2: unify update metrics
    df_update_clean = df_update[["query", "scale", "technology", "exec_time"]].copy()
    df_update_clean["type"] = "update"
    df_update_clean.rename(columns={"query": "update_query"}, inplace=True)

    df_query_mean["type"] = "read"
    df_query_mean.rename(columns={"wquery": "update_query", "query": "read_query"}, inplace=True)

    # Step 3: assign plotting order
    result = []
    for uq in df_update_clean["update_query"].unique():
        for scale in df_update_clean["scale"].unique():
            for tech in df_update_clean["technology"].unique():
                # update row
                urow = df_update_clean[
                    (df_update_clean["update_query"] == uq) &
                    (df_update_clean["scale"] == scale) &
                    (df_update_clean["technology"] == tech)
                ]
                if not urow.empty:
                    urow = urow.copy()
                    urow["plot_label"] = urow["update_query"]
                    urow["order"] = 0
                    result.append(urow)

                # associated read queries
                qrows = df_query_mean[
                    (df_query_mean["update_query"] == uq) &
                    (df_query_mean["scale"] == scale) &
                    (df_query_mean["technology"] == tech)
                ].copy()
                if not qrows.empty:
                    qrows["plot_label"] = qrows["read_query"]
                    qrows["order"] = qrows.groupby("update_query").cumcount() + 1
                    result.append(qrows)

    return pd.concat(result, ignore_index=True)


def plot_update_read_timeline(df_timeline):
    fig = px.bar(
        df_timeline,
        x="plot_label",
        y="exec_time",
        color="technology",
        facet_col="scale",
        barmode="group",
        text="type",  # annotate update vs read
        category_orders={"plot_label": list(df_timeline.sort_values("order")["plot_label"].unique())}
    )
    fig.update_layout(
        title="Update Queries with Their Associated Read Queries (Mean Exec Time)",
        xaxis_title="Query sequence",
        yaxis_title="Execution time (s)",
        height=700,
        template="plotly_white"
    )
    fig.show()



import pandas as pd
import plotly.express as px


# load both datasets
df_update = load_metrics(files, metric_type="update")
df_query = load_metrics(files, metric_type="query")

# build sequence dataset
df_seq = prepare_update_read_sequence(df_update, df_query)

# plot
plot_update_read_sequence(df_seq)



df_update = load_metrics(files, metric_type="update")
df_query = load_metrics(files, metric_type="query")

df_timeline = prepare_update_read_timeline(df_update, df_query)
plot_update_read_timeline(df_timeline)



def load_seq_metrics(files, metric_type="query"):
    """
    Load multiple CSVs (scale × technology) and return a long-format DataFrame.
    """
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

    # Keep the query name + reference to update query
    extra_fields = ["query", "wquery", "timestamp", "run_id"]  # <-- make sure wquery is here

    long_data = []

    for scale, tech_files in files.items():
        for tech, file in tech_files.items():
            df = pd.read_csv(file)
            # Keep only relevant columns
            cols_to_keep = [c for c in metrics_map.keys() if c in df.columns] + \
                           [c for c in extra_fields if c in df.columns]
            df = df[cols_to_keep].copy()

            for idx, row_data in df.iterrows():
                row_dict = {"scale": scale, "technology": tech}
                for f in extra_fields:
                    if f in row_data:
                        row_dict[f] = row_data[f]

                for orig, new in metrics_map.items():
                    if orig in row_data:
                        row_dict[new] = row_data[orig]

                long_data.append(row_dict)

    return pd.DataFrame(long_data)

def prepare_update_read_sequence(df_update, df_query):
    rows = []

    for (scale, tech), group in df_update.groupby(["scale", "technology"]):
        for _, update_row in group.iterrows():
            uq = update_row["query"]

            # 1. Add the update query first
            rows.append({
                "scale": scale,
                "technology": tech,
                "parent_update": uq,
                "query": uq,   # keep own name
                "exec_time": update_row["exec_time"],
                "type": "update",
                "seq_id": f"{uq}_0"
            })

            # 2. Add its associated read queries
            assoc_reads = df_query[
                (df_query["wquery"] == uq) &
                (df_query["scale"] == scale) &
                (df_query["technology"] == tech)
            ].copy()

            if not assoc_reads.empty:
                # mean exec time per query name
                assoc_reads = assoc_reads.groupby("query", as_index=False).agg({"exec_time": "mean"})
                assoc_reads = assoc_reads.sort_values("query")  # enforce Q1..Qn order

                for i, row in enumerate(assoc_reads.itertuples(), start=1):
                    rows.append({
                        "scale": scale,
                        "technology": tech,
                        "parent_update": uq,
                        "query": row.query,   # label with query name (Q1, Q2…)
                        "exec_time": row.exec_time,
                        "type": "read",
                        "seq_id": f"{uq}_{i}"
                    })

    return pd.DataFrame(rows)


import plotly.express as px
import plotly.graph_objects as go

def plot_update_read_sequence(df_seq):
    # Ensure correct ordering
    df_seq = df_seq.sort_values(["scale", "parent_update", "seq_id"]).reset_index(drop=True)

    # Make sure seq_id is numeric
    df_seq["seq_id"] = pd.to_numeric(df_seq["seq_id"], errors="coerce")

    fig = px.bar(
        df_seq,
        x="seq_id",
        y="exec_time",
        color="technology",          # color by technology
        pattern_shape="type",        # ✅ different fill for update vs read
        facet_col="scale",
        text="query",                # ✅ show query name on top of each bar
        hover_data=["parent_update", "query", "type", "technology"]
    )

    # Add vertical separators at each update start
    update_indices = df_seq[df_seq["type"] == "update"].index.tolist()
    for idx in update_indices:
        x_pos = df_seq.loc[idx, "seq_id"]
        if pd.notna(x_pos):  # only if it's numeric
            fig.add_vline(
                x=x_pos - 0.5,
                line_width=1,
                line_dash="dash",
                line_color="grey"
            )

    fig.update_layout(
        title="Update Queries followed by their Read Queries",
        xaxis_title="Update + Read Query Sequence",
        yaxis_title="Execution Time (s)",
        height=700,
        template="plotly_white",
        legend_title="Technology"
    )

    # Better annotation placement
    fig.update_traces(
        textposition="outside",
        marker=dict(line=dict(width=0.5, color="black"))
    )

    fig.show()

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


