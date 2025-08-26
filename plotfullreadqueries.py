

df_long = load_query_metrics(files, scales=None, technologies=None)

fig = px.bar(
    df_long,
    x="query",
    y="exec_time",
    color="technology",
    barmode="group",
    facet_col="scale",
    hover_data=["bytes_read", "records_read", "bytes_written", "records_written"]
)

fig.update_layout(
    title="Query Execution Time Comparison Across Scales and Technologies",
    xaxis_title="Query",
    yaxis_title="Execution Time (s)"
)

fig.show()
