import plotly.express as px

fig = px.bar(
    df_long,
    x="query",
    y="exec_time",
    color="technology",
    barmode="group",
    facet_col="scale",  # one panel per scale
    hover_data=["rows", "processed_size_MB", "file_count", "total_size_GB"]
)

fig.update_layout(
    title="Execution Time Comparison Across Technologies and Scales",
    yaxis_title="Execution Time (seconds)",
    xaxis_title="Query",
    height=700,
    template="plotly_white"
)
fig.update_xaxes(tickangle=-45)

fig.show()
