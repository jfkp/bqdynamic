import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_and_prepare(files):
    all_data = []
    for scale, techs in files.items():
        for tech, paths in techs.items():
            upd = pd.read_csv(paths["update"])
            read = pd.read_csv(paths["read"])

            upd["query_type"] = "update"
            read["query_type"] = "read"

            # Keep original query names, just add update_query linkage
            upd["update_query"] = upd["query"]        # the update itself
            read = read.rename(columns={"wquery": "update_query"})

            # Select columns while keeping both 'query' and 'update_query'
            upd = upd[["update_query", "query", "exec_time", "query_type"]]
            read = read[["update_query", "query", "exec_time", "query_type"]]

            for df in (upd, read):
                df["technology"] = tech
                df["scale"] = scale

            all_data.append(pd.concat([upd, read], ignore_index=True))

    return pd.concat(all_data, ignore_index=True)



def plot_exec_times(df, scale):
    # Filter for the scale
    df_scale = df[df['scale'] == scale]

    # Set plot style
    sns.set(style="whitegrid")
    
    # Create a grouped bar plot
    plt.figure(figsize=(15, 6))
    ax = sns.barplot(
        data=df_scale,
        x='query',           # each query name
        y='exec_time',
        hue='technology',    # tech differences in color
        dodge=True
    )

    # Add update_query as a label grouping separator
    query_positions = range(len(df_scale['query'].unique()))
    update_queries = df_scale['update_query'].unique()
    
    # Draw vertical lines to separate groups
    last_pos = -0.5
    for uq in update_queries:
        n_queries = len(df_scale[df_scale['update_query'] == uq]['query'].unique())
        plt.axvline(x=last_pos + n_queries, color='grey', linestyle='--', alpha=0.5)
        last_pos += n_queries

    # Labels and title
    plt.xlabel("Query (reads labeled by their own name)")
    plt.ylabel("Execution Time (s)")
    plt.title(f"Execution Times for Scale {scale}")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Technology")
    plt.tight_layout()
    plt.show()

plt.show()

# --- Prepare data ---
all_data = load_and_prepare(files)
# Plot all scales together
plot_grouped_bar(all_data)
