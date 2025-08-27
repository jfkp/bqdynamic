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


def plot_grouped_exec_times(df, scale):
    # Filter the scale
    df_scale = df[df['scale'] == scale].copy()

    # Create a new column for proper sorting: update_query + query name
    df_scale['sort_key'] = df_scale['update_query'].astype(str) + "_" + df_scale['query'].astype(str)
    df_scale = df_scale.sort_values(by='sort_key')

    # Set x-axis order
    x_order = df_scale['query'].tolist()

    # Plot
    plt.figure(figsize=(15, 6))
    sns.barplot(
        data=df_scale,
        x='query',
        y='exec_time',
        hue='technology',
        order=x_order
    )

    plt.xlabel("Queries (reads labeled by their name)")
    plt.ylabel("Execution Time (s)")
    plt.title(f"Execution Times for Scale {scale}")
    plt.xticks(rotation=45, ha='right')
    plt.legend(title="Technology")
    plt.tight_layout()
    plt.show()


# --- Prepare data ---
all_data = load_and_prepare(files)
# Plot all scales together
plot_grouped_bar(all_data)
