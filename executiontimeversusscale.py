import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def plot_exec_time_vs_scale_annotated(files_dict):
    all_data = []

    for scale, tech_files in files_dict.items():
        for tech, file_paths in tech_files.items():
            # Load update and read data
            df_update = pd.read_csv(file_paths['update'])
            df_read = pd.read_csv(file_paths['read'])

            # Add metadata
            df_update['scale'] = scale
            df_update['technology'] = tech
            df_update['query_type'] = 'update'
            df_update['query_name'] = df_update['query']

            df_read['scale'] = scale
            df_read['technology'] = tech
            df_read['query_type'] = 'read'
            df_read['query_name'] = df_read['query']
            df_read['update_query'] = df_read['wquery']

            all_data.append(df_update)
            all_data.append(df_read)

    # Concatenate all data
    df_all = pd.concat(all_data, ignore_index=True)

    plt.figure(figsize=(14, 7))
    sns.set(style="whitegrid")

    # Plot lines for update and read queries
    lineplot = sns.lineplot(
        data=df_all,
        x='scale',
        y='exec_time',
        hue='technology',
        style='query_type',
        markers=True,
        dashes=False,
        ci=None
    )

    # Annotate read queries
    read_queries = df_all[df_all['query_type'] == 'read']
    for i, row in read_queries.iterrows():
        lineplot.text(
            x=row['scale'],
            y=row['exec_time'] + 0.5,  # slightly above the point
            s=row['query_name'],
            horizontalalignment='center',
            fontsize=8,
            rotation=45
        )

    plt.title("Execution Time vs Scale for Update and Read Queries")
    plt.ylabel("Execution Time (s)")
    plt.xlabel("Data Scale")
    plt.legend(title="Technology / Query Type", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()

    return df_all

# Example usage:
df_all = plot_exec_time_vs_scale_annotated(files)
