import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def plot_grouped_bars_with_labels(df):
    sns.set(style="whitegrid")
    scales = df['scale'].unique()
    
    for scale in scales:
        plt.figure(figsize=(12, 6))
        df_scale = df[df['scale'] == scale]

        # Sort for consistent plotting
        df_scale = df_scale.sort_values(by=['update_query', 'query'])

        # Create grouped barplot
        ax = sns.barplot(
            data=df_scale,
            x='update_query',
            y='exec_time',
            hue='query',
            ci=None
        )

        # Annotate read query bars
        for p, query_name, update_query in zip(ax.patches, df_scale['query'], df_scale['update_query']):
            # Only annotate read queries
            is_read = query_name != update_query
            if is_read:
                height = p.get_height()
                ax.annotate(
                    query_name,
                    (p.get_x() + p.get_width() / 2., height),
                    ha='center',
                    va='bottom',
                    fontsize=8,
                    rotation=90
                )

        plt.title(f"Execution Time per Query (Scale: {scale})")
        plt.ylabel("Execution Time (s)")
        plt.xlabel("Update Query")
        plt.xticks(rotation=45)
        plt.legend(title="Query", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.show()

def load_and_prepare(files_dict):
    dfs = []
    for scale, tech_dict in files_dict.items():
        for tech, file_dict in tech_dict.items():
            # Load update queries
            df_update = pd.read_csv(file_dict['update'])
            df_update['technology'] = tech
            df_update['scale'] = scale
            df_update['update_query'] = df_update['query']  # update query itself

            # Load read queries
            df_read = pd.read_csv(file_dict['read'])
            df_read['technology'] = tech
            df_read['scale'] = scale
            df_read['update_query'] = df_read['wquery']  # associated update query

            # Combine update + read
            df_combined = pd.concat([df_update, df_read], ignore_index=True)
            dfs.append(df_combined[['scale', 'technology', 'query', 'update_query', 'exec_time']])
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_grouped_bar(df_scale, scale):
    # Count the number of update queries to adjust figure width
    num_updates = df_scale['update_query'].nunique()
    plt.figure(figsize=(max(12, num_updates * 1.5), 6))

    # Create grouped bar plot
    ax = sns.barplot(
        data=df_scale,
        x='update_query',
        y='exec_time',
        hue='query',
        ci=None,
        dodge=True,
        width=0.7
    )

    # Rotate x-ticks for readability
    plt.xticks(rotation=45, ha='right')

    # Annotate bars with read query names on top of read query bars
    for p in ax.patches:
        height = p.get_height()
        x = p.get_x() + p.get_width() / 2
        label = p.get_label()
        # Only annotate read queries
        if 'read' in label.lower():
            ax.text(
                x=x, 
                y=height + 0.05*height, 
                s=label, 
                ha='center', 
                va='bottom', 
                fontsize=8,
                rotation=45
            )

    plt.title(f"Grouped Bar Plot for Scale {scale}")
    plt.xlabel("Update Queries")
    plt.ylabel("Execution Time (s)")
    plt.legend(title="Query")
    plt.tight_layout()
    plt.show()

# Example usage
combined_df = load_and_prepare(files)
plot_grouped_bars(combined_df)
