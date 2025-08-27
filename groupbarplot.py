import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def load_and_prepare(files):
    all_data = []

    for scale, tech_files in files.items():
        for tech, paths in tech_files.items():
            # Load update queries
            df_update = pd.read_csv(paths['update'])
            df_update['technology'] = tech
            df_update['scale'] = scale
            df_update['query_type'] = 'update'

            # Load read queries
            df_read = pd.read_csv(paths['read'])
            df_read['technology'] = tech
            df_read['scale'] = scale
            df_read['query_type'] = 'read'

            # Merge read queries with their associated update queries using wquery
            df_read = df_read.merge(
                df_update[['query', 'exec_time']],
                left_on='wquery',
                right_on='query',
                suffixes=('_read', '_update')
            )

            # Keep relevant columns and rename
            df_read = df_read.rename(columns={
                'query_read': 'query',
                'exec_time_read': 'exec_time',
                'query_update': 'update_query',
                'exec_time_update': 'update_exec_time'
            })

            # Append both update and read queries to combined DataFrame
            all_data.append(df_update)
            all_data.append(df_read)

    combined_df = pd.concat(all_data, ignore_index=True)
    return combined_df


def plot_grouped_bar(df, scale):
    df_scale = df[df['scale'] == scale]
    technologies = df_scale['technology'].unique()

    colors = {'blms': 'C0', 'bqms': 'C1', 'bqmn': 'C2'}  # Color per technology
    max_reads = df_scale.groupby('update_query').size().max()
    bar_width = 0.4 / (max_reads + 1)  # dynamically adjust width

    plt.figure(figsize=(16, 6))

    group_positions = []
    group_labels = []

    pos = 0
    for tech in technologies:
        df_tech = df_scale[df_scale['technology'] == tech]
        update_queries = df_tech[df_tech['query_type'] == 'update']['query'].unique()

        for u_query in update_queries:
            # Update query bar
            u_exec_time = df_tech[(df_tech['query'] == u_query) & (df_tech['query_type'] == 'update')]['exec_time'].values[0]
            plt.bar(pos, u_exec_time, width=bar_width, color=colors[tech], label=f'{tech} update' if pos == 0 else "")
            plt.text(pos, u_exec_time + 0.02*u_exec_time, u_query, rotation=90, ha='center', va='bottom', fontsize=8)

            # Read queries
            reads = df_tech[(df_tech['update_query'] == u_query) & (df_tech['query_type'] == 'read')]
            for i, (_, row) in enumerate(reads.iterrows()):
                read_pos = pos + bar_width*(i+1)
                plt.bar(read_pos, row['exec_time'], width=bar_width, color=colors[tech], alpha=0.7,
                        label=f'{tech} read' if pos == 0 and i == 0 else "")
                plt.text(read_pos, row['exec_time'] + 0.02*row['exec_time'], row['query'], rotation=90, ha='center', va='bottom', fontsize=8)

            group_positions.append(pos + bar_width*(len(reads)/2))
            group_labels.append(u_query)
            pos += bar_width*(len(reads)+2)  # space to next group

    plt.ylabel("Execution Time (s)")
    plt.title(f"Update and Read Queries Execution Time - {scale}")
    plt.xticks(group_positions, group_labels, rotation=45, ha='right')
    plt.legend()
    plt.tight_layout()
    plt.show()


# Example usage
files = {
    "10GB": {
        "blms": {"update": "blms_update_10GB.csv", "read": "blms_read_10GB.csv"},
        "bqms": {"update": "bqms_update_10GB.csv", "read": "bqms_read_10GB.csv"},
        "bqmn": {"update": "bqmn_update_10GB.csv", "read": "bqmn_read_10GB.csv"},
    }
}

df_all = load_and_prepare(files)
plot_grouped_bar(df_all, "10GB")
