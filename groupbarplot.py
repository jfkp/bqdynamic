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

    plt.figure(figsize=(16, 6))

    for i, tech in enumerate(technologies):
        df_tech = df_scale[df_scale['technology'] == tech]

        # Get unique update queries
        update_queries = df_tech[df_tech['query_type'] == 'update']['query'].unique()
        positions = np.arange(len(update_queries)) * (len(technologies) * 1.5) + i * 1.5  # spacing between techs

        for j, u_query in enumerate(update_queries):
            # Plot the update query bar
            u_exec_time = df_tech[(df_tech['query'] == u_query) & (df_tech['query_type'] == 'update')]['exec_time'].values[0]
            plt.bar(positions[j], u_exec_time, width=0.4, label=f'{tech} update' if j == 0 else "", color='C0')

            # Plot associated read queries next to the update bar
            reads = df_tech[(df_tech['update_query'] == u_query) & (df_tech['query_type'] == 'read')]
            for k, (_, row) in enumerate(reads.iterrows()):
                plt.bar(positions[j] + 0.4 + 0.2*k, row['exec_time'], width=0.2, label=f'{tech} read' if j == 0 and k == 0 else "", color='C1')
                # Add query name on top
                plt.text(positions[j] + 0.4 + 0.2*k, row['exec_time'] + 0.05*row['exec_time'], row['query'], rotation=90, ha='center', va='bottom', fontsize=8)

            # Add update query name on top
            plt.text(positions[j], u_exec_time + 0.05*u_exec_time, u_query, rotation=90, ha='center', va='bottom', fontsize=8)

    plt.ylabel("Execution Time (s)")
    plt.title(f"Update and Read Queries Execution Time - {scale}")
    plt.xticks([])
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
