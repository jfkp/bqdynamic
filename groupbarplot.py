import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

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

def plot_grouped_bars(df):
    sns.set(style="whitegrid")
    scales = df['scale'].unique()
    
    for scale in scales:
        plt.figure(figsize=(12, 6))
        df_scale = df[df['scale'] == scale]

        # Sort for consistent plotting
        df_scale = df_scale.sort_values(by=['update_query', 'query'])

        # Grouped bar plot
        sns.barplot(
            data=df_scale,
            x='update_query',
            y='exec_time',
            hue='query',
            ci=None
        )
        plt.title(f"Execution Time per Query (Scale: {scale})")
        plt.ylabel("Execution Time (s)")
        plt.xlabel("Update Query")
        plt.xticks(rotation=45)
        plt.legend(title="Query", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.show()

# Example usage
combined_df = load_and_prepare(files)
plot_grouped_bars(combined_df)
