import pandas as pd
import matplotlib.pyplot as plt

# Example file dictionary
files = {
    "10GB": {
        "blms": {"update": "blms_update_10GB.csv", "read": "blms_read_10GB.csv"},
        "bqms": {"update": "bqms_update_10GB.csv", "read": "bqms_read_10GB.csv"},
        "bqmn": {"update": "bqmn_update_10GB.csv", "read": "bqmn_read_10GB.csv"},
    },
    "20GB": {
        "blms": {"update": "blms_update_20GB.csv", "read": "blms_read_20GB.csv"},
        "bqms": {"update": "bqms_update_20GB.csv", "read": "bqms_read_20GB.csv"},
        "bqmn": {"update": "bqmn_update_20GB.csv", "read": "bqmn_read_20GB.csv"},
    },
}

# Load all data into a single dataframe
def load_data(files):
    df_list = []
    for scale, tech_dict in files.items():
        for tech, paths in tech_dict.items():
            # Update queries
            df_update = pd.read_csv(paths['update'])
            df_update['technology'] = tech
            df_update['scale'] = scale
            df_update['query_type'] = 'update'
            df_list.append(df_update)

            # Read queries
            df_read = pd.read_csv(paths['read'])
            df_read['technology'] = tech
            df_read['scale'] = scale
            df_read['query_type'] = 'read'
            df_list.append(df_read)
    df_all = pd.concat(df_list, ignore_index=True)
    return df_all

# Prepare data for plotting
def prepare_plot_data(files, tech_list):
    all_data = []

    for scale, tech_files in files.items():
        scale_num = int(scale.replace("GB", ""))  # Fix here
        for tech in tech_list:
            # Load update CSV
            df_update = pd.read_csv(tech_files[tech]['update'])
            df_update['technology'] = tech
            df_update['scale'] = scale_num

            # Load read CSV
            df_read = pd.read_csv(tech_files[tech]['read'])
            df_read['technology'] = tech
            df_read['scale'] = scale_num

            # Merge update + read
            df_update_read = pd.concat([df_update, df_read], ignore_index=True)
            all_data.append(df_update_read)

    return pd.concat(all_data, ignore_index=True)

# Plot function
def plot_exec_times(df_plot, scale_order):
    colors = {'blms':'#1f77b4', 'bqms':'#ff7f0e', 'bqmn':'#2ca02c'}
    plt.figure(figsize=(14, 7))

    for tech in df_plot['technology'].unique():
        df_tech = df_plot[df_plot['technology'] == tech]
        for update_query in df_tech[df_tech['query_type']=='update']['query'].unique():
            # Filter update query
            df_update = df_tech[(df_tech['query_type']=='update') & (df_tech['query']==update_query)]
            plt.plot(df_update['scale_num'], df_update['exec_time'], marker='o', linestyle='-', color=colors[tech], label=f"{tech} - {update_query}")

            # Plot associated read queries for this update
            df_reads = df_tech[(df_tech['query_type']=='read') & (df_tech['wquery']==update_query)]
            for _, row in df_reads.iterrows():
                plt.plot(row['scale_num'], row['exec_time'], marker='s', color=colors[tech])
                # Label the read query on the left
                plt.text(
                    x=row['scale_num'] - 0.15,
                    y=row['exec_time'],
                    s=row['query'],
                    horizontalalignment='right',
                    fontsize=8
                )

    plt.xticks(range(len(scale_order)), scale_order)
    plt.xlabel("Scale")
    plt.ylabel("Execution Time (s)")
    plt.title("Execution Time for Update and Read Queries Across Scales")
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3)
    plt.grid(axis='y')
    plt.tight_layout()
    plt.show()

# Main
df_all = load_data(files)
df_plot, scale_order = prepare_plot_data(df_all)
plot_exec_times(df_plot, scale_order)
