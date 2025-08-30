import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

def plot_read_write_scaling_trends(combined_df: pd.DataFrame, save_plots: bool = False, output_dir: str = 'plots'):
    """Plot scaling trends for read and write operations."""
    
    # Classify queries
    df_classified = classify_query_type(combined_df.copy())
    
    # Separate read and write data
    read_data = df_classified[df_classified['query_operation'] == 'READ']
    write_data = df_classified[df_classified['query_operation'] == 'WRITE']
    
    if read_data.empty and write_data.empty:
        print("⚠️ No data found for plotting scaling trends")
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8), sharey=True)
    
    # Collect all unique technologies for consistent coloring
    all_techs = pd.concat([read_data['base_type'], write_data['base_type']]).unique()
    colors = cm.get_cmap('tab10', len(all_techs))  # tab10 gives distinct colors
    tech_color_map = {tech: colors(i) for i, tech in enumerate(all_techs)}

    def plot_trends(ax, data, title, marker):
        if not data.empty:
            trends = data.groupby(['scale', 'base_type'])['exec_time'].mean().unstack()
            sorted_scales = sort_scales(trends.index.tolist())
            trends = trends.reindex(sorted_scales)
            
            for tech in trends.columns:
                series = trends[tech]
                if series.notna().any():  # avoid plotting empty series
                    ax.plot(
                        series.index, series.values,
                        marker=marker, linewidth=2, markersize=8,
                        color=tech_color_map[tech], label=tech
                    )
            
            ax.set_title(title, fontsize=14, fontweight='bold')
            ax.set_xlabel('Data Scale', fontsize=12)
            ax.set_ylabel('Average Execution Time (seconds)', fontsize=12)
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            ax.grid(True, alpha=0.3)
        else:
            ax.text(0.5, 0.5, f'No {title.split()[0]} Data Available',
                    ha='center', va='center', transform=ax.transAxes, fontsize=14)
            ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Plot read & write trends
    plot_trends(ax1, read_data, 'Read Operations Scaling Trends', marker='o')
    plot_trends(ax2, write_data, 'Write Operations Scaling Trends', marker='s')
    
    plt.tight_layout(rect=[0, 0, 0.85, 1])  # leave room for legends
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'read_write_scaling_trends.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"✅ Plot saved: {filename}")
    
    plt.show()
