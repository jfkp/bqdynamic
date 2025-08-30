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
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
    
    # Read operations scaling trends
    if not read_data.empty:
        read_trends = read_data.groupby(['scale', 'base_type'])['exec_time'].mean().unstack()
        sorted_scales = sort_scales(read_trends.index.tolist())
        read_trends = read_trends.reindex(sorted_scales)
        
        for tech in read_trends.columns:
            ax1.plot(read_trends.index, read_trends[tech], marker='o', linewidth=2, 
                    markersize=8, label=tech)
        
        ax1.set_title('Read Operations Scaling Trends', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Data Scale', fontsize=12)
        ax1.set_ylabel('Average Execution Time (seconds)', fontsize=12)
        ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax1.grid(True, alpha=0.3)
    else:
        ax1.text(0.5, 0.5, 'No Read Data Available', ha='center', va='center', 
                transform=ax1.transAxes, fontsize=14)
        ax1.set_title('Read Operations Scaling Trends', fontsize=14, fontweight='bold')
    
    # Write operations scaling trends
    if not write_data.empty:
        write_trends = write_data.groupby(['scale', 'base_type'])['exec_time'].mean().unstack()
        sorted_scales = sort_scales(write_trends.index.tolist())
        write_trends = write_trends.reindex(sorted_scales)
        
        for tech in write_trends.columns:
            ax2.plot(write_trends.index, write_trends[tech], marker='s', linewidth=2, 
                    markersize=8, label=tech)
        
        ax2.set_title('Write Operations Scaling Trends', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Data Scale', fontsize=12)
        ax2.set_ylabel('Average Execution Time (seconds)', fontsize=12)
        ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax2.grid(True, alpha=0.3)
    else:
        ax2.text(0.5, 0.5, 'No Write Data Available', ha='center', va='center', 
                transform=ax2.transAxes, fontsize=14)
        ax2.set_title('Write Operations Scaling Trends', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'read_write_scaling_trends.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()
