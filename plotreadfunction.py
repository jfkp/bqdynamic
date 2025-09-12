def plot_read_operations_comparison(combined_df: pd.DataFrame, save_plots: bool = False, output_dir: str = 'plots'):
    """Plot read operations performance comparison across scales and technologies."""
    
    # Classify queries
    df_classified = classify_query_type(combined_df.copy())
    
    # Filter for read operations
    read_data = df_classified[df_classified['query_operation'] == 'READ']
    
    if read_data.empty:
        print("⚠️ No read data found for plotting")
        return
    
    # Group by scale and base_type, calculate mean execution time
    read_pivot = read_data.groupby(['scale', 'base_type'])['exec_time'].mean().unstack()
    
    sorted_scales = sort_scales(read_pivot.index.tolist())
    read_pivot = read_pivot.reindex(sorted_scales)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Create grouped bar chart
    #read_pivot.plot(kind='bar', ax=ax, width=0.8)
    read_pivot.plot(kind='bar', ax=ax, width=0.9, color=['#ff7f0e', '#2ca02c', '#d62728','#b40ee4','#189bcc','#CECECE','#FFDE59','#80391e'])
    ax.set_title('Read Operations Performance Comparison\nAverage Execution Time by Scale and Technology', 
                fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Data Scale', fontsize=12, fontweight='bold')
    ax.set_ylabel('Average Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax.legend(title='Technology', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for container in ax.containers:
        ax.bar_label(container, fmt='%.2f', rotation=0, padding=3)
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'read_operations_comparison.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()
