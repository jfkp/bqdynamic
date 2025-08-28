import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import numpy as np
import os
from typing import Dict, List, Optional, Tuple

# Set style for better-looking plots
plt.style.use('default')
sns.set_palette("husl")

def load_data_from_urls(file_dict: Dict[str, str]) -> pd.DataFrame:
    """
    Load data from multiple CSV URLs and combine them into a single DataFrame.
    
    Args:
        file_dict: Dictionary mapping table types to URLs
        
    Returns:
        Combined DataFrame with all data
    """
    combined_data = []
    
    for table_type, url in file_dict.items():
        try:
            print(f"Loading data from {table_type}...")
            response = requests.get(url)
            response.raise_for_status()
            
            # Save to temporary file and read with pandas
            temp_filename = f"temp_{table_type}.csv"
            with open(temp_filename, 'wb') as f:
                f.write(response.content)
            
            df = pd.read_csv(temp_filename)
            df['table_type'] = table_type
            
            # Extract scale and base type from table_type
            parts = table_type.split('_')
            if len(parts) >= 2:
                df['base_type'] = parts[0]  # BQMS, BQMN, BLMS
                df['scale'] = parts[1]      # 10G, 20G, etc.
            else:
                df['base_type'] = table_type
                df['scale'] = 'Unknown'
            
            combined_data.append(df)
            
            # Clean up temp file
            os.remove(temp_filename)
            print(f"✓ Loaded {len(df)} records from {table_type}")
            
        except Exception as e:
            print(f"✗ Error loading {table_type}: {str(e)}")
            continue
    
    if not combined_data:
        raise ValueError("No data could be loaded from the provided URLs")
    
    combined_df = pd.concat(combined_data, ignore_index=True)
    print(f"\n📊 Total combined records: {len(combined_df)}")
    print(f"📊 Scales found: {sorted(combined_df['scale'].unique())}")
    print(f"📊 Base types found: {sorted(combined_df['base_type'].unique())}")
    
    return combined_df

def load_data_from_local_files(file_dict: Dict[str, str], base_directory: str) -> pd.DataFrame:
    """
    Load data from local CSV files and combine them into a single DataFrame.
    
    Args:
        file_dict: Dictionary mapping table types to filenames
        base_directory: Base directory path where CSV files are located
        
    Returns:
        Combined DataFrame with all data
    """
    combined_data = []
    
    for table_type, filename in file_dict.items():
        try:
            file_path = os.path.join(base_directory, filename)
            print(f"Loading data from {file_path}...")
            
            if not os.path.exists(file_path):
                print(f"✗ File not found: {file_path}")
                continue
            
            df = pd.read_csv(file_path)
            df['table_type'] = table_type
            
            # Extract scale and base type from table_type
            parts = table_type.split('_')
            if len(parts) >= 2:
                df['base_type'] = parts[0]  # BQMS, BQMN, BLMS
                df['scale'] = parts[1]      # 10G, 20G, etc.
            else:
                df['base_type'] = table_type
                df['scale'] = 'Unknown'
            
            combined_data.append(df)
            print(f"✓ Loaded {len(df)} records from {filename}")
            
        except Exception as e:
            print(f"✗ Error loading {filename}: {str(e)}")
            continue
    
    if not combined_data:
        raise ValueError("No data could be loaded from the provided files")
    
    combined_df = pd.concat(combined_data, ignore_index=True)
    print(f"\n📊 Total combined records: {len(combined_df)}")
    print(f"📊 Scales found: {sorted(combined_df['scale'].unique())}")
    print(f"📊 Base types found: {sorted(combined_df['base_type'].unique())}")
    
    return combined_df

def load_data_flexible(file_dict: Dict[str, str], base_directory: Optional[str] = None, use_local: bool = False) -> pd.DataFrame:
    """
    Flexible data loading function that can load from URLs or local files.
    
    Args:
        file_dict: Dictionary mapping table types to URLs or filenames
        base_directory: Base directory for local files (required if use_local=True)
        use_local: Whether to load from local files or URLs
        
    Returns:
        Combined DataFrame with all data
    """
    if use_local:
        if base_directory is None:
            raise ValueError("base_directory must be provided when use_local=True")
        return load_data_from_local_files(file_dict, base_directory)
    else:
        return load_data_from_urls(file_dict)

def classify_query_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classify queries as READ or WRITE operations based on file type.
    
    Classification rules:
    - QUERIES files: READ operations
    - UPDATE files: WRITE operations (including bulk_load)
    """
    
    def get_operation_type(row):
        table_type = str(row.get('table_type', '')).upper()
        
        # If from UPDATE files, always WRITE
        if 'UPDATE' in table_type:
            return 'WRITE'
        # If from QUERIES files, always READ
        elif 'QUERIES' in table_type:
            return 'READ'
        else:
            # Default fallback
            return 'READ'
    
    # Apply classification
    df['query_operation'] = df.apply(get_operation_type, axis=1)
    
    # Debug information
    print("\n=== QUERY CLASSIFICATION DEBUG ===")
    print(f"Total records: {len(df)}")
    print(f"READ operations: {len(df[df['query_operation'] == 'READ'])}")
    print(f"WRITE operations: {len(df[df['query_operation'] == 'WRITE'])}")
    
    # Show breakdown by table type
    classification_summary = df.groupby(['table_type', 'query_operation']).size().unstack(fill_value=0)
    print(f"\nClassification by table type:")
    print(classification_summary)
    
    return df

def sort_scales(scales):
    """
    Sort scales in logical order: 10G, 20G, 50G, 100G, etc.
    
    Args:
        scales: List of scale strings
        
    Returns:
        Sorted list of scales
    """
    def scale_sort_key(scale):
        # Extract numeric value from scale (e.g., "10G" -> 10)
        try:
            return int(scale.replace('G', ''))
        except:
            return float('inf')  # Put unknown scales at the end
    
    return sorted(scales, key=scale_sort_key)

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
    read_pivot.plot(kind='bar', ax=ax, width=0.8)
    
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

def plot_write_operations_comparison(combined_df: pd.DataFrame, save_plots: bool = False, output_dir: str = 'plots'):
    """Plot write operations performance comparison across scales and technologies."""
    
    # Classify queries
    df_classified = classify_query_type(combined_df.copy())
    
    # Filter for write operations
    write_data = df_classified[df_classified['query_operation'] == 'WRITE']
    
    if write_data.empty:
        print("⚠️ No write data found for plotting")
        return
    
    # Group by scale and base_type, calculate mean execution time
    write_pivot = write_data.groupby(['scale', 'base_type'])['exec_time'].mean().unstack()
    
    sorted_scales = sort_scales(write_pivot.index.tolist())
    write_pivot = write_pivot.reindex(sorted_scales)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Create grouped bar chart
    write_pivot.plot(kind='bar', ax=ax, width=0.8, color=['#ff7f0e', '#2ca02c', '#d62728'])
    
    ax.set_title('Write Operations Performance Comparison\nAverage Execution Time by Scale and Technology', 
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
        filename = os.path.join(output_dir, 'write_operations_comparison.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()

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

def plot_read_write_ratio_analysis(combined_df: pd.DataFrame, save_plots: bool = False, output_dir: str = 'plots'):
    """Enhanced plot showing read performance, write performance, ratios, and side-by-side comparison."""
    
    # Classify queries
    df_classified = classify_query_type(combined_df.copy())
    
    # Separate read and write data
    read_data = df_classified[df_classified['query_operation'] == 'READ']
    write_data = df_classified[df_classified['query_operation'] == 'WRITE']
    
    if read_data.empty or write_data.empty:
        print("⚠️ Need both read and write data for ratio analysis")
        return
    
    # Calculate averages
    read_avg = read_data.groupby(['scale', 'base_type'])['exec_time'].mean().reset_index()
    write_avg = write_data.groupby(['scale', 'base_type'])['exec_time'].mean().reset_index()
    
    # Merge for ratio calculation
    merged = pd.merge(write_avg, read_avg, on=['scale', 'base_type'], suffixes=('_write', '_read'))
    merged['ratio'] = merged['exec_time_write'] / merged['exec_time_read']
    
    sorted_scales = sort_scales(merged['scale'].unique())
    merged['scale'] = pd.Categorical(merged['scale'], categories=sorted_scales, ordered=True)
    merged = merged.sort_values('scale')
    
    # Create 2x2 subplot layout
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
    
    # 1. Read Performance
    read_pivot = read_avg.pivot(index='scale', columns='base_type', values='exec_time')
    read_pivot = read_pivot.reindex(sorted_scales)
    read_pivot.plot(kind='bar', ax=ax1, width=0.8)
    ax1.set_title('READ Operations Performance', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Execution Time (seconds)')
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax1.grid(True, alpha=0.3)
    
    # 2. Write Performance
    write_pivot = write_avg.pivot(index='scale', columns='base_type', values='exec_time')
    write_pivot = write_pivot.reindex(sorted_scales)
    write_pivot.plot(kind='bar', ax=ax2, width=0.8, color=['#ff7f0e', '#2ca02c', '#d62728'])
    ax2.set_title('WRITE Operations Performance', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Execution Time (seconds)')
    ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax2.grid(True, alpha=0.3)
    
    # 3. Ratio Analysis
    ratio_pivot = merged.pivot(index='scale', columns='base_type', values='ratio')
    ratio_pivot = ratio_pivot.reindex(sorted_scales)
    ratio_pivot.plot(kind='bar', ax=ax3, width=0.8)
    ax3.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='Equal Performance')
    ax3.set_title('Write vs Read Performance Ratio\n(Values > 1 mean Write is slower)', fontsize=14, fontweight='bold')
    ax3.set_ylabel('Ratio (Write/Read)')
    ax3.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax3.grid(True, alpha=0.3)
    
    # 4. Side-by-side comparison
    x = np.arange(len(sorted_scales))
    width = 0.15
    
    for i, tech in enumerate(merged['base_type'].unique()):
        tech_data = merged[merged['base_type'] == tech].sort_values('scale')
        read_vals = tech_data['exec_time_read'].values
        write_vals = tech_data['exec_time_write'].values
        
        ax4.bar(x + i*width*2, read_vals, width, label=f'{tech} READ', alpha=0.7)
        ax4.bar(x + i*width*2 + width, write_vals, width, label=f'{tech} WRITE', alpha=0.7)
    
    ax4.set_title('Direct Read vs Write Comparison', fontsize=14, fontweight='bold')
    ax4.set_ylabel('Execution Time (seconds)')
    ax4.set_xlabel('Data Scale')
    ax4.set_xticks(x + width)
    ax4.set_xticklabels(sorted_scales)
    ax4.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'read_write_ratio_analysis.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()

def plot_wquery_grouped_analysis(combined_df: pd.DataFrame, save_plots: bool = False, output_dir: str = 'plots'):
    """Plot wquery-based analysis with grouped bars for each scale."""
    
    # Classify queries
    df_classified = classify_query_type(combined_df.copy())
    
    if 'wquery' not in df_classified.columns:
        print("⚠️ 'wquery' column not found in data")
        return
    
    # Filter out null wquery values
    df_wquery = df_classified.dropna(subset=['wquery'])
    
    if df_wquery.empty:
        print("⚠️ No wquery data found for plotting")
        return
    
    # Get unique scales and sort them
    scales = sort_scales(df_wquery['scale'].unique())
    n_scales = len(scales)
    
    if n_scales == 0:
        print("⚠️ No scale data found")
        return
    
    # Create subplots - arrange in rows
    fig, axes = plt.subplots(n_scales, 1, figsize=(18, 10 * n_scales))
    if n_scales == 1:
        axes = [axes]
    
    for idx, scale in enumerate(scales):
        scale_data = df_wquery[df_wquery['scale'] == scale]
        
        if scale_data.empty:
            continue
        
        # Group by wquery and base_type, calculate mean execution time
        grouped = scale_data.groupby(['wquery', 'base_type'])['exec_time'].mean().unstack(fill_value=0)
        
        if grouped.empty:
            continue
        
        # Create grouped bar chart
        ax = grouped.plot(kind='bar', ax=axes[idx], width=0.8, figsize=(18, 10))
        
        # Customize the plot
        ax.set_title(f'Query Performance Analysis - {scale} Scale\nExecution Time by Query Type and Technology', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Query Type (wquery)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Average Execution Time (seconds)', fontsize=12, fontweight='bold')
        
        ax.legend(title='Technology', bbox_to_anchor=(1.02, 1), loc='upper left', 
                 fontsize=10, title_fontsize=11, frameon=True, fancybox=True, shadow=True)
        ax.grid(True, alpha=0.3)
        
        # Add value labels on bars with smaller font
        for container in ax.containers:
            ax.bar_label(container, fmt='%.2f', rotation=0, padding=3, fontsize=9)
        
        # Rotate x-axis labels for better readability
        ax.tick_params(axis='x', rotation=45)
    
    plt.subplots_adjust(right=0.82, left=0.08, top=0.92, bottom=0.15)
    plt.tight_layout(pad=3.0, w_pad=2.0, h_pad=2.0)
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'wquery_grouped_analysis.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()

def plot_update_query_combined_analysis(combined_df: pd.DataFrame, save_plots: bool = False, output_dir: str = 'plots'):
    """
    Plot combined analysis showing UPDATE operations with their related QUERIES for each technology and scale.
    Shows individual execution times for each run to check caching effects, not averages.
    Links UPDATE files 'query' column with QUERIES files 'wquery' column.
    """
    
    # Classify queries
    df_classified = classify_query_type(combined_df.copy())
    
    # Separate UPDATE and QUERIES data
    update_data = df_classified[df_classified['query_operation'] == 'WRITE']
    query_data = df_classified[df_classified['query_operation'] == 'READ']
    
    if update_data.empty or query_data.empty:
        print("⚠️ Need both UPDATE and QUERIES data for combined analysis")
        return
    
    if 'query' not in update_data.columns:
        print("⚠️ 'query' column not found in UPDATE data")
        return
        
    if 'wquery' not in query_data.columns:
        print("⚠️ 'wquery' column not found in QUERIES data")
        return
    
    # Filter data with valid linking values
    update_data = update_data.dropna(subset=['query'])
    query_data = query_data.dropna(subset=['wquery'])
    
    if update_data.empty or query_data.empty:
        print("⚠️ No valid linking data found")
        print(f"UPDATE data with 'query': {len(update_data)}")
        print(f"QUERIES data with 'wquery': {len(query_data)}")
        return
    
    # Debug: Show what values we have for linking
    print(f"UPDATE query values: {sorted(update_data['query'].unique())}")
    print(f"QUERIES wquery values: {sorted(query_data['wquery'].unique())}")
    
    # Find common values for linking
    update_queries = set(update_data['query'].unique())
    query_wqueries = set(query_data['wquery'].unique())
    common_queries = update_queries.intersection(query_wqueries)
    
    if not common_queries:
        print("⚠️ No matching queries found between UPDATE 'query' and QUERIES 'wquery' columns")
        print(f"UPDATE queries: {update_queries}")
        print(f"QUERIES wqueries: {query_wqueries}")
        return
    
    print(f"✅ Found {len(common_queries)} matching queries: {sorted(common_queries)}")
    
    # Get unique scales and technologies
    scales = sort_scales(df_classified['scale'].unique())
    technologies = sorted(df_classified['base_type'].unique())
    
    # Create subplots for each scale
    n_scales = len(scales)
    fig, axes = plt.subplots(n_scales, 1, figsize=(24, 10 * n_scales))
    if n_scales == 1:
        axes = [axes]
    
    for idx, scale in enumerate(scales):
        ax = axes[idx]
        
        # Filter data for current scale
        scale_updates = update_data[update_data['scale'] == scale]
        scale_queries = query_data[query_data['scale'] == scale]
        
        if scale_updates.empty and scale_queries.empty:
            ax.text(0.5, 0.5, f'No data available for {scale}', 
                   ha='center', va='center', transform=ax.transAxes, fontsize=14)
            ax.set_title(f'UPDATE + QUERIES Performance - {scale} Scale', 
                        fontsize=16, fontweight='bold')
            continue
        
        # Prepare data for plotting - group by technology and linked queries
        plot_data = []
        x_positions = []
        x_labels = []
        bar_width = 0.15
        current_x = 0
        
        for tech in technologies:
            tech_updates = scale_updates[scale_updates['base_type'] == tech]
            tech_queries = scale_queries[scale_queries['base_type'] == tech]
            
            if tech_updates.empty and tech_queries.empty:
                continue
            
            # Get queries that can be linked for this technology
            tech_update_queries = set(tech_updates['query'].unique()) if not tech_updates.empty else set()
            tech_query_wqueries = set(tech_queries['wquery'].unique()) if not tech_queries.empty else set()
            tech_common_queries = tech_update_queries.intersection(tech_query_wqueries)
            
            if not tech_common_queries:
                continue
            
            for query_name in sorted(tech_common_queries):
                # Get update data for this query (using 'query' column)
                query_updates = tech_updates[tech_updates['query'] == query_name] if not tech_updates.empty else pd.DataFrame()
                # Get queries data for this query (using 'wquery' column)  
                related_queries = tech_queries[tech_queries['wquery'] == query_name] if not tech_queries.empty else pd.DataFrame()
                
                # Plot individual runs for updates
                if not query_updates.empty:
                    update_times = query_updates['exec_time'].tolist()
                    for i, exec_time in enumerate(update_times):
                        x_pos = current_x + i * bar_width
                        bar = ax.bar(x_pos, exec_time, bar_width, 
                                   color='#ff7f0e', alpha=0.8, 
                                   label='UPDATE' if current_x == 0 and i == 0 else "")
                        # Add value label
                        ax.text(x_pos, exec_time + exec_time*0.01, f'{exec_time:.2f}s',
                               ha='center', va='bottom', fontsize=8, rotation=90)
                
                # Plot individual runs for related queries  
                if not related_queries.empty:
                    query_times = related_queries['exec_time'].tolist()
                    offset = len(query_updates) if not query_updates.empty else 0
                    for i, exec_time in enumerate(query_times):
                        x_pos = current_x + (offset + i) * bar_width
                        bar = ax.bar(x_pos, exec_time, bar_width, 
                                   color='#2ca02c', alpha=0.8,
                                   label='QUERIES' if current_x == 0 and i == 0 and offset == 0 else "")
                        # Add value label
                        ax.text(x_pos, exec_time + exec_time*0.01, f'{exec_time:.2f}s',
                               ha='center', va='bottom', fontsize=8, rotation=90)
                
                # Calculate center position for label
                total_bars = len(query_updates) + len(related_queries)
                center_x = current_x + (total_bars - 1) * bar_width / 2
                x_positions.append(center_x)
                x_labels.append(f'{tech}\n{query_name}\n({len(query_updates)}u,{len(related_queries)}q)')
                
                current_x += total_bars * bar_width + bar_width  # Add spacing between groups
        
        if not x_positions:
            ax.text(0.5, 0.5, f'No linkable data for {scale}', 
                   ha='center', va='center', transform=ax.transAxes, fontsize=14)
            ax.set_title(f'UPDATE + QUERIES Performance - {scale} Scale', 
                        fontsize=16, fontweight='bold')
            continue
        
        # Customize the plot
        ax.set_title(f'UPDATE + QUERIES Individual Run Performance - {scale} Scale\n'
                    f'Individual Execution Times by Technology and Query (Check Caching Effects)', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Technology - Query Type (Update count, Query count)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
        ax.set_xticks(x_positions)
        ax.set_xticklabels(x_labels, fontsize=9, rotation=45, ha='right')
        ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=11)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add note about caching and linking
        ax.text(0.02, 0.98, 'Note: Individual run times shown to identify caching effects\n'
                           'Look for decreasing times across runs for the same query\n'
                           'Links UPDATE "query" column with QUERIES "wquery" column', 
               transform=ax.transAxes, fontsize=10, verticalalignment='top',
               bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.7))
    
    plt.subplots_adjust(right=0.82, left=0.08, top=0.92, bottom=0.20)
    plt.tight_layout(pad=3.0, w_pad=2.0, h_pad=3.0)
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'update_query_individual_runs.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()

def generate_read_write_report(combined_df: pd.DataFrame) -> str:
    """Generate a comprehensive report of read/write performance analysis."""
    
    # Classify queries
    df_classified = classify_query_type(combined_df.copy())
    
    report = []
    report.append("=" * 80)
    report.append("BIGQUERY ICEBERG TABLE PERFORMANCE ANALYSIS REPORT")
    report.append("=" * 80)
    
    # Overall statistics
    total_records = len(df_classified)
    read_records = len(df_classified[df_classified['query_operation'] == 'READ'])
    write_records = len(df_classified[df_classified['query_operation'] == 'WRITE'])
    
    report.append(f"\n📊 OVERALL STATISTICS")
    report.append(f"Total Records: {total_records:,}")
    report.append(f"Read Operations: {read_records:,} ({read_records/total_records*100:.1f}%)")
    report.append(f"Write Operations: {write_records:,} ({write_records/total_records*100:.1f}%)")
    
    # Performance by operation type
    if read_records > 0:
        read_data = df_classified[df_classified['query_operation'] == 'READ']
        read_stats = read_data.groupby(['scale', 'base_type'])['exec_time'].agg(['count', 'mean', 'std']).round(3)
        
        report.append(f"\n📈 READ OPERATIONS PERFORMANCE")
        report.append(str(read_stats))
    
    if write_records > 0:
        write_data = df_classified[df_classified['query_operation'] == 'WRITE']
        write_stats = write_data.groupby(['scale', 'base_type'])['exec_time'].agg(['count', 'mean', 'std']).round(3)
        
        report.append(f"\n📈 WRITE OPERATIONS PERFORMANCE")
        report.append(str(write_stats))
    
    # Best performers
    if read_records > 0 and write_records > 0:
        read_avg = read_data.groupby(['scale', 'base_type'])['exec_time'].mean()
        write_avg = write_data.groupby(['scale', 'base_type'])['exec_time'].mean()
        
        report.append(f"\n🏆 BEST PERFORMERS BY SCALE")
        for scale in df_classified['scale'].unique():
            if scale in read_avg.index.get_level_values(0):
                best_read = read_avg[scale].idxmin()
                best_read_time = read_avg[scale].min()
                report.append(f"{scale} - Best READ: {best_read} ({best_read_time:.3f}s)")
            
            if scale in write_avg.index.get_level_values(0):
                best_write = write_avg[scale].idxmin()
                best_write_time = write_avg[scale].min()
                report.append(f"{scale} - Best WRITE: {best_write} ({best_write_time:.3f}s)")
    
    report.append("\n" + "=" * 80)
    
    return "\n".join(report)

def generate_plots(combined_df: pd.DataFrame, 
                  plots_to_generate: List[str] = None, 
                  save_plots: bool = False, 
                  output_dir: str = 'plots'):
    """
    Generate selected plots for the analysis.
    
    Args:
        combined_df: Combined DataFrame with all data
        plots_to_generate: List of plot names to generate. If None, generates all plots.
        save_plots: Whether to save plots to disk
        output_dir: Directory to save plots
    
    Available plots:
        - 'read_comparison': Read operations comparison
        - 'write_comparison': Write operations comparison
        - 'scaling_trends': Read/write scaling trends
        - 'ratio_analysis': Read/write ratio analysis
        - 'wquery_analysis': WQuery grouped analysis
        - 'update_query_combined': UPDATE + QUERIES combined analysis
    """
    
    if plots_to_generate is None:
        plots_to_generate = ['read_comparison', 'write_comparison', 'scaling_trends', 
                           'ratio_analysis', 'wquery_analysis', 'update_query_combined']
    
    print(f"🎨 Generating {len(plots_to_generate)} plots...")
    
    if 'read_comparison' in plots_to_generate:
        print("📊 Generating Read Operations Comparison...")
        plot_read_operations_comparison(combined_df, save_plots, output_dir)
    
    if 'write_comparison' in plots_to_generate:
        print("📊 Generating Write Operations Comparison...")
        plot_write_operations_comparison(combined_df, save_plots, output_dir)
    
    if 'scaling_trends' in plots_to_generate:
        print("📊 Generating Scaling Trends...")
        plot_read_write_scaling_trends(combined_df, save_plots, output_dir)
    
    if 'ratio_analysis' in plots_to_generate:
        print("📊 Generating Ratio Analysis...")
        plot_read_write_ratio_analysis(combined_df, save_plots, output_dir)
    
    if 'wquery_analysis' in plots_to_generate:
        print("📊 Generating WQuery Analysis...")
        plot_wquery_grouped_analysis(combined_df, save_plots, output_dir)
    
    if 'update_query_combined' in plots_to_generate:
        print("📊 Generating UPDATE + QUERIES Combined Analysis...")
        plot_update_query_combined_analysis(combined_df, save_plots, output_dir)
    
    print("✅ Plot generation complete!")

def run_with_local_files(base_directory: str, custom_file_dict: Dict[str, str] = None) -> pd.DataFrame:
    """
    Run the analysis with local files.
    
    Args:
        base_directory: Path to directory containing CSV files
        custom_file_dict: Custom file dictionary mapping table types to filenames
        
    Returns:
        Combined DataFrame with analysis results
    """
    
    if custom_file_dict is None:
        # Default file dictionary - modify as needed
        custom_file_dict = {
            'BQMS_20G_QUERIES': 'bqms_20G_queries.csv',
            'BQMN_20G_QUERIES': 'bqmn_20G_queries.csv',
            'BLMS_20G_QUERIES': 'blms_20G_queries.csv',
            'BQMS_20G_UPDATE': 'bqms_20G_update.csv',
            'BQMN_20G_UPDATE': 'bqmn_20G_update.csv',
            'BLMS_20G_UPDATE': 'blms_20G_update.csv',
        }
    
    print(f"🔄 Loading data from local directory: {base_directory}")
    
    # Load data
    combined_df = load_data_from_local_files(custom_file_dict, base_directory)
    
    # Generate report
    report = generate_read_write_report(combined_df)
    print(report)
    
    # Generate all plots
    generate_plots(combined_df, save_plots=True)
    
    return combined_df

def main():
    """Main execution function with URL-based data loading."""
    
    # Define data sources - 20G scale data
    file_dict = {
        'BQMN_20G_QUERIES': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bqmn_20G_store_sale_denorm_bench_20G_queries_metrics-MnLv6qGvHyy9yCIgtoX1S46w2U93WA.csv',
        'BLMS_20G_UPDATE': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/blms_20G_store_sale_denorm_bench_20G_update_metrics-siKv68ubIZjN9tnondPwf9Ca09B661.csv',
        'BQMS_20G_UPDATE': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bqms_20G_store_sale_denorm_bench_20G_update_metrics-Y3oV8A7P9OaLmL0LWQcIL9u4DIclnN.csv',
        'BQMS_20G_QUERIES': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bqms_20G_store_sale_denorm_bench_20G_queries_metrics-4OswO1dVGO2ZsEtBl6yN2jV6lsDDcD.csv',
        'BLMS_20G_QUERIES': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/blms_20G_store_sale_denorm_bench_20G_queries_metrics-330NZrWHn91bXDl241oaABqavwwi0A.csv',
        'BQMN_20G_UPDATE': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bqmn_20G_store_sale_denorm_bench_20G_update_metrics-DaBg852LzbA22SWT9fscKYGX9LEc07.csv',
    }
    
    print("🚀 Starting BigQuery Iceberg Table Performance Analysis")
    print("=" * 60)
    
    try:
        # Load data from URLs
        combined_df = load_data_from_urls(file_dict)
        
        # Generate comprehensive report
        report = generate_read_write_report(combined_df)
        print(report)
        
        # Generate all plots with saving enabled
        generate_plots(combined_df, save_plots=True)
        
        print("\n✅ Analysis completed successfully!")
        print(f"📁 Plots saved to: ./plots/")
        
        return combined_df
        
    except Exception as e:
        print(f"❌ Error during analysis: {str(e)}")
        raise

if __name__ == "__main__":
    # Run the main analysis
    df = main()
