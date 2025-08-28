import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import requests
from io import StringIO
import os

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")

def load_data_from_local_files(file_dict, base_directory):
    """Load CSV data from local files using base directory and file dictionary"""
    dataframes = []
    
    for table_type, filename in file_dict.items():
        try:
            # Construct full file path
            file_path = os.path.join(base_directory, filename)
            
            # Check if file exists
            if not os.path.exists(file_path):
                print(f"⚠️ File not found: {file_path}")
                continue
            
            # Read CSV data
            df = pd.read_csv(file_path)
            
            # Add table type identifier
            df['table_type'] = table_type
            
            # Clean column names (remove unnamed columns)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            
            print(f"✅ Loaded {len(df)} records from {table_type} ({filename})")
            dataframes.append(df)
            
        except Exception as e:
            print(f"❌ Error loading {table_type} from {filename}: {e}")
            continue
    
    return dataframes

def load_data_from_file_dict(file_dict):
    """Load CSV data from file dictionary with URLs"""
    dataframes = []
    
    for table_type, url in file_dict.items():
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            # Read CSV data
            df = pd.read_csv(StringIO(response.text))
            
            # Add table type identifier
            df['table_type'] = table_type
            
            # Clean column names (remove unnamed columns)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            
            print(f"✅ Loaded {len(df)} records from {table_type}")
            dataframes.append(df)
            
        except Exception as e:
            print(f"❌ Error loading {table_type}: {e}")
            continue
    
    return dataframes

def load_data_flexible(file_dict, base_directory=None, use_local=False):
    """
    Flexible data loading function that can handle both URLs and local files
    
    Args:
        file_dict: Dictionary with table_type as key and URL/filename as value
        base_directory: Base directory for local files (required if use_local=True)
        use_local: If True, treat file_dict values as filenames and load from base_directory
                  If False, treat file_dict values as URLs
    
    Returns:
        List of dataframes
    """
    if use_local:
        if base_directory is None:
            raise ValueError("base_directory must be provided when use_local=True")
        return load_data_from_local_files(file_dict, base_directory)
    else:
        return load_data_from_file_dict(file_dict)

def extract_scale_and_type(table_type):
    """Extract scale (10G, 20G, etc.) and base type (BQMS, BQMN, BLMS) from table_type"""
    parts = table_type.split('_')
    base_type = parts[0]  # BQMS, BQMN, BLMS
    scale = None
    
    for part in parts:
        if 'G' in part and any(char.isdigit() for char in part):
            scale = part
            break
    
    return base_type, scale

def classify_query_type(df):
    """Classify queries as READ or WRITE based on query patterns"""
    df['query_operation'] = 'READ'  # Default to READ
    
    # Check for write operations in different columns
    write_indicators = ['update', 'insert', 'delete', 'merge', 'upsert']
    
    # Check in 'query' column if it exists
    if 'query' in df.columns:
        for indicator in write_indicators:
            mask = df['query'].str.contains(indicator, case=False, na=False)
            df.loc[mask, 'query_operation'] = 'WRITE'
    
    # Check in 'wquery' column if it exists
    if 'wquery' in df.columns:
        for indicator in write_indicators:
            mask = df['wquery'].str.contains(indicator, case=False, na=False)
            df.loc[mask, 'query_operation'] = 'WRITE'
    
    # Check table_type for update metrics
    if 'table_type' in df.columns:
        update_mask = df['table_type'].str.contains('UPDATE', case=False, na=False)
        df.loc[update_mask, 'query_operation'] = 'WRITE'
    
    return df

def plot_read_write_scaling_trends(combined_df):
    """Plot read vs write performance trends across scales"""
    combined_df['base_type'], combined_df['scale'] = zip(*combined_df['table_type'].apply(extract_scale_and_type))
    combined_df = classify_query_type(combined_df)
    scales = sorted(combined_df['scale'].dropna().unique())
    
    if len(scales) <= 1:
        print("⚠️ Need multiple scales for scaling trends plot")
        return None
    
    plt.figure(figsize=(12, 8))
    
    # Plot read operations
    read_data = combined_df[combined_df['query_operation'] == 'READ']
    if not read_data.empty:
        read_by_scale = read_data.groupby(['scale', 'base_type'])['exec_time'].mean().unstack()
        for base_type in read_by_scale.columns:
            plt.plot(read_by_scale.index, read_by_scale[base_type], 
                    marker='o', linewidth=2, label=f'{base_type} (READ)', linestyle='-')
    
    # Plot write operations
    write_data = combined_df[combined_df['query_operation'] == 'WRITE']
    if not write_data.empty:
        write_by_scale = write_data.groupby(['scale', 'base_type'])['exec_time'].mean().unstack()
        for base_type in write_by_scale.columns:
            plt.plot(write_by_scale.index, write_by_scale[base_type], 
                    marker='s', linewidth=2, label=f'{base_type} (WRITE)', linestyle='--')
    
    plt.xlabel('Data Scale', fontsize=12, fontweight='bold')
    plt.ylabel('Average Execution Time (seconds)', fontsize=12, fontweight='bold')
    plt.title('Read vs Write Performance Across Scales', fontsize=14, fontweight='bold')
    plt.legend(bbox_to_anchor=(1.15, 1), loc='upper left', fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout(pad=3.0)
    plt.subplots_adjust(right=0.85)
    plt.show()
    return plt.gcf()

def plot_read_operations_comparison(combined_df):
    """Plot read operations performance comparison"""
    combined_df['base_type'], combined_df['scale'] = zip(*combined_df['table_type'].apply(extract_scale_and_type))
    combined_df = classify_query_type(combined_df)
    scales = sorted(combined_df['scale'].dropna().unique())
    
    plt.figure(figsize=(12, 8))
    read_data = combined_df[combined_df['query_operation'] == 'READ']
    
    if read_data.empty:
        print("⚠️ No read operations found")
        return None
    
    if len(scales) > 1:
        read_pivot = read_data.groupby(['scale', 'base_type'])['exec_time'].mean().unstack()
        x = np.arange(len(read_pivot.index))
        width = 0.25
        colors = ['#3498db', '#e74c3c', '#2ecc71']
        
        for i, base_type in enumerate(read_pivot.columns):
            offset = (i - len(read_pivot.columns)/2 + 0.5) * width
            bars = plt.bar(x + offset, read_pivot[base_type], width, 
                         label=base_type, color=colors[i % len(colors)], alpha=0.8)
            
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                           f'{height:.2f}', ha='center', va='bottom', fontsize=8)
        
        plt.xlabel('Data Scale', fontsize=12, fontweight='bold')
        plt.xticks(x, read_pivot.index)
    else:
        # Single scale
        read_by_type = read_data.groupby('base_type')['exec_time'].mean()
        bars = plt.bar(read_by_type.index, read_by_type.values, color='#3498db', alpha=0.8)
        
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                   f'{height:.2f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.ylabel('Average Execution Time (seconds)', fontsize=12, fontweight='bold')
    plt.title('READ Operations Performance', fontsize=14, fontweight='bold')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout(pad=3.0)
    plt.subplots_adjust(right=0.85)
    plt.show()
    return plt.gcf()

def plot_write_operations_comparison(combined_df):
    """Plot write operations performance comparison"""
    combined_df['base_type'], combined_df['scale'] = zip(*combined_df['table_type'].apply(extract_scale_and_type))
    combined_df = classify_query_type(combined_df)
    scales = sorted(combined_df['scale'].dropna().unique())
    
    plt.figure(figsize=(12, 8))
    write_data = combined_df[combined_df['query_operation'] == 'WRITE']
    
    if write_data.empty:
        print("⚠️ No write operations found")
        return None
    
    if len(scales) > 1:
        write_pivot = write_data.groupby(['scale', 'base_type'])['exec_time'].mean().unstack()
        x = np.arange(len(write_pivot.index))
        width = 0.25
        colors = ['#e74c3c', '#f39c12', '#9b59b6']
        
        for i, base_type in enumerate(write_pivot.columns):
            offset = (i - len(write_pivot.columns)/2 + 0.5) * width
            bars = plt.bar(x + offset, write_pivot[base_type], width, 
                         label=base_type, color=colors[i % len(colors)], alpha=0.8)
            
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                           f'{height:.2f}', ha='center', va='bottom', fontsize=8)
        
        plt.xlabel('Data Scale', fontsize=12, fontweight='bold')
        plt.xticks(x, write_pivot.index)
    else:
        # Single scale
        write_by_type = write_data.groupby('base_type')['exec_time'].mean()
        bars = plt.bar(write_by_type.index, write_by_type.values, color='#e74c3c', alpha=0.8)
        
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                   f'{height:.2f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.ylabel('Average Execution Time (seconds)', fontsize=12, fontweight='bold')
    plt.title('WRITE Operations Performance', fontsize=14, fontweight='bold')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout(pad=3.0)
    plt.subplots_adjust(right=0.85)
    plt.show()
    return plt.gcf()

def plot_read_write_ratio_analysis(combined_df):
    """Plot read/write ratio analysis"""
    combined_df['base_type'], combined_df['scale'] = zip(*combined_df['table_type'].apply(extract_scale_and_type))
    combined_df = classify_query_type(combined_df)
    scales = sorted(combined_df['scale'].dropna().unique())
    
    plt.figure(figsize=(12, 8))
    
    ratio_data = []
    for scale in scales:
        for base_type in combined_df['base_type'].unique():
            scale_type_data = combined_df[(combined_df['scale'] == scale) & 
                                        (combined_df['base_type'] == base_type)]
            
            read_avg = scale_type_data[scale_type_data['query_operation'] == 'READ']['exec_time'].mean()
            write_avg = scale_type_data[scale_type_data['query_operation'] == 'WRITE']['exec_time'].mean()
            
            if pd.notna(read_avg) and pd.notna(write_avg) and write_avg > 0:
                ratio = write_avg / read_avg if read_avg > 0 else float('inf')
                ratio_data.append({'scale': scale, 'base_type': base_type, 'write_read_ratio': ratio})
    
    if not ratio_data:
        print("⚠️ No ratio data available")
        return None
    
    ratio_df = pd.DataFrame(ratio_data)
    ratio_pivot = ratio_df.pivot(index='scale', columns='base_type', values='write_read_ratio')
    
    x = np.arange(len(ratio_pivot.index))
    width = 0.25
    colors = ['#f39c12', '#9b59b6', '#1abc9c']
    
    for i, base_type in enumerate(ratio_pivot.columns):
        offset = (i - len(ratio_pivot.columns)/2 + 0.5) * width
        bars = plt.bar(x + offset, ratio_pivot[base_type], width, 
                     label=base_type, color=colors[i % len(colors)], alpha=0.8)
        
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                       f'{height:.1f}x', ha='center', va='bottom', fontsize=8)
    
    plt.xlabel('Data Scale', fontsize=12, fontweight='bold')
    plt.ylabel('Write/Read Execution Time Ratio', fontsize=12, fontweight='bold')
    plt.title('Write vs Read Performance Ratio', fontsize=14, fontweight='bold')
    plt.xticks(x, ratio_pivot.index)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.axhline(y=1, color='red', linestyle='--', alpha=0.7, label='Equal Performance')
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout(pad=3.0)
    plt.subplots_adjust(right=0.85)
    plt.show()
    return plt.gcf()

def plot_scale_detailed_analysis(combined_df, target_scale=None):
    """Plot detailed analysis for a specific scale"""
    combined_df['base_type'], combined_df['scale'] = zip(*combined_df['table_type'].apply(extract_scale_and_type))
    combined_df = classify_query_type(combined_df)
    scales = sorted(combined_df['scale'].dropna().unique())
    
    if target_scale is None:
        target_scale = scales[0] if scales else None
    
    if target_scale is None:
        print("⚠️ No scale data available")
        return None
    
    plt.figure(figsize=(12, 8))
    scale_data = combined_df[combined_df['scale'] == target_scale]
    
    # Create grouped bar chart for this scale
    scale_summary = scale_data.groupby(['base_type', 'query_operation'])['exec_time'].mean().unstack(fill_value=0)
    
    if scale_summary.empty:
        print(f"⚠️ No data available for scale {target_scale}")
        return None
    
    x = np.arange(len(scale_summary.index))
    width = 0.35
    
    if 'READ' in scale_summary.columns:
        bars1 = plt.bar(x - width/2, scale_summary['READ'], width, 
                      label='READ', color='#3498db', alpha=0.8)
        for bar in bars1:
            height = bar.get_height()
            if height > 0:
                plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                       f'{height:.2f}', ha='center', va='bottom', fontsize=8)
    
    if 'WRITE' in scale_summary.columns:
        bars2 = plt.bar(x + width/2, scale_summary['WRITE'], width, 
                      label='WRITE', color='#e74c3c', alpha=0.8)
        for bar in bars2:
            height = bar.get_height()
            if height > 0:
                plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                       f'{height:.2f}', ha='center', va='bottom', fontsize=8)
    
    plt.xlabel('Table Type', fontsize=12, fontweight='bold')
    plt.ylabel('Average Execution Time (seconds)', fontsize=12, fontweight='bold')
    plt.title(f'{target_scale} Scale - Read vs Write Performance', fontsize=14, fontweight='bold')
    plt.xticks(x, scale_summary.index)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout(pad=3.0)
    plt.subplots_adjust(right=0.85)
    plt.show()
    return plt.gcf()

def plot_all_scales_detailed_analysis(combined_df):
    """Plot detailed analysis for all available scales"""
    combined_df['base_type'], combined_df['scale'] = zip(*combined_df['table_type'].apply(extract_scale_and_type))
    combined_df = classify_query_type(combined_df)
    scales = sorted(combined_df['scale'].dropna().unique())
    
    if not scales:
        print("⚠️ No scale data available")
        return None
    
    n_scales = len(scales)
    cols = min(3, n_scales)
    rows = (n_scales + cols - 1) // cols
    
    fig, axes = plt.subplots(rows, cols, figsize=(8*cols, 6*rows))
    if n_scales == 1:
        axes = [axes]
    elif rows == 1:
        axes = axes.reshape(1, -1)
    
    for idx, scale in enumerate(scales):
        row = idx // cols
        col = idx % cols
        ax = axes[row, col] if rows > 1 else axes[col]
        
        scale_data = combined_df[combined_df['scale'] == scale]
        scale_summary = scale_data.groupby(['base_type', 'query_operation'])['exec_time'].mean().unstack(fill_value=0)
        
        if not scale_summary.empty:
            x = np.arange(len(scale_summary.index))
            width = 0.35
            
            if 'READ' in scale_summary.columns:
                bars1 = ax.bar(x - width/2, scale_summary['READ'], width, 
                              label='READ', color='#3498db', alpha=0.8)
                for bar in bars1:
                    height = bar.get_height()
                    if height > 0:
                        ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                               f'{height:.2f}', ha='center', va='bottom', fontsize=8)
            
            if 'WRITE' in scale_summary.columns:
                bars2 = ax.bar(x + width/2, scale_summary['WRITE'], width, 
                              label='WRITE', color='#e74c3c', alpha=0.8)
                for bar in bars2:
                    height = bar.get_height()
                    if height > 0:
                        ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                               f'{height:.2f}', ha='center', va='bottom', fontsize=8)
            
            ax.set_xlabel('Table Type', fontsize=10, fontweight='bold')
            ax.set_ylabel('Avg Exec Time (s)', fontsize=10, fontweight='bold')
            ax.set_title(f'{scale} Scale - Read vs Write', fontsize=12, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(scale_summary.index)
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
            ax.grid(True, alpha=0.3, axis='y')
    
    # Hide empty subplots
    for idx in range(n_scales, rows * cols):
        row = idx // cols
        col = idx % cols
        if rows > 1:
            axes[row, col].set_visible(False)
        else:
            axes[col].set_visible(False)
    
    plt.tight_layout(pad=3.0, w_pad=2.0, h_pad=2.0)
    plt.subplots_adjust(right=0.85)
    plt.show()
    return fig

def plot_wquery_grouped_analysis(combined_df):
    """Plot grouped bar chart for wquery with update operations and related queries by technology"""
    combined_df['base_type'], combined_df['scale'] = zip(*combined_df['table_type'].apply(extract_scale_and_type))
    
    # Filter data that has wquery field
    wquery_data = combined_df[combined_df['wquery'].notna()].copy()
    
    if wquery_data.empty:
        print("⚠️ No wquery data found")
        return None
    
    # Get unique wqueries and scales
    unique_wqueries = sorted(wquery_data['wquery'].unique())
    scales = sorted(wquery_data['scale'].dropna().unique())
    
    if not scales:
        print("⚠️ No scale data available for wquery analysis")
        return None
    
    # Create subplots for each scale
    n_scales = len(scales)
    cols = min(2, n_scales)
    rows = (n_scales + cols - 1) // cols
    
    fig, axes = plt.subplots(rows, cols, figsize=(14*cols, 8*rows))
    if n_scales == 1:
        axes = [axes]
    elif rows == 1:
        axes = axes.reshape(1, -1)
    
    colors = ['#3498db', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c']
    
    for idx, scale in enumerate(scales):
        row = idx // cols
        col = idx % cols
        ax = axes[row, col] if rows > 1 else axes[col]
        
        scale_data = wquery_data[wquery_data['scale'] == scale]
        
        if scale_data.empty:
            ax.set_title(f'{scale} Scale - No Data Available')
            continue
        
        # Group by wquery and base_type, calculate mean exec_time
        grouped_data = scale_data.groupby(['wquery', 'base_type'])['exec_time'].mean().unstack(fill_value=0)
        
        if grouped_data.empty:
            ax.set_title(f'{scale} Scale - No Grouped Data Available')
            continue
        
        # Create grouped bar chart
        x = np.arange(len(grouped_data.index))
        width = 0.25
        base_types = grouped_data.columns
        
        for i, base_type in enumerate(base_types):
            offset = (i - len(base_types)/2 + 0.5) * width
            bars = ax.bar(x + offset, grouped_data[base_type], width, 
                         label=base_type, color=colors[i % len(colors)], alpha=0.8)
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                           f'{height:.2f}', ha='center', va='bottom', fontsize=8)
        
        # Customize the subplot
        ax.set_xlabel('Query Type (wquery)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Average Execution Time (seconds)', fontsize=12, fontweight='bold')
        ax.set_title(f'{scale} Scale - Query Performance by Technology\n(Update + Related Queries)', 
                    fontsize=13, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(grouped_data.index, rotation=45, ha='right')
        ax.legend(bbox_to_anchor=(1.15, 1), loc='upper left', fontsize=10)
        ax.grid(True, alpha=0.3, axis='y')
    
    # Hide empty subplots
    for idx in range(n_scales, rows * cols):
        row = idx // cols
        col = idx % cols
        if rows > 1:
            axes[row, col].set_visible(False)
        else:
            axes[col].set_visible(False)
    
    plt.tight_layout(pad=3.0, w_pad=2.0, h_pad=2.0)
    plt.subplots_adjust(right=0.85)
    plt.show()
    return fig

def generate_plots(combined_df, plots_to_generate=None):
    """
    Global function to generate specific plots based on user selection
    
    Args:
        combined_df: Combined dataframe with all data
        plots_to_generate: List of plot names to generate. If None, generates all plots.
                          Available options:
                          - 'scaling_trends': Read vs write performance across scales
                          - 'read_comparison': Read operations comparison
                          - 'write_comparison': Write operations comparison  
                          - 'ratio_analysis': Read/write ratio analysis
                          - 'scale_detailed': Detailed analysis for first scale
                          - 'all_scales_detailed': Detailed analysis for all scales
                          - 'wquery_grouped': Grouped bar chart for wquery analysis
                          - 'all': Generate all available plots
    
    Returns:
        Dictionary of generated figures
    """
    
    if plots_to_generate is None or 'all' in plots_to_generate:
        plots_to_generate = ['scaling_trends', 'read_comparison', 'write_comparison', 
                           'ratio_analysis', 'scale_detailed', 'all_scales_detailed', 'wquery_grouped']
    
    generated_figures = {}
    
    print(f"\n📈 Generating {len(plots_to_generate)} plot(s)...")
    
    if 'scaling_trends' in plots_to_generate:
        print("📊 Creating scaling trends plot...")
        fig = plot_read_write_scaling_trends(combined_df)
        if fig:
            generated_figures['scaling_trends'] = fig
    
    if 'read_comparison' in plots_to_generate:
        print("📊 Creating read operations comparison...")
        fig = plot_read_operations_comparison(combined_df)
        if fig:
            generated_figures['read_comparison'] = fig
    
    if 'write_comparison' in plots_to_generate:
        print("📊 Creating write operations comparison...")
        fig = plot_write_operations_comparison(combined_df)
        if fig:
            generated_figures['write_comparison'] = fig
    
    if 'ratio_analysis' in plots_to_generate:
        print("📊 Creating ratio analysis plot...")
        fig = plot_read_write_ratio_analysis(combined_df)
        if fig:
            generated_figures['ratio_analysis'] = fig
    
    if 'scale_detailed' in plots_to_generate:
        print("📊 Creating detailed scale analysis...")
        fig = plot_scale_detailed_analysis(combined_df)
        if fig:
            generated_figures['scale_detailed'] = fig
    
    if 'all_scales_detailed' in plots_to_generate:
        print("📊 Creating all scales detailed analysis...")
        fig = plot_all_scales_detailed_analysis(combined_df)
        if fig:
            generated_figures['all_scales_detailed'] = fig
    
    if 'wquery_grouped' in plots_to_generate:
        print("📊 Creating wquery grouped analysis...")
        fig = plot_wquery_grouped_analysis(combined_df)
        if fig:
            generated_figures['wquery_grouped'] = fig
    
    print(f"✅ Generated {len(generated_figures)} plot(s) successfully!")
    return generated_figures

def create_read_write_comparison_plots(combined_df):
    """Create comprehensive read vs write performance plots across scales (legacy function)"""
    print("📊 Using legacy comprehensive plotting function...")
    return generate_plots(combined_df, ['all']), combined_df

def generate_read_write_report(combined_df):
    """Generate detailed read/write performance report"""
    
    print("\n🎯 READ vs WRITE PERFORMANCE ANALYSIS")
    print("=" * 60)
    
    # Overall read vs write statistics
    read_data = combined_df[combined_df['query_operation'] == 'READ']
    write_data = combined_df[combined_df['query_operation'] == 'WRITE']
    
    if not read_data.empty:
        read_stats = read_data.groupby('base_type')['exec_time'].agg(['count', 'mean', 'std']).round(4)
        print("\n📖 READ Operations Summary:")
        print(read_stats)
    
    if not write_data.empty:
        write_stats = write_data.groupby('base_type')['exec_time'].agg(['count', 'mean', 'std']).round(4)
        print("\n✏️ WRITE Operations Summary:")
        print(write_stats)
    
    # Performance comparison
    if not read_data.empty and not write_data.empty:
        print("\n⚡ PERFORMANCE COMPARISON")
        print("-" * 40)
        
        for base_type in combined_df['base_type'].unique():
            read_avg = read_data[read_data['base_type'] == base_type]['exec_time'].mean()
            write_avg = write_data[write_data['base_type'] == base_type]['exec_time'].mean()
            
            if pd.notna(read_avg) and pd.notna(write_avg):
                ratio = write_avg / read_avg if read_avg > 0 else float('inf')
                print(f"{base_type}:")
                print(f"  READ avg:  {read_avg:.3f}s")
                print(f"  WRITE avg: {write_avg:.3f}s")
                print(f"  Ratio:     {ratio:.1f}x ({'WRITE slower' if ratio > 1 else 'READ slower'})")
                print()
    
    # Scale-specific insights
    if 'scale' in combined_df.columns:
        print("\n📊 SCALE-SPECIFIC INSIGHTS")
        print("-" * 40)
        
        for scale in sorted(combined_df['scale'].dropna().unique()):
            scale_data = combined_df[combined_df['scale'] == scale]
            scale_read = scale_data[scale_data['query_operation'] == 'READ']
            scale_write = scale_data[scale_data['query_operation'] == 'WRITE']
            
            print(f"\n{scale} Scale:")
            if not scale_read.empty:
                best_read = scale_read.groupby('base_type')['exec_time'].mean().idxmin()
                best_read_time = scale_read.groupby('base_type')['exec_time'].mean().min()
                print(f"  Best READ:  {best_read} ({best_read_time:.3f}s)")
            
            if not scale_write.empty:
                best_write = scale_write.groupby('base_type')['exec_time'].mean().idxmin()
                best_write_time = scale_write.groupby('base_type')['exec_time'].mean().min()
                print(f"  Best WRITE: {best_write} ({best_write_time:.3f}s)")

def main(use_local=False, base_directory=None, custom_file_dict=None, plots_to_generate=None):
    """
    Main analysis function for read/write query performance
    
    Args:
        use_local: If True, load files from local disk instead of URLs
        base_directory: Base directory for local files (required if use_local=True)
        custom_file_dict: Custom file dictionary to override default
        plots_to_generate: List of specific plots to generate (see generate_plots function)
    """
    
    print("🚀 BigQuery Read/Write Query Performance Analysis")
    print("=" * 60)
    
    # Default file dictionary - easily configurable for different scales
    default_file_dict = {
        # 10G Data (uncomment and add URLs/filenames when available)
        # 'BQMS_10G_QUERIES': 'your_10g_bqms_queries_url_or_filename',
        # 'BQMN_10G_QUERIES': 'your_10g_bqmn_queries_url_or_filename', 
        # 'BLMS_10G_QUERIES': 'your_10g_blms_queries_url_or_filename',
        # 'BQMS_10G_UPDATES': 'your_10g_bqms_updates_url_or_filename',
        # 'BQMN_10G_UPDATES': 'your_10g_bqmn_updates_url_or_filename',
        # 'BLMS_10G_UPDATES': 'your_10g_blms_updates_url_or_filename',
        
        # 20G Data
        'BQMN_20G_QUERIES': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bqmn_20G_store_sale_denorm_bench_20G_queries_metrics-MnLv6qGvHyy9yCIgtoX1S46w2U93WA.csv',
        'BLMS_20G_QUERIES': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/blms_20G_store_sale_denorm_bench_20G_queries_metrics-330NZrWHn91bXDl241oaABqavwwi0A.csv',
        'BQMS_20G_QUERIES': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bqms_20G_store_sale_denorm_bench_20G_queries_metrics-4OswO1dVGO2ZsEtBl6yN2jV6lsDDcD.csv',
        'BLMS_20G_UPDATES': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/blms_20G_store_sale_denorm_bench_20G_update_metrics-siKv68ubIZjN9tnondPwf9Ca09B661.csv',
        'BQMS_20G_UPDATES': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bqms_20G_store_sale_denorm_bench_20G_update_metrics-Y3oV8A7P9OaLmL0LWQcIL9u4DIclnN.csv',
        'BQMN_20G_UPDATES': 'https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bqmn_20G_store_sale_denorm_bench_20G_update_metrics-DaBg852LzbA22SWT9fscKYGX9LEc07.csv',
        
        # 50G Data (uncomment and add URLs/filenames when available)
        # 'BQMS_50G_QUERIES': 'your_50g_bqms_queries_url_or_filename',
        # 'BQMN_50G_QUERIES': 'your_50g_bqmn_queries_url_or_filename',
        # 'BLMS_50G_QUERIES': 'your_50g_blms_queries_url_or_filename',
        # 'BQMS_50G_UPDATES': 'your_50g_bqms_updates_url_or_filename',
        # 'BQMN_50G_UPDATES': 'your_50g_bqmn_updates_url_or_filename',
        # 'BLMS_50G_UPDATES': 'your_50g_blms_updates_url_or_filename',
        
        # 100G Data (uncomment and add URLs/filenames when available)
        # 'BQMS_100G_QUERIES': 'your_100g_bqms_queries_url_or_filename',
        # 'BQMN_100G_QUERIES': 'your_100g_bqmn_queries_url_or_filename',
        # 'BLMS_100G_QUERIES': 'your_100g_blms_queries_url_or_filename',
        # 'BQMS_100G_UPDATES': 'your_100g_bqms_updates_url_or_filename',
        # 'BQMN_100G_UPDATES': 'your_100g_bqmn_updates_url_or_filename',
        # 'BLMS_100G_UPDATES': 'your_100g_blms_updates_url_or_filename',
    }
    
    # Use custom file dictionary if provided, otherwise use default
    file_dict = custom_file_dict if custom_file_dict is not None else default_file_dict
    
    if use_local:
        print(f"📁 Loading files from local directory: {base_directory}")
        dataframes = load_data_flexible(file_dict, base_directory, use_local=True)
    else:
        print("🌐 Loading files from URLs")
        dataframes = load_data_flexible(file_dict, use_local=False)
    
    if not dataframes:
        print("❌ No data could be loaded. Please check the URLs/files in file_dict.")
        return
    
    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Convert exec_time to numeric
    combined_df['exec_time'] = pd.to_numeric(combined_df['exec_time'], errors='coerce')
    
    print(f"\n📊 Loaded {len(combined_df)} total records")
    print(f"📊 Table types: {combined_df['table_type'].unique()}")
    
    # Generate specific plots
    print(f"\n📈 Creating read vs write performance analysis...")
    generated_figures = generate_plots(combined_df, plots_to_generate)
    
    # Generate detailed report
    generate_read_write_report(combined_df)
    
    print(f"\n✅ Read/Write analysis complete! Generated {len(generated_figures)} plot(s).")
    
    return combined_df

def run_with_urls():
    """Run analysis with URLs (default behavior)"""
    return main(use_local=False)

def run_with_local_files(base_directory, custom_file_dict=None):
    """
    Run analysis with local files
    
    Example usage:
        # Define your local file dictionary
        local_files = {
            'BQMS_20G_QUERIES': 'bqms_20G_queries.csv',
            'BQMN_20G_QUERIES': 'bqmn_20G_queries.csv',
            'BLMS_20G_QUERIES': 'blms_20G_queries.csv',
            # ... add more files
        }
        
        # Run analysis
        df = run_with_local_files('/path/to/your/data', local_files)
    """
    return main(use_local=True, base_directory=base_directory, custom_file_dict=custom_file_dict)

# Run the analysis
if __name__ == "__main__":
    # Default: run with URLs
    df = main()
    
    # To run with local files, uncomment and modify:

         #To run with local files, uncomment and modify:
    local_file_dict = {
        'BLMS_10G_QUERIES': 'blms_10G_store_sale_denorm_bench_10G_queries_metrics.csv',
        'BQMS_10G_QUERIES': 'bqms_10G_store_sale_denorm_bench_10G_queries_metrics.csv',
        'BQMN_10G_QUERIES': 'bqmn_10G_store_sale_denorm_bench_10G_queries_metrics.csv',
        'BLMS_20G_QUERIES': 'blms_20G_store_sale_denorm_bench_20G_queries_metrics.csv',
        'BQMS_20G_QUERIES': 'bqms_20G_store_sale_denorm_bench_20G_queries_metrics.csv',
        'BQMN_20G_QUERIES': 'bqmn_20G_store_sale_denorm_bench_20G_queries_metrics.csv',
        'BLMS_50G_QUERIES': 'blms_50G_store_sale_denorm_bench_50G_queries_metrics.csv',
        'BQMS_50G_QUERIES': 'bqms_50G_store_sale_denorm_bench_50G_queries_metrics.csv',
        'BQMN_50G_QUERIES': 'bqmn_50G_store_sale_denorm_bench_50G_queries_metrics.csv',
        'BLMS_100G_QUERIES': 'blms_100G_store_sale_denorm_bench_100G_queries_metrics.csv',
        'BQMS_100G_QUERIES': 'bqms_100G_store_sale_denorm_bench_100G_queries_metrics.csv',
        'BQMN_100G_QUERIES': 'bqmn_100G_store_sale_denorm_bench_100G_queries_metrics.csv',
        'BLMS_10G_UPDATES': 'blms_10G_store_sale_denorm_bench_10G_update_metrics.csv',
        'BQMS_10G_UPDATES': 'bqms_10G_store_sale_denorm_bench_10G_update_metrics.csv',
        'BQMN_10G_UPDATES': 'bqmn_10G_store_sale_denorm_bench_10G_update_metrics.csv',
        'BLMS_20G_UPDATES': 'blms_20G_store_sale_denorm_bench_20G_update_metrics.csv',
        'BQMS_20G_UPDATES': 'bqms_20G_store_sale_denorm_bench_20G_update_metrics.csv',
        'BQMN_20G_UPDATES': 'bqmn_20G_store_sale_denorm_bench_20G_update_metrics.csv',
        'BLMS_50G_UPDATES': 'blms_50G_store_sale_denorm_bench_50G_update_metrics.csv',
        'BQMS_50G_UPDATES': 'bqms_50G_store_sale_denorm_bench_50G_update_metrics.csv',
        'BQMN_50G_UPDATES': 'bqmn_50G_store_sale_denorm_bench_50G_update_metrics.csv',
        'BLMS_100G_UPDATES': 'blms_100G_store_sale_denorm_bench_100G_update_metrics.csv',
        'BQMS_100G_UPDATES': 'bqms_100G_store_sale_denorm_bench_100G_update_metrics.csv',
        'BQMN_100G_UPDATES': 'bqmn_100G_store_sale_denorm_bench_100G_update_metrics.csv'

    }
    df = run_with_local_files('/path/to/your/data/directory', local_file_dict)
