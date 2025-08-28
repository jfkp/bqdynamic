import os
import matplotlib.pyplot as plt

def classify_query_type(df):
    """
    Classify queries as READ or WRITE operations based on file type and query content.
    
    Classification rules:
    - QUERIES files: Generally READ operations, except for specific write operations
    - UPDATE files: Always WRITE operations
    - Special cases: bulk_load is always WRITE regardless of file type
    """
    
    def get_operation_type(row):
        query_str = str(row.get('query', '')).lower()
        wquery_str = str(row.get('wquery', '')).lower()
        table_type = str(row.get('table_type', '')).upper()
        
        if 'bulk_load' in query_str or 'bulk_load' in wquery_str:
            return 'WRITE'
        
        # Check for other write indicators in query content
        write_indicators = ['update', 'insert', 'delete', 'merge', 'upsert', 'create', 'drop', 'alter']
        
        for indicator in write_indicators:
            if indicator in query_str or indicator in wquery_str:
                return 'WRITE'
        
        # If from UPDATE files, always WRITE (this includes bulk_load in UPDATE files)
        if 'UPDATE' in table_type:
            return 'WRITE'
            
        # Default for QUERIES files
        return 'READ'
    
    # Apply classification
    df['query_operation'] = df.apply(get_operation_type, axis=1)
    
    # Debug information
    print("\n=== QUERY CLASSIFICATION DEBUG ===")
    print(f"Total records: {len(df)}")
    print(f"READ operations: {len(df[df['query_operation'] == 'READ'])}")
    print(f"WRITE operations: {len(df[df['query_operation'] == 'WRITE'])}")
    
    # Show sample of each type
    read_samples = df[df['query_operation'] == 'READ']['query'].unique()[:5]
    write_samples = df[df['query_operation'] == 'WRITE']['query'].unique()[:5]
    
    print(f"\nSample READ queries: {list(read_samples)}")
    print(f"Sample WRITE queries: {list(write_samples)}")
    
    bulk_load_data = df[df['query'].str.contains('bulk_load', case=False, na=False)]
    if not bulk_load_data.empty:
        print(f"\nBulk_load operations found: {len(bulk_load_data)}")
        print(f"Bulk_load classified as: {bulk_load_data['query_operation'].unique()}")
    
    return df

def plot_read_operations_comparison(combined_df, save_plots=False, output_dir='plots'):
    """Plot read operations performance comparison across scales and technologies."""
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'read_operations_comparison.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()

def plot_write_operations_comparison(combined_df, save_plots=False, output_dir='plots'):
    """Plot write operations performance comparison across scales and technologies."""
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'write_operations_comparison.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()

def plot_read_write_scaling_trends(combined_df, save_plots=False, output_dir='plots'):
    """Plot scaling trends for read and write operations."""
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'read_write_scaling_trends.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()

def plot_read_write_ratio_analysis(combined_df, save_plots=False, output_dir='plots'):
    """Enhanced plot showing read performance, write performance, ratios, and side-by-side comparison."""
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'read_write_ratio_analysis.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()

def plot_wquery_grouped_analysis(combined_df, save_plots=False, output_dir='plots'):
    """Plot wquery-based analysis with grouped bars for each scale."""
    
    if save_plots:
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, 'wquery_grouped_analysis.png')
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Plot saved: {filename}")
    
    plt.show()
