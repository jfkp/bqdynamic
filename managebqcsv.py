import pandas as pd
from pathlib import Path

# Base directory where CSV files are located
base_dir = Path("path/to/your/base_directory")  # <-- change this to your folder

# List of CSV filenames to process (only filenames, not full paths)
files_to_process = [
    "file1.csv",
    "file2.csv",
    "file3.csv"
]

# Column mapping
column_mapping = {
    "query_name": "query",
    "w_query_name": "wquery",
    "elapsed_millisecond": "exec_time"
}

for file_name in files_to_process:
    file_path = base_dir / file_name
    
    if not file_path.is_file():
        print(f"File not found: {file_name}")
        continue
    
    try:
        # Read CSV
        df = pd.read_csv(file_path)
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        # Convert exec_time from milliseconds to seconds
        if "exec_time" in df.columns:
            df["exec_time"] = df["exec_time"] / 1000.0
        
        # Save back to the same file (overwrite)
        df.to_csv(file_path, index=False)
        
        print(f"Processed: {file_name}")
        
    except Exception as e:
        print(f"Failed to process {file_name}: {e}")
