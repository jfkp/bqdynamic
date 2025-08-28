import pandas as pd
from pathlib import Path

# Base directory where your CSV files are located
base_dir = Path("path/to/your/base_directory")  # <-- change this to your folder

# Column mapping
column_mapping = {
    "query_name": "query",
    "w_query_name": "wquery",
    "elapsed_millisecond": "exec_time"
}

# Find all CSV files in the base directory
csv_files = list(base_dir.glob("*.csv"))

for file_path in csv_files:
    try:
        # Read CSV
        df = pd.read_csv(file_path)
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        # Convert exec_time from ms to seconds if the column exists
        if "exec_time" in df.columns:
            df["exec_time"] = df["exec_time"] / 1000.0
        
        # Save back to the same file (overwrite)
        df.to_csv(file_path, index=False)
        
        print(f"Processed: {file_path.name}")
    except Exception as e:
        print(f"Failed to process {file_path.name}: {e}")
