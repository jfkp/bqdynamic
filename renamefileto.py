from pathlib import Path

# Base directory
base_dir = Path(r"C:\users\U50HO63\PROJECT\CDP\metrics_brut")

# Iterate recursively over all CSV files
for csv_file in base_dir.rglob("*.csv"):
    # Relative path from base directory to the file's parent
    relative_path = csv_file.parent.relative_to(base_dir)
    
    # Build the prefix by replacing folder separators with '_'
    prefix = "_".join(relative_path.parts)
    
    # New file name with prefix
    new_name = f"{prefix}_{csv_file.name}"
    
    # Full path for the renamed file
    new_file_path = csv_file.parent / new_name
    
    # Rename the file
    csv_file.rename(new_file_path)
    
    print(f"Renamed: {csv_file} -> {new_file_path}")
