import pandas as pd
import matplotlib.pyplot as plt

# Load CSV file
file_path = "blms_10G_store_sale_denorm_bench_10G_update_metrics (1).csv"
df = pd.read_csv(file_path)

# Queries
queries = df['query']

# Execution time (already in seconds in your file)
exec_time = df['exec_time']

# Rows processed = added + deleted records
rows_processed = df['s_after_added-records'].fillna(0) + df['s_before_deleted-records'].fillna(0)

# File size processed (added + removed, bytes → MB)
size_processed = (df['s_after_added-files-size'].fillna(0) + df['s_before_removed-files-size'].fillna(0)) / (1024*1024)

# File count after operation
file_count_after = df['s_after_total-data-files'].fillna(0).astype(int)

# Total file size after operation (bytes → GB)
total_size_after = df['s_after_total-files-size'].fillna(0) / (1024*1024*1024)

# Plot bar chart
plt.figure(figsize=(14,7))
bars = plt.bar(queries, exec_time, color='skyblue', alpha=0.8)

# Annotate each bar
for bar, rows, size, fcount, fsize in zip(bars, rows_processed, size_processed, file_count_after, total_size_after):
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, height,
             f"{int(rows):,} rows\n{size:.1f} MB processed\n"
             f"{fcount} files after\n{fsize:.2f} GB total",
             ha='center', va='bottom', fontsize=8)

# Formatting
plt.title("Query Execution Time with Rows, File Count, and Total Size", fontsize=14)
plt.xlabel("Query", fontsize=12)
plt.ylabel("Execution Time (seconds)", fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.6)

plt.tight_layout()
plt.show()
