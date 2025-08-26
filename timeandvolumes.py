import pandas as pd
import matplotlib.pyplot as plt

# Load CSV file
file_path = "blms_10G_store_sale_denorm_bench_10G_update_metrics (1).csv"
df = pd.read_csv(file_path)

# Queries
queries = df['query']

# Execution time (already in seconds in your file)
exec_time = df['exec_time']

# File count after operation
file_count_after = df['s_after_total-data-files'].fillna(0).astype(int)

# Total file size after operation (bytes → GB)
total_size_after = df['s_after_total-files-size'].fillna(0) / (1024*1024*1024)

# --- Create subplots ---
fig, axes = plt.subplots(1, 2, figsize=(16,6), sharex=True)

# --- Left plot: Execution time ---
bars1 = axes[0].bar(queries, exec_time, color='skyblue', alpha=0.8)
axes[0].set_title("Query Execution Time", fontsize=14)
axes[0].set_ylabel("Execution Time (seconds)")
axes[0].grid(axis='y', linestyle='--', alpha=0.6)

# Annotate execution times
for bar, val in zip(bars1, exec_time):
    axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                 f"{val:.1f}s", ha='center', va='bottom', fontsize=9)

# --- Right plot: File count & file size ---
bars2a = axes[1].bar(queries, file_count_after, color='orange', alpha=0.7, label="File Count (after)")
axes[1].set_title("File Count and File Size After Operation", fontsize=14)
axes[1].set_ylabel("File Count")
axes[1].grid(axis='y', linestyle='--', alpha=0.6)

# Secondary axis for file size
ax2b = axes[1].twinx()
bars2b = ax2b.bar(queries, total_size_after, color='green', alpha=0.4, label="Total File Size (GB)")
ax2b.set_ylabel("Total File Size (GB)")

# Legends
axes[1].legend(loc='upper left')
ax2b.legend(loc='upper right')

# X-axis formatting
for ax in axes:
    ax.set_xticks(range(len(queries)))
    ax.set_xticklabels(queries, rotation=45, ha='right')

plt.tight_layout()
plt.show()
