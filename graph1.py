import pandas as pd
import matplotlib.pyplot as plt

# Load CSV file
file_path = "blms_10G_store_sale_denorm_bench_10G_update_metrics (1).csv"
df = pd.read_csv(file_path)

# Extract queries and elapsed time
queries = df['query']
elapsed_time = df['elapsedTime']   # elapsed time in ms

# Snapshot-level record counts
records_before_snap = df['s_before_total-records']
records_after_snap = df['s_after_total-records']

# Partition-level record counts (average per partition)
records_before_part = df['p_before_avg_record_count']
records_after_part = df['p_after_avg_record_count']

# File-level record counts (average per file)
records_before_file = df['f_before_avg_record_count']
records_after_file = df['f_after_avg_record_count']

# Create subplots
fig, axes = plt.subplots(3, 1, figsize=(14,12), sharex=True)

# --- Snapshot subplot ---
ax1 = axes[0]
ax1.plot(queries, records_before_snap, marker='o', label='Before Snapshot Records')
ax1.plot(queries, records_after_snap, marker='o', linestyle='--', label='After Snapshot Records')
ax1.set_ylabel("Records")
ax1.set_title("Snapshot-Level Record Counts + Elapsed Time")
ax1.legend(loc='upper left')
ax1.grid(True, linestyle='--', alpha=0.6)

# Secondary axis for elapsed time (bar chart)
ax1b = ax1.twinx()
ax1b.bar(queries, elapsed_time, alpha=0.3, color='red', label='Elapsed Time (ms)')
ax1b.set_ylabel("Elapsed Time (ms)")
ax1b.legend(loc='upper right')

# --- Partition subplot ---
ax2 = axes[1]
ax2.plot(queries, records_before_part, marker='s', label='Before Partition Avg Records')
ax2.plot(queries, records_after_part, marker='s', linestyle='--', label='After Partition Avg Records')
ax2.set_ylabel("Records")
ax2.set_title("Partition-Level Avg Record Counts + Elapsed Time")
ax2.legend(loc='upper left')
ax2.grid(True, linestyle='--', alpha=0.6)

# Secondary axis for elapsed time (bar chart)
ax2b = ax2.twinx()
ax2b.bar(queries, elapsed_time, alpha=0.3, color='red', label='Elapsed Time (ms)')
ax2b.set_ylabel("Elapsed Time (ms)")
ax2b.legend(loc='upper right')

# --- File subplot ---
ax3 = axes[2]
ax3.plot(queries, records_before_file, marker='^', label='Before File Avg Records')
ax3.plot(queries, records_after_file, marker='^', linestyle='--', label='After File Avg Records')
ax3.set_ylabel("Records")
ax3.set_title("File-Level Avg Record Counts + Elapsed Time")
ax3.legend(loc='upper left')
ax3.grid(True, linestyle='--', alpha=0.6)

# Secondary axis for elapsed time (bar chart)
ax3b = ax3.twinx()
ax3b.bar(queries, elapsed_time, alpha=0.3, color='red', label='Elapsed Time (ms)')
ax3b.set_ylabel("Elapsed Time (ms)")
ax3b.legend(loc='upper right')

# Formatting
plt.xlabel("Query")
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()
