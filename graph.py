import pandas as pd
import matplotlib.pyplot as plt

# Load your file (update the path if needed)
file_path = "blms_10G_store_sale_denorm_bench_10G_update_metrics (1).csv"
df = pd.read_csv(file_path)

# Convert timestamp if available
if 'timestamp' in df.columns:
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

# Detect operation type from the query
def detect_operation(q):
    if isinstance(q, str):
        q_lower = q.lower()
        if 'delete' in q_lower:
            return 'delete'
        elif 'insert' in q_lower:
            return 'insert'
        elif 'upsert' in q_lower:
            return 'upsert'
        elif 'start' in q_lower:
            return 'start'
    return 'other'

df['operation_type'] = df['query'].apply(detect_operation)

# --- Plot 1: Average Execution Time per operation type ---
exec_time_summary = df.groupby('operation_type')['exec_time'].mean()

plt.figure(figsize=(8, 5))
exec_time_summary.plot(kind='bar', color='skyblue', edgecolor='black')
plt.title("Average Execution Time by Operation Type")
plt.ylabel("Execution Time (ms)")
plt.xlabel("Operation Type")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# --- Plot 2: Execution Time vs Number of Tasks ---
plt.figure(figsize=(8, 5))
plt.scatter(df['numTasks'], df['exec_time'], alpha=0.6, color='orange')
plt.title("Execution Time vs Number of Tasks")
plt.xlabel("Number of Tasks")
plt.ylabel("Execution Time (ms)")
plt.tight_layout()
plt.show()

# --- Plot 3: Deleted Records Before vs After ---
if 's_before_deleted-records' in df.columns and 's_after_deleted-records' in df.columns:
    deleted_summary = df.groupby('operation_type')[['s_before_deleted-records', 's_after_deleted-records']].mean()

    deleted_summary.plot(kind='bar', figsize=(8, 5), color=['red', 'green'])
    plt.title("Deleted Records Before vs After by Operation Type (avg)")
    plt.ylabel("Records")
    plt.tight_layout()
    plt.show()
