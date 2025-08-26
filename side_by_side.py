queries = df_merged['query']
x = np.arange(len(queries))
width = 0.25
colors = ['skyblue', 'salmon', 'lightgreen']

plt.figure(figsize=(16,8))

for i, scale in enumerate(scales):
    exec_time = df_merged[f'exec_time_{scale}']
    rows_processed = df_merged[f'added_records_{scale}'].fillna(0) + df_merged[f'deleted_records_{scale}'].fillna(0)
    size_processed = (df_merged[f'added_size_{scale}'].fillna(0) + df_merged[f'removed_size_{scale}'].fillna(0)) / (1024*1024)
    file_count_after = df_merged[f'file_count_{scale}'].fillna(0).astype(int)
    total_size_after = df_merged[f'total_size_{scale}'].fillna(0) / (1024*1024*1024)

    bars = plt.bar(x + i*width, exec_time, width, color=colors[i], alpha=0.8, label=f'Scale {scale}')

    # Annotate
    for bar, rows, size, fcount, fsize in zip(bars, rows_processed, size_processed, file_count_after, total_size_after):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height,
                 f"{int(rows):,} rows\n{size:.1f} MB\n{fcount} files\n{fsize:.2f} GB",
                 ha='center', va='bottom', fontsize=7)

# Formatting
plt.title("Query Execution Time Comparison Across Scales", fontsize=16)
plt.xlabel("Query", fontsize=12)
plt.ylabel("Execution Time (seconds)", fontsize=12)
plt.xticks(x + width*(len(scales)-1)/2, queries, rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.legend()
plt.tight_layout()
plt.show()
