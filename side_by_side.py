# --- 5️⃣ Prepare for plotting ---
queries = df_merged['query']
x = np.arange(len(queries))
width = 0.25
colors = ['skyblue', 'salmon', 'lightgreen','yellow']

plt.figure(figsize=(16,8))

for i, scale in enumerate(scales):
    exec_time = df_merged[f'exec_time_{scale}']
    
    # Rows processed (added + before)
    rows = df_merged[f's_after_added-records_{scale}'].fillna(0) + df_merged[f's_before_added-records_{scale}'].fillna(0)
    
    # File size processed (added + removed) in MB
    size_mb = (df_merged[f's_after_removed-files-size_{scale}'].fillna(0) + df_merged[f's_before_removed-files-size_{scale}'].fillna(0)) / (1024*1024)
    
    # File count after operation
    file_count = df_merged[f's_after_total-data-files_{scale}'].fillna(0).astype(int)
    
    # Total file size after operation in GB
    total_size_gb = df_merged[f's_after_total-files-size_{scale}'].fillna(0) / (1024*1024*1024)
    
    # Plot bars
    bars = plt.bar(x + i*width, exec_time, width, color=colors[i], alpha=0.8, label=f'Scale {scale}')
    
    # Compact annotation
    for bar, r, s, fcount, fsize in zip(bars, rows, size_mb, file_count, total_size_gb):
        height = bar.get_height()
        # Abbreviate numbers
        rows_str = f"{r/1000:.1f}k" if r >= 1000 else f"{int(r)}"
        size_str = f"{s:.1f}MB" if s < 1024 else f"{s/1024:.1f}GB"
        plt.text(
            bar.get_x() + bar.get_width()/2,
            height + 0.01*max(exec_time),  # slightly above the bar
            f"{rows_str} r, {size_str}, {fcount} f, {fsize:.2f} GB",
            ha='center',
            va='bottom',
            fontsize=7
        )

# --- 6️⃣ Formatting ---
plt.title("Query Execution Time Comparison Across Scales", fontsize=16)
plt.xlabel("Query", fontsize=12)
plt.ylabel("Execution Time (seconds)", fontsize=12)
plt.xticks(x + width*(len(scales)-1)/2, queries, rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.legend()
plt.tight_layout()
plt.show()
✅ Improvements in This Version
All annotation info on one line: rows, processed size, file count, total size.

Abbreviated numbers: thousands as k, MB/GB units.

Text slightly above the bar: avoids overlapping bars.

Compact font: small enough to be readable but not cluttered.

This keeps the chart clean, readable, and still informative.

If you want, the next step could be to use colors for different metrics in the annotation or interactive hover annotations for even better readability when there are many queries.

Do you want me to do that?








Ask ChatGPT
