import matplotlib.pyplot as plt
import numpy as np

# Use relevant columns
queries = df['query']
exec_time = df['exec_time']
num_rows = df['s_after_total-data-files']  # as a proxy for row count/scale
file_size = df['s_after_total-files-size']

# Normalize file size for color mapping
colors = (file_size - min(file_size)) / (max(file_size) - min(file_size))

# Scatter plot with execution time vs query
plt.figure(figsize=(12,6))
plt.scatter(queries, exec_time, s=num_rows*5, c=colors, cmap='viridis', alpha=0.7)
plt.colorbar(label='File size (normalized)')
plt.xticks(rotation=45, ha='right')
plt.xlabel('Query')
plt.ylabel('Execution Time (s)')
plt.title('Query Execution Time by Query with Number of Rows and File Size')
plt.tight_layout()
plt.show()
