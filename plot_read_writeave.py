import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Rectangle

def plot_grouped_performance(df, operation="WRITE"):
    """
    Plot average execution time of operations grouped by scale and query,
    with rectangles around each group of bars per scale.
    """
    op_df = df[df['operation'] == operation]
    summary = op_df.groupby(['base_type', 'scale', 'query'])['exec_time'].mean().reset_index()

    plt.figure(figsize=(16,6))
    ax = sns.barplot(
        data=summary,
        x='query',
        y='exec_time',
        hue='base_type',
        ci=None
    )
    plt.title(f'{operation} Operation Performance by Scale and Technology')
    plt.xlabel('Query')
    plt.ylabel('Average Execution Time (s)')
    plt.legend(title='Base Type')
    plt.xticks(rotation=45, ha="right")

    # Get unique scales and queries in consistent order
    scales = summary['scale'].unique()
    queries = summary['query'].unique()
    num_queries = len(queries)

    # Loop over scales and add rectangles above groups
    for i, scale in enumerate(scales):
        start = i * num_queries
        end = start + num_queries - 1

        # Rectangle spans horizontally over that block of queries
        ax.add_patch(Rectangle(
            (start-0.5, ax.get_ylim()[1]*1.01),  # start just above bars
            num_queries,                         # width
            ax.get_ylim()[1]*0.05,               # small height for grouping
            fill=False,
            lw=2,
            edgecolor='black',
            clip_on=False
        ))

        # Label the rectangle with the scale
        ax.text(
            start + num_queries/2 - 0.5, ax.get_ylim()[1]*1.07,
            scale,
            ha='center', va='bottom', fontsize=12, fontweight='bold'
        )

    plt.tight_layout()
    plt.show()


def main():
    # Load CSV file
    df = pd.read_csv("benchmark.csv")
    df['exec_time'] = pd.to_numeric(df['exec_time'], errors='coerce')

    # Plot write and read performance
    plot_grouped_performance(df, operation="WRITE")
    plot_grouped_performance(df, operation="READ")


if __name__ == "__main__":
    main()
