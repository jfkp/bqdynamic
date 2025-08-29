import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Rectangle

def plot_write_performance(df):
    """
    Plot average execution time of WRITE operations grouped by scale and query,
    with rectangles marking groups of queries per scale.
    """
    write_df = df[df['operation'] == 'WRITE']
    write_summary = write_df.groupby(['base_type', 'scale', 'query'])['exec_time'].mean().reset_index()

    plt.figure(figsize=(16,6))
    ax = sns.barplot(
        data=write_summary,
        x='query',
        y='exec_time',
        hue='base_type',
        ci=None
    )
    plt.title('Write Operation Performance by Scale and Technology')
    plt.xlabel('Query')
    plt.ylabel('Average Execution Time (s)')
    plt.legend(title='Base Type')
    plt.xticks(rotation=45, ha="right")

    # Draw rectangles around groups by scale
    scales = write_summary['scale'].unique()
    queries = write_summary['query'].unique()
    num_queries = len(queries)

    for i, scale in enumerate(scales):
        start = i * num_queries
        end = start + num_queries - 1

        # Rectangle spanning the queries for this scale
        ax.add_patch(Rectangle(
            (start-0.5, 0),   # (x,y) bottom-left corner
            num_queries,      # width
            ax.get_ylim()[1], # height (span full y-axis)
            fill=False,
            lw=2,
            edgecolor='black',
            clip_on=False
        ))

        # Label the rectangle with the scale
        ax.text(
            start + num_queries/2 - 0.5, ax.get_ylim()[1] * 1.01,
            scale,
            ha='center', va='bottom', fontsize=12, fontweight='bold'
        )

    plt.tight_layout()
    plt.show()


def plot_read_performance(df):
    """
    Plot average execution time of READ operations grouped by scale and query,
    with rectangles marking groups of queries per scale.
    """
    read_df = df[df['operation'] == 'READ']
    read_summary = read_df.groupby(['base_type', 'scale', 'query'])['exec_time'].mean().reset_index()

    plt.figure(figsize=(16,6))
    ax = sns.barplot(
        data=read_summary,
        x='query',
        y='exec_time',
        hue='base_type',
        ci=None
    )
    plt.title('Read Operation Performance by Scale and Technology')
    plt.xlabel('Query')
    plt.ylabel('Average Execution Time (s)')
    plt.legend(title='Base Type')
    plt.xticks(rotation=45, ha="right")

    # Draw rectangles around groups by scale
    scales = read_summary['scale'].unique()
    queries = read_summary['query'].unique()
    num_queries = len(queries)

    for i, scale in enumerate(scales):
        start = i * num_queries
        end = start + num_queries - 1

        ax.add_patch(Rectangle(
            (start-0.5, 0),
            num_queries,
            ax.get_ylim()[1],
            fill=False,
            lw=2,
            edgecolor='black',
            clip_on=False
        ))

        ax.text(
            start + num_queries/2 - 0.5, ax.get_ylim()[1] * 1.01,
            scale,
            ha='center', va='bottom', fontsize=12, fontweight='bold'
        )

    plt.tight_layout()
    plt.show()


def main():
    # Load CSV file
    df = pd.read_csv("benchmark.csv")

    # Ensure exec_time is numeric
    df['exec_time'] = pd.to_numeric(df['exec_time'], errors='coerce')

    # Plot write and read performance
    plot_write_performance(df)
    plot_read_performance(df)


if __name__ == "__main__":
    main()
