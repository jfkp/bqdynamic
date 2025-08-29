import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_write_performance(df):
    """
    Plot average execution time of WRITE operations grouped by scale and query.
    """
    write_df = df[df['operation'] == 'WRITE']
    write_summary = write_df.groupby(['base_type', 'scale', 'query'])['exec_time'].mean().reset_index()

    # Composite x-axis: scale first, then query
    write_summary['scale_query'] = write_summary['scale'].astype(str) + " | " + write_summary['query']

    plt.figure(figsize=(16,6))
    sns.barplot(
        data=write_summary,
        x='scale_query',
        y='exec_time',
        hue='base_type',
        ci=None
    )
    plt.title('Write Operation Performance by Scale and Technology')
    plt.xlabel('Scale and Query')
    plt.ylabel('Average Execution Time (s)')
    plt.legend(title='Base Type')
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()


def plot_read_performance(df):
    """
    Plot average execution time of READ operations grouped by scale and query.
    """
    read_df = df[df['operation'] == 'READ']
    read_summary = read_df.groupby(['base_type', 'scale', 'query'])['exec_time'].mean().reset_index()

    # Composite x-axis: scale first, then query
    read_summary['scale_query'] = read_summary['scale'].astype(str) + " | " + read_summary['query']

    plt.figure(figsize=(16,6))
    sns.barplot(
        data=read_summary,
        x='scale_query',
        y='exec_time',
        hue='base_type',
        ci=None
    )
    plt.title('Read Operation Performance by Scale and Technology')
    plt.xlabel('Scale and Query')
    plt.ylabel('Average Execution Time (s)')
    plt.legend(title='Base Type')
    plt.xticks(rotation=45, ha="right")
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
