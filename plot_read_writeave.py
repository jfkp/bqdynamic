import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_write_performance(df):
    """
    Plot average execution time of WRITE operations by base_type, scale, and query.
    """
    write_df = df[df['operation'] == 'WRITE']
    write_summary = write_df.groupby(['base_type', 'scale', 'query'])['exec_time'].mean().reset_index()

    # Create composite x-axis label
    write_summary['label'] = write_summary['scale'].astype(str) + "\n" + write_summary['query']

    plt.figure(figsize=(14,6))
    sns.barplot(
        data=write_summary,
        x='label',
        y='exec_time',
        hue='base_type',
        ci=None
    )
    plt.title('Write Operation Performance Comparison by Scale and Technology')
    plt.xlabel('Scale and Write Operation')
    plt.ylabel('Average Execution Time (s)')
    plt.legend(title='Base Type')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def plot_read_performance(df):
    """
    Plot average execution time of READ operations by base_type, scale, and wquery.
    """
    read_df = df[df['operation'] == 'READ']
    read_summary = read_df.groupby(['base_type', 'scale', 'wquery'])['exec_time'].mean().reset_index()

    # Create composite x-axis label
    read_summary['label'] = read_summary['scale'].astype(str) + "\n" + read_summary['wquery']

    plt.figure(figsize=(14,6))
    sns.barplot(
        data=read_summary,
        x='label',
        y='exec_time',
        hue='base_type',
        ci=None
    )
    plt.title('Read Operation Performance After Different Writes by Scale and Technology')
    plt.xlabel('Scale and Previous Write Operation (wquery)')
    plt.ylabel('Average Execution Time (s)')
    plt.legend(title='Base Type')
    plt.xticks(rotation=45)
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
