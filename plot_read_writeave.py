import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot_write_performance(df):
    """
    Plot average execution time of WRITE operations by base_type, scale, and query.
    """
    write_df = df[df['operation'] == 'WRITE']
    write_summary = write_df.groupby(['base_type', 'scale', 'query'])['exec_time'].mean().reset_index()

    plt.figure(figsize=(12,6))
    sns.barplot(
        data=write_summary,
        x='query',
        y='exec_time',
        hue='base_type'
    )
    plt.title('Write Operation Performance Comparison by Scale and Technology')
    plt.xlabel('Write Operation')
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

    plt.figure(figsize=(12,6))
    sns.barplot(
        data=read_summary,
        x='wquery',
        y='exec_time',
        hue='base_type'
    )
    plt.title('Read Operation Performance After Different Writes by Scale and Technology')
    plt.xlabel('Previous Write Operation (wquery)')
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
