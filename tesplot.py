import pandas as pd
import matplotlib.pyplot as plt

# Put your data in a CSV string or file
data = """base_type,scale,operation,exec_time,run_id,query,wquery
BLMS,10G,READ,4.364005184004782,0.0,q1,merge_insert
BLMS,10G,READ,2.4169755289913155,1.0,q1,merge_insert
BLMS,10G,READ,2.4371245600050315,2.0,q1,merge_insert
BLMS,10G,READ,18.898035134989183,0.0,q2,merge_insert
BLMS,10G,WRITE,410.31895041100506,bulk_load,
BLMS,10G,WRITE,34.45093625300797,merge_insert,
BLMS,10G,WRITE,61.13515410499531,merge_update,
BLMS,10G,WRITE,23.80685163399903,merge_xdelete,
BLMS,10G,WRITE,19.685425229996326,merge_sdelete,
BLMT,10G,WRITE,10.581,d_delete,
BLMT,10G,WRITE,4.359,merge_xdelete,
BLMT,10G,WRITE,8.047,merge_sdelete,
BLMT,10G,WRITE,16.237,merge_update,
BLMT,10G,READ,2.933,1.0,q6,d_delete
BLMT,10G,READ,7.138,1.0,q8,d_delete
BLMT,10G,READ,7.867,1.0,q2,d_delete
BLMT,10G,READ,3.162,1.0,q7,d_delete
BLMT,10G,READ,2.076,1.0,q9,d_delete
BQMS,10G,READ,3.3754528909921646,0.0,q1,merge_insert
BQMS,10G,READ,2.0856350289977854,1.0,q1,merge_insert
BQMS,10G,READ,1.999142432003282,2.0,q1,merge_insert
BQMS,10G,READ,23.95874382198963,0.0,q2,merge_insert
BQMS,10G,READ,14.769499998001264,1.0,q2,merge_insert
BQMS,10G,READ,12.692071147001116,2.0,q2,merge_insert
BQMS,10G,WRITE,256.049575846002,bulk_load,
BQMS,10G,WRITE,25.04492214600032,merge_insert,
BQMS,10G,WRITE,64.7249343739968,merge_update,
BQMS,10G,WRITE,24.24865570499969,merge_xdelete,
BQMS,10G,WRITE,23.780658219999168,merge_sdelete,
BQMS,10G,WRITE,24.64359486999456,merge_mdelete,
BQMS,10G,WRITE,10.707954704004806,d_delete,
BQMN,10G,WRITE,316.1889176140103,bulk_load
BQMN,10G,WRITE,31.34931990399491,merge_insert
BQMN,10G,WRITE,63.15389136900194,merge_update
BQMN,10G,WRITE,24.146586082002614,merge_xdelete
BQMN,10G,WRITE,21.7237542399962,merge_sdelete
BQMN,10G,READ,11.971865069004709,0.0,q1,merge_insert
BQMN,10G,READ,2.3824095799936917,1.0,q1,merge_insert
BQMN,10G,READ,2.339508040997316,2.0,q1,merge_insert
BQMN,10G,READ,19.23397877700336,0.0,q2,merge_insert
BQMN,10G,READ,13.689742728005513,1.0,q2,merge_insert
BQMN,10G,READ,12.867627685001937,2.0,q2,merge_insert
BQMN,10G,READ,3.056708757008892,0.0,q3,merge_insert
BQMN,10G,READ,2.4306927060097223,1.0,q3,merge_insert
BQMN,10G,READ,2.241507275000913,2.0,q3,merge_insert
BQST,10G,READ,3.559,1.0,q2,d_delete
BQST,10G,READ,1.95,1.0,q3,d_delete
BQST,10G,READ,10.848,1.0,q8,d_delete
BQST,10G,READ,1.175,1.0,q9,d_delete
BQST,10G,READ,0.682,1.0,q5,d_delete
BQST,10G,READ,0.953,1.0,q1,d_delete
BQST,10G,READ,1.929,1.0,q7,d_delete
BQST,10G,READ,0.453,1.0,q6,d_delete
BQST,10G,WRITE,15.059,merge_mdelete,
BQST,10G,WRITE,17.333,merge_update,
BQST,10G,WRITE,13.455,merge_sdelete,
BQST,10G,WRITE,8.835,merge_xdelete,
BQST,10G,WRITE,8.46,d_delete,
BQST,10G,WRITE,5.717,merge_insert,
BQST,10G,WRITE,17.267,load_query,
BQST,20G,WRITE,6.487,d_delete,
BQST,20G,WRITE,8.058,merge_mdelete,
BQST,20G,WRITE,26.708,load_query,
"""

import seaborn as sns
import matplotlib.pyplot as plt

import seaborn as sns
import matplotlib.pyplot as plt

def plot_read_line(df):
    """
    Line plot for READ execution times across run_id.
    """
    read_df = df[df['operation'] == 'READ']

    plt.figure(figsize=(10,6))
    sns.lineplot(
        data=read_df,
        x="run_id",
        y="exec_time",
        hue="query",
        style="base_type",
        markers=True,
        dashes=False
    )
    plt.title("Read Execution Times Across Runs")
    plt.ylabel("Execution Time (s)")
    plt.xlabel("Run ID")
    plt.legend(title="Query / Base Type", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()


def plot_read_box(df):
    """
    Boxplot for READ execution times per query.
    """
    read_df = df[df['operation'] == 'READ']

    plt.figure(figsize=(10,6))
    sns.boxplot(
        data=read_df,
        x="query",
        y="exec_time",
        hue="base_type"
    )
    plt.title("Boxplot of Read Execution Times per Query")
    plt.ylabel("Execution Time (s)")
    plt.xlabel("Query")
    plt.legend(title="Base Type", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()


def plot_write_strip(df):
    """
    Stripplot for WRITE execution times per write operation.
    """
    write_df = df[df['operation'] == 'WRITE']

    plt.figure(figsize=(10,6))
    sns.stripplot(
        data=write_df,
        x="wquery",
        y="exec_time",
        hue="base_type",
        dodge=True,
        jitter=True
    )
    plt.title("Strip Plot of Write Execution Times per Operation")
    plt.ylabel("Execution Time (s)")
    plt.xlabel("Write Operation")
    plt.xticks(rotation=45)
    plt.legend(title="Base Type", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()


def plot_dashboard(df, scale=None):
    """
    Dashboard with:
    1. Line plot for READ across run_id
    2. Boxplot for READ per query
    3. Stripplot for WRITE per operation
    
    Parameters:
        df (pd.DataFrame): performance dataset
        scale (str, optional): filter by scale (e.g. "10G"). 
                               If None, uses all scales.
    """

    # Filter by scale if provided
    if scale is not None:
        df = df[df["scale"] == scale]

    # Split READ and WRITE
    read_df = df[df['operation'] == 'READ']
    write_df = df[df['operation'] == 'WRITE']

    # Create subplots
    fig, axes = plt.subplots(1, 3, figsize=(22, 6))

    # --- Line plot for READ ---
    if not read_df.empty:
        sns.lineplot(
            data=read_df,
            x="run_id",
            y="exec_time",
            hue="query",
            style="base_type",
            markers=True,
            dashes=False,
            ax=axes[0]
        )
        axes[0].set_title(f"Read Times Across Runs ({scale})" if scale else "Read Times Across Runs")
        axes[0].set_xlabel("Run ID")
        axes[0].set_ylabel("Exec Time (s)")
        axes[0].legend(title="Query / Base Type", fontsize=8)
    else:
        axes[0].set_visible(False)

    # --- Boxplot for READ ---
    if not read_df.empty:
        sns.boxplot(
            data=read_df,
            x="query",
            y="exec_time",
            hue="base_type",
            ax=axes[1]
        )
        axes[1].set_title(f"Read Times per Query ({scale})" if scale else "Read Times per Query (Boxplot)")
        axes[1].set_xlabel("Query")
        axes[1].set_ylabel("Exec Time (s)")
        axes[1].legend(title="Base Type", fontsize=8)
    else:
        axes[1].set_visible(False)

    # --- Stripplot for WRITE ---
    if not write_df.empty:
        sns.stripplot(
            data=write_df,
            x="wquery",
            y="exec_time",
            hue="base_type",
            dodge=True,
            jitter=True,
            ax=axes[2]
        )
        axes[2].set_title(f"Write Times per Operation ({scale})" if scale else "Write Times per Operation (Stripplot)")
        axes[2].set_xlabel("Write Operation")
        axes[2].set_ylabel("Exec Time (s)")
        axes[2].tick_params(axis='x', rotation=45)
        axes[2].legend(title="Base Type", fontsize=8)
    else:
        axes[2].set_visible(False)

    plt.tight_layout()
    plt.show()


import seaborn as sns
import matplotlib.pyplot as plt

def plot_dashboard1(df, scale=None):
    """
    Dashboard with:
    1. Line plot for READ across run_id
    2. Violin+Swarm plot for READ per query
    3. Stripplot for WRITE per operation
    
    Parameters:
        df (pd.DataFrame): performance dataset
        scale (str, optional): filter by scale (e.g. "10G"). 
                               If None, uses all scales.
    """

    # Filter by scale if provided
    if scale is not None:
        df = df[df["scale"] == scale]

    # Split READ and WRITE
    read_df = df[df['operation'] == 'READ']
    write_df = df[df['operation'] == 'WRITE']

    # Create subplots
    fig, axes = plt.subplots(1, 3, figsize=(26, 7))

    # --- Line plot for READ across runs ---
    if not read_df.empty:
        sns.lineplot(
            data=read_df,
            x="run_id",
            y="exec_time",
            hue="query",
            style="base_type",
            markers=True,
            markersize=8,
            linewidth=2,
            dashes=False,
            ax=axes[0]
        )
        axes[0].set_title(f"Read Times Across Runs ({scale})" if scale else "Read Times Across Runs")
        axes[0].set_xlabel("Run ID")
        axes[0].set_ylabel("Execution Time (s)")
        axes[0].legend(title="Query / Base Type", fontsize=8, bbox_to_anchor=(1.05, 1), loc='upper left')
    else:
        axes[0].set_visible(False)

    # --- Violin + Swarm for READ per query ---
    if not read_df.empty:
        sns.violinplot(
            data=read_df,
            x="query",
            y="exec_time",
            hue="base_type",
            split=True,
            inner=None,
            ax=axes[1]
        )
        sns.swarmplot(
            data=read_df,
            x="query",
            y="exec_time",
            hue="base_type",
            dodge=True,
            size=4,
            color="k",
            alpha=0.6,
            ax=axes[1]
        )
        axes[1].set_title(f"Read Times per Query ({scale})" if scale else "Read Times per Query")
        axes[1].set_xlabel("Query")
        axes[1].set_ylabel("Execution Time (s)")
        axes[1].tick_params(axis="x", rotation=45)
        axes[1].legend([],[], frameon=False)  # avoid duplicate legends
    else:
        axes[1].set_visible(False)

    # --- Stripplot for WRITE ---
    if not write_df.empty:
        sns.stripplot(
            data=write_df,
            x="wquery",
            y="exec_time",
            hue="base_type",
            dodge=True,
            jitter=True,
            size=6,
            ax=axes[2]
        )
        axes[2].set_title(f"Write Times per Operation ({scale})" if scale else "Write Times per Operation")
        axes[2].set_xlabel("Write Operation")
        axes[2].set_ylabel("Execution Time (s)")
        axes[2].tick_params(axis='x', rotation=45)
        axes[2].legend(title="Base Type", fontsize=8, bbox_to_anchor=(1.05, 1), loc='upper left')
    else:
        axes[2].set_visible(False)

    plt.tight_layout()
    plt.show()


