def load_and_prepare(files):
all_data = []
for scale, techs in files.items():
for tech, paths in techs.items():
upd = pd.read_csv(paths["update"])
read = pd.read_csv(paths["read"])


upd["query_type"] = "update"
read["query_type"] = "read"


read = read.rename(columns={"wquery": "update_query"})
upd = upd.rename(columns={"query": "update_query"})


upd = upd[["update_query", "query", "exec_time", "query_type"]]
read = read[["update_query", "query", "exec_time", "query_type"]]


for df in (upd, read):
df["technology"] = tech
df["scale"] = scale


all_data.append(pd.concat([upd, read], ignore_index=True))


return pd.concat(all_data, ignore_index=True)

def plot_grouped_bar(df):
plt.figure(figsize=(18,8))


# Label: scale + actual query name
df["label"] = df["scale"] + ":" + df["query"]


# Pivot for technologies
pivot = df.pivot_table(index=["label","query_type"], columns="technology", values="exec_time")


# Plot grouped bars
ax = pivot.plot(kind="bar", width=0.8, figsize=(18,8))


# Highlight update vs read with background shading
query_types = pivot.index.get_level_values("query_type")
for i, qtype in enumerate(query_types):
if qtype == "update":
ax.axvspan(i-0.5, i+0.5, color="lightblue", alpha=0.2)


plt.title("Execution Time Comparison Across Scales (Update vs Read grouped)")
plt.ylabel("Execution Time (s)")
plt.xlabel("Scale : Query")
plt.xticks(rotation=60, ha="right")
plt.legend(title="Technology")
plt.tight_layout()
plt.show()

# --- Prepare data ---
all_data = load_and_prepare(files)
# Plot all scales together
plot_grouped_bar(all_data)
