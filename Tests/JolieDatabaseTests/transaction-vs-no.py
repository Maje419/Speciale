import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

font = {'size'   : 12}
matplotlib.rc('font', **font)

def getDataframe(path: str) -> pd.DataFrame:
    df = pd.read_csv(filepath_or_buffer=path, sep=' ')
    df_transactions = df[df["test"].str.endswith("transaction")]
    df_transactions = df_transactions[df_transactions["test"].str.endswith("transactions") == False]
    df_transactions['test'] = df_transactions['test'].map(lambda x: x.replace("_transaction", ""))

    df_no_transactions= df[df["test"].str.endswith("transaction") == False]
    df_no_transactions= df_no_transactions[df_no_transactions["test"].str.endswith("transactions") == False]

    dfNew = pd.merge(df_transactions, df_no_transactions, on='test', how="right")

    return dfNew.groupby("test").mean()

df = getDataframe("results/Hikari_Postgres.csv")

df.columns = ["Using beginTx", "No transaction"]

df = df.sort_values(by="test", ascending=True)

ax = df.plot(kind="barh", logx=True, rot=0)

ax.set_xlabel("Avg. ms")

patches = [" \\\\ ", "//"]
colors = [ax.containers[0].patches[0].get_facecolor(), ax.containers[1].patches[0].get_facecolor()]
handles = [mpatches.Patch(facecolor=colors[1],hatch='//',label='No transaction'), mpatches.Patch(facecolor=colors[0],hatch=r'\\\\',label='Using beginTx')]

ax.legend(handles = handles, loc=1)
for patch in ax.containers[0]:
    patch.set_hatch(patches[0])

for patch in ax.containers[1]:
    patch.set_hatch(patches[1])

for bars in ax.containers:
    ax.bar_label(bars, padding=5)

plt.show()