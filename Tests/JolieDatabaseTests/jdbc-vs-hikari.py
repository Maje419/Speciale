import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

import matplotlib.patches as mpatches


font = {'size'   : 12}
matplotlib.rc('font', **font)

def getMean(path: str) -> pd.DataFrame:
    df = pd.read_csv(filepath_or_buffer=path, sep=' ')
    return df.groupby("test").mean()

noConnectionpoolDf = getMean("results/JDBC_Postgres.csv")
connectionPoolDf = getMean("results/Hikari_Postgres.csv")

index = connectionPoolDf.columns

dfNew = pd.merge(noConnectionpoolDf, connectionPoolDf, on='test', how="inner")

dfNew.columns = ["JDBC implementation", "HikariCP implementation"]

dfNew = dfNew.sort_values(by="HikariCP implementation", ascending=False)

ax = dfNew.plot(kind="barh", logx=True, rot=0)

ax.set_xlabel("Avg. ms")

patches = [" \\\\ ", "//"]
colors = [ax.containers[0].patches[0].get_facecolor(), ax.containers[1].patches[0].get_facecolor()]
handles = [mpatches.Patch(facecolor=colors[1],hatch='//',label='HikariCP implementation'), mpatches.Patch(facecolor=colors[0],hatch=r'\\\\',label='JDBC implementation') ]

ax.legend(handles = handles, loc=1)
for patch in ax.containers[0]:
    patch.set_hatch(patches[0])

for patch in ax.containers[1]:
    patch.set_hatch(patches[1])

for bars in ax.containers:
    ax.bar_label(bars, padding=5)

plt.show()