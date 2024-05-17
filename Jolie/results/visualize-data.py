import pandas as pds
from matplotlib import pyplot as plt

plt.figure()
plt.rcParams["figure.figsize"] = [7.00, 3.50]
plt.rcParams["figure.autolayout"] = True

def graphMean(fileName: str,  label = None, ax = None):
    # Load non-normalized data
    df = pds.read_csv(filepath_or_buffer=fileName, sep=' ')

    # Don't care about the id column
    df = df.drop(columns=['RequestId'])

    # Copy the SentTime column, as this is the one we normalize around
    sentTime = df["SentTime"]

    # For every other column, normalize to ms after SentTime
    for column in df.columns:
        df[column] = df[column] - sentTime

    # Find the mean of each column
    mean = df.mean()
    if (ax == None):
        return mean.plot(rot=0, title="Message propagation time", label = label, ylabel="ms after Curl request sent", xlabel="step", legend=True)
    else:
        return mean.plot(ax=ax, label=label, legend=True)

ax = graphMean("safe-safe_10.csv", "safe-safe_10")
graphMean("safe-safe_1.csv", "safe-safe_1", ax)
graphMean("safe-safe_100.csv", "safe-safe_100", ax)
graphMean("unsafe-unsafe.csv", "unsafe-unsafe", ax)
plt.show()
