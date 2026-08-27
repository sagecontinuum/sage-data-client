"""
Bulk download across multiple nodes and tasks over a date range.

Before running, save your credentials:
    sagecli login

Equivalent CLI command:
    sagecli download --start 2026-07-01 --end 2026-07-31 \
        --vsn W020 W039 \
        --task imagesampler-left imagesampler-right \
        --dest ./data \
        --layout "{date}/{vsn}/{task}/{filename}"
"""
import sage_data_client
import pandas as pd

# Replace with the VSNs you have access to.
# Request / view "My Nodes" access at https://portal.sagecontinuum.org
VSNS = ["W020", "W039"]
TASKS = ["imagesampler-left", "imagesampler-right"]

START = "2026-07-01"
END = "2026-07-31"

dfs = []
for vsn in VSNS:
    for task in TASKS:
        print("Querying vsn={}, task={}...".format(vsn, task))
        data = sage_data_client.query_downloads(
            start=START,
            end=END,
            filter={"vsn": vsn, "task": task},
        )
        print("  {} file(s) found.".format(len(data)))
        dfs.append(data.df)

combined = sage_data_client.DownloadResponse(pd.concat(dfs, ignore_index=True))
print("Found {} file(s) total.".format(len(combined)))

combined.download_all(
    dest="./data",
    layout="{date}/{vsn}/{task}/{filename}",
)
