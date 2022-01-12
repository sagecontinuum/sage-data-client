"""
This example demonstrates querying all temperature data and printing basic stats grouped by VSN and sensor.
"""
import sage_data_client

# query and load data into pandas data frame
df = sage_data_client.query(
    start="-1h",
    filter={
        "name": "env.temperature",
    }
)

# print stats of the temperature data grouped by node + sensor.
print(df.groupby(["meta.vsn", "meta.sensor"]).value.agg(["size", "min", "max", "mean"]))
