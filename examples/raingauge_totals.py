"""
This example demonstrates querying rain gauge data and printing the total
number of measurements grouped by VSN and sensor.
"""
import sage_data_client

# query and load data into pandas data frame
df = sage_data_client.query(
    start="-1h",
    filter={
        "name": "env.raingauge.*",
    }
)

# print number of results of each name
print(df.groupby(["meta.vsn", "name"]).size())
