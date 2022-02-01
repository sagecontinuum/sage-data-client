"""
This example demonstrates loading a local data file containing 5 minutes of temperature data
and printing the mean value grouped by VSN and sensor.
"""
import sage_data_client

# load results from local file
df = sage_data_client.load("data.json")

# print number of results of each name
print(df.groupby(["meta.vsn", "meta.sensor"]).value.mean())
