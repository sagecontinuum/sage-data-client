# Sage Data API Client

This is the official Sage Python data API client. Its main goal is to make writing queries and working with the results easy. It does this by:

1. Providing a simple query function which talks to the data API.
2. Providing the results in an easy to use Pandas data frame.

## Usage Example

```python
import sage_data_client

# query and load data into pandas data frame
df = sage_data_client.query(
    start="-1h",
    filter={
        "name": "env.temperature",
    }
)

# print results in data frame
print(df)

# meta columns are expanded into meta.fieldname. for example, here we print the unique nodes
print(df["meta.node"].unique())

# print stats of the temperature data grouped by node + sensor.
print(df.groupby(["meta.node", "meta.sensor"]).value.agg(["min", "max", "mean"]))
```
