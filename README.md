# Sage Data API Client

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
