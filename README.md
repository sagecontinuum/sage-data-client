# Sage Data API Client

This is the official Sage Python data API client. Its main goal is to make writing queries and working with the results easy. It does this by:

1. Providing a simple query function which talks to the data API.
2. Providing the results in an easy to use [Pandas](https://pandas.pydata.org) data frame.

## Installation

1. Go to the [latest release page](https://github.com/sagecontinuum/sage-data-client/releases/latest).
2. Copy the URL for `sage_data_client*.whl` asset. For example:
```
https://github.com/sagecontinuum/sage-data-client/releases/download/0.1.0/sage_data_client-0.1.0-py3-none-any.whl
```
3. Run `pip3 install url`. For example:
```
pip3 install https://github.com/sagecontinuum/sage-data-client/releases/download/0.1.0/sage_data_client-0.1.0-py3-none-any.whl
```

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
