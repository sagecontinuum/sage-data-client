# Sage Data Client

This is the official Sage Python data API client. Its main goal is to make writing queries and working with the results easy. It does this by:

1. Providing a simple query function which talks to the data API.
2. Providing the results in an easy to use [Pandas](https://pandas.pydata.org) data frame.

## Installation

Sage Data Client can be installed with pip using:

```sh
pip3 install sage-data-client
```

If you prefer to install this package into a Python virtual environment or are unable to install it system wide, you can use the [venv](https://docs.python.org/3/library/venv.html) module as follows:

```sh
# 1. Create a new virtual environment called my-venv.
python3 -m venv my-venv

# 2. Activate the virtual environment
source my-venv/bin/activate

# 3. Install sage data client in the virtual environment
pip3 install sage-data-client
```

Note: If you are using Linux, you may need to install the `python3-venv` package which is outside of the scope of this document.

Note: You will need to activate this virtual environment when opening a new terminal before running any Python scripts using Sage Data Client.

## Usage Examples

### Query API

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
print(df["meta.vsn"].unique())

# print stats of the temperature data grouped by node + sensor.
print(df.groupby(["meta.vsn", "meta.sensor"]).value.agg(["size", "min", "max", "mean"]))
```

```python
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
```

### Load results from file

If we have saved the results of a query to a file `data.json`, we can also load using the `load` function as follows:

```python
import sage_data_client

# load results from local file
df = sage_data_client.load("data.json")

# print number of results of each name
print(df.groupby(["meta.vsn", "name"]).size())
```

### Integration with Notebooks

Since we leverage the fantastic work provided by the Pandas library, performing things like looking at dataframes or creating plots is easy.

A basic example of querying and plotting data can be found [here](https://github.com/sagecontinuum/sage-data-client/blob/main/examples/plotting_example.ipynb).

### Additional Examples

Additional code examples can be found in the [examples](https://github.com/sagecontinuum/sage-data-client/tree/main/examples) directory.

If you're interested in contributing your own examples, feel free to add them to [examples/contrib](https://github.com/sagecontinuum/sage-data-client/tree/main/examples/contrib) and open a PR!

## Reference

The `query` function accepts the following arguments:

* `start`. Absolute or relative start timestamp. (**required**)
* `end`. Absolute or relative end timestamp.
* `head`. Limit results to `head` earliest values per series. (Only one of `head` or `tail` can be provided.)
* `tail`. Limit results to `tail` latest values per series. (Only one of `head` or `tail` can be provided.)
* `filter`. Key-value patterns to filter data on.
