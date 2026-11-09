# Sage Data Client

Sage Data Client queries the Sage data API and loads measurements into pandas
dataframes. Its public functions are `query()` for HTTP queries and `load()`
for saved NDJSON files or streams.

`query()` requests Arrow by default and decodes NDJSON when that is what the
server returns. Released clients and deployed services may differ from this
source tree.

## Install

Python 3.9 or newer is required. NumPy, pandas, and PyArrow are installed as
dependencies.

```sh
python -m pip install sage-data-client
```

To use this checkout's implementation, see the
[development guide](docs/development.md).

## Query measurements

```python
import sage_data_client

frame = sage_data_client.query(
    start="-5min",
    filter={"name": "env.temperature", "vsn": "W02E"},
)
print(frame.head())
```

The default endpoint is `https://data.sagecontinuum.org/api/v1/query`.
Choose a service and time range with the data you need; an empty response is
normal when no records match. The client returns a complete dataframe, so
allow memory for the result.

## Documentation

Start with the [documentation index](docs/README.md).

| Guide | Contents |
| --- | --- |
| [Usage](docs/usage.md) | Installation, queries, files, dataframe analysis and notebooks |
| [Reference](docs/reference.md) | All public parameters, defaults, inputs and exceptions |
| [Formats](docs/formats.md) | Arrow/NDJSON, compression, timestamps, metadata and memory |
| [Development](docs/development.md) | Modules, offline tests, fixtures, wheel builds and CI |

More examples are in [examples](examples), including the
[plotting notebook](examples/plotting_example.ipynb) and
[contributed notebooks](examples/contrib).
