# Usage

[Documentation index](README.md) · [Public reference](reference.md)

## Install

Use Python 3.9 or newer. For a separate environment:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install sage-data-client
```

The package depends on NumPy, pandas and PyArrow. Some Linux installations
require their distribution's Python venv package first. Activate the environment
again in each new shell. To work with this source tree instead of a released
package, use the [development installation](development.md#editable-install).

## Query a time range

Use the default endpoint by omitting `endpoint`, or select your service:

```python
import sage_data_client

frame = sage_data_client.query(
    start="2025-03-01T00:00:00Z",
    end="2025-03-01T00:01:00Z",
    filter={"name": "wxt.env.temp", "vsn": "W0A0"},
    endpoint="http://localhost:10000/api/v1/query",
)
print(frame.head())
```

The example selects one minute on one node. Start your own service separately
or substitute a reachable deployment and an available range. The client does
not choose hot versus cold storage; that is the server's routing decision.

For relative time, use values such as `-5min`, `-1h`, or `datetime.timedelta`:

```python
from datetime import timedelta
import sage_data_client

frame = sage_data_client.query(
    start=timedelta(hours=-1),
    end=timedelta(minutes=-55),
    filter={"name": "env.temperature", "vsn": "W02E"},
)
print(len(frame))
```

Relative inputs are resolved on the client before transport. Absolute strings,
Python datetimes and pandas timestamps are also accepted. Naive timestamps
are interpreted as UTC; aware timestamps are normalized to UTC. Nanosecond
fractions are preserved using ISO formatting. See
[time semantics](reference.md#time-inputs).

## Filters and per-series limits

Filter keys use measurement/tag names such as `name`, `vsn`, and `sensor`.
Use `meta.vsn` when reading a dataframe column, not when specifying a filter.
The client forwards patterns to the server unchanged.

```python
import sage_data_client

frame = sage_data_client.query(
    start="-1h",
    tail=1,
    filter={"name": "env.raingauge.*", "vsn": "W02E"},
)
print(frame.groupby(["meta.vsn", "name"]).size() if not frame.empty else frame)
```

`head` and `tail` are per-series limits, not a global row count; supply only
one. Regex-style filter syntax and restrictions belong to the server. The
current Beehive API treats values containing `*` or `|` as anchored regex
patterns, not shell globs. A tiered service can reject head/tail or aggregation
across its hot/cold watermark. See [parameter reference](reference.md).

## Work with a dataframe

Columns begin with `timestamp`, `name`, and `value`. Metadata becomes
`meta.<key>` columns, which depend on the returned rows. Inspect their presence
before using an optional tag:

```python
import sage_data_client

frame = sage_data_client.load("tests/test-data.ndjson")
print(frame.head())
if "meta.vsn" in frame:
    print(frame["meta.vsn"].dropna().unique())

numeric = frame[frame["name"] == "env.temperature"]
if not numeric.empty and {"meta.vsn", "meta.sensor"}.issubset(numeric.columns):
    print(numeric.groupby(["meta.vsn", "meta.sensor"]).value.agg(["size", "min", "max", "mean"]))
```

This example runs from the repository root using its saved fixture, without
an API connection. Aggregate numeric series separately from string or mixed
measurements. Arrow and NDJSON can produce different pandas dtypes; consult
[formats](formats.md#dataframe-columns) before depending on a specific dtype.

## Load saved data

`load()` reads newline-delimited JSON, including `.gz` filenames. A `.json`
filename is also accepted if its contents are NDJSON rather than one JSON array.
String paths and `pathlib.Path` are supported:

```python
from pathlib import Path
import sage_data_client

plain = sage_data_client.load("tests/test-data.ndjson")
compressed = sage_data_client.load(Path("tests/test-data.ndjson.gz"))
print(len(plain), len(compressed))
```

You can pass an already-open line-oriented stream. The caller owns and closes
that stream:

```python
import sage_data_client

with open("tests/test-data.ndjson", "rb") as stream:
    frame = sage_data_client.load(stream)
    assert not stream.closed
print(len(frame))
```

A passed file object is not automatically decompressed. Open gzip content
through a gzip reader yourself, or pass its `.gz` filename for automatic
handling:

```python
import gzip
import sage_data_client

with gzip.open("tests/test-data.ndjson.gz", "rb") as stream:
    frame = sage_data_client.load(stream)
print(len(frame))
```

To read saved Arrow IPC, use PyArrow instead of `load()`; see
[saved Arrow streams](formats.md#saved-arrow-streams).

## Notebooks and additional examples

Dataframes work with normal pandas analysis and plotting. The repository has
[analysis scripts and notebooks](../examples), a
[plotting example](../examples/plotting_example.ipynb), and
[contributed examples](../examples/contrib). Those examples can require extra
plotting dependencies and their own reachable data services.
