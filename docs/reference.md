# Public reference

[Documentation index](README.md) · [Examples](usage.md)

## query

Returns a complete `pandas.DataFrame`. Signature, with annotations omitted:

```text
sage_data_client.query(
    start,
    end=None,
    head=None,
    tail=None,
    filter=None,
    endpoint="https://data.sagecontinuum.org/api/v1/query",
    bucket=None,
    experimental_func=None,
    experimental_window=None,
)
```

| Parameter | Default | Behavior |
| --- | --- | --- |
| `start` | Required | Inclusive query start; resolved and serialized on the client |
| `end` | `None` | Exclusive end; omitted from the request when `None`, leaving the server's default |
| `head` | `None` | First N records per series; forwarded when supplied, including zero |
| `tail` | `None` | Last N records per series; forwarded when supplied, including zero |
| `filter` | `None` | Dictionary of measurement/tag patterns; sent unchanged when supplied |
| `endpoint` | `https://data.sagecontinuum.org/api/v1/query` | Full query URL |
| `bucket` | `None` | Optional bucket selection forwarded to the server |
| `experimental_func` | `None` | Optional server-side aggregation function |
| `experimental_window` | `None` | Optional server-side aggregation interval |

The client rejects simultaneous `head` and `tail`. It does not implement a
full server-side query validator: limit values, filter patterns, bucket access,
aggregation support and tier-boundary restrictions are validated by the
selected API. In the current Beehive API, bucket selection affects only hot
storage, cold aggregation is unsupported, and crossing head/tail or aggregation
requests are rejected. These restrictions are server behavior, not client
parameters for selecting a storage tier.

There is no public format, codec, timeout, retry, pagination or streaming
iterator argument. Arrow is requested by default, but the function still
returns one complete dataframe. See [response negotiation](formats.md#http-negotiation).

### Time inputs

The resolver accepts absolute timestamp strings, Python datetime objects,
pandas timestamps, relative strings such as `-30s`, `-5min`, and `-1h`, and
duration-like inputs such as `datetime.timedelta`. Parsing follows the installed
pandas version; prefer explicit units such as `min` for minutes.

Relative values are resolved against client time before an HTTP request is
sent. Start and end are resolved separately, not against a documented shared
clock snapshot. For reproducible ranges, supply explicit absolute bounds.

Naive inputs are interpreted as UTC. Timezone-aware values are converted to
UTC. The request uses ISO timestamps ending in `Z`, retaining existing
nanosecond precision without a fixed number of fractional digits. For example,
whole seconds may appear as `2025-03-01T00:00:00Z`, while a precise bound can be
`2025-03-01T00:00:00.026403194Z`.

### Exceptions and resource handling

- Invalid time inputs propagate pandas parsing/formatting exceptions.
- Supplying both limits raises `ValueError` before transport.
- Values that cannot be JSON-serialized propagate serialization errors.
- HTTP and network failures propagate urllib exceptions such as `HTTPError` and `URLError`.
- Malformed/truncated data and gzip/Arrow decoding errors propagate; the client does not return a successful partial dataframe for those decoding failures.

The HTTP response and any client-created gzip wrapper close on success or
failure. There is no automatic retry with NDJSON after an Arrow error.
A syntactically valid but incomplete server response can still be accepted;
completeness requires independent server/count/content validation.

## load

```text
sage_data_client.load(path_or_buf)
```

Returns a complete `pandas.DataFrame` from NDJSON. Supported inputs:

| Input | Behavior |
| --- | --- |
| String filename | Open locally; decompress when the name ends with `.gz` |
| `pathlib.Path` | Same filename handling |
| Line-oriented file-like object | Read its current position; caller retains ownership |

Paths are opened and closed by the loader. Supplied streams stay open, including
on errors. File-like gzip content needs an explicit gzip reader; the loader
does not sniff compression. Other filesystem-path abstractions are not part
of the documented input contract.

Each NDJSON line must contain `timestamp`, `name`, `value`, and a `meta` object.
Timestamps can be API RFC3339 strings; integer epoch timestamps are interpreted
in nanoseconds. Metadata keys are expanded into dataframe columns. Missing
required fields, malformed JSON, unsupported metadata and file I/O errors
propagate their underlying exceptions.

`load()` does not read JSON arrays, CSV, Parquet or saved Arrow IPC streams.
See the [formats guide](formats.md) for schemas and dataframe behavior.

Source: [public request facade](../src/sage_data_client/query.py) and
[NDJSON loader](../src/sage_data_client/_ndjson.py). Decoder modules and time
helpers are implementation details rather than additional public entrypoints.
