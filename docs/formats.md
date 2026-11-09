# Formats and dataframe behavior

[Documentation index](README.md) · [Loading examples](usage.md#load-saved-data)

## HTTP negotiation

`query()` sends one JSON POST with these headers:

```text
Content-Type: application/json
Accept: application/vnd.apache.arrow.stream, application/x-ndjson;q=0.1
Accept-Encoding: gzip
```

Arrow is requested by default. The client normalizes the actual response's
`Content-Type`, ignoring case and parameters. It uses the Arrow decoder for
`application/vnd.apache.arrow.stream`; other media types, including a missing
header, use the NDJSON loader. This supports older APIs that return NDJSON.
It does not verify every other response type before attempting NDJSON.

There is no automatic second request when Arrow fails. An HTTP 406, transport
error or Arrow decoding failure propagates rather than triggering an NDJSON
retry. There is no public request-format override argument.

## Wire records

NDJSON contains one JSON record per line. For example:

```json
{"timestamp":"2025-03-01T00:00:00.026403194Z","name":"wxt.env.temp","value":11.7,"meta":{"vsn":"W0A0"}}
```

The timestamp in an API JSON record is an RFC3339 string. The file loader also
accepts integer epoch timestamps interpreted in nanoseconds. `value` is a JSON
value and `meta` is a tag object, not expanded dataframe columns on the wire.

The supported public Arrow IPC stream has:

| Field | Type | Interpretation |
| --- | --- | --- |
| `timestamp` | `timestamp[ns]` without a timezone annotation | UTC instant |
| `name` | UTF-8 string | Measurement name |
| `value` | UTF-8 string | JSON text containing the measurement value |
| `meta` | UTF-8 string | JSON text containing the metadata object |

Arrow schema fields permit nulls. Non-null value text `11.7` becomes a number,
while text `"image.jpg"` becomes a string. The client expands metadata after
JSON decoding. It does not expose a stored VARIANT or MAP directly as wire
columns, and does not support the retired split-value or MAP wire prototypes.

The current Beehive API uses the same public fields across hot, cold and
combined queries. The client itself does not determine which tier served a row.

## Compression

HTTP gzip and Arrow IPC compression are separate layers. The client advertises
HTTP gzip support and decompresses a gzip response wrapper before decoding
its contents. PyArrow handles IPC buffer compression, such as Zstd or LZ4,
inside the Arrow stream. Both layers can be present together.

The client does not choose an IPC codec or require one particular codec header.
That is a server setting. File loading chooses gzip only for paths ending in
`.gz`; a supplied file object must already provide decompressed lines.

## Dataframe columns

Both public functions return a complete pandas dataframe:

| Column | Behavior |
| --- | --- |
| `timestamp` | UTC-aware pandas datetime; nanosecond instants are retained when present |
| `name` | Measurement name |
| `value` | Values inferred across the full Arrow response or by the NDJSON dataframe constructor |
| `meta.<key>` | One column per returned metadata key |

For nonempty Arrow responses, timestamp, name and metadata columns use
Arrow-backed pandas arrays; metadata uses nullable Arrow strings. NDJSON
uses ordinary pandas construction, whose inferred dtypes depend on values and
pandas version. The two formats do not promise identical dtypes.

Missing tags become missing dataframe values. A metadata column can first
appear in a later batch; earlier rows are filled with nulls. Null/empty Arrow
metadata contributes no tag fields. NDJSON metadata is expected to be an
object. An empty result has `timestamp`, `name`, and `value`, with no invented
metadata columns; do not depend on its dtypes matching a nonempty result.

JSON integers are decoded without an initial float conversion. The final
pandas column can still infer a common numeric type when values are mixed;
inspect dtypes if exact mixed numeric types matter. There is no dtype override
parameter. Repeated JSON containers retain independent mutable values rather
than sharing the same list or dictionary across rows.

The client retains response order. It does not sort or deduplicate results,
and does not guarantee global timestamp order across arbitrary server series.

## Memory and batch decoding

The Arrow decoder reads the IPC stream batch by batch and bounds its Python
work to slices of 65,536 rows. Repeated value/metadata JSON is decoded once per
distinct text within each bounded slice. Value inference happens after all
batches so mixed values are handled consistently across batch boundaries.
Retained columns are copied away from shared IPC buffers where needed.

This reduces intermediate allocations but does not make the public function
a streaming dataframe iterator. Scalar arrays, retained columns and the final
result still consume memory proportional to the query. Choose a narrower
query when the complete dataframe is too large.

The current decoder implementation is in
[the private Arrow module](../src/sage_data_client/_arrow.py). Its helpers and
batch constant are not public tuning APIs.

## Saved Arrow streams

`load()` does not detect or load saved Arrow IPC. For an actual IPC stream
saved from an Arrow-enabled API, use PyArrow explicitly:

```python
import json
import pyarrow.ipc as ipc

with open("measurements.arrows", "rb") as stream:
    with ipc.open_stream(stream) as reader:
        for batch in reader:
            values = [json.loads(text) if text is not None else None
                      for text in batch.column("value").to_pylist()]
            metadata = [json.loads(text) if text is not None else None
                        for text in batch.column("meta").to_pylist()]
            print(batch.num_rows, values[:1], metadata[:1])
```

Use `open_stream`, not the random-access Arrow file reader. A filename suffix
alone does not establish the format. Calling `batch.to_pandas()` directly
retains the wire `value`/`meta` strings; it is not the same metadata-expanded
result as the client's public query function.
