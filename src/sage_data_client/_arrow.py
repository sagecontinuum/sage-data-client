"""Bounded Arrow IPC decoding for the public four-column response schema."""

import json

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

# Bound work even when the producer sends oversized IPC batches.
_ARROW_DECODE_ROWS = 65536


def _load_arrow(fileobj) -> pd.DataFrame:
    """Decode batches without retaining the complete IPC body or Python rows."""

    timestamps, names, values = [], [], []
    metadata = {}
    lengths = []
    with ipc.open_stream(fileobj) as reader:
        if "value" not in reader.schema.names:
            raise ValueError("Arrow response requires the public value column")
        for incoming in reader:
            for offset in range(0, incoming.num_rows, _ARROW_DECODE_ROWS):
                batch = incoming.slice(offset, _ARROW_DECODE_ROWS)
                size = batch.num_rows
                # IPC columns can share one message buffer. Copy these small
                # retained columns so they do not keep the original JSON body.
                timestamps.append(pa.concat_arrays([batch.column("timestamp")]))
                name = batch.column("name")
                if pa.types.is_dictionary(name.type):
                    name = name.dictionary_decode()
                names.append(pa.concat_arrays([name]))
                values.append(_arrow_values(batch))
                decoded = _meta_columns(batch.column("meta"))
                for key in decoded:
                    if key not in metadata:
                        metadata[key] = [pa.nulls(n, pa.string()) for n in lengths]
                for key, chunks in metadata.items():
                    chunks.append(decoded[key] if key in decoded else pa.nulls(size, pa.string()))
                lengths.append(size)
                del batch, name, decoded
            del incoming

    if not lengths:
        return pd.DataFrame(
            {"timestamp": pd.to_datetime([], utc=True),
             "name": pd.Series([], dtype=str), "value": []}
        )

    # Infer the scalar dtype only after seeing all batches. Per-batch coercion
    # could round an integer before a later string makes this a mixed column.
    scalars = values[0] if len(values) == 1 else np.concatenate(values)
    values.clear()
    scalar_series = pd.Series(scalars, copy=False).infer_objects()
    del scalars
    timestamp = pa.chunked_array(timestamps)
    timestamp = timestamp.cast(pa.timestamp(timestamp.type.unit, tz="UTC"))
    frame = {
        "timestamp": timestamp.to_pandas(types_mapper=pd.ArrowDtype),
        "name": pa.chunked_array(names).to_pandas(types_mapper=pd.ArrowDtype),
        "value": scalar_series,
    }
    frame.update({
        key: pa.chunked_array(chunks).to_pandas(types_mapper=pd.ArrowDtype)
        for key, chunks in metadata.items()
    })
    return pd.DataFrame(frame)


def _arrow_values(table):
    """Parse distinct scalar texts within each bounded batch, preserving types."""
    column = table.column("value")
    if isinstance(column, pa.ChunkedArray):
        # Also support local experiment callers passing a table.
        parts = [_decode_value_array(chunk.slice(i, _ARROW_DECODE_ROWS))
                 for chunk in column.chunks
                 for i in range(0, len(chunk), _ARROW_DECODE_ROWS)]
        return np.concatenate(parts) if parts else np.empty(0, dtype=object)
    return _decode_value_array(column)


def _decode_value_array(column):
    """Decode the value array for the arrow values."""
    if pa.types.is_dictionary(column.type):
        column = column.dictionary_decode()
    encoded = column.dictionary_encode()
    raw = encoded.dictionary.to_pylist()
    if not raw:
        return np.full(len(column), None, dtype=object)
    parsed = [json.loads(value) if value is not None else None for value in raw]
    dictionary = np.empty(len(parsed), dtype=object)
    dictionary[:] = parsed
    indices = encoded.indices.fill_null(0).to_numpy(zero_copy_only=False)
    values = dictionary[indices]
    if column.null_count:
        values[encoded.indices.is_null().to_numpy(zero_copy_only=False)] = None
    # Repeated JSON containers need independent objects rather than sharing
    # mutable values across rows.
    if any(isinstance(value, (dict, list)) for value in parsed):
        for i, value in enumerate(values):
            if isinstance(value, (dict, list)):
                values[i] = json.loads(raw[indices[i]])
    return values


def _meta_columns(column):
    """Expand one batch's tags into Arrow strings, decoding distinct JSON once.

    No cache spans batches: even all-unique metadata bounds Python objects to
    one batch. Keys retain first-seen order; absent and null tags become nulls.
    """
    if isinstance(column, pa.ChunkedArray):
        column = column.combine_chunks()
    if pa.types.is_dictionary(column.type):
        column = column.dictionary_decode()
    if not (pa.types.is_string(column.type) or pa.types.is_large_string(column.type)):
        raise ValueError("Arrow meta must contain JSON strings")
    encoded = column.dictionary_encode()
    parsed = [dict((json.loads(raw) or {})) for raw in encoded.dictionary.to_pylist()]
    keys = dict.fromkeys(key for row in parsed for key in row)
    return {
        "meta." + key: pc.take(pa.array([row.get(key) for row in parsed], type=pa.string()),
                              encoded.indices)
        for key in keys
    }
