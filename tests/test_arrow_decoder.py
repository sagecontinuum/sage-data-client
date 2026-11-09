"""Offline decoder and transport-contract checks; no sockets or services."""
import importlib
import io
import json
import math
import unittest
from unittest.mock import patch

import pandas as pd
import pyarrow as pa

query = importlib.import_module("sage_data_client.query")
decoder = importlib.import_module("sage_data_client._arrow")


def table(values, metadata=None):
    if metadata is None:
        metadata = ['{"vsn":"W0A0"}'] * len(values)
    return pa.table({
        "timestamp": pa.array(range(len(values)), type=pa.timestamp("ns")),
        "name": pa.array(["test"] * len(values), type=pa.string()),
        "value": pa.array([json.dumps(v) if v is not None else None for v in values], type=pa.string()),
        "meta": pa.array(metadata, type=pa.string()),
    })


def body(table, batch_size=None):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table, max_chunksize=batch_size)
    return sink.getvalue()


def decode(table, batch_size=None):
    return decoder._load_arrow(pa.BufferReader(body(table, batch_size)))


class TestArrowDecoder(unittest.TestCase):
    def test_scalar_inference_matches_original_whole_column(self):
        cases = [
            [1, 2], [1, None], [1.5, 2], [True, False], [True, 1],
            ["a", None], [2**64, 1.5], [2**63, None], [None, None],
            [2**63 + 1, 1.5, "mixed"], [2**64 - 1, 0],
            [-2**70, "large"], ["12", 12, True, 1.2, None],
            [float("nan"), float("inf"), -0.0],
            ["quoted\" and \\ slash", "line\n\tbreak", "中文🌡️"],
        ]
        for values in cases:
            with self.subTest(values=values):
                actual = decode(table(values), batch_size=1).value
                expected = pd.Series(values, name="value")
                pd.testing.assert_series_equal(actual, expected)
        self.assertTrue(math.copysign(1, decode(table([-0.0])).value.iloc[0]) < 0)

    def test_mutable_json_values_are_independent(self):
        for value in [[1, 2], {"a": "b"}]:
            actual = decode(table([value, value])).value
            self.assertEqual(actual.iloc[0], value)
            self.assertEqual(actual.iloc[1], value)
            self.assertIsNot(actual.iloc[0], actual.iloc[1])

    def test_compressed_ipc_matches_uncompressed(self):
        t = table([1, True, "🌡️", None, 2**63 + 1, 1.5],
                  ['{"a":"x"}', None, '{}', '{"late":"y"}', '{"a":null}', '{}'])
        expected = decode(t, batch_size=2)
        for codec in ["zstd", "lz4"]:
            with self.subTest(codec=codec):
                sink = pa.BufferOutputStream()
                with pa.ipc.new_stream(sink, t.schema,
                                      options=pa.ipc.IpcWriteOptions(compression=codec)) as writer:
                    writer.write_table(t, max_chunksize=2)
                actual = decoder._load_arrow(pa.BufferReader(sink.getvalue()))
                pd.testing.assert_frame_equal(actual, expected)

    def test_keys_appearing_late_missing_null_and_order(self):
        metadata = [None, "{}", '{"z":"first","a":"x"}',
                    '{"a":null,"later":"🌡️"}', "null", '{"z":"last"}']
        df = decode(table([1] * len(metadata), metadata), batch_size=2)
        self.assertEqual(list(df), ["timestamp", "name", "value", "meta.z", "meta.a", "meta.later"])
        self.assertEqual(df["meta.z"].dropna().tolist(), ["first", "last"])
        self.assertEqual(df["meta.later"].dropna().tolist(), ["🌡️"])
        self.assertTrue(pd.isna(df.loc[3, "meta.a"]))
        self.assertEqual(len(df), 6)
        self.assertEqual(df.groupby("meta.z").value.sum().to_dict(), {"first": 1, "last": 1})

    def test_high_cardinality_tags_and_oversized_batch(self):
        n = 10003
        metadata = [json.dumps({"id": str(i), "group": str(i % 3)}) for i in range(n)]
        with patch.object(decoder, "_ARROW_DECODE_ROWS", 127):
            df = decode(table(list(range(n)), metadata))
        self.assertEqual(df["meta.id"].tolist(), [str(i) for i in range(n)])
        self.assertEqual(df.value.tolist(), list(range(n)))
        self.assertEqual(str(df["meta.id"].dtype), "string[pyarrow]")

    def test_repeated_json_is_decoded_per_batch_not_per_row(self):
        with patch.object(decoder, "_ARROW_DECODE_ROWS", 32):
            with patch.object(decoder.json, "loads", wraps=json.loads) as loads:
                df = decode(table([1] * 1000))
        self.assertEqual(len(df), 1000)
        self.assertEqual(loads.call_count, 2 * math.ceil(1000 / 32))

    def test_dictionary_encoded_wire_columns_ignore_unused_entries(self):
        t = table([1, 2])
        for name in ["name", "value"]:
            i = t.schema.get_field_index(name)
            t = t.set_column(i, name, t[name].dictionary_encode())
        metadata = pa.DictionaryArray.from_arrays(pa.array([1, 2], type=pa.int8()),
            pa.array(['{"unused":"x"}', '{"z":"one"}', '{"a":"two"}']))
        t = t.set_column(3, "meta", metadata)
        df = decode(t)
        self.assertEqual(list(df), ["timestamp", "name", "value", "meta.z", "meta.a"])
        self.assertEqual(df.value.tolist(), [1, 2])
        self.assertEqual(df["meta.z"].dropna().tolist(), ["one"])

    def test_rejects_obsolete_map_wire_metadata(self):
        t = table([1])
        meta = pa.array([[('x', 'a')]], type=pa.map_(pa.string(), pa.string()))
        with self.assertRaisesRegex(ValueError, "meta must contain JSON strings"):
            decode(t.set_column(3, "meta", meta))

    def test_rejects_missing_public_value_column(self):
        with self.assertRaisesRegex(ValueError, "public value column"):
            decode(table([1]).drop(["value"]))

    def test_timestamp_precision_null_and_timezone(self):
        for zone in [None, "UTC", "America/Chicago"]:
            with self.subTest(zone=zone):
                t = table([1, 2])
                times = pa.array([1735689600000000001, None], type=pa.timestamp("ns", tz=zone))
                df = decode(t.set_column(0, "timestamp", times), batch_size=1)
                self.assertEqual(df.timestamp.iloc[0], pd.Timestamp("2025-01-01T00:00:00.000000001Z"))
                self.assertTrue(pd.isna(df.timestamp.iloc[1]))
                self.assertEqual(str(df.timestamp.dt.tz), "UTC")

    def test_empty_stream_and_zero_row_batch(self):
        empty = table([])
        for write_empty_batch in [False, True]:
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, empty.schema) as writer:
                if write_empty_batch:
                    writer.write_batch(pa.RecordBatch.from_arrays(
                        [column.combine_chunks() for column in empty.columns], schema=empty.schema))
            df = decoder._load_arrow(pa.BufferReader(sink.getvalue()))
            self.assertEqual(list(df), ["timestamp", "name", "value"])
            self.assertEqual(len(df), 0)
            self.assertEqual(str(df.timestamp.dt.tz), "UTC")

    def test_bad_json_and_truncated_batch_raise(self):
        t = table([1])
        for col in ["value", "meta"]:
            with self.subTest(col=col):
                bad = t.set_column(t.schema.get_field_index(col), col, pa.array(["{broken"]))
                with self.assertRaises(json.JSONDecodeError):
                    decode(bad)
        raw = body(table(list(range(100)))).to_pybytes()
        with self.assertRaises((pa.ArrowInvalid, OSError)):
            decoder._load_arrow(io.BytesIO(raw[:-100]))

    def test_streams_batches_and_copies_retained_ipc_columns(self):
        batch = table([1, 2]).to_batches()[0]

        class Reader:
            schema = batch.schema
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def __iter__(self): return iter([batch])
            def read_all(self): raise AssertionError("whole stream was retained")

        with patch("pyarrow.ipc.open_stream", return_value=Reader()):
            df = decoder._load_arrow(None)
        for name, buffer in [("timestamp", 1), ("name", 2)]:
            output = df[name].array.__arrow_array__().chunk(0)
            self.assertNotEqual(output.buffers()[buffer].address, batch.column(name).buffers()[buffer].address)

    def test_query_transport_negotiates_and_handles_arrow_and_json(self):
        class Response(io.BytesIO):
            def __init__(self, data, mime):
                super().__init__(data)
                self.headers = {"Content-Type": mime}

        for mime, raw in [
            ("application/vnd.apache.arrow.stream", body(table([1])).to_pybytes()),
            ("application/x-ndjson", b'{"timestamp":"2025-01-01T00:00:00Z","name":"test","value":1,"meta":{}}\n'),
        ]:
            with self.subTest(mime=mime):
                with patch.object(query, "urlopen", return_value=Response(raw, mime)) as opened:
                    df = query.query("2025-01-01T00:00:00Z", endpoint="http://local.invalid/query")
                self.assertEqual(df.value.tolist(), [1])
                self.assertIn("application/vnd.apache.arrow.stream", opened.call_args.args[0].get_header("Accept"))


if __name__ == "__main__":
    unittest.main()
