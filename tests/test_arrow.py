"""Public query transport checks without sockets or external services."""
import unittest
import gzip
import importlib
import io
import json
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import sage_data_client

query_module = importlib.import_module("sage_data_client.query")


def arrow_bytes(table, compression=None):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema,
                          options=pa.ipc.IpcWriteOptions(compression=compression)) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def sample_table():
    schema = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("ns")),
            pa.field("name", pa.string()),
            pa.field("value", pa.string()),
            pa.field("meta", pa.string()),
        ]
    )
    return pa.table(
        {
            "timestamp": pa.array(
                [pd.Timestamp("2025-03-01T00:00:00.026403Z"), pd.Timestamp("2025-03-01T00:00:01Z")],
                type=pa.timestamp("ns"),
            ),
            "name": ["wxt.env.temp", "sys.flag"],
            "value": ["11.7", "true"],
            "meta": [
                '{"plugin": "waggle-wxt536", "vsn": "W0A0"}',
                '{"vsn": "W0A0"}',
            ],
        },
        schema=schema,
    )


class Response(io.BytesIO):
    def __init__(self, body, content_type=None, encoding=None):
        super().__init__(body)
        self.headers = {}
        if content_type:
            self.headers["Content-Type"] = content_type
        if encoding:
            self.headers["Content-Encoding"] = encoding


class TestArrowQuery(unittest.TestCase):
    def endpoint(self, body, content_type, capture, encoding=None):
        response = Response(body, content_type, encoding)
        capture["response"] = response

        def open_response(request):
            capture["accept"] = request.get_header("Accept")
            capture["content_type"] = request.get_header("Content-type")
            return response

        mocked = patch.object(query_module, "urlopen", side_effect=open_response)
        mocked.start()
        self.addCleanup(mocked.stop)
        return "http://local.invalid/api/v1/query"

    def test_arrow_response_becomes_dataframe(self):
        capture = {}
        url = self.endpoint(arrow_bytes(sample_table()), "application/vnd.apache.arrow.stream", capture)
        df = sage_data_client.query(start="2025-03-01T00:00:00Z", endpoint=url)
        self.assertIn("application/vnd.apache.arrow.stream", capture["accept"])
        self.assertEqual(capture["content_type"], "application/json")
        self.assertTrue(capture["response"].closed)
        self.assertEqual(len(df), 2)
        self.assertAlmostEqual(df.iloc[0].value, 11.7)
        self.assertIs(df.iloc[1].value, True)
        self.assertEqual(df.iloc[0]["meta.vsn"], "W0A0")
        self.assertEqual(df.iloc[0]["meta.plugin"], "waggle-wxt536")
        self.assertTrue(pd.isna(df.iloc[1]["meta.plugin"]))
        self.assertEqual(df.iloc[0].timestamp, pd.Timestamp("2025-03-01T00:00:00.026403Z"))
        self.assertEqual(str(df.timestamp.dt.tz), "UTC")

    def test_empty_arrow_response(self):
        schema = sample_table().schema
        url = self.endpoint(arrow_bytes(schema.empty_table()), "application/vnd.apache.arrow.stream", {})
        df = sage_data_client.query(start="2025-03-01T00:00:00Z", endpoint=url)
        self.assertEqual(len(df), 0)
        self.assertIn("timestamp", df.columns)
        self.assertIn("name", df.columns)
        self.assertIn("value", df.columns)

    def test_json_response_still_loads(self):
        body = b'{"timestamp":"2025-03-01T00:00:00.026403Z","name":"wxt.env.temp","value":11.7,"meta":{"vsn":"W0A0"}}\n'
        url = self.endpoint(body, None, {})
        df = sage_data_client.query(start="2025-03-01T00:00:00Z", endpoint=url)
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df.iloc[0].value, 11.7)
        self.assertEqual(df.iloc[0]["meta.vsn"], "W0A0")

    def test_http_gzip_and_ipc_compression(self):
        table = sample_table()
        raw = arrow_bytes(table, compression="zstd")
        capture = {}
        url = self.endpoint(gzip.compress(raw),
                            "Application/Vnd.Apache.Arrow.Stream; charset=utf-8",
                            capture, "GZip")
        actual = sage_data_client.query(start="2025-03-01T00:00:00Z", endpoint=url)
        expected = query_module._load_arrow(pa.BufferReader(raw))
        pd.testing.assert_frame_equal(actual, expected)
        self.assertTrue(capture["response"].closed)

    def test_gzip_ndjson_retains_large_integers(self):
        raw = b'{"timestamp":"2025-03-01T00:00:00.026403194Z","name":"m","value":9007199254740993,"meta":{}}\n'
        capture = {}
        url = self.endpoint(gzip.compress(raw), "application/x-ndjson", capture, "gzip")
        frame = sage_data_client.query(start="2025-03-01T00:00:00Z", endpoint=url)
        self.assertEqual(frame.value.iloc[0], 9007199254740993)
        self.assertEqual(frame.timestamp.iloc[0].value, 1740787200026403194)
        self.assertTrue(capture["response"].closed)

    def test_response_closes_when_decoding_fails(self):
        for raw, media in [(b'{invalid\n', "application/x-ndjson"),
                           (b'not an IPC stream', "application/vnd.apache.arrow.stream")]:
            with self.subTest(media=media):
                capture = {}
                url = self.endpoint(gzip.compress(raw), media, capture, "gzip")
                with self.assertRaises((json.JSONDecodeError, pa.ArrowInvalid, OSError)):
                    sage_data_client.query(start="2025-03-01T00:00:00Z", endpoint=url)
                self.assertTrue(capture["response"].closed)


if __name__ == "__main__":
    unittest.main()
