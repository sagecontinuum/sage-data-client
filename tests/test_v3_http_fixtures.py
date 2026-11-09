"""Decode actual beehive-data-api v3 HTTP fixtures through the public client.

Set COLD_V3_HTTP_FIXTURE_DIR to the fresh export from TestIcebergV3RoundTrip.
Only transport is replayed; bodies come from the API's real Iceberg readers
and HTTP writers. No synthetic replacement of the client decoders.
"""
from email.message import Message
import importlib
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import pandas as pd

query=importlib.import_module('sage_data_client.query')


class Response(io.BytesIO):
    def __init__(self,body,headers):
        super().__init__(body)
        self.headers=Message()
        for key,value in headers.items():self.headers[key]=value


@unittest.skipUnless(os.environ.get('COLD_V3_HTTP_FIXTURE_DIR'),'requires native API Iceberg v3 HTTP fixture export')
class TestV3HTTPFixtures(unittest.TestCase):
    def test_all_native_v3_http_responses_decode_through_public_client(self):
        root=Path(os.environ['COLD_V3_HTTP_FIXTURE_DIR'])
        expected={case+'-'+fmt for case in ('cold','head','tail','exclusive_ns_bound','tiered') for fmt in ('arrow','ndjson')}
        self.assertEqual({p.stem for p in root.glob('*.json')},expected)
        for stem in sorted(expected):
            with self.subTest(response=stem):
                manifest=json.loads((root/(stem+'.json')).read_text())
                response=Response((root/(stem+'.body')).read_bytes(),manifest['headers'])
                with patch.object(query,'urlopen',return_value=response):
                    frame=query.query(start='2025-03-01T00:00:07.420753193Z',end='2025-03-01T00:00:08.000000003Z',endpoint='http://fixture/api/v1/query')
                self.assertEqual(len(frame),len(manifest['values']))
                self.assertEqual([stamp.value for stamp in frame.timestamp],manifest['timestamps_ns'])
                self.assertEqual(str(frame.timestamp.dt.tz),'UTC')
                self.assertEqual(frame.name.tolist(),manifest['names'])
                values=[json.loads(text) for text in manifest['values']]
                self.assertEqual(frame.value.tolist(),values)
                for actual,want in zip(frame.value,values):
                    # bool, numeric-looking strings and >2^53 ints must stay distinct.
                    self.assertEqual(type(actual),type(want))
                wanted_columns={'timestamp','name','value'}|{'meta.'+key for meta in manifest['metadata'] for key in meta}
                self.assertEqual(set(frame),wanted_columns)
                for i,meta in enumerate(manifest['metadata']):
                    for column in wanted_columns-{'timestamp','name','value'}:
                        actual=frame[column].iloc[i]
                        if column[5:] in meta:self.assertEqual(actual,meta[column[5:]])
                        else:self.assertTrue(pd.isna(actual))


if __name__=='__main__':unittest.main()
