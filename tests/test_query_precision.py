import importlib
import io
import json
import unittest
from unittest.mock import patch

query=importlib.import_module('sage_data_client.query')


class TestQueryPrecision(unittest.TestCase):
    def test_public_query_preserves_nanosecond_half_open_bounds(self):
        class Response(io.BytesIO):
            headers={'Content-Type':'application/x-ndjson'}
        with patch.object(query,'urlopen',return_value=Response(b'')) as opened:
            query.query(start='2025-03-01T00:00:00.026403194Z',end='2025-03-01T00:00:00.026403195Z')
        request=json.loads(opened.call_args.args[0].data)
        self.assertEqual(request['start'],'2025-03-01T00:00:00.026403194Z')
        self.assertEqual(request['end'],'2025-03-01T00:00:00.026403195Z')

    def test_offset_bounds_are_converted_to_utc_before_adding_z(self):
        self.assertEqual(query.timestr(query.resolve_time('2025-02-28T18:00:00.026403194-06:00')),
                         '2025-03-01T00:00:00.026403194Z')

    def test_naive_and_utc_bounds_preserve_the_instant(self):
        for fraction in ('', '.123456', '.000000001', '.026403194'):
            expected = '2025-03-01T00:00:00' + fraction + 'Z'
            for suffix in ('', 'Z'):
                with self.subTest(fraction=fraction, suffix=suffix):
                    raw = '2025-03-01T00:00:00' + fraction + suffix
                    self.assertEqual(query.timestr(query.resolve_time(raw)), expected)


if __name__=='__main__':unittest.main()
