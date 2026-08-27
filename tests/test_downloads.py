import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from sage_data_client.downloads import (
    DEFAULT_LAYOUT,
    DownloadError,
    DownloadRecord,
    DownloadResponse,
    _download_with_retry,
    load_downloads,
    query_downloads,
)

# Fake values... not using real node VSNs or node IDs in tests.
TEST_VSN = "WTEST"
TEST_VSN_2 = "WTEST2"
TEST_NODE = "000000000000test"
TEST_URL = "https://example.com/data/sample.jpg"
TEST_TASK = "test-sampler"
TEST_CAMERA = "test-camera"


def _make_row(url=TEST_URL, vsn=TEST_VSN, filename="sample.jpg", node=TEST_NODE, **extra_meta):
    meta = {"vsn": vsn, "filename": filename, "node": node, **extra_meta}
    row = {"value": url, "timestamp": pd.Timestamp("2024-06-01 12:00:00", tz="UTC")}
    for k, v in meta.items():
        row["meta." + k] = v
    return row


class TestDownloadRecord(unittest.TestCase):
    def setUp(self):
        self.row = _make_row(camera=TEST_CAMERA, task=TEST_TASK)
        self.record = DownloadRecord(self.row)

    def test_basic_fields(self):
        self.assertEqual(self.record.vsn, TEST_VSN)
        self.assertEqual(self.record.filename, "sample.jpg")
        self.assertEqual(self.record.node, TEST_NODE)
        self.assertEqual(self.record.url, TEST_URL)

    def test_layout_vars_standard_keys(self):
        v = self.record.layout_vars()
        self.assertEqual(v["vsn"], TEST_VSN)
        self.assertEqual(v["filename"], "sample.jpg")
        self.assertEqual(v["date"], "2024-06-01")
        self.assertIn("datetime", v)

    def test_layout_vars_includes_meta_fields(self):
        v = self.record.layout_vars()
        self.assertEqual(v["camera"], TEST_CAMERA)
        self.assertEqual(v["task"], TEST_TASK)

    def test_path_for_default_layout(self):
        path = self.record.path_for(dest="/data")
        self.assertEqual(path, Path("/data") / TEST_VSN / "sample.jpg")

    def test_path_for_custom_layout(self):
        path = self.record.path_for(dest="/data", layout="{date}/{vsn}/{filename}")
        self.assertEqual(path, Path("/data/2024-06-01") / TEST_VSN / "sample.jpg")

    def test_path_for_unknown_key_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self.record.path_for(layout="{does_not_exist}/{filename}")
        self.assertIn("does_not_exist", str(ctx.exception))
        self.assertIn("Available:", str(ctx.exception))

    def test_download_skips_existing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / TEST_VSN / "sample.jpg"
            path.parent.mkdir()
            path.write_bytes(b"existing")

            with patch("sage_data_client.downloads._download_with_retry") as mock_dl:
                result = self.record.download(dest=tmp, skip_existing=True)
                mock_dl.assert_not_called()
            self.assertEqual(result, path)

    def test_download_calls_retry_fn(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch("sage_data_client.downloads._download_with_retry") as mock_dl:
                self.record.download(dest=tmp, skip_existing=False, credentials=("user", "tok"))
                mock_dl.assert_called_once()
                call_args = mock_dl.call_args
                self.assertEqual(call_args[0][0], self.record.url)
                self.assertEqual(call_args[1]["credentials"], ("user", "tok"))


class TestDownloadResponse(unittest.TestCase):
    def setUp(self):
        rows = [
            _make_row(vsn=TEST_VSN, filename="a.jpg"),
            _make_row(vsn=TEST_VSN_2, filename="b.jpg"),
        ]
        self.df = pd.DataFrame(rows)
        self.resp = DownloadResponse(self.df)

    def test_len(self):
        self.assertEqual(len(self.resp), 2)

    def test_iter(self):
        records = list(self.resp)
        self.assertEqual(len(records), 2)
        self.assertIsInstance(records[0], DownloadRecord)

    def test_df_property(self):
        self.assertIs(self.resp.df, self.df)

    def test_repr(self):
        self.assertIn("2", repr(self.resp))

    def test_download_all_calls_download_per_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(DownloadRecord, "download", return_value=Path(tmp) / "x") as mock_dl:
                self.resp.download_all(dest=tmp)
                self.assertEqual(mock_dl.call_count, 2)

    def test_download_all_raises_on_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(DownloadRecord, "download", side_effect=Exception("network error")):
                with self.assertRaises(DownloadError) as ctx:
                    self.resp.download_all(dest=tmp)
                self.assertIn("2 of 2", str(ctx.exception))
                self.assertEqual(len(ctx.exception.errors), 2)


class TestRetry(unittest.TestCase):
    def test_succeeds_on_first_try(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "file.jpg"
            fake_resp = io.BytesIO(b"data")
            fake_resp.read = MagicMock(side_effect=[b"data", b""])
            ctx_mgr = MagicMock()
            ctx_mgr.__enter__ = MagicMock(return_value=fake_resp)
            ctx_mgr.__exit__ = MagicMock(return_value=False)

            with patch("sage_data_client.downloads.urlopen", return_value=ctx_mgr):
                _download_with_retry("http://example.com/file.jpg", dest)
            self.assertTrue(dest.exists())

    def test_retries_on_url_error(self):
        from urllib.error import URLError

        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "file.jpg"
            fake_resp = io.BytesIO(b"data")
            fake_resp.read = MagicMock(side_effect=[b"data", b""])
            success_ctx = MagicMock()
            success_ctx.__enter__ = MagicMock(return_value=fake_resp)
            success_ctx.__exit__ = MagicMock(return_value=False)

            call_count = {"n": 0}

            def side_effect(req):
                call_count["n"] += 1
                if call_count["n"] < 3:
                    raise URLError("connection refused")
                return success_ctx

            with patch("sage_data_client.downloads.urlopen", side_effect=side_effect):
                with patch("sage_data_client.downloads.time.sleep"):
                    _download_with_retry("http://example.com/file.jpg", dest, max_retries=5)

            self.assertEqual(call_count["n"], 3)

    def test_raises_after_max_retries(self):
        from urllib.error import URLError

        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "file.jpg"

            with patch("sage_data_client.downloads.urlopen", side_effect=URLError("timeout")):
                with patch("sage_data_client.downloads.time.sleep"):
                    with self.assertRaises(URLError):
                        _download_with_retry("http://example.com/file.jpg", dest, max_retries=3)

    def test_no_retry_on_404(self):
        from urllib.error import HTTPError

        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "file.jpg"
            err = HTTPError("http://x", 404, "Not Found", {}, None)

            with patch("sage_data_client.downloads.urlopen", side_effect=err):
                with self.assertRaises(HTTPError) as ctx:
                    _download_with_retry("http://example.com/file.jpg", dest, max_retries=5)
                self.assertEqual(ctx.exception.code, 404)

    def test_no_retry_on_403(self):
        from urllib.error import HTTPError

        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "file.jpg"
            err = HTTPError("http://x", 403, "Forbidden", {}, None)

            with patch("sage_data_client.downloads.urlopen", side_effect=err):
                with patch("sage_data_client.downloads.time.sleep") as mock_sleep:
                    with self.assertRaises(HTTPError) as ctx:
                        _download_with_retry("http://example.com/file.jpg", dest, max_retries=5)
                    self.assertEqual(ctx.exception.code, 403)
                    mock_sleep.assert_not_called()

    def test_403_errors_attached_to_download_error(self):
        from urllib.error import HTTPError

        rows = [
            _make_row(vsn=TEST_VSN, filename="a.jpg"),
            _make_row(vsn=TEST_VSN, filename="b.jpg"),
            _make_row(vsn=TEST_VSN, filename="c.jpg"),
        ]
        resp = DownloadResponse(pd.DataFrame(rows))
        err = HTTPError("http://x", 403, "Forbidden", {}, None)

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(DownloadRecord, "download", side_effect=err):
                with self.assertRaises(DownloadError) as ctx:
                    resp.download_all(dest=tmp)

        download_err = ctx.exception
        self.assertEqual(len(download_err.errors), 3)
        vsns = {r.vsn for r, _ in download_err.errors}
        self.assertEqual(vsns, {TEST_VSN})
        for _, exc in download_err.errors:
            self.assertIsInstance(exc, HTTPError)
            self.assertEqual(exc.code, 403)

    def test_sends_basic_auth_header(self):
        import base64
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp) / "file.jpg"
            fake_resp = io.BytesIO(b"")
            fake_resp.read = MagicMock(return_value=b"")
            ctx_mgr = MagicMock()
            ctx_mgr.__enter__ = MagicMock(return_value=fake_resp)
            ctx_mgr.__exit__ = MagicMock(return_value=False)

            captured = {}

            def capture(req):
                captured["req"] = req
                return ctx_mgr

            with patch("sage_data_client.downloads.urlopen", side_effect=capture):
                _download_with_retry("http://example.com/file.jpg", dest, credentials=("user", "mytoken"))

            expected = "Basic " + base64.b64encode(b"user:mytoken").decode()
            self.assertEqual(captured["req"].get_header("Authorization"), expected)


class TestSaveLoad(unittest.TestCase):
    def setUp(self):
        rows = [
            _make_row(vsn=TEST_VSN, filename="a.jpg", task=TEST_TASK),
            _make_row(vsn=TEST_VSN_2, filename="b.jpg", task=TEST_TASK),
        ]
        self.resp = DownloadResponse(pd.DataFrame(rows))

    def test_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "downloads.csv"
            self.resp.save(str(path))
            self.assertTrue(path.exists())

            loaded = load_downloads(str(path))
            self.assertIsInstance(loaded, DownloadResponse)
            self.assertEqual(len(loaded), len(self.resp))

    def test_round_trip_preserves_urls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "downloads.csv"
            self.resp.save(str(path))
            loaded = load_downloads(str(path))
            original_urls = [r.url for r in self.resp]
            loaded_urls = [r.url for r in loaded]
            self.assertEqual(original_urls, loaded_urls)

    def test_round_trip_preserves_timestamps_as_utc(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "downloads.csv"
            self.resp.save(str(path))
            loaded = load_downloads(str(path))
            for record in loaded:
                self.assertIsNotNone(record.timestamp.tzinfo)

    def test_round_trip_preserves_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "downloads.csv"
            self.resp.save(str(path))
            loaded = load_downloads(str(path))
            records = list(loaded)
            self.assertEqual(records[0].vsn, TEST_VSN)
            self.assertEqual(records[1].vsn, TEST_VSN_2)

    def test_load_file_not_found(self):
        with self.assertRaises(FileNotFoundError) as ctx:
            load_downloads("/nonexistent/path/downloads.csv")
        self.assertIn("downloads.csv", str(ctx.exception))

    def test_load_missing_required_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.csv"
            pd.DataFrame({"col1": [1, 2]}).to_csv(path, index=False)
            with self.assertRaises(ValueError) as ctx:
                load_downloads(str(path))
            self.assertIn("missing required columns", str(ctx.exception))

    def test_load_bad_timestamp(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.csv"
            pd.DataFrame({"value": ["http://x"], "timestamp": ["not-a-date"]}).to_csv(path, index=False)
            with self.assertRaises(ValueError) as ctx:
                load_downloads(str(path))
            self.assertIn("timestamp", str(ctx.exception))

    def test_save_creates_parent_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "subdir" / "nested" / "downloads.csv"
            self.resp.save(str(path))
            self.assertTrue(path.exists())


class TestQueryDownloads(unittest.TestCase):
    def test_sets_upload_filter(self):
        empty_df = pd.DataFrame(
            {"value": [], "timestamp": pd.to_datetime([], utc=True)}
        )
        with patch("sage_data_client.downloads.query", return_value=empty_df) as mock_query:
            query_downloads(start="-1h", filter={"vsn": TEST_VSN})
            call_filter = mock_query.call_args[1]["filter"]
            self.assertEqual(call_filter["name"], "upload")
            self.assertEqual(call_filter["vsn"], TEST_VSN)

    def test_upload_filter_without_user_filter(self):
        empty_df = pd.DataFrame(
            {"value": [], "timestamp": pd.to_datetime([], utc=True)}
        )
        with patch("sage_data_client.downloads.query", return_value=empty_df) as mock_query:
            query_downloads(start="-1h")
            call_filter = mock_query.call_args[1]["filter"]
            self.assertEqual(call_filter, {"name": "upload"})

    def test_returns_download_response(self):
        empty_df = pd.DataFrame(
            {"value": [], "timestamp": pd.to_datetime([], utc=True)}
        )
        with patch("sage_data_client.downloads.query", return_value=empty_df):
            result = query_downloads(start="-1h")
            self.assertIsInstance(result, DownloadResponse)


if __name__ == "__main__":
    unittest.main()
