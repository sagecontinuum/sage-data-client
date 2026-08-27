import stat
import unittest
from pathlib import Path
import tempfile

from sage_data_client.auth import load_credentials, load_token, save_credentials


class TestAuth(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.creds_path = Path(self.tmp.name) / ".sage" / "credentials"

    def tearDown(self):
        self.tmp.cleanup()

    def test_load_credentials_missing_file(self):
        self.assertEqual(load_credentials(self.creds_path), {})

    def test_save_and_load_credentials(self):
        save_credentials("jgers32", "mytoken123", path=self.creds_path)
        creds = load_credentials(self.creds_path)
        self.assertEqual(creds["username"], "jgers32")
        self.assertEqual(creds["token"], "mytoken123")

    def test_load_token(self):
        save_credentials("jgers32", "mytoken123", path=self.creds_path)
        self.assertEqual(load_token(self.creds_path), "mytoken123")

    def test_load_token_missing_file(self):
        self.assertIsNone(load_token(self.creds_path))

    def test_save_creates_parent_dirs(self):
        self.assertFalse(self.creds_path.parent.exists())
        save_credentials("user", "tok", path=self.creds_path)
        self.assertTrue(self.creds_path.exists())

    def test_save_sets_permissions(self):
        save_credentials("user", "tok", path=self.creds_path)
        mode = stat.S_IMODE(self.creds_path.stat().st_mode)
        self.assertEqual(mode, 0o600)

    def test_load_token_returns_none_when_empty(self):
        self.creds_path.parent.mkdir(parents=True)
        self.creds_path.write_text("username=user\ntoken=\n")
        self.assertIsNone(load_token(self.creds_path))


if __name__ == "__main__":
    unittest.main()
