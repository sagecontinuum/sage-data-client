import unittest
import sage_data_client


class TestQuery(unittest.TestCase):

    def test_query(self):
        df = sage_data_client.query(start="-1h", filter={
            "name": "env.temperature",
        })
        self.assertIn("name", df.columns)
        self.assertIn("timestamp", df.columns)
        self.assertIn("value", df.columns)


if __name__ == "__main__":
    unittest.main()
