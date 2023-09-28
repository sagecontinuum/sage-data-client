import unittest
import sage_data_client
from io import BytesIO
from datetime import datetime, timedelta
import pandas as pd


class TestQuery(unittest.TestCase):
    def assertValueResponse(self, df):
        self.assertIn("name", df.columns)
        df.name.str
        self.assertIn("timestamp", df.columns)
        df.timestamp.dt
        self.assertIn("value", df.columns)

    def test_empty_response(self):
        self.assertValueResponse(
            sage_data_client.query(
                start="-2000d",
                filter={
                    "name": "should.not.every.exist.XYZ",
                },
            )
        )

    def test_check_one_of_head_or_tail(self):
        with self.assertRaises(ValueError):
            sage_data_client.query(
                start="2021-01-01T10:30:00",
                end="2021-01-01T10:31:00",
                head=3,
                tail=3,
                filter={
                    "name": "env.temperature",
                },
            )

    def test_queries(self):
        self.assertValueResponse(
            sage_data_client.query(
                start="2021-01-01T10:30:00",
                end="2021-01-01T10:31:00",
                filter={
                    "name": "env.temperature",
                },
            )
        )

        self.assertValueResponse(
            sage_data_client.query(
                start="2021-01-01T10:30:00Z",
                end="2021-01-01T10:31:00Z",
                filter={
                    "name": "env.temperature",
                },
            )
        )

        self.assertValueResponse(
            sage_data_client.query(
                start="2021-01-01T10:30:00.123Z",
                end="2021-01-01T10:31:00.123Z",
                filter={
                    "name": "env.temperature",
                },
            )
        )

        self.assertValueResponse(
            sage_data_client.query(
                start="2021-01-01T10:30:00.123456Z",
                end="2021-01-01T10:31:00.123456Z",
                filter={
                    "name": "env.temperature",
                },
            )
        )

        self.assertValueResponse(
            sage_data_client.query(
                start="2021-01-01 10:30:00",
                end="2021-01-01 10:31:00",
                filter={
                    "name": "env.temperature",
                },
            )
        )

        self.assertValueResponse(
            sage_data_client.query(
                start=datetime(2021, 1, 1, 10, 31, 0),
                end=datetime(2021, 1, 1, 10, 32, 0),
                filter={
                    "name": "env.temperature",
                },
            )
        )

        self.assertValueResponse(
            sage_data_client.query(
                start=pd.to_datetime("2021-01-01 10:30:00"),
                end=pd.to_datetime("2021-01-01 10:31:00"),
                filter={
                    "name": "env.temperature",
                },
            )
        )

        for dt in ["-30s", "-3m", "-3min", "-1d", "-1w"]:
            self.assertValueResponse(
                sage_data_client.query(
                    start=dt,
                    tail=1,
                    filter={
                        "name": "env.temperature",
                    },
                )
            )

        self.assertValueResponse(
            sage_data_client.query(
                start="-4h",
                end="-2h",
                tail=1,
                filter={
                    "name": "env.temperature",
                },
            )
        )

        for dt in [
            timedelta(seconds=-30),
            timedelta(minutes=-1),
            timedelta(hours=-1),
            timedelta(days=-1),
        ]:
            self.assertValueResponse(
                sage_data_client.query(
                    start=dt,
                    tail=1,
                    filter={
                        "name": "env.temperature",
                    },
                )
            )

    def test_load(self):
        sample_data = BytesIO(
            b"""{"timestamp":"2021-10-14T21:42:21.149425156Z","name":"env.temperature","value":21.74,"meta":{"host":"0000dca632a3069f.ws-rpi","job":"sage","node":"000048b02d15c31f","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-rpi","vsn":"W01C"}}
{"timestamp":"2021-10-14T21:42:09.201150729Z","name":"env.temperature","value":26.09,"meta":{"host":"0000dca632a306d8.ws-rpi","job":"sage","node":"000048b02d15c31a","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-shield","vsn":"W01A"}}
{"timestamp":"2021-10-14T21:42:19.087014343Z","name":"env.temperature","value":28.14,"meta":{"host":"0000dca632a3074d.ws-rpi","job":"sage","node":"000048b02d15bc73","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-rpi","vsn":"W024"}}
{"timestamp":"2021-10-14T21:42:23.475857326Z","name":"env.temperature","value":28.16,"meta":{"host":"0000dca632a3076b.ws-rpi","job":"sage","node":"000048b02d15bc6d","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-rpi","vsn":"W01B"}}
{"timestamp":"2021-10-14T21:42:34.995766556Z","name":"env.temperature","value":33.27,"meta":{"host":"0000dca632a3078f.ws-rpi","job":"sage","node":"000048b02d15bdc7","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-rpi","vsn":"W020"}}
{"timestamp":"2021-10-14T21:42:09.803472584Z","name":"env.temperature","value":9.8,"meta":{"host":"0000dca632a30792.ws-rpi","job":"sage","node":"000048b02d15bc42","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-rpi","vsn":"W01D"}}
{"timestamp":"2021-10-14T21:42:30.9261079Z","name":"env.temperature","value":25.63,"meta":{"host":"0000dca632a307b6.ws-rpi","job":"sage","node":"000048b02d15bc8c","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-rpi","vsn":"W039"}}
{"timestamp":"2021-10-14T21:42:24.228048661Z","name":"env.temperature","value":23.96,"meta":{"host":"0000dca632a307bf.ws-rpi","job":"sage","node":"000048b02d15bc7d","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-rpi","vsn":"W028"}}
{"timestamp":"2021-10-14T21:42:13.914329997Z","name":"env.temperature","value":21.84,"meta":{"host":"0000dca632a307e6.ws-rpi","job":"sage","node":"000048b02d05a1c2","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-rpi","vsn":"W02C"}}
{"timestamp":"2021-10-14T21:42:17.300924641Z","name":"env.temperature","value":30.12,"meta":{"host":"0000dca632a307fb.ws-rpi","job":"sage","node":"000048b02d15c328","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-rpi","vsn":"W016"}}
"""
        )
        df = sage_data_client.load(sample_data)
        self.assertEqual(len(df), 10)
        self.assertValueResponse(df)

    def test_load_small_numbers(self):
        sample_data = BytesIO(
            b"""{"timestamp":"2023-09-25T19:26:26.18944512Z","name":"sensor_body_temperature","value":-655230609742226300000,"meta":{"applicationId":"ac81e18b-1925-47f9-839a-27d999a8af55","applicationName":"ATMOS test app","devAddr":"00f06b4b","devEui":"98208e0000032a15","deviceName":"MFR Node","deviceProfileId":"cf2aec2f-03e1-4a60-a32c-0faeef5730d8","deviceProfileName":"MFR node","host":"0000e45f014caee8.ws-rpi","node":"000048b02d15bc8c","plugin":"registry.sagecontinuum.org/flozano/lorawan-listener:0.0.6","task":"lorawan-listener","tenantId":"52f14cd4-c6f1-4fbd-8f87-4025e1d49242","tenantName":"ChirpStack","vsn":"W039","zone":"shield"}}
"""
        )
        df = sage_data_client.load(sample_data)
        self.assertValueResponse(df)
        self.assertEqual(len(df), 1)

    def test_load_empty_file(self):
        df = sage_data_client.load("tests/test-empty.ndjson")
        self.assertEqual(len(df), 0)
        self.assertValueResponse(df)

        df = sage_data_client.load("tests/test-empty.ndjson.gz")
        self.assertEqual(len(df), 0)
        self.assertValueResponse(df)

    def test_load_compressed_file(self):
        df1 = sage_data_client.load("tests/test-data.ndjson")
        df2 = sage_data_client.load("tests/test-data.ndjson.gz")
        self.assertEqual(len(df1), 1611)
        self.assertEqual(len(df2), 1611)
        # NOTE In Pandas Nones and NaNs do not equal themselves, so will fill them to make df1 == df2 work.
        self.assertTrue((df1.fillna("") == df2.fillna("")).all().all())

    def test_mixed_types(self):
        sample_data = BytesIO(
            b"""{"timestamp":"2021-10-14T21:42:21.149425156Z","name":"test","value":21.74,"meta":{"host":"0000dca632a3069f.ws-rpi","job":"sage","node":"000048b02d15c31f","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-rpi","vsn":"W01C"}}
{"timestamp":"2021-10-14T21:42:09.201150729Z","name":"test","value":"26.09","meta":{"host":"0000dca632a306d8.ws-rpi","job":"sage","node":"000048b02d15c31a","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-shield","vsn":"W01A"}}
{"timestamp":"2021-10-14T21:42:09.201150729Z","name":"test","value":123,"meta":{"host":"0000dca632a306d8.ws-rpi","job":"sage","node":"000048b02d15c31a","plugin":"plugin-iio:0.4.5","sensor":"bme680","task":"iio-shield","vsn":"W01A"}}
"""
        )
        df = sage_data_client.load(sample_data)
        self.assertEqual(len(df), 3)
        self.assertValueResponse(df)
        self.assertAlmostEqual(df.iloc[0].value, 21.74)
        self.assertEqual(df.iloc[1].value, "26.09")
        self.assertEqual(df.iloc[2].value, 123)


if __name__ == "__main__":
    unittest.main()
