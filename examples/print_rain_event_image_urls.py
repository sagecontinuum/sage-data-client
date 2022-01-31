"""
This example demonstrates cross referencing rain gauge data to find rainy images. It outputs a list
of urls which can be saved and downloaded as follows:

python3 print_rain_event_image_urls.py > urls.txt
wget -r -N -i urls.txt
"""
import sage_data_client
import pandas as pd

vsn = "W039"

# query raingauge data for the last week
df = sage_data_client.query(
    start="2021-12-20",
    end="2021-12-27",
    filter={
        "name": "env.raingauge.acc",
        "vsn": vsn,
    }
)

# compute mean rain in hour window
mean_acc = df.resample("1h", on="timestamp").value.mean()

# find rain accumulation events
rain_events = mean_acc[mean_acc > 0]

# collect uploads in each rain event window
uploads = pd.concat(sage_data_client.query(
        start=ts,
        end=ts + pd.to_timedelta("1h"),
        filter={
            "name": "upload",
            "vsn": vsn,
            "task": "imagesampler-top",
        }
    ) for ts in rain_events.index)

# print all urls found
for url in uploads.value.values:
    print(url)
