"""
This example is a skeleton of how to poll the data system every minute for unusual
pressure events.

In this case, events are determined windows with a stddev above an example
threshold. For applications, you will need to provide your own criteria for
events.
"""
import sage_data_client
import time

while True:
    # query recent pressure data
    df = sage_data_client.query(
        start="-10m",
        filter={
            "name": "env.pressure",
            "sensor": "bme680",
        }
    )

    # compute stddev for nodes' pressure data in window
    std = df.groupby("meta.vsn").value.std()

    # find all events events exceeding an example threshold
    events = std[std > 8.0]

    # "post" vsn to alert system
    for vsn in events.index:
        print(f"post {vsn} to alert system")

    time.sleep(60)
