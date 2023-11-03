"""
This is an example of a simple edge-to-cloud stream trigger which uses sage-data-client
to watch the latest internal temperature values and print records which exceed a threshold.

Although it's simple, this example could easily be extended in multiple ways. For example:
* Instead of just printing, an alert could be posted to Slack.
* Instead of a fixed threshold, you could learn a moving average per node and flag outliers.

Note: In the future, this kind of streaming functionality *might* be provided by sage-data-client,
but for now you can adapt this example to fit you use case.
"""
import sage_data_client
import pandas as pd
import time


def watch(start=None, filter=None):
    if start is None:
        start = pd.Timestamp.utcnow()

    while True:
        df = sage_data_client.query(
            start=start,
            filter=filter,
        )

        if len(df) > 0:
            start = df.timestamp.max()
            yield df

        time.sleep(3.0)


def main():
    filter = {
        "name": "env.temperature",
        "sensor": "bme280",
    }

    threshold = 50.0

    for df in watch(filter=filter):
        # print values which exceed threshold
        print(df[df.value > threshold].sort_values("timestamp"))


if __name__ == "__main__":
    main()
