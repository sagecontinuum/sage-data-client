"""
This example shows how to implement a simple data API watcher to stream events
which can be used to trigger events.

In the future, this kind of functionality *might* be provided by this module, but
for now you can adapt this example to fit you use case.
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

    for df in watch(filter=filter):
        # print values which exceed threshold
        print(df[df.value > 50.0].sort_values("timestamp"))


if __name__ == "__main__":
    main()
