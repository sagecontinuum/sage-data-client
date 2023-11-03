"""
This is an example of a simple edge-to-cloud batch trigger which uses sage-data-client
to gather and aggregate internal temperature data every 5 minutes and prints all
nodes which exceed a threshold.

Although it's simple, this example could easily be extended in multiple ways. For example:
* Instead of just printing, an alert to be posted to Slack.
* Instead of a fixed threshold, the typical value across all nodes could be used to determine outliers.
"""
import sage_data_client
import time


def main():
    filter = {
        "name": "env.temperature",
        "sensor": "bme280",
    }

    threshold = 55.0

    while True:
        # get the last 5m of temperature data
        df = sage_data_client.query(start="-5m", filter=filter)

        # get mean temperature by node in batch query
        mean_temps = df.groupby("meta.vsn").value.mean()

        # print values which exceed threshold
        print(mean_temps[mean_temps > threshold])

        # wait 5m
        time.sleep(300)


if __name__ == "__main__":
    main()
