"""
This example shows how to implement a simple batch trigger and prints all temperature
events above a threshold.
"""
import sage_data_client
import time


def main():
    filter = {
        "name": "env.temperature",
        "sensor": "bme280",
    }

    while True:
        # get the last 5m of temperature data
        df = sage_data_client.query(start="-5m", filter=filter)

        # get mean temperature by node in batch query
        mean_temps = df.groupby("meta.vsn").value.mean()

        # print values which exceed threshold
        print(mean_temps[mean_temps > 55.0])

        # wait 5m
        time.sleep(300)


if __name__ == "__main__":
    main()
