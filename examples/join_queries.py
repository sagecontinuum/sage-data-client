import sage_data_client
import pandas as pd


def join_resampled_queries(start, end, window, filters):
    """
    join_resampled_queries joins resampled data for a set of filters together
    into a single data frame
    """
    return pd.DataFrame({
        name: sage_data_client.query(
            start=start,
            end=end,
            filter=filter,
        ).resample(window, on="timestamp").value.mean()
        for name, filter in filters.items()
    })


def main():
    start = "2022-01-10T00:00:00Z"
    end = "2022-01-11T00:00:00Z"
    vsn = "W023"

    df = join_resampled_queries(start, end, "30min", {
        "lat": {
            "name": "sys.gps.lat",
            "vsn": vsn,
        },
        "lon": {
            "name": "sys.gps.lon",
            "vsn": vsn,
        },
        "temperature": {
            "name": "env.temperature",
            "vsn": vsn,
            "sensor": "bme680"
        },
        "pressure": {
            "name": "env.pressure",
            "vsn": vsn,
            "sensor": "bme680"
        },
        "humidity": {
            "name": "env.relative_humidity",
            "vsn": vsn,
            "sensor": "bme680"
        },
    })

    # print out data for quick inspection
    print(df)

    # save data to csv
    df.to_csv("combined.csv")


if __name__ == "__main__":
    main()
