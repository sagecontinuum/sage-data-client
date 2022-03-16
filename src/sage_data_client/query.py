from urllib.request import urlopen
import json
import pandas as pd


def resolve_time(t):
    try:
        return pd.to_datetime(t)
    except (TypeError, ValueError):
        pass
    return pd.to_datetime("now", utc=True) + pd.to_timedelta(t)


def timestr(t):
    return t.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def query(start, end = None, tail: int = None, bucket: str = None, filter: dict = None,
    endpoint: str = "https://data.sagecontinuum.org/api/v1/query") -> pd.DataFrame:
    """
    query makes a query request to the data API and returns the results in a data frame.

    Parameters
    ----------
    start : query start time, required
        Timestamps can be a relative like "-1h" or absolute like "2021-05-01T10:30:00Z".

    end : query end time, default: None
        Timestamps can be a relative like "-1h" or absolute like "2021-05-01T10:30:00Z".

    tail : limit query response to latest tail records, default: None

    bucket: name of bucket to query

    filter : dictionary of query filters, default: None

    endpoint : url of query api, default: "https://data.sagecontinuum.org/api/v1/query"

    Returns
    -------
    result : pandas.DataFrame
        The data frame will contain the query response records.

        See the Returns section for the `load` function for more details.

    Examples
    --------

    Querying and perform simple data aggregation

    ```python
    import sage_data_client

    # query and load data into pandas data frame
    df = sage_data_client.query(
        start="-1h",
        filter={
            "name": "env.temperature",
        }
    )

    # print results in data frame
    print(df)

    # meta columns are expanded into meta.fieldname. for example, here we print the unique nodes
    print(df["meta.node"].unique())

    # print stats of the temperature data grouped by node + sensor.
    print(df.groupby(["meta.node", "meta.sensor"]).value.agg(["size", "min", "max", "mean"]))
    ```
    """
    # build query
    q = {"start": timestr(resolve_time(start))}
    if end is not None:
        q["end"] = timestr(resolve_time(end))
    if filter is not None:
        q["filter"] = filter
    if tail is not None:
        q["tail"] = tail
    if bucket is not None:
        q["bucket"] = bucket

    request_body = json.dumps(q).encode()
    with urlopen(endpoint, request_body) as f:
        return load(f)


def load(path_or_buf) -> pd.DataFrame:
    """
    load reads a path or file like object containing a response from the data api and returns the results in a data frame.

    Parameters
    ----------
    path_or_buf : path like or file like object

    Returns
    -------
    result : pandas.DataFrame
        The data frame will contain the query response records. Standard columns names are:

        `name`: measurement name (ex. "env.temperature")

        `timestamp`: measurement timestamp (nanoseconds since epoch resolution)

        `value`: measurement value

        Metadata fields like "node" and "vsn" are stored in columns named "meta.node" or "meta.vsn".

    Examples
    --------

    Loading saved query results from a a file

    Suppose we've saved the results of a query to a file `data.json`. We can load them using the following:

    ```python
    import sage_data_client

    # load results from local file
    df = sage_data_client.load("data.json")

    # print number of results of each name
    print(df.groupby(["meta.node", "name"]).size())
    ```
    """
    df = pd.read_json(path_or_buf, lines=True, date_unit="ns", dtype={"name": str})

    # if dataframe is empty, return empty with known columns
    if len(df) == 0:
        return pd.DataFrame({
            "timestamp": pd.to_datetime([], utc=True),
            "name": pd.Series([], dtype=str),
            "value": [],
        })

    # ensure timestamp is in proper format
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # otherwise, normalize meta columns to meta.* columns
    meta = pd.DataFrame(df["meta"].tolist())
    meta.rename({c: "meta." + c for c in meta.columns}, axis="columns", inplace=True)
    df = df.join(meta)
    df.drop(columns=["meta"], inplace=True)
    return df
