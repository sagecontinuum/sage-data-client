from gzip import GzipFile
import json
from pathlib import Path
from urllib.request import urlopen, Request
import pandas as pd

# NOTE Using the deprecated type aliases to maintain compatibility with Python 3.6
from typing import Optional, Dict


def resolve_time(t):
    try:
        return pd.to_datetime(t)
    except (TypeError, ValueError):
        pass
    return pd.to_datetime("now", utc=True) + pd.to_timedelta(t)


def timestr(t):
    return t.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def query(
    start,
    end=None,
    head: Optional[int] = None,
    tail: Optional[int] = None,
    filter: Optional[Dict[str, str]] = None,
    endpoint: str = "https://data.sagecontinuum.org/api/v1/query",
    bucket: Optional[str] = None,
    experimental_func: Optional[str] = None,
    experimental_window: Optional[str] = None,
) -> pd.DataFrame:
    """
    query makes a query request to the data API and returns the results in a data frame.

    Parameters
    ----------
    start : query start time, required
        Timestamps can be a relative like "-1h" or absolute like "2021-05-01T10:30:00Z".

    end : query end time, default: None
        Timestamps can be a relative like "-1h" or absolute like "2021-05-01T10:30:00Z".

    head : limit query response to earliest `head` records, default: None (only one of `head` or `tail` can be provided)

    tail : limit query response to latest `tail` records, default: None (only one of `head` or `tail` can be provided)

    filter : dictionary of query filters, default: None

    endpoint : url of query api, default: "https://data.sagecontinuum.org/api/v1/query"

    bucket: name of bucket to query

    experimental_func : aggregation function to apply to series.

    experimental_window : aggregation window to apply function over.

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
    if head is not None and tail is not None:
        raise ValueError("only one of `head` or `tail` can be provided")
    elif head is not None:
        q["head"] = head
    elif tail is not None:
        q["tail"] = tail
    if experimental_func is not None:
        q["experimental_func"] = experimental_func
    if experimental_window is not None:
        q["experimental_window"] = experimental_window
    if bucket is not None:
        q["bucket"] = bucket

    data = json.dumps(q).encode()
    headers = {"Accept-Encoding": "gzip"}
    req = Request(endpoint, data, headers=headers)

    with urlopen(req) as f:
        content_encoding = f.headers.get("Content-Encoding", "")
        if "gzip" in content_encoding:
            f = GzipFile(fileobj=f, mode="rb")
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

    Loading saved query results from a file

    Suppose we've saved the results of a query to a file `data.json`. We can load them using the following:

    ```python
    import sage_data_client

    # load results from local file
    df = sage_data_client.load("data.ndjson")

    # print number of results of each name
    print(df.groupby(["meta.node", "name"]).size())
    ```
    """
    if isinstance(path_or_buf, str):
        if path_or_buf.endswith(".gz"):
            with GzipFile(path_or_buf, "rb") as f:
                return _load(f)
        else:
            with open(path_or_buf, "rb") as f:
                return _load(f)

    if isinstance(path_or_buf, Path):
        if path_or_buf.suffix == ".gz":
            with GzipFile(path_or_buf, "rb") as f:
                return _load(f)
        else:
            with open(path_or_buf, "rb") as f:
                return _load(f)

    return _load(path_or_buf)


def _load_row(r):
    input = json.loads(r)
    output = {}
    output["timestamp"] = pd.to_datetime(input["timestamp"], unit="ns", utc=True)
    output["name"] = input["name"]
    output["value"] = input["value"]
    for k, v in input["meta"].items():
        output[f"meta.{k}"] = v
    return output


def _load(fileobj) -> pd.DataFrame:
    df = pd.DataFrame(map(_load_row, fileobj))

    # if dataframe is empty, return empty with known columns
    if len(df) == 0:
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime([], utc=True),
                "name": pd.Series([], dtype=str),
                "value": [],
            }
        )

    return df
