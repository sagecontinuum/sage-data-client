"""HTTP query construction, timestamp bounds, and response negotiation."""

from gzip import GzipFile
import json
from typing import Dict, Optional
from urllib.request import Request, urlopen

import pandas as pd

from ._arrow import _load_arrow
from ._ndjson import load


def resolve_time(t):
    """Resolve absolute or relative input using the existing pandas semantics."""
    now = pd.to_datetime("now", utc=True)
    # Explicitly check if string times start with +/- to address change in Pandas 3 behavior.
    #  Before: pd.to_datetime("-2h") raised an exception
    #  After:  pd.to_datetime("-2h") returns Timestamp('1-01-01 02:00:00')
    if isinstance(t, str) and (t.startswith("+") or t.startswith("-")):
        return now + pd.to_timedelta(t)
    try:
        return pd.to_datetime(t)
    except (TypeError, ValueError):
        pass
    return now + pd.to_timedelta(t)


def timestr(t):
    """Format a query bound as a UTC ISO timestamp at its existing precision."""
    stamp = pd.Timestamp(t)
    stamp = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
    return stamp.isoformat().replace("+00:00", "Z")


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
    headers = {
        "Accept": "application/vnd.apache.arrow.stream, application/x-ndjson;q=0.1",
        "Accept-Encoding": "gzip",
        "Content-Type": "application/json",
    }
    req = Request(endpoint, data, headers=headers)

    with urlopen(req) as response:
        media = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        loader = _load_arrow if media == "application/vnd.apache.arrow.stream" else load
        encoding = response.headers.get("Content-Encoding", "").lower()
        if "gzip" in encoding:
            with GzipFile(fileobj=response, mode="rb") as body:
                return loader(body)
        return loader(response)
