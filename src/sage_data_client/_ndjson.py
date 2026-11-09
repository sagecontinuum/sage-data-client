"""Load NDJSON responses and saved plain/gzip measurement files."""

from gzip import GzipFile
import json
from pathlib import Path

import pandas as pd


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

        `timestamp`: measurement timestamp as a UTC pandas datetime

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
    if isinstance(path_or_buf, (str, Path)):
        opener = GzipFile if str(path_or_buf).endswith(".gz") else open
        with opener(path_or_buf, "rb") as stream:
            return _load(stream)

    return _load(path_or_buf)


def _load_row(raw):
    """Load a row from the NDJSON file."""
    record = json.loads(raw)
    row = {
        "timestamp": pd.to_datetime(record["timestamp"], unit="ns", utc=True),
        "name": record["name"],
        "value": record["value"],
    }
    row.update((f"meta.{key}", value) for key, value in record["meta"].items())
    return row


def _load(fileobj) -> pd.DataFrame:
    """Load the NDJSON file into a pandas dataframe."""
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
