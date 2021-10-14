from urllib.request import urlopen
import json
import pandas as pd


def query(start, end=None, tail=None, filter=None, endpoint="https://data.sagecontinuum.org/api/v1/query") -> pd.DataFrame:
    """query makes a query request to the data API and returns the results in a data frame."""
    # build query
    q = {"start": start}
    if end is not None:
        q["end"] = end
    if filter is not None:
        q["filter"] = filter
    if tail is not None:
        q["tail"] = tail

    request_body = json.dumps(q).encode()
    with urlopen(endpoint, request_body) as f:
        return load(f)


def load(path_or_buf) -> pd.DataFrame:
    """load reads a path or file like object containing a response from the data api and returns the results in a data frame."""
    df = pd.read_json(path_or_buf, lines=True, date_unit="ns")

    # if dataframe is empty, return empty with known columns
    if len(df) == 0:
        return pd.DataFrame({
            "timestamp": [],
            "name": [],
            "value": [],
        })

    # otherwise, normalize meta columns to meta.* columns
    meta = pd.DataFrame(df["meta"].tolist())
    meta.rename({c: "meta." + c for c in meta.columns}, axis="columns", inplace=True)
    df = df.join(meta)
    df.drop(columns=["meta"], inplace=True)
    return df
