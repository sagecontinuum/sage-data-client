import base64
import random
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd

from .auth import load_credentials, load_token
from .query import query

DEFAULT_LAYOUT = "{vsn}/{filename}"
_QUERY_ENDPOINT = "https://data.sagecontinuum.org/api/v1/query"


class DownloadRecord:
    """A single upload record with a URL and metadata for downloading."""

    def __init__(self, row: dict):
        self.url: str = row["value"]
        self.timestamp: pd.Timestamp = row["timestamp"]
        self._meta: Dict[str, str] = {
            k[5:]: v for k, v in row.items() if k.startswith("meta.")
        }
        self.vsn: str = self._meta.get("vsn", "")
        self.node: str = self._meta.get("node", "")
        self.filename: str = self._meta.get("filename", "")

    def layout_vars(self) -> dict:
        """Variables available for use in layout templates.

        Includes: vsn, node, filename, date (YYYY-MM-DD), datetime (YYYY-MM-DDTHH-MM-SS),
        and any meta field present on this record (e.g. task, plugin, camera).
        """
        return {
            "date": self.timestamp.strftime("%Y-%m-%d"),
            "datetime": self.timestamp.strftime("%Y-%m-%dT%H-%M-%S"),
            "vsn": self.vsn,
            "node": self.node,
            "filename": self.filename,
            **self._meta,
        }

    def path_for(self, dest: str = ".", layout: str = DEFAULT_LAYOUT) -> Path:
        """Resolve the output path for this record without downloading."""
        try:
            rel = layout.format_map(self.layout_vars())
        except KeyError as e:
            available = ", ".join(sorted(self.layout_vars()))
            raise ValueError(
                "Layout uses unknown variable {}. Available: {}".format(e, available)
            ) from None
        return Path(dest) / rel

    def download(
        self,
        dest: str = ".",
        layout: str = DEFAULT_LAYOUT,
        credentials: Optional[Tuple[str, str]] = None,
        skip_existing: bool = True,
        max_retries: int = 5,
    ) -> Path:
        """Download this file.

        Parameters
        ----------
        dest : root directory for downloads
        layout : path template relative to dest (default: "{vsn}/{filename}")
            Available variables: {vsn}, {node}, {filename}, {date}, {datetime},
            and any meta field (e.g. {task}, {plugin}, {camera}).
        credentials : (username, token) tuple; if None, loads from ~/.sage/credentials
        skip_existing : skip download if file already exists
        max_retries : retry attempts on transient failure

        Returns
        -------
        Path to downloaded file.
        """
        if credentials is None:
            credentials = _load_credentials_tuple()

        path = self.path_for(dest, layout)

        if skip_existing and path.exists():
            return path

        path.parent.mkdir(parents=True, exist_ok=True)
        _download_with_retry(self.url, path, credentials=credentials, max_retries=max_retries)
        return path


class DownloadResponse:
    """Result of query_downloads().

    Iterate to get individual DownloadRecord items, or call download_all()
    to fetch everything at once.

    Examples
    --------
    >>> data = sage_data_client.query_downloads(start="-1h", filter={"vsn": "W020"})
    >>> print(len(data), "files found")

    >>> # download with default layout ({vsn}/{filename})
    >>> data.download_all(dest="./data")

    >>> # download with custom layout
    >>> data.download_all(dest="./data", layout="{date}/{vsn}/{filename}")

    >>> # iterate and download individually
    >>> for record in data:
    ...     record.download(dest="./data")
    """

    def __init__(self, df: pd.DataFrame):
        self._df = df
        self._records: List[DownloadRecord] = [
            DownloadRecord(row) for _, row in df.iterrows()
        ]

    @property
    def df(self) -> pd.DataFrame:
        """The underlying DataFrame of upload records."""
        return self._df

    def __iter__(self):
        return iter(self._records)

    def __len__(self) -> int:
        return len(self._records)

    def __repr__(self) -> str:
        return "DownloadResponse({} records)".format(len(self._records))

    def save(self, path: str) -> None:
        """Save URL list and metadata to a CSV file for later use.

        The saved file can be reloaded with load_downloads().

        Parameters
        ----------
        path : destination file path (e.g. "downloads.csv")

        Examples
        --------
        >>> data = sage_data_client.query_downloads(start="-1h", filter={"vsn": "W020"})
        >>> data.save("downloads.csv")
        >>>
        >>> # later, in another session:
        >>> data = sage_data_client.load_downloads("downloads.csv")
        >>> data.download_all(dest="./data")
        """
        dest = Path(path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        self._df.to_csv(dest, index=False)
        print("Saved {} record(s) to {}".format(len(self._records), dest))

    def download_all(
        self,
        dest: str = ".",
        layout: str = DEFAULT_LAYOUT,
        credentials: Optional[Tuple[str, str]] = None,
        skip_existing: bool = True,
        workers: int = 4,
        max_retries: int = 5,
    ) -> List[Path]:
        """Download all files in this response.

        Parameters
        ----------
        dest : root directory for downloads
        layout : path template relative to dest (default: "{vsn}/{filename}")
            Available variables: {vsn}, {node}, {filename}, {date}, {datetime},
            and any meta field (e.g. {task}, {plugin}, {camera}).
        credentials : (username, token) tuple; if None, loads from ~/.sage/credentials
        skip_existing : skip download if file already exists
        workers : number of concurrent downloads (controls server load)
        max_retries : retry attempts per file on transient failure

        Returns
        -------
        List of Paths to downloaded files.

        Raises
        ------
        DownloadError if any files failed after all retries.
        """
        if credentials is None:
            credentials = _load_credentials_tuple()
            if credentials is None:
                print(
                    "Warning: no credentials found. Run 'sagecli login' if downloads fail.\n"
                    "  Credentials are stored in ~/.sage/credentials"
                )

        total = len(self._records)
        results: List[Path] = []
        errors = []

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    record.download,
                    dest=dest,
                    layout=layout,
                    credentials=credentials,
                    skip_existing=skip_existing,
                    max_retries=max_retries,
                ): record
                for record in self._records
            }

            done = 0
            for future in as_completed(futures):
                record = futures[future]
                done += 1
                try:
                    path = future.result()
                    results.append(path)
                except Exception as e:
                    errors.append((record, e))
                print("\rDownloading... [{}/{}]".format(done, total), end="", flush=True)
            print()

        if errors:
            raise DownloadError(
                "{} of {} downloads failed".format(len(errors), total),
                errors=errors,
            )

        return results


class DownloadError(Exception):
    """Raised when one or more downloads fail after all retries."""

    def __init__(self, message: str, errors: Optional[list] = None):
        super().__init__(message)
        self.errors = errors or []


def query_downloads(
    start,
    end=None,
    filter: Optional[Dict[str, str]] = None,
    head: Optional[int] = None,
    tail: Optional[int] = None,
    endpoint: str = _QUERY_ENDPOINT,
) -> DownloadResponse:
    """Query for uploaded files and return a DownloadResponse.

    Equivalent to query() with filter={"name": "upload"}, but returns a
    DownloadResponse that supports bulk downloading.

    Parameters
    ----------
    start : query start time, required
        Timestamps can be relative like "-1h" or absolute like "2024-01-01T00:00:00Z".
    end : query end time, default None
    filter : additional filters (e.g. {"vsn": "W020", "task": "imagesampler-right"})
    head : limit to earliest `head` records
    tail : limit to latest `tail` records
    endpoint : URL of query API

    Returns
    -------
    DownloadResponse

    Examples
    --------
    >>> import sage_data_client
    >>>
    >>> data = sage_data_client.query_downloads(
    ...     start="-1h",
    ...     filter={"vsn": "W020"},
    ... )
    >>> print(data)
    DownloadResponse(42 records)
    >>>
    >>> data.download_all(dest="./data", layout="{date}/{vsn}/{filename}")
    """
    merged_filter = {"name": "upload"}
    if filter:
        merged_filter.update(filter)

    df = query(
        start=start,
        end=end,
        head=head,
        tail=tail,
        filter=merged_filter,
        endpoint=endpoint,
    )
    return DownloadResponse(df)


_REQUIRED_COLUMNS = {"value", "timestamp"}


def load_downloads(path: str) -> "DownloadResponse":
    """Load a previously saved URL list and return a DownloadResponse.

    Parameters
    ----------
    path : path to a CSV file saved with DownloadResponse.save()

    Returns
    -------
    DownloadResponse

    Raises
    ------
    FileNotFoundError if the file does not exist.
    ValueError if the file is missing required columns or is not a valid download CSV.

    Examples
    --------
    >>> data = sage_data_client.load_downloads("downloads.csv")
    >>> print(data)
    DownloadResponse(42 records)
    >>> data.download_all(dest="./data")
    """
    src = Path(path)
    if not src.exists():
        raise FileNotFoundError(
            "No such file: '{}'\n"
            "Save a URL list first with: data.save('{}')".format(src, src)
        )

    try:
        df = pd.read_csv(src)
    except Exception as e:
        raise ValueError("Could not read '{}': {}".format(src, e)) from e

    missing = _REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            "'{}' is missing required columns: {}.\n"
            "Make sure the file was saved with DownloadResponse.save().".format(src, sorted(missing))
        )

    # parse timestamps and ensure timezone-aware (UTC)
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    except Exception as e:
        raise ValueError("Could not parse 'timestamp' column in '{}': {}".format(src, e)) from e

    return DownloadResponse(df)


def _load_credentials_tuple() -> Optional[Tuple[str, str]]:
    creds = load_credentials()
    username = creds.get("username")
    token = creds.get("token")
    if username and token:
        return (username, token)
    return None


def _download_with_retry(
    url: str,
    dest: Path,
    credentials: Optional[Tuple[str, str]] = None,
    max_retries: int = 5,
    backoff_base: float = 1.0,
) -> None:
    headers = {}
    if credentials:
        username, token = credentials
        encoded = base64.b64encode("{}:{}".format(username, token).encode()).decode()
        headers["Authorization"] = "Basic {}".format(encoded)

    last_error = None
    for attempt in range(max_retries):
        if attempt > 0:
            wait = backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 1)
            print("  Retry {}/{} in {:.1f}s...".format(attempt, max_retries - 1, wait))
            time.sleep(wait)
        try:
            req = Request(url, headers=headers)
            try:
                with tempfile.NamedTemporaryFile(dir=dest.parent, delete=False) as tmp:
                    tmp_path = Path(tmp.name)
                    with urlopen(req) as resp:
                        while True:
                            chunk = resp.read(65536)
                            if not chunk:
                                break
                            tmp.write(chunk)
                tmp_path.rename(dest)
            except Exception:
                if tmp_path.exists():
                    tmp_path.unlink()
                raise
            return
        except HTTPError as e:
            last_error = e
            # Don't retry on client errors (4xx) except 429 (Too Many Requests)
            if e.code != 429 and e.code < 500:
                raise
        except URLError as e:
            last_error = e

    raise last_error
