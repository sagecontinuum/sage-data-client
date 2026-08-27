# Sage Data Client

This is the official Sage Python data API client. It makes querying sensor data and downloading files (images, audio, etc.) straightforward.

## Installation

```sh
pip3 install sage-data-client
```

If you prefer to install this package into a Python virtual environment or are unable to install it system wide (new standard in Python 3.13+), you can use the [venv](https://docs.python.org/3/library/venv.html) module as follows:

```sh
# 1. Create a new virtual environment called my-venv.
python3 -m venv my-venv

# 2. Activate the virtual environment
source my-venv/bin/activate

# 3. Install sage data client in the virtual environment
pip3 install sage-data-client
```

Note: If you are using Linux, you may need to install the `python3-venv` package which is outside of the scope of this document.

---

## Downloading Files

Some Sage tasks produce media files — images, audio clips, video — and upload them to storage. The download commands below are for fetching those files. If you're looking for sensor measurements (temperature, humidity, etc.), see [Querying Sensor Data](#querying-sensor-data) instead.

Access to file downloads may require a Sage portal account. Please refer to [Getting Started with Sage](https://sagecontinuum.org/docs/getting-started) for more information.

### 1. Save your credentials

```sh
sagecli login
```

This prompts for your username and access token from [portal.sagecontinuum.org](https://portal.sagecontinuum.org) and saves them to `~/.sage/credentials`.

### 2. Download files from the command line

```sh
# everything uploaded in the last hour from node W020
sagecli download --start -1h --vsn W020

# all files from a specific day & saving to specific destination (./data)
sagecli download --date 2026-07-01 --vsn W020 --dest ./data

# a date range (both endpoints inclusive)
sagecli download --start 2026-07-01 --end 2026-08-10 --vsn W020 --dest ./data

# multiple nodes at once
sagecli download --date 2026-07-01 --vsn W020 W039 W023 --dest ./data

# filter by a single task
sagecli download --start -6h --vsn W020 --task imagesampler-right

# filter by multiple tasks (runs one query per task, results are combined)
sagecli download --start -6h --vsn W020 --task imagesampler-left imagesampler-right

# preview what would be downloaded without fetching
sagecli download --date 2026-07-01 --vsn W020 --dry-run
```

Files are organized by VSN by default: `{dest}/{vsn}/{filename}`. Use `--layout` to change the structure:

```sh
sagecli download --date 2026-07-01 --vsn W020 --layout "{date}/{vsn}/{task}/{filename}"
```

Available layout variables: `{vsn}`, `{node}`, `{filename}`, `{date}`, `{datetime}`, `{task}`, and any other metadata field present on the record.

> Run `sagecli download --help` for the full list of options.

### How downloads work and server load

When you run a download, two things happen:

1. **One query to the data API** to find matching upload records. This is the same lightweight request used by `query()` — it returns metadata only, no files.
2. **One HTTP request per file** to the storage server to fetch the actual content.

To avoid overloading the server, downloads are capped at **4 concurrent requests** by default. You can lower this with `--workers` if you want to be more conservative, or raise it slightly for faster bulk downloads on a good connection. Please be considerate of both others and the server itself if you set the number of workers beyond the default. 

If a download fails (network drop, temporary server error), it is **retried automatically** up to 5 times with exponential backoff — waiting roughly 1s, 2s, 4s, 8s between attempts. Client errors (e.g. 404 Not Found, 403 Forbidden) are not retried since they won't resolve on their own.

If you run the same download command twice, **existing files are skipped** by default. Re-running after an interrupted bulk download will only fetch what's missing.

### 3. Download files from Python

```python
import sage_data_client

# query uploads and get a downloadable response
data = sage_data_client.query_downloads(
    start="-1h",
    filter={"vsn": "W020"},
)

print(data)  # DownloadResponse(27 records)

# download all files (credentials loaded automatically from ~/.sage/credentials)
data.download_all(dest="./data")

# custom layout
data.download_all(dest="./data", layout="{date}/{vsn}/{task}/{filename}")

# iterate and download individually
for record in data:
    print(record.vsn, record.filename, record.url)
    record.download(dest="./data")

# save the URL list now and download later (useful for large datasets)
data.save("downloads.csv")
```

To reload and download in a later session:

```python
data = sage_data_client.load_downloads("downloads.csv")
data.download_all(dest="./data")
```

---

## Querying Sensor Data

### Query API

```python
import sage_data_client

# query and load data into a pandas data frame
df = sage_data_client.query(
    start="-1h",
    filter={
        "name": "env.temperature",
    }
)

# print results
print(df)

# meta columns are expanded into meta.fieldname
print(df["meta.vsn"].unique())

# stats grouped by node and sensor
print(df.groupby(["meta.vsn", "meta.sensor"]).value.agg(["size", "min", "max", "mean"]))
```

```python
import sage_data_client

# query and load data into pandas data frame
df = sage_data_client.query(
    start="-1h",
    filter={
        "name": "env.raingauge.*",
    }
)

# print number of results of each name
print(df.groupby(["meta.vsn", "name"]).size())
```

### Load results from file

If we have saved the results of a query to a file `data.json`, we can also load using the `load` function as follows:

```python
import sage_data_client

df = sage_data_client.load("data.json")
print(df.groupby(["meta.vsn", "name"]).size())
```

### Integration with Notebooks

A basic example of querying and plotting data can be found [here](https://github.com/sagecontinuum/sage-data-client/blob/main/examples/plotting_example.ipynb).

Additional examples are in the [examples](https://github.com/sagecontinuum/sage-data-client/tree/main/examples) directory, including:

- [`download_files.py`](examples/download_files.py) — basic file download from a single node
- [`download_bulk.py`](examples/download_bulk.py) — bulk download across multiple nodes and a date range with a custom layout

Contributions welcome — add to [examples/contrib](https://github.com/sagecontinuum/sage-data-client/tree/main/examples/contrib) and open a PR!

---

## Reference

### `query(start, end, filter, head, tail)`

Query sensor measurements and return a pandas DataFrame.

* `start`: start timestamp, required. Relative (`"-1h"`, `"-7d"`) or absolute (`"2026-07-01"`, `"2026-07-01T12:00:00Z"`).
* `end`: end timestamp. Same formats as `start`. Default: now.
* `filter`: dict of metadata filters (e.g. `{"name": "env.temperature", "vsn": "W020"}`).
* `head`: limit to earliest N records.
* `tail`: limit to latest N records.

### `query_downloads(start, end, filter, head, tail)`

Query uploaded files and return a `DownloadResponse`. Same parameters as `query()`. Automatically filters to upload records only — any additional filters you pass are applied on top.

### `DownloadResponse`

Returned by `query_downloads()`.

* `len(data)` — number of records.
* `data.df` — the underlying pandas DataFrame with URLs and metadata. Nothing is downloaded until you call `download_all()` or `download()` on individual records.
* `iter(data)` — iterate over `DownloadRecord` objects.
* `data.download_all(dest=".", layout="{vsn}/{filename}", workers=4, skip_existing=True)` — download all files.
* `data.save("downloads.csv")` — save the URL list to CSV for downloading later.

### `DownloadRecord`

A single upload record.

* `record.url` — download URL.
* `record.vsn`, `record.node`, `record.filename`, `record.timestamp` — common fields.
* `record.path_for(dest, layout)` — resolve the output path without downloading.
* `record.download(dest=".", layout="{vsn}/{filename}", skip_existing=True)` — download this file.

### `load_downloads(path)`

Reload a previously saved URL list and return a `DownloadResponse`.

```python
# save for later
data.save("downloads.csv")

# reload and download in another session
data = sage_data_client.load_downloads("downloads.csv")
data.download_all(dest="./data")
```

### `sagecli` CLI

```
sagecli login                       save credentials to ~/.sage/credentials
sagecli download --help             full list of download options
```

Key `sagecli download` flags:

* `--date 2026-07-01` — all files for a single day.
* `--start TIME --end TIME` — date range; bare dates are treated as inclusive.
* `--vsn W020 W039 W023` — one or more node VSNs.
* `--task TASK [TASK ...]` — filter by one or more task names.
* `--layout TEMPLATE` — customize output structure (default: `{vsn}/{filename}`).
* `--dry-run` — preview what would be downloaded without fetching anything.
* `--workers N` — number of concurrent downloads (default: 4).
