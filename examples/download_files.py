"""
This example demonstrates downloading uploaded files (images, audio, etc.) from a Sage node.

Before running, save your credentials:
    sagecli login

Then run:
    python3 download_files.py
"""
import sage_data_client

# Replace with the VSN(s) you have access to.
# Request access at https://portal.sagecontinuum.org
VSN = "W020"

# Query for uploaded files from the last hour.
# This only contacts the data API once — no files are fetched yet.
data = sage_data_client.query_downloads(
    start="-1h",
    filter={"vsn": VSN},
)

print(data)  # e.g. DownloadResponse(27 records)

# Download all files. Credentials are loaded automatically from ~/.sage/credentials.
# Files are organized as ./data/{vsn}/{filename} by default.
# Already-downloaded files are skipped automatically.
data.download_all(dest="./data")
