"""
sagecli - Command line interface for Sage data downloads.

Commands:
  sagecli login         Save username & token to ~/.sage/credentials
  sagecli download      Download uploaded files matching specific query
"""
import argparse
import getpass
import re
import sys

import pandas as pd

from .auth import CREDENTIALS_PATH, PORTAL_URL, load_credentials, save_credentials
from .downloads import DEFAULT_LAYOUT, DownloadError, DownloadResponse, query_downloads

_BARE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _end_of_day(date_str):
    """Advance a bare date (YYYY-MM-DD) to the start of the next day, making --end inclusive."""
    if _BARE_DATE.match(date_str):
        next_day = pd.to_datetime(date_str) + pd.Timedelta(days=1)
        return next_day.strftime("%Y-%m-%dT%H:%M:%SZ")
    return date_str


def cmd_login(args):
    existing = load_credentials()
    if existing:
        print("Credentials already saved at {}.".format(CREDENTIALS_PATH))
        print("  username: {}".format(existing.get("username", "(not set)")))
        print("  token:    {}".format("*" * 8))
        overwrite = input("Overwrite? [y/N] ").strip().lower()
        if overwrite != "y":
            print("Aborted.")
            return

    print()
    print("To get your Sage credentials:")
    print("  1. Go to {}".format(PORTAL_URL))
    print("  2. Sign in and navigate to your Access Credentials page")
    print("  3. Copy your username and access token")
    print()

    username = input("Username: ").strip()
    if not username:
        print("Error: username cannot be empty.", file=sys.stderr)
        sys.exit(1)

    token = getpass.getpass("Access token: ").strip()
    if not token:
        print("Error: token cannot be empty.", file=sys.stderr)
        sys.exit(1)

    save_credentials(username, token)
    print("Credentials saved to {} (permissions: 600)".format(CREDENTIALS_PATH))


def cmd_download(args):
    # resolve time range
    if args.date:
        start = args.date
        end = _end_of_day(args.date)
    else:
        if not args.start:
            print("Error: --start or --date is required.", file=sys.stderr)
            sys.exit(1)
        start = args.start
        end = _end_of_day(args.end) if args.end else None

    # one query per VSN+task combination, combined into a single response
    vsns = args.vsn or [None]
    tasks = args.task or [None]
    dfs = []
    for vsn in vsns:
        for task in tasks:
            f = {}
            if vsn:
                f["vsn"] = vsn
            if task:
                f["task"] = task
            parts = []
            if vsn:
                parts.append("vsn={}".format(vsn))
            if task:
                parts.append("task={}".format(task))
            label = ", ".join(parts) + ", " if parts else ""
            print("Querying uploads ({}{} to {})...".format(label, start, end or "now"))
            data = query_downloads(start=start, end=end, filter=f or None)
            print("  {} file(s) found.".format(len(data)))
            dfs.append(data.df)

    combined = DownloadResponse(pd.concat(dfs, ignore_index=True))

    if len(combined) == 0:
        print("No files found.")
        return

    print("Found {} file(s).".format(len(combined)))

    if args.dry_run:
        print("\nDry run — files that would be downloaded:")
        for record in combined:
            try:
                path = record.path_for(args.dest, args.layout)
                print("  {} -> {}".format(record.url, path))
            except ValueError as e:
                print("  ERROR: {}".format(e), file=sys.stderr)
        return

    try:
        combined.download_all(
            dest=args.dest,
            layout=args.layout,
            workers=args.workers,
            skip_existing=not args.no_skip,
        )
    except DownloadError as e:
        print("\nError: {}".format(e), file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        prog="sagecli",
        description="Sage data command line interface.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  sagecli login

  # Download all files from the last hour for W020
  sagecli download --start -1h --vsn W020

  # Download all files from 2026-07-01 for W020 to ./data
  sagecli download --date 2026-07-01 --vsn W020 --dest ./data

  # Download all files from 2026-07-01 to 2026-08-10 for W020, W039, and W023
  sagecli download --start 2026-07-01 --end 2026-08-10 --vsn W020 W039 W023

  # Download all files from the last 6 hours for W020, filtering by tasks
  sagecli download --start -6h --vsn W020 --task imagesampler-left imagesampler-right

  # Download all files from 2026-07-01 for W020, using a custom layout and dry-run (no files will be downloaded)
  sagecli download --date 2026-07-01 --vsn W020 --layout "{date}/{vsn}/{task}/{filename}" --dry-run

layout variables: {vsn}, {node}, {filename}, {date}, {datetime}, {task}
usage: sagecli download [-h] (--date DATE | --start TIME) [--end TIME] [--vsn VSN [VSN ...]] [--task TASK [TASK ...]] [--dest DIR] [--layout TEMPLATE] [--workers N] [--dry-run] [--no-skip]
""",
    )

    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    subparsers.add_parser(
        "login",
        help="Save Sage username & token to ~/.sage/credentials",
    )

    dl = subparsers.add_parser(
        "download",
        help="Download uploaded files matching a query",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    time_group = dl.add_mutually_exclusive_group(required=True)
    time_group.add_argument(
        "--date",
        metavar="DATE",
        help="Download all files for a single day (e.g. 2026-07-01).",
    )
    time_group.add_argument(
        "--start",
        metavar="TIME",
        help='Start of time range. Relative (e.g. "-1h", "-7d") or absolute (e.g. "2026-07-01" or "2026-07-01T12:00:00Z").',
    )

    dl.add_argument(
        "--end",
        metavar="TIME",
        help="End of time range. Same formats as --start. Bare dates are treated as end of that day. Default: now.",
    )
    dl.add_argument(
        "--vsn",
        nargs="+",
        metavar="VSN",
        help="Node VSN(s) to download from (e.g. --vsn W020 or --vsn W020 W021 W039).",
    )
    dl.add_argument(
        "--task",
        nargs="+",
        metavar="TASK",
        help="Task name(s) to filter by (e.g. --task imagesampler-right or --task imagesampler-left imagesampler-right).",
    )
    dl.add_argument(
        "--dest",
        default=".",
        metavar="DIR",
        help="Output directory. Default: current directory.",
    )
    dl.add_argument(
        "--layout",
        default=DEFAULT_LAYOUT,
        metavar="TEMPLATE",
        help='File layout template. Default: "{vsn}/{filename}". See layout variables above.',
    )
    dl.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="N",
        help="Concurrent downloads. Default: 4.",
    )
    dl.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be downloaded without downloading.",
    )
    dl.add_argument(
        "--no-skip",
        action="store_true",
        help="Re-download files that already exist.",
    )

    args = parser.parse_args()

    if args.command == "login":
        cmd_login(args)
    elif args.command == "download":
        cmd_download(args)


if __name__ == "__main__":
    main()
