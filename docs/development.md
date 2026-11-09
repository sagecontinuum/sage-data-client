# Development

[Documentation index](README.md) · [Decoder behavior](formats.md)

## Editable install

From the repository root, in an activated environment:

```sh
python -m pip install -e .
```

The declared runtime requirements are Python >= 3.9, NumPy >= 1.23.2,
pandas >= 2.0.0, and PyArrow >= 14.0.0. This is a pure-Python client package;
it does not link the server's DuckDB or need its Go build tag. PyArrow supplies
the IPC reader and compression support.

## Module responsibilities

| Module | Responsibility |
| --- | --- |
| `src/sage_data_client/__init__.py` | Public `query` and `load` exports |
| `src/sage_data_client/query.py` | Time resolution, JSON requests and response negotiation |
| `src/sage_data_client/_arrow.py` | Bounded Arrow decoding, value inference and metadata expansion |
| `src/sage_data_client/_ndjson.py` | NDJSON parsing and plain/gzip file handling |

Keep the public signatures stable when refactoring internal modules. Shared
callers use the package-level functions; private decoder helpers are not an
extension API. Preserve bounded decoding, scalar inference across batches,
metadata null/order semantics, timestamp precision and resource ownership in
regression coverage.

The request facade closes responses and wrappers it creates. Local loading
closes files it opens, but the caller owns a supplied stream. Neither public
function changes the API warehouse or controls server storage routing.

## Default offline tests

Run from the repository root so fixture paths resolve correctly:

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
```

With integration variables unset, the suite uses local fixtures and mocked
HTTP responses. It does not bind sockets or query production. Coverage includes
Arrow/NDJSON dispatch, gzip plus IPC compression, scalar/metadata behavior,
large integers, nanosecond bounds, malformed streams and cleanup on errors.
Native response replay and live-query tests skip without their explicit inputs.

## Optional live queries

To enable the two live-query tests, explicitly select a service:

```sh
SAGE_DATA_CLIENT_TEST_ENDPOINT=http://localhost:10000/api/v1/query \
  PYTHONPATH=src python -m unittest discover -s tests -v
```

Those tests require a reachable API and perform real requests. They do not
start the API, configure storage or act as performance benchmarks. Omitting
the variable does not silently select production for test execution. Ordinary
application calls still use the public function's documented default endpoint.

## Native API fixture replay

Set `COLD_V3_HTTP_FIXTURE_DIR` to a fresh export from Beehive's
`TestIcebergV3RoundTrip`. The directory contains the actual HTTP Arrow/NDJSON
bodies and JSON manifests for cold, head, tail, exclusive-nanosecond-bound and
tiered cases. The client test replays those bytes through the public query
entrypoint with mocked transport; it does not contact the API itself.

```sh
COLD_V3_HTTP_FIXTURE_DIR=/absolute/path/to/exported-http-fixtures \
  PYTHONPATH=src python -m unittest discover -s tests -v
```

Fixture generation is a separate API operation requiring its isolated REST
catalog and warehouse. It is not part of ordinary client testing. The optional
[Beehive development guide](../../beehive-data-api/docs/development.md#native-iceberg-v3-fixture)
explains that producer workflow; the client tests only need the exported files.

## Build and test a wheel

The build backend is setuptools; `build` is a development tool rather than a
runtime dependency:

```sh
python -m pip install build
python -m build
```

Test the built wheel in a fresh environment so an editable source install does
not hide missing packaged modules:

```sh
python -m venv .venv-wheel
. .venv-wheel/bin/activate
python -m pip install dist/*.whl
env -u PYTHONPATH python -m unittest discover -s tests -v
```

Run these commands at the repository root. Keep live-query and fixture-replay
variables unset for an offline wheel check. The wheel must include both private
decoder modules as well as the public facade.

CI builds the package, installs the wheel, and runs unittest on selected
Python interpreters. Its current matrix is Python 3.9 through 3.12; setup-python
selects each interpreter explicitly. Matrix configuration is not evidence that
a local test run exercised all versions. Publishing and release jobs remain
separate from local build/test commands.

## Documentation and examples

For docs-only edits, validate relative links, Python and shell syntax, and
local fixture examples. Mock HTTP responses when checking query examples;
do not run production queries to validate a guide. Distinguish skipped
integration checks from completed tests and keep historical benchmark claims
outside the user-facing reference.
