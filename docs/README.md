# Documentation

[Project README](../README.md)

These guides describe this checkout of Sage Data Client. A package installed
from a release or a deployed API may have different capabilities.

| Start here | Then read |
| --- | --- |
| [Usage](usage.md) for querying, loading and analysis | [Reference](reference.md) for every public argument |
| [Formats](formats.md) for Arrow, NDJSON and dataframe behavior | [Usage](usage.md#load-saved-data) for file-loading examples |
| [Development](development.md) for contributing and testing | [Formats](formats.md#memory-and-batch-decoding) for decoder constraints |

The public API is `sage_data_client.query()` and `sage_data_client.load()`.
Private decoder modules are implementation details, not additional supported
entrypoints.

The optional sibling [Beehive API guide](../../beehive-data-api/docs/api.md)
documents the current workspace server, including storage-tier restrictions.
The client does not require that repository to be installed. Historical
performance reports remain in the broader workspace rather than serving as
client performance guarantees.
