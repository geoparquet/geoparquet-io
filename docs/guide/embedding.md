# Embedding gpio's Core

Host applications that cannot depend on gpio from PyPI — above all QGIS
plugins, which ship as a zip and cannot rely on the plugin manager installing
dependencies — can **vendor** a small set of core modules as source files
instead of reimplementing them. This page names that set and the contract that
comes with it.

## The embeddable surface

These modules are the declared, semver-tracked embeddable surface:

| Module | What it gives a host |
|--------|---------------------|
| `geoparquet_io/core/bbox_structure.py` | bbox column and covering detection/resolution |
| `geoparquet_io/core/duckdb_metadata.py` | Parquet footer and `geo` metadata readers |
| `geoparquet_io/core/geo_metadata.py` | the `geo` block: parse, sanitize, versions, WKB type codes |
| `geoparquet_io/core/format_writers.py` | GeoPackage/FlatGeobuf/Shapefile/CSV/GeoJSON export |
| `geoparquet_io/core/extract.py` | column/row extraction with spatial predicates |
| `geoparquet_io/core/preview.py` | bbox query-plan preview from the Parquet footer |
| `geoparquet_io/core/remote.py` | remote-URL detection and `needs_httpfs` |

Vendor them together with the internal modules they import (`duckdb_utils`,
`crs_utils`, `exceptions`, `logging_config`, and so on — your bundler or a
`python -c "import ..."` smoke test will name the full closure). The modules
above are the surface the contract covers; everything else in `core/`
travels with them as an implementation detail.

## The contract

- **Import-clean without DuckDB and without click.** Every module above imports
  with only [pyarrow](https://arrow.apache.org/docs/python/) installed. DuckDB
  is required at *call* time — the first function that needs a connection
  raises `ModuleNotFoundError: duckdb` — so a plugin can import, detect the
  missing dependency, and tell the user to install it instead of dying at
  import. psutil is optional: without it, a connection's default memory limit
  comes from the cgroup cap alone, or is left to DuckDB, and orphaned spill
  directories from crashed runs are left to the OS temp reaper instead of
  being reclaimed at the next connection. click is never required: `core/` cannot import it (enforced by an
  import-linter contract).
- **`import geoparquet_io` is side-effect-light.** The package root resolves
  its public names lazily (PEP 562), so importing it pulls in neither the CLI
  stack (click, rich) nor DuckDB.
- **Semver-tracked.** Function signatures and behavior in the surface modules
  follow the package version: breaking changes to them arrive only in a major
  release and are called out in the changelog. Everything else in `core/` is
  internal and may change in any release — vendored copies of it are carried
  at your own risk.
- **Guarded in CI.** The `embeddable-import` job in
  `.github/workflows/tests.yml` imports the whole surface in a fresh venv that
  contains only pyarrow — neither DuckDB nor click installed — on every push.
  `tests/test_embeddable_imports.py` keeps this page, that job, and the module
  list in sync.

You can check the import behavior yourself in a full gpio environment:

```python
import sys

import geoparquet_io
import geoparquet_io.core.bbox_structure
import geoparquet_io.core.duckdb_metadata
import geoparquet_io.core.extract
import geoparquet_io.core.format_writers
import geoparquet_io.core.geo_metadata
import geoparquet_io.core.preview
import geoparquet_io.core.remote

assert "click" not in sys.modules
assert "duckdb" not in sys.modules
print("imports stayed lazy")
```

## What a host must supply

**Python 3.11 or newer, to import.** This is gpio's supported floor
(`requires-python` in pyproject.toml). Maintenance tooling uses the standard
library's `tomllib`, available starting in Python 3.11.

**More than pyarrow, to call.** Each of these is imported inside the function
that needs it, so its absence surfaces as `ModuleNotFoundError` at that call,
not at import:

| Package | First needed by |
|---------|-----------------|
| `duckdb` | any function that opens a connection |
| `fsspec` | GeoPackage, FlatGeobuf, Shapefile and CSV export (`format_writers.write_gdal_format`, `write_csv`), which read the input's schema through it — local files included. HTTP(S) inputs also need `aiohttp`; S3 inputs need `s3fs` |
| `pyproj` | CRS normalization in `crs_utils`, `duckdb_metadata`, `geodesic_bbox` and the write funnels |
| `geoarrow-pyarrow` | the Arrow-side geometry paths: geometry types or a bbox computed from the data (`geo_metadata`), and pyarrow reads and writes that must keep a native Parquet `GEOMETRY`/`GEOGRAPHY` type and its CRS |
| `obstore` and `botocore` | `remote.upload_if_remote` — any upload of a written file to a remote URL goes through obstore, and its AWS credential resolution through botocore |

## Detecting a missing DuckDB from a host

A vendoring host should probe for DuckDB once, up front, and turn the result
into its own user-facing message:

```python
import importlib.util

if importlib.util.find_spec("duckdb") is None:
    # QGIS example: raise the plugin's own "please install duckdb" dialog here.
    print("DuckDB is not installed; install it to enable GeoParquet extraction")
else:
    print("DuckDB available")
```

## What stays internal

Everything in `core/` outside the table above — and all of `cli/` and `api/` —
is gpio's internal layering (see
[contributing](../contributing.md) for the architecture). The Python API
(`geoparquet_io.api`) remains the supported interface for applications that
*can* install gpio from PyPI; vendoring is only for hosts that cannot.

## Core helper migrations

See the [one-owner migration reference](one-owner-migration.md) for the owner table,
removed public names and signature changes. Import policy helpers from their owner;
former convenience re-exports and deleted wrappers are unsupported.
