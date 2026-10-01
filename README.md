# geoparquet-io

[![PyPI version](https://badge.fury.io/py/geoparquet-io.svg)](https://badge.fury.io/py/geoparquet-io)
[![Tests](https://github.com/geoparquet/geoparquet-io/actions/workflows/tests.yml/badge.svg?event=push)](https://github.com/geoparquet/geoparquet-io/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/geoparquet/geoparquet-io/branch/main/graph/badge.svg)](https://codecov.io/gh/geoparquet/geoparquet-io)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/geoparquet/geoparquet-io/blob/main/LICENSE)

Fast I/O and transformation tools for [GeoParquet](https://geoparquet.org/) files, powered by [DuckDB](https://duckdb.org/) and [PyArrow](https://arrow.apache.org/docs/python/).

- **One interface** for conversion, sorting, partitioning, and spatial indexing.
- CLI and Python API with **full type hints**.
- **Unix pipes** with Arrow IPC streaming—no intermediate files.
- Read/write to **S3, GCS, Azure, HTTPS** via DuckDB and [obstore](https://github.com/developmentseed/obstore).
- Automatic **Hilbert sorting**, **ZSTD compression**, **bbox columns**.
- Add **H3, S2, A5, quadkey, KD-tree** spatial indices.
- **Aggregate** huge datasets into A5/H3 grid cells or admin regions for low-zoom visualization.
- **GeoParquet 1.1 and 2.0** support, including Parquet geometry types.

## Installation

```sh
uv tool install geoparquet-io   # CLI (recommended)
uv add geoparquet-io            # Python library
```

Or with pip:

```sh
pip install geoparquet-io
```

## Documentation

[Full documentation](https://geoparquet.io) is available on the website.

Head to [Getting Started](https://geoparquet.io/getting-started/quickstart/) to dig in.

## Development

```sh
git clone https://github.com/geoparquet/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
uv run pytest
```

See [Contributing Guide](https://geoparquet.io/contributing/) for details.

## License

[Apache 2.0](LICENSE)
