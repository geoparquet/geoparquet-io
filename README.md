<p align="center">
  <img class="gpio-logo" src="https://raw.githubusercontent.com/geoparquet/geoparquet-io/main/docs/assets/logo.png" alt="geoparquet-io logo: a globe robot carrying a stack of Parquet row groups" width="320">
</p>

# geoparquet-io

[![PyPI version](https://badge.fury.io/py/geoparquet-io.svg)](https://badge.fury.io/py/geoparquet-io)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/geoparquet-io.svg)](https://anaconda.org/conda-forge/geoparquet-io)
[![Tests](https://github.com/geoparquet/geoparquet-io/actions/workflows/tests.yml/badge.svg?event=push)](https://github.com/geoparquet/geoparquet-io/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/geoparquet/geoparquet-io/branch/main/graph/badge.svg)](https://codecov.io/gh/geoparquet/geoparquet-io)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
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

Or with conda (also works with mamba or pixi):

```sh
conda install -c conda-forge geoparquet-io
```

## Quickstart

```sh
# Convert any vector format to optimized GeoParquet (ZSTD, bbox, Hilbert order)
gpio convert places.geojson places.parquet
# Converting places.geojson...
# Done in 0.5s
# Output: places.parquet (63.45 KB)
# ✓ Output passes GeoParquet validation (metadata checks)

# Inspect the file
gpio inspect summary places.parquet
# 📄 places.parquet (63.45 KB)
# Rows: 766 | Row Groups: 1 | Compression: ZSTD
# GeoParquet Version: 1.1.0 | CRS: OGC:CRS84 (default)
# Geometry Types: Point
# Bbox: [-0.989102, 9.798559, 1.480825, 12.391399]

# Re-sort and upgrade to GeoParquet 2.0, with native per-row-group bbox stats
gpio sort hilbert places.parquet sorted.parquet --geoparquet-version 2.0

# Check it against best practices
gpio check all sorted.parquet
# ✓ Version 2.0.0
# ✓ Uses native Parquet GEOMETRY/GEOGRAPHY types
# ✓ Bbox covering column 'bbox' declared (optional in 2.0; enables page-level pruning)
# ✓ Row group statistics available for spatial filtering
# ✓ ZSTD compression on geometry column 'geom'
# ✓ Data appears to be spatially ordered
# Spec Validation: ✓ 40 checks passed
```

The same flow in Python, in-memory and without intermediate files:

```python
import geoparquet_io as gpio

table = gpio.convert('places.geojson').add_bbox().sort_hilbert()
table.write('sorted.parquet', geoparquet_version='2.0')
result = gpio.check_all('sorted.parquet')
result.passed()  # True
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
