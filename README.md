# geoparquet-io

[![Tests](https://github.com/cholmes/geoparquet-io/actions/workflows/tests.yml/badge.svg)](https://github.com/cholmes/geoparquet-io/actions/workflows/tests.yml)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://github.com/cholmes/geoparquet-io)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/cholmes/geoparquet-io/blob/main/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Fast I/O and transformation tools for GeoParquet files using PyArrow and DuckDB.

**📚 [Full Documentation](https://geoparquet.org/geoparquet-io/)** | **[Quick Start Tutorial](https://geoparquet.org/geoparquet-io/getting-started/quickstart/)**

## Features

- **Fast**: Built on PyArrow and DuckDB for high-performance operations
- **Pipeable**: Chain commands with Unix pipes using Arrow IPC streaming - no intermediate files
- **Comprehensive**: Sort, extract, partition, enhance, validate, and upload GeoParquet files
- **Cloud-Native**: Read from and write to S3, GCS, Azure, and HTTPS sources
- **Spatial Indexing**: Add bbox, H3 hexagonal cells, KD-tree partitions, and admin divisions
- **Best Practices**: Automatic optimization following GeoParquet 1.1 and 2.0 specs
- **Parquet Geo Types support**: Read and write Parquet geometry and geography types.
- **Flexible**: CLI and Python API for any workflow
- **Tested**: Extensive test suite across Python 3.10-3.13 and all platforms

## Installation

```bash
pip install geoparquet-io
```

See the [Installation Guide](https://geoparquet.org/geoparquet-io/getting-started/installation/) for other options (uv, from source) and requirements.

## Quick Start

```bash
# Inspect file structure and metadata
gpio inspect myfile.parquet

# Check file quality and best practices
gpio check all myfile.parquet

# Add bounding box column for faster queries
gpio add bbox input.parquet output.parquet

# Sort using Hilbert curve for spatial locality
gpio sort hilbert input.parquet output_sorted.parquet

# Partition by admin boundaries
gpio partition admin buildings.parquet output_dir/ --dataset gaul --levels continent,country

# Remote-to-remote processing (S3, GCS, Azure, HTTPS)
gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet --profile my-aws
gpio partition h3 gs://bucket/data.parquet gs://bucket/partitions/ --resolution 9
gpio sort hilbert https://example.com/data.parquet s3://bucket/sorted.parquet

# Chain commands with Unix pipes - no intermediate files needed
gpio extract --bbox "-122.5,37.5,-122.0,38.0" input.parquet | gpio add bbox - | gpio sort hilbert - output.parquet
```

For more examples and detailed usage, see the [Quick Start Tutorial](https://geoparquet.org/geoparquet-io/getting-started/quickstart/) and [User Guide](https://geoparquet.org/geoparquet-io/guide/inspect/).

## Python API

Use gpio programmatically for the best performance:

```python
import geoparquet_io as gpio

# Read, transform, and write in a fluent chain
gpio.read('input.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')

# Convert from other formats (Shapefile, GeoJSON, GeoPackage, CSV)
gpio.convert('data.gpkg') \
    .add_h3(resolution=9) \
    .partition_by_h3('output/', resolution=5)

# Upload to cloud storage
gpio.read('data.parquet') \
    .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
    .add_bbox() \
    .upload('s3://bucket/filtered.parquet')
```

The Python API keeps data in memory as Arrow tables, providing up to 5x better performance than CLI operations. See the [Python API documentation](https://geoparquet.org/geoparquet-io/api/python-api/) for full details.

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](docs/contributing.md) for development setup, coding standards, and how to submit changes.

## Links

- **Documentation**: [https://geoparquet.org/geoparquet-io/](https://geoparquet.org/geoparquet-io/)
- **PyPI**: [https://pypi.org/project/geoparquet-io/](https://pypi.org/project/geoparquet-io/)
- **Issues**: [https://github.com/cholmes/geoparquet-io/issues](https://github.com/cholmes/geoparquet-io/issues)

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
