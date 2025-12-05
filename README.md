# geoparquet-io

[![Tests](https://github.com/cholmes/geoparquet-io/actions/workflows/tests.yml/badge.svg)](https://github.com/cholmes/geoparquet-io/actions/workflows/tests.yml)
[![Python Version](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://github.com/cholmes/geoparquet-io)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/cholmes/geoparquet-io/blob/main/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Fast I/O and transformation tools for GeoParquet files using PyArrow and DuckDB.

**📚 [Full Documentation](https://geoparquet.org/geoparquet-io/)** | **[Quick Start Tutorial](https://geoparquet.org/geoparquet-io/getting-started/quickstart/)**

## Features

- **Fast**: Built on PyArrow and DuckDB for high-performance operations
- **Comprehensive**: Sort, partition, enhance, validate, and upload GeoParquet files
- **Cloud-Native**: Read from and write to S3, GCS, Azure, and HTTPS sources
- **Spatial Indexing**: Add bbox, H3 hexagonal cells, KD-tree partitions, and admin divisions
- **Best Practices**: Automatic optimization following GeoParquet 1.1 spec
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
```

For more examples and detailed usage, see the [Quick Start Tutorial](https://geoparquet.org/geoparquet-io/getting-started/quickstart/) and [User Guide](https://geoparquet.org/geoparquet-io/guide/inspect/).

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](docs/contributing.md) for development setup, coding standards, and how to submit changes.

## Links

- **Documentation**: [https://cholmes.github.io/geoparquet-io/](https://cholmes.github.io/geoparquet-io/)
- **PyPI**: [https://pypi.org/project/geoparquet-io/](https://pypi.org/project/geoparquet-io/) (coming soon)
- **Issues**: [https://github.com/cholmes/geoparquet-io/issues](https://github.com/cholmes/geoparquet-io/issues)

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
