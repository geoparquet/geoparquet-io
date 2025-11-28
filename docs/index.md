# geoparquet-io

[![Tests](https://github.com/cholmes/geoparquet-io/actions/workflows/tests.yml/badge.svg)](https://github.com/cholmes/geoparquet-io/actions/workflows/tests.yml)
[![Python Version](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://github.com/cholmes/geoparquet-io)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/cholmes/geoparquet-io/blob/main/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Fast I/O and transformation tools for GeoParquet files using PyArrow and DuckDB.

## Features

- **Fast**: Built on PyArrow and DuckDB for high-performance operations
- **Comprehensive**: Sort, partition, enhance, validate, and upload GeoParquet files
- **Cloud-Native**: Read from and write to S3, GCS, Azure, and HTTPS sources
- **Spatial Indexing**: Add bbox, H3 hexagonal cells, KD-tree partitions, and admin divisions
- **Best Practices**: Automatic optimization following GeoParquet 1.1 spec
- **Flexible**: CLI and Python API for any workflow
- **Tested**: Extensive test suite across Python 3.9-3.13 and all platforms

## Quick Example

```bash
# Install
pip install geoparquet-io

# Convert Shapefile/GeoJSON/GeoPackage/CSV to optimized GeoParquet
gpio convert input.shp output.parquet

# Inspect file structure and metadata
gpio inspect myfile.parquet

# Check file quality and best practices
gpio check all myfile.parquet

# Add bounding box column for faster queries
gpio add bbox input.parquet output.parquet

# Sort using Hilbert curve for spatial locality
gpio sort hilbert input.parquet output_sorted.parquet

# Partition into separate files by country
gpio partition admin buildings.parquet output_dir/
```

## Why geoparquet-io?

GeoParquet is a cloud-native geospatial data format that combines the efficiency of Parquet with geospatial capabilities. This toolkit helps you:

- **Optimize file layout** for cloud-native access patterns
- **Add spatial indices** for faster queries and analysis
- **Validate compliance** with GeoParquet best practices
- **Transform large datasets** efficiently using columnar operations

## Getting Started

New to geoparquet-io? Start here:

- [Installation Guide](getting-started/installation.md) - Get up and running quickly
- [Quick Start Tutorial](getting-started/quickstart.md) - Learn the basics in 5 minutes
- [User Guide](guide/inspect.md) - Detailed documentation for all features

## Command Reference

- [convert](cli/convert.md) - Convert vector formats to optimized GeoParquet
- [inspect](cli/inspect.md) - Examine file metadata and preview data
- [check](cli/check.md) - Validate files and fix issues automatically
- [sort](cli/sort.md) - Spatially sort using Hilbert curves
- [add](cli/add.md) - Enhance files with spatial indices
- [partition](cli/partition.md) - Split files into optimized partitions
- [upload](cli/upload.md) - Upload files to cloud storage (S3, GCS, Azure)

## Support

- **Issues**: [GitHub Issues](https://github.com/cholmes/geoparquet-io/issues)
- **Source Code**: [GitHub Repository](https://github.com/cholmes/geoparquet-io)
- **Contributing**: See our [Contributing Guide](contributing.md)

## License

Apache 2.0 - See [LICENSE](https://github.com/cholmes/geoparquet-io/blob/main/LICENSE) for details.
