# geoparquet-io

[![Tests](https://github.com/cholmes/geoparquet-io/actions/workflows/tests.yml/badge.svg)](https://github.com/cholmes/geoparquet-io/actions/workflows/tests.yml)
[![Python Version](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://github.com/cholmes/geoparquet-io)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/cholmes/geoparquet-io/blob/main/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

Fast I/O and transformation tools for GeoParquet files using PyArrow and DuckDB.

## Features

- **Fast**: Built on PyArrow and DuckDB for high-performance operations
- **Comprehensive**: Sort, partition, enhance, and validate GeoParquet files
- **Spatial Indexing**: Add bbox, H3 hexagonal cells, KD-tree partitions, and hierarchical admin divisions
- **Best Practices**: Automatic optimization following GeoParquet 1.1 spec
- **Flexible**: CLI and Python API for any workflow
- **Tested**: Extensive test suite across Python 3.9-3.13 and all platforms

## Installation

```bash
# With uv (recommended)
uv pip install geoparquet-io

# Or with pip
pip install geoparquet-io

# From source
git clone https://github.com/cholmes/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
```

For full development set up see the [getting started](CONTRIBUTING.md#getting-started) instructions.

### Requirements

- Python 3.9 or higher
- PyArrow 12.0.0+
- DuckDB 1.1.3+

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

# Partition by admin boundaries (downloads and caches GAUL dataset)
gpio partition admin buildings.parquet output_dir/ --dataset gaul --levels continent,country
```

## Documentation

Full documentation is available at: **[https://cholmes.github.io/geoparquet-io/](https://cholmes.github.io/geoparquet-io/)**

- **[Getting Started](https://cholmes.github.io/geoparquet-io/getting-started/installation/)** - Installation and quick start guide
- **[User Guide](https://cholmes.github.io/geoparquet-io/guide/inspect/)** - Detailed documentation for all features
- **[CLI Reference](https://cholmes.github.io/geoparquet-io/cli/overview/)** - Complete command reference
- **[Python API](https://cholmes.github.io/geoparquet-io/api/overview/)** - Python API documentation
- **[Examples](https://cholmes.github.io/geoparquet-io/examples/basic/)** - Real-world usage patterns

## Usage Examples

### Inspect and Validate

```bash
# Quick metadata inspection
gpio inspect data.parquet

# Preview first 10 rows
gpio inspect data.parquet --head 10

# Check against best practices
gpio check all data.parquet
```

### Enhance with Spatial Indices

```bash
# Add bounding boxes
gpio add bbox input.parquet output.parquet

# Add H3 hexagonal cell IDs
gpio add h3 input.parquet output.parquet --resolution 9

# Add KD-tree partition IDs (auto-balanced)
gpio add kdtree input.parquet output.parquet

# Add country codes via spatial join (default dataset)
gpio add admin-divisions buildings.parquet output.parquet

# Add GAUL hierarchical admin divisions (continent, country, department)
gpio add admin-divisions buildings.parquet output.parquet --dataset gaul
```

### Optimize and Partition

```bash
# Sort by Hilbert curve
gpio sort hilbert input.parquet sorted.parquet

# Partition by H3 cells
gpio partition h3 large.parquet output_dir/ --resolution 7

# Partition by admin boundaries (auto-caches remote datasets)
gpio partition admin buildings.parquet by_admin/ --dataset gaul --levels continent,country

# Multi-level Hive-style partitioning (continent=Africa/country=Kenya/...)
gpio partition admin buildings.parquet by_admin/ --dataset gaul --levels continent,country,department --hive

# Manage cache for remote datasets
gpio cache list      # View cached datasets
gpio cache clear     # Clear cache to free space
```

### Python API

```python
from geoparquet_io.core.add_bbox_column import add_bbox_column
from geoparquet_io.core.hilbert_order import hilbert_order

# Add bounding box
add_bbox_column("input.parquet", "output.parquet", verbose=True)

# Sort by Hilbert curve
hilbert_order("input.parquet", "sorted.parquet", add_bbox=True)
```

## Contributing

Contributions are welcome! See our [Contributing Guide](https://cholmes.github.io/geoparquet-io/contributing/) for details.

## Development

```bash
# Clone repository
git clone https://github.com/cholmes/geoparquet-io.git
cd geoparquet-io

# Install with all development dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Run linting
uv run ruff check .

# Build docs locally
uv run mkdocs serve
```

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

## Links

- **Documentation**: [https://cholmes.github.io/geoparquet-io/](https://cholmes.github.io/geoparquet-io/)
- **PyPI**: [https://pypi.org/project/geoparquet-io/](https://pypi.org/project/geoparquet-io/) (coming soon)
- **Issues**: [https://github.com/cholmes/geoparquet-io/issues](https://github.com/cholmes/geoparquet-io/issues)
- **Source**: [https://github.com/cholmes/geoparquet-io](https://github.com/cholmes/geoparquet-io)
