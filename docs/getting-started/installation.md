# Installation

## With uv (Recommended)

[uv](https://github.com/astral-sh/uv) is a fast Python package manager that we use for this project:

```bash
# Install from PyPI (coming soon)
uv pip install geoparquet-io

# Or install from source
git clone https://github.com/cholmes/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
```

## With pip

If you prefer using pip:

```bash
pip install geoparquet-io
```

## From Source

For the latest development version:

```bash
git clone https://github.com/cholmes/geoparquet-io.git
cd geoparquet-io
uv sync  # recommended
# or
pip install -e .
```

## Requirements

- **Python**: 3.10 or higher
- **PyArrow**: 12.0.0+
- **DuckDB**: 1.1.3+

All dependencies are automatically installed when you install geoparquet-io.

## Optional Dependencies

### Development Tools

For contributing to geoparquet-io:

```bash
uv sync --all-extras
# or
pip install geoparquet-io[dev]
```

This installs:

- pytest for testing
- ruff for linting
- pre-commit for git hooks
- mypy for type checking

### Documentation

For building documentation:

```bash
uv pip install geoparquet-io[docs]
# or
pip install geoparquet-io[docs]
```

This installs:

- mkdocs for documentation generation
- mkdocs-material theme
- mkdocstrings for API documentation

## Verifying Installation

After installation, verify everything works:

```bash
# Check version
gpio --version

# Get help
gpio --help

# Run a simple command (requires a GeoParquet file)
gpio inspect your_file.parquet
```

## Upgrading

To upgrade to the latest version:

```bash
uv pip install --upgrade geoparquet-io
# or
pip install --upgrade geoparquet-io
```

## Uninstalling

To remove geoparquet-io:

```bash
uv pip uninstall geoparquet-io
# or
pip uninstall geoparquet-io
```

## Platform Support

geoparquet-io is tested on:

- **Operating Systems**: Linux, macOS, Windows
- **Python Versions**: 3.10, 3.11, 3.12, 3.13
- **Architectures**: x86_64, ARM64

## Troubleshooting

### DuckDB Installation Issues

If you encounter issues with DuckDB installation, try:

```bash
uv pip install --upgrade duckdb
```

### PyArrow Compatibility

Ensure you have PyArrow 12.0.0 or higher:

```bash
uv pip install --upgrade pyarrow>=12.0.0
```

### Using Virtual Environments with uv

uv automatically manages virtual environments, but if you need a fresh environment:

```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install geoparquet-io
```

## Next Steps

Once installed, head to the [Quick Start Guide](quickstart.md) to learn how to use geoparquet-io.
