# Installation

## Quick Install

**CLI tool**:
```bash
pipx install geoparquet-io
# or: uv tool install geoparquet-io
```

**Python library**:
```bash
pip install geoparquet-io
# or: uv add geoparquet-io
```

pipx and uv tool install the CLI in isolation while keeping it globally available. Use pip/uv add when you need the Python API in your project.

## From Source

For the latest development version:

```bash
# Install from PyPI into current environment
uv pip install geoparquet-io

# Or install from source
git clone https://github.com/geoparquet/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
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
# CLI tool
pipx upgrade geoparquet-io
# or: uv tool upgrade geoparquet-io

# Python library
pip install --upgrade geoparquet-io
# or: uv add geoparquet-io (automatically gets latest)
```

## Uninstalling

To remove geoparquet-io:

```bash
# CLI tool
pipx uninstall geoparquet-io
# or: uv tool uninstall geoparquet-io

# Python library
pip uninstall geoparquet-io
# or: uv remove geoparquet-io
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
pip install --upgrade duckdb
```

### PyArrow Compatibility

Ensure you have PyArrow 12.0.0 or higher:

```bash
pip install --upgrade pyarrow>=12.0.0
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
