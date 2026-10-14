# Installation

## Quick Install

**CLI tool**:
```bash
uv tool install geoparquet-io
```

**Python library**:
```bash
uv add geoparquet-io
```

!!! tip "Using pip/pipx"
    If you prefer pip: `pip install geoparquet-io`
    If you prefer pipx: `pipx install geoparquet-io`

## PMTiles Support

PMTiles generation is built-in. Just install tippecanoe:

```bash
# macOS
brew install tippecanoe

# Ubuntu
sudo apt install tippecanoe
```

Then use `gpio pmtiles create` to generate PMTiles from GeoParquet files.
See the [GeoJSON guide](../guide/geojson.md#using-gpio-pmtiles-built-in) for usage.

## Plugins

gpio supports a plugin system for community extensions.
See the [Plugins Guide](../guide/plugins.md) for more information.

## From Source

```bash
git clone https://github.com/geoparquet/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
```

## Requirements

- **Python**: 3.10+
- **PyArrow**: 12.0.0+
- **DuckDB**: >=1.5.6,<1.6 (the range the `geography` extension is published for)

All dependencies are installed automatically.

## Optional Dependencies

### Development

```bash
uv sync --all-extras
```

### Documentation

```bash
uv sync --extra docs
uv run zensical serve
```

## Verifying Installation

```bash
gpio --version
gpio --help
gpio inspect your_file.parquet
```

## Shell Completion

=== "Bash"

    ```bash
    # Add to ~/.bashrc
    eval "$(_GPIO_COMPLETE=bash_source gpio)"
    ```

=== "Zsh"

    ```bash
    # Add to ~/.zshrc
    eval "$(_GPIO_COMPLETE=zsh_source gpio)"
    ```

=== "Fish"

    ```bash
    # Add to ~/.config/fish/config.fish
    eval (env _GPIO_COMPLETE=fish_source gpio)
    ```

## Upgrading

```bash
uv tool upgrade geoparquet-io     # CLI
uv add --upgrade geoparquet-io    # Library
```

## Uninstalling

```bash
uv tool uninstall geoparquet-io   # CLI
uv remove geoparquet-io           # Library
```

## Platform Support

- **OS**: Linux, macOS, Windows
- **Python**: 3.10, 3.11, 3.12, 3.13
- **Arch**: x86_64, ARM64

## Troubleshooting

### DuckDB Issues

```bash
uv add --upgrade duckdb
```

### PyArrow Compatibility

```bash
uv add --upgrade "pyarrow>=12.0.0"
```

## Next Steps

Head to [Quick Start](quickstart.md) to learn the basics.
