# Quick Start

Get started with geoparquet-io in 5 minutes.

## Installation

=== "CLI"

    ```bash
    uv tool install geoparquet-io
    ```

=== "Python"

    ```bash
    uv add geoparquet-io
    ```

See [Installation Guide](installation.md) for more options.

## Reading and Writing

=== "CLI"

    ```bash
    # Convert Shapefile/GeoJSON/GeoPackage to optimized GeoParquet
    gpio convert input.shp output.parquet

    # Inspect file structure
    gpio inspect myfile.parquet
    # Rows: 1,523,847 | Size: 245.3 MB | CRS: EPSG:4326

    # Preview first 5 rows
    gpio inspect myfile.parquet --head 5
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Read a file
    table = gpio.read('data.parquet')
    table.num_rows
    # 1523847

    # Convert from other formats
    table = gpio.convert('data.shp')
    table.write('output.parquet')
    ```

## Transforming Data

=== "CLI"

    ```bash
    # Add bbox column for faster spatial queries
    gpio add bbox input.parquet output.parquet

    # Sort using Hilbert curve for spatial locality
    gpio sort hilbert input.parquet sorted.parquet

    # Chain with pipes—no intermediate files
    gpio add bbox input.parquet | gpio sort hilbert - output.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Chain operations fluently
    gpio.read('input.parquet') \
        .add_bbox() \
        .sort_hilbert() \
        .write('output.parquet')
    ```

## Adding Spatial Indices

=== "CLI"

    ```bash
    # H3 hexagonal cells
    gpio add h3 input.parquet output.parquet --resolution 9

    # A5 pentagonal cells
    gpio add a5 input.parquet output.parquet --resolution 15

    # Chain multiple indices
    gpio add bbox input.parquet | gpio add h3 -r 9 - | gpio sort hilbert - output.parquet
    ```

=== "Python"

    ```python
    gpio.read('input.parquet') \
        .add_bbox() \
        .add_h3(resolution=9) \
        .sort_hilbert() \
        .write('output.parquet')
    ```

## Partitioning

=== "CLI"

    ```bash
    # Partition by H3 cells
    gpio partition h3 input.parquet output_dir/ --resolution 6

    # Preview first
    gpio partition h3 input.parquet --resolution 6 --preview
    ```

=== "Python"

    ```python
    gpio.read('input.parquet') \
        .add_h3(resolution=9) \
        .partition_by_h3('output/', resolution=6)
    ```

## Performance: CLI vs Python

| Approach | Time (75MB file) | Notes |
|----------|------------------|-------|
| CLI (file-based) | 34s | Each command writes intermediate file |
| CLI (piped) | 16s | Arrow IPC streaming between commands |
| **Python API** | **7s** | In-memory, no I/O overhead |

The Python API is fastest because data stays in memory. Use CLI for shell scripts and one-off commands; use Python for applications and maximum performance.

## Next Steps

- [User Guide](../guide/inspect.md) - Detailed feature documentation
- [Python API Reference](../api/python-api.md) - Full API documentation
- [CLI Reference](../cli/overview.md) - Complete command reference
