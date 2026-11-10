# Inspecting Files

The `inspect` command provides quick, human-readable summaries of GeoParquet files. `gpio inspect` is a shortcut for `gpio inspect summary` — both show the same output.

!!! tip "Need more detail?"
    For comprehensive metadata analysis including row group details and full schema information, use `gpio inspect meta`.

!!! note "Works without the spatial extension on DuckDB 2.0"
    On DuckDB 2.0+, `gpio inspect`, `inspect head`/`tail`, and `inspect meta` run
    on DuckDB's built-in GEOMETRY support and do not load the spatial extension
    up front: they start faster (the load costs 40-90 ms per connection) and
    keep working on air-gapped hosts where DuckDB extensions cannot be
    downloaded. The preview loads the extension on demand only if a file
    requires a function outside DuckDB's core set. On DuckDB 1.5 the extension
    is loaded as before.

## Basic Usage

=== "CLI"

    ```bash
    gpio inspect data.parquet
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Or inspect remote file
    gpio inspect s3://bucket/data.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Print summary to stdout
    gpio.read('data.parquet').info()

    # Get as dictionary
    info = gpio.read('data.parquet').info(verbose=False)
    print(info['rows'], info['bounds'])
    ```

Shows:

- File size and row count
- CRS and bounding box
- Column schema with types

## Preview Data

=== "CLI"

    ```bash
    # First 10 rows (default)
    gpio inspect head data.parquet

    # First 20 rows
    gpio inspect head data.parquet 20

    # Last 10 rows (default)
    gpio inspect tail data.parquet

    # Last 5 rows
    gpio inspect tail data.parquet 5
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read('data.parquet')

    # First 10 rows (default)
    first_10 = table.head()

    # First 20 rows
    first_20 = table.head(20)

    # Last 10 rows (default)
    last_10 = table.tail()

    # Last 5 rows
    last_5 = table.tail(5)

    # Chain with other operations
    preview = table.head(100).add_bbox()
    ```

### Fitting to Terminal Width

When writing to an interactive terminal, `gpio inspect head` and `gpio inspect tail` fit the preview to your terminal width, showing as many whole columns as fit on screen. Cell values are truncated with ellipsis (`…`) to one line per cell. When columns are hidden, a note appears: `… +N more columns (--no-truncate or --json to see all)`.

When output is redirected or piped (e.g. `gpio inspect head data.parquet | cat`), width fitting is disabled and all columns are shown with full values, so scripts and pipes always see every column. Pass `--max-columns N` to limit columns even when piping.

Use `--max-columns N` to show exactly N columns instead of auto-fitting:

```bash
# Show exactly 3 columns
gpio inspect head data.parquet --max-columns 3
```

Use `--no-truncate` to disable width fitting and show all columns with full (untrimmed) values:

```bash
# Show all columns with complete values
gpio inspect head data.parquet --no-truncate
```

For machine-readable output with all columns and full values, use `--json`:

```bash
# Complete data as JSON
gpio inspect head data.parquet --json
```

## Statistics

=== "CLI"

    ```bash
    # Column statistics (nulls, min/max, unique counts)
    gpio inspect stats data.parquet
    ```

=== "Python"

    <!-- doctest: skip="reads a 'population' key the sample data does not produce" -->
    ```python
    import geoparquet_io as gpio

    table = gpio.read('data.parquet')

    # Get column statistics as dictionary
    stats = table.stats()

    # Access stats for a specific column
    print(stats['population']['min'])
    print(stats['population']['max'])
    print(stats['population']['nulls'])
    print(stats['population']['unique'])

    # Geometry columns have null counts only
    print(stats['geometry']['nulls'])
    ```

### Compression Ratios

`gpio inspect stats` also shows per-column compression ratios, including the compression codec, compressed and uncompressed sizes, and the compression ratio:

=== "CLI"

    ```bash
    gpio inspect stats data.parquet
    ```

=== "Python"

    ```python
    from geoparquet_io.api import ops

    stats = ops.compression_stats('data.parquet')
    for col in stats:
        print(f"{col['column_name']}: {col['compression']} ratio={col['ratio']:.2f}")
    ```

Output includes a "Compression Ratios" table showing which columns benefit most from compression.

## GeoParquet Metadata

=== "CLI"

    View the complete GeoParquet metadata from the 'geo' key:

    ```bash
    # Human-readable format
    gpio inspect meta data.parquet --geo

    # JSON format (exact metadata content)
    gpio inspect meta data.parquet --geo --json
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read('data.parquet')

    # Get full metadata as dictionary
    meta = table.metadata()

    # Access specific metadata
    print(meta['geoparquet_version'])
    print(meta['geometry_column'])
    print(meta['crs'])
    print(meta['bounds'])

    # Get full geo metadata (from 'geo' key)
    geo_meta = meta.get('geo_metadata', {})
    print(geo_meta.get('columns', {}).get('geometry', {}))

    # Include raw Parquet schema metadata
    full_meta = table.metadata(include_parquet_metadata=True)
    ```

The human-readable format shows:
- GeoParquet version
- Primary geometry column
- Column-specific metadata (encoding, geometry types, CRS, bbox, covering, etc.)
- Simplified CRS display (use `--json` to see full PROJJSON definition)
- Default values for optional fields (CRS, orientation, edges, epoch, covering) when not present in the file

## Parquet File Metadata

View the complete Parquet file metadata (low-level details):

```bash
# Human-readable format
gpio inspect meta data.parquet --parquet

# JSON format (detailed metadata)
gpio inspect meta data.parquet --parquet --json
```

The metadata includes:
- Row group structure and sizes
- Column-level compression and encoding
- Physical storage details
- Schema information

## Parquet Geospatial Metadata

View geospatial metadata from the Parquet footer (column-level statistics and logical types):

```bash
# Human-readable format
gpio inspect meta data.parquet --parquet-geo

# JSON format
gpio inspect meta data.parquet --parquet-geo --json
```

This shows metadata from the Parquet specification for geospatial types:
- GEOMETRY and GEOGRAPHY logical type annotations
- Bounding box statistics (xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax)
- Geospatial types (WKB integer codes)
- Custom geospatial key-value metadata

**Note:** This is different from `--geo` which shows GeoParquet metadata from the 'geo' key.

## Bloom Filter Information

`gpio inspect meta` includes a bloom filter summary section showing which columns have bloom filters, coverage percentages, and total bloom filter bytes:

```bash
# Bloom filter info is shown automatically
gpio inspect meta data.parquet

# JSON output includes bloom filter details
gpio inspect meta data.parquet --json
```

Bloom filters enable efficient point lookups on low-cardinality columns. DuckDB 1.5+ automatically writes bloom filters when creating Parquet files.

## Per-Row-Group Geo Statistics

View bounding box statistics for each row group to verify spatial locality:

=== "CLI"

    ```bash
    # Show per-row-group geo_bbox statistics
    gpio inspect meta data.parquet --geo-stats

    # Limit to first 5 row groups
    gpio inspect meta data.parquet --geo-stats --row-groups 5

    # JSON output
    gpio inspect meta data.parquet --geo-stats --json
    ```

=== "Python"

    <!-- doctest: setup="gpio sort hilbert input.parquet hilbert_sorted.parquet" -->
    ```python
    from geoparquet_io.api.ops import get_row_group_geo_stats

    stats = get_row_group_geo_stats('hilbert_sorted.parquet')

    for rg in stats:
        print(f"RG {rg['row_group_id']}: {rg['num_rows']} rows, "
              f"bbox=[{rg['xmin']:.2f}, {rg['ymin']:.2f}, "
              f"{rg['xmax']:.2f}, {rg['ymax']:.2f}]")
    ```

This is useful for:

- Verifying spatial locality after Hilbert sorting
- Checking that row groups have tight bounding boxes
- Debugging spatial filter pushdown performance

**Stats Sources:** Works with native Parquet geo statistics (GeoParquet 2.0, parquet-geo-only) and bbox column statistics.

## Listing Layers

For multi-layer formats (GeoPackage, FileGDB), list available layers:

=== "CLI"

    <!-- doctest: skip="needs multi.gpkg, which the harness does not seed" -->
    ```bash
    # List layers in GeoPackage
    gpio inspect layers multi.gpkg

    # List layers in FileGDB
    gpio inspect layers data.gdb

    # JSON output for scripting
    gpio inspect layers multi.gpkg --json
    ```

=== "Python"

    <!-- doctest: skip="needs GeoPackage, which the harness does not seed" -->
    ```python
    import geoparquet_io as gpio

    # List layers in a multi-layer file
    layers = gpio.list_layers('multi.gpkg')
    print(layers)  # ['buildings', 'roads', 'parcels']

    # Read a specific layer
    table = gpio.read('multi.gpkg', layer='buildings')
    ```

Returns layer names for files with 2+ layers. Single-layer files return nothing.

## JSON Output

```bash
# Machine-readable summary
gpio inspect data.parquet --json

# Use with jq
gpio inspect data.parquet --json | jq '.file_info.rows'

# Metadata as JSON
gpio inspect meta data.parquet --json
```

## Inspecting Partitioned Data

=== "CLI"

    When inspecting a directory containing partitioned data, you can aggregate information across all files:

    <!-- doctest: setup="gpio partition quadkey input.parquet partitions/ --resolution 6 --partition-resolution 2" -->
    ```bash
    # By default, inspects first file with a notice
    gpio inspect partitions/
    # Output: Inspecting first file (of 4 total). Use --check-all to aggregate all files.

    # Aggregate info from all files in partition
    gpio inspect partitions/ --check-all
    ```

=== "Python"

    <!-- doctest: setup="gpio partition quadkey input.parquet partitions/ --resolution 6 --partition-resolution 2" -->
    ```python
    import geoparquet_io as gpio

    # Read all partitions into a single Table
    table = gpio.read_partition('partitions/')

    # Get info about combined data
    table.info()

    # Access properties
    print(f"Total rows: {table.num_rows}")
    print(f"Bounds: {table.bounds}")
    ```

The `--check-all` option shows:

- Total file count and combined row count
- Total size across all files
- Combined bounding box (union of all file bounds)
- Schema consistency check
- Compression types used
- GeoParquet versions found
- Per-file breakdown (filename, rows, size)

<!-- doctest: setup="gpio partition quadkey input.parquet partitions/ --resolution 6 --partition-resolution 2" -->
```bash
# JSON output for scripted processing
gpio inspect partitions/ --check-all --json

# Markdown output for documentation
gpio inspect partitions/ --check-all --markdown
```

## See Also

- [CLI Reference: inspect](../cli/inspect.md)
- [Checking Best Practices](check.md) - Validate GeoParquet files
