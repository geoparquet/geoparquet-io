# Streaming Write

Write large GeoParquet files with minimal memory usage by streaming data through DuckDB.

## Overview

When writing large GeoParquet files, loading the entire dataset into memory can exhaust available RAM and cause out-of-memory errors. The streaming write approach solves this by:

1. Using DuckDB's `COPY TO` command to stream data directly to Parquet format
2. Rewriting only the Parquet footer to add GeoParquet metadata

This allows processing datasets larger than available memory while still producing valid GeoParquet files with proper metadata.

## When to Use

Consider streaming write when:

- **Processing files larger than available RAM** - Datasets that would cause OOM errors with standard Arrow-based writes
- **Memory-constrained environments** - Cloud functions, containers, or edge computing with limited memory
- **Batch processing many large files** - When memory accumulation across multiple operations is a concern
- **ETL pipelines** - Long-running processes where memory efficiency matters more than speed

For smaller files or when maximum write performance is needed, the default Arrow-based write path is typically faster.

## How It Works

The streaming write process has four main steps:

1. **Prepare metadata** - Extract or compute GeoParquet metadata (bbox, geometry_types, CRS)
2. **Compute missing metadata via SQL** - If metadata needs recalculation, run aggregate queries
3. **Stream write with DuckDB COPY TO** - Write Parquet without loading all data into memory
4. **Rewrite footer** - Add GeoParquet metadata to the file footer using fastparquet

```
Input Query → DuckDB COPY TO → Parquet File → Footer Rewrite → GeoParquet
                (streaming)                    (metadata only)
```

The footer rewrite only modifies the last few KB of the file, making it efficient even for multi-GB files.

## Python API

### Using write_geoparquet_via_duckdb

For direct control over streaming writes, use the `write_geoparquet_via_duckdb` function:

=== "Python"

    ```python
    from geoparquet_io.core.common import (
        get_duckdb_connection,
        write_geoparquet_via_duckdb,
    )

    # Create connection with spatial extension
    con = get_duckdb_connection(load_spatial=True)

    # Stream write a large file
    write_geoparquet_via_duckdb(
        con=con,
        query="SELECT * FROM read_parquet('large_input.parquet')",
        output_path="output.parquet",
        geometry_column="geometry",
        geoparquet_version="1.1",
        compression="zstd",
        verbose=True,
    )
    ```

### Using write_parquet_with_metadata

The `write_parquet_with_metadata` function supports streaming via the `use_streaming` flag:

=== "Python"

    ```python
    from geoparquet_io.core.common import (
        get_duckdb_connection,
        write_parquet_with_metadata,
    )

    con = get_duckdb_connection(load_spatial=True)

    # Read input metadata for preservation
    import pyarrow.parquet as pq
    pf = pq.ParquetFile("large_input.parquet")
    original_metadata = dict(pf.schema_arrow.metadata or {})

    # Write with streaming enabled
    write_parquet_with_metadata(
        con=con,
        query="SELECT * FROM read_parquet('large_input.parquet')",
        output_file="output.parquet",
        original_metadata=original_metadata,
        use_streaming=True,
        preserve_bbox=True,
        preserve_geometry_types=True,
        compression="ZSTD",
        verbose=True,
    )
    ```

### Transformations with Streaming

Streaming write works with any SQL transformation:

=== "Python"

    ```python
    from geoparquet_io.core.common import (
        get_duckdb_connection,
        write_geoparquet_via_duckdb,
    )

    con = get_duckdb_connection(load_spatial=True)

    # Filter and transform while streaming
    query = """
        SELECT
            id,
            name,
            ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857') as geometry
        FROM read_parquet('input.parquet')
        WHERE population > 10000
    """

    write_geoparquet_via_duckdb(
        con=con,
        query=query,
        output_path="transformed.parquet",
        geometry_column="geometry",
        geoparquet_version="1.1",
        preserve_bbox=False,  # Recalculate since coordinates changed
        preserve_geometry_types=True,
    )
    ```

## Metadata Preservation

Control how metadata is handled during streaming writes:

| Parameter | Default | Behavior |
|-----------|---------|----------|
| `preserve_bbox=True` | Yes | Keep bbox from input metadata |
| `preserve_bbox=False` | - | Recalculate bbox via SQL aggregation |
| `preserve_geometry_types=True` | Yes | Keep geometry types from input |
| `preserve_geometry_types=False` | - | Recalculate geometry types via SQL |

### When to Recalculate

Set `preserve_bbox=False` when:

- Filtering rows (bbox may shrink)
- Reprojecting coordinates (coordinate system changes)
- Modifying geometries (clipping, buffering, etc.)

Set `preserve_geometry_types=False` when:

- Filtering may remove some geometry types
- Converting geometry types (e.g., Multi* to single)

=== "Python"

    ```python
    # Recalculate both after spatial filtering
    write_geoparquet_via_duckdb(
        con=con,
        query="SELECT * FROM read_parquet('input.parquet') WHERE ST_Within(geometry, ?)",
        output_path="filtered.parquet",
        geometry_column="geometry",
        preserve_bbox=False,          # Bbox will be smaller
        preserve_geometry_types=False, # Some types may be filtered out
    )
    ```

## Compression Options

Streaming write supports standard Parquet compression codecs:

| Codec | Description |
|-------|-------------|
| `zstd` | Zstandard - best balance of speed and compression (default) |
| `gzip` | Wide compatibility, slower |
| `snappy` | Fast compression, lower ratio |
| `lz4` | Very fast, moderate compression |
| `none` | No compression |

=== "Python"

    ```python
    write_geoparquet_via_duckdb(
        con=con,
        query=query,
        output_path="output.parquet",
        geometry_column="geometry",
        compression="zstd",  # or "gzip", "snappy", "lz4", "none"
    )
    ```

!!! note "Compression Level"
    DuckDB's `COPY TO` uses default compression levels. The `compression_level` parameter is accepted for API compatibility but not applied during streaming writes.

## GeoParquet Versions

Specify the target GeoParquet version:

| Version | Description |
|---------|-------------|
| `1.0` | Original spec, WKB encoding |
| `1.1` | Current stable, WKB encoding (default) |
| `2.0` | Native Parquet geometry types |
| `parquet-geo-only` | No GeoParquet metadata |

=== "Python"

    ```python
    # Write GeoParquet 1.1 (recommended)
    write_geoparquet_via_duckdb(
        con=con,
        query=query,
        output_path="output.parquet",
        geometry_column="geometry",
        geoparquet_version="1.1",
    )
    ```

## Limitations

### Local Files Only for Footer Rewrite

The footer rewrite operation requires direct filesystem access. For remote outputs (S3, GCS, Azure):

1. Data is written to a local temporary file
2. Footer is rewritten with GeoParquet metadata
3. File is uploaded to the remote destination
4. Temporary file is cleaned up

This means remote writes still work, but require temporary local disk space equal to the output file size.

### No Row Group Size Control

DuckDB's `COPY TO` command uses its own row group sizing. The `row_group_size_mb` and `row_group_rows` parameters are not supported in streaming mode.

### Single Geometry Column

Streaming write currently supports files with a single geometry column. For multi-geometry files, use the standard Arrow-based write path.

## Performance Comparison

| Aspect | Arrow Path | Streaming Path |
|--------|------------|----------------|
| Memory usage | O(dataset size) | O(row group size) |
| Write speed | Faster for small files | Similar for large files |
| Row group control | Full control | DuckDB default |
| Remote output | Direct | Via temp file |

## See Also

- [extract](extract.md) - Extract subsets from GeoParquet files
- [convert](convert.md) - Convert between formats
- [Python API](../api/python-api.md) - Full API reference
