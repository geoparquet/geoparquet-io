# Inspecting Files

The `inspect` command provides quick, human-readable summaries of GeoParquet files.

!!! tip "Need more detail?"
    For comprehensive metadata analysis including row group details and full schema information, use `gpio inspect --meta` or see the [meta guide](meta.md).

## Basic Usage

```bash
gpio inspect data.parquet

# Or inspect remote file
gpio inspect s3://bucket/data.parquet
```

Shows:

- File size and row count
- CRS and bounding box
- Column schema with types

## Preview Data

```bash
# First 10 rows (default when no value given)
gpio inspect data.parquet --head

# First 20 rows
gpio inspect data.parquet --head 20

# Last 10 rows (default when no value given)
gpio inspect data.parquet --tail

# Last 5 rows
gpio inspect data.parquet --tail 5
```

## Statistics

```bash
# Column statistics (nulls, min/max, unique counts)
gpio inspect data.parquet --stats

# Combine with preview
gpio inspect data.parquet --head --stats
```

## GeoParquet Metadata

View the complete GeoParquet metadata from the 'geo' key:

```bash
# Human-readable format
gpio inspect data.parquet --geo-metadata

# JSON format (exact metadata content)
gpio inspect data.parquet --geo-metadata --json
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
gpio inspect data.parquet --parquet-metadata

# JSON format (detailed metadata)
gpio inspect data.parquet --parquet-metadata --json
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
gpio inspect data.parquet --parquet-geo-metadata

# JSON format
gpio inspect data.parquet --parquet-geo-metadata --json
```

This shows metadata from the Parquet specification for geospatial types:
- GEOMETRY and GEOGRAPHY logical type annotations
- Bounding box statistics (xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax)
- Geospatial types (WKB integer codes)
- Custom geospatial key-value metadata

**Note:** This is different from `--geo-metadata` which shows GeoParquet metadata from the 'geo' key.

## JSON Output

```bash
# Machine-readable output
gpio inspect data.parquet --json

# Use with jq
gpio inspect data.parquet --json | jq '.file_info.rows'
```

## Inspecting Partitioned Data

When inspecting a directory containing partitioned data, you can aggregate information across all files:

```bash
# By default, inspects first file with a notice
gpio inspect partitions/
# Output: Inspecting first file (of 4 total). Use --check-all to aggregate all files.

# Aggregate info from all files in partition
gpio inspect partitions/ --check-all
```

The `--check-all` option shows:

- Total file count and combined row count
- Total size across all files
- Combined bounding box (union of all file bounds)
- Schema consistency check
- Compression types used
- GeoParquet versions found
- Per-file breakdown (filename, rows, size)

```bash
# JSON output for scripted processing
gpio inspect partitions/ --check-all --json

# Markdown output for documentation
gpio inspect partitions/ --check-all --markdown
```

!!! note "Preview options not available with --check-all"
    The `--head`, `--tail`, and `--stats` options cannot be combined with `--check-all` since they apply to individual files.

## See Also

- [CLI Reference: inspect](../cli/inspect.md)
- [Viewing Metadata](meta.md) - Deep dive into file structure
- [Checking Best Practices](check.md) - Validate GeoParquet files
