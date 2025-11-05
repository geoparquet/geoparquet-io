# Inspecting Files

The `inspect` command provides quick, human-readable summaries of GeoParquet files.

## Basic Usage

```bash
gpio inspect data.parquet
```

Shows:

- File size and row count
- CRS and bounding box
- Column schema with types

## Preview Data

```bash
# First 10 rows
gpio inspect data.parquet --head 10

# Last 5 rows
gpio inspect data.parquet --tail 5
```

## Statistics

```bash
# Column statistics (nulls, min/max, unique counts)
gpio inspect data.parquet --stats

# Combine with preview
gpio inspect data.parquet --head 5 --stats
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

## See Also

- [CLI Reference: inspect](../cli/inspect.md)
- [check command](check.md)
