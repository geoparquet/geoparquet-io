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
