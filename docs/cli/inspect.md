# inspect Command

For detailed usage and examples, see the [Inspect User Guide](../guide/inspect.md).

## Quick Reference

```bash
gpio inspect --help
```

This will show all available options for the `inspect` command.

## Options

- `--head [N]` - Show first N rows (defaults to 10 if N not specified)
- `--tail [N]` - Show last N rows (defaults to 10 if N not specified)
- `--stats` - Show column statistics (nulls, min/max, unique counts)
- `--json` - Output as JSON for scripting
- `--geo-metadata` - Show GeoParquet metadata from 'geo' key
- `--parquet-metadata` - Show Parquet file metadata
- `--parquet-geo-metadata` - Show geospatial metadata from Parquet footer

## Examples

```bash
# Basic inspection
gpio inspect data.parquet

# Preview first 10 rows (default when no value given)
gpio inspect data.parquet --head

# Preview first 20 rows
gpio inspect data.parquet --head 20

# Preview last 10 rows (default when no value given)
gpio inspect data.parquet --tail

# Preview last 5 rows
gpio inspect data.parquet --tail 5

# Preview with statistics
gpio inspect data.parquet --head --stats

# View GeoParquet metadata
gpio inspect data.parquet --geo-metadata

# View GeoParquet metadata as JSON
gpio inspect data.parquet --geo-metadata --json

# View Parquet file metadata
gpio inspect data.parquet --parquet-metadata

# View geospatial metadata from Parquet footer
gpio inspect data.parquet --parquet-geo-metadata
```

## Metadata Flags Comparison

- `--geo-metadata`: Shows GeoParquet metadata from the 'geo' key (application-level metadata)
- `--parquet-metadata`: Shows complete Parquet file metadata (row groups, compression, schema)
- `--parquet-geo-metadata`: Shows geospatial metadata from Parquet footer (GEOMETRY/GEOGRAPHY logical types, bounding boxes, geospatial statistics)
