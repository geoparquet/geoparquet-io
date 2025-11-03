# inspect Command

For detailed usage and examples, see the [Inspect User Guide](../guide/inspect.md).

## Quick Reference

```bash
gpio inspect --help
```

This will show all available options for the `inspect` command.

## Options

- `--head N` - Show first N rows
- `--tail N` - Show last N rows
- `--stats` - Show column statistics (nulls, min/max, unique counts)
- `--json` - Output as JSON for scripting
- `--geo-metadata` - Show GeoParquet metadata from 'geo' key
- `--parquet-metadata` - Show Parquet file metadata
- `--parquet-geo-metadata` - Show geospatial metadata from Parquet footer

## Examples

```bash
# Basic inspection
gpio inspect data.parquet

# View GeoParquet metadata
gpio inspect data.parquet --geo-metadata

# View GeoParquet metadata as JSON
gpio inspect data.parquet --geo-metadata --json

# View Parquet file metadata
gpio inspect data.parquet --parquet-metadata

# View geospatial metadata from Parquet footer
gpio inspect data.parquet --parquet-geo-metadata

# Preview with statistics
gpio inspect data.parquet --head 10 --stats
```

## Metadata Flags Comparison

- `--geo-metadata`: Shows GeoParquet metadata from the 'geo' key (application-level metadata)
- `--parquet-metadata`: Shows complete Parquet file metadata (row groups, compression, schema)
- `--parquet-geo-metadata`: Shows geospatial metadata from Parquet footer (GEOMETRY/GEOGRAPHY logical types, bounding boxes, geospatial statistics)
