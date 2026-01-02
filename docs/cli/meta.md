# inspect --meta Command

For detailed usage and examples, see the [Meta User Guide](../guide/meta.md).

## Quick Reference

```bash
gpio inspect --meta --help
```

This will show all available options for the `inspect` command including `--meta`.

## Options

### Metadata Selection

- `--parquet` - Show only Parquet file metadata (schema, row groups, compression)
- `--geoparquet` - Show only GeoParquet metadata from 'geo' key
- `--parquet-geo` - Show only Parquet geospatial metadata (GEOMETRY/GEOGRAPHY types)

### Display Options

- `--row-groups N` - Number of row groups to display (default: 1)
- `--json` - Output as JSON for scripting

--8<-- "_includes/common-cli-options.md"

## Examples

```bash
# Show all metadata sections
gpio inspect --meta data.parquet

# Show only Parquet file metadata
gpio inspect --meta data.parquet --parquet

# Show only GeoParquet metadata (from 'geo' key)
gpio inspect --meta data.parquet --geoparquet

# Show only Parquet geospatial metadata
gpio inspect --meta data.parquet --parquet-geo

# Show all row groups (not just the first)
gpio inspect --meta data.parquet --row-groups 10

# JSON output for scripting
gpio inspect --meta data.parquet --json

# Multiple specific sections
gpio inspect --meta data.parquet --parquet --geoparquet

# Remote file with AWS profile
gpio inspect --meta s3://bucket/data.parquet --profile my-aws
```

## Output Sections

By default, `gpio inspect --meta` shows three metadata sections:

### 1. Parquet File Metadata

File structure information including:
- Schema with column names and types
- Row group details (row count, sizes, compression)
- Column statistics (min/max values, null counts)

### 2. GeoParquet Metadata

GeoParquet-specific metadata from the 'geo' key:
- Primary geometry column
- Geometry types
- CRS (Coordinate Reference System) in PROJJSON
- Bounding box metadata (covering)
- GeoParquet version

### 3. Parquet Geospatial Metadata

Geospatial metadata from the Parquet format specification:
- GEOMETRY/GEOGRAPHY logical types
- Per-column bounding boxes
- Geometry encoding information

## Comparison with inspect

| Feature | `gpio inspect` | `gpio inspect --meta` |
|---------|---------------|-------------|
| Quick overview | Yes | No |
| Data preview (--head/--tail) | Yes | No |
| Column statistics | Via --stats | Per row group |
| Row group details | No | Yes |
| Full schema with types | Basic | Detailed |
| GeoParquet metadata | Basic | Full |
| Parquet geospatial metadata | Basic | Full |

Use `inspect` for a quick overview and data preview. Use `meta` for deep dives into file structure and metadata.
