# extract Command

For detailed usage and examples, see the [Extract User Guide](../guide/extract.md).

## Quick Reference

```bash
gpio extract --help
```

This will show all available options for the `extract` command.

## Options

### Column Selection

- `--include-cols COLS` - Comma-separated columns to include (geometry and bbox auto-added unless in --exclude-cols)
- `--exclude-cols COLS` - Comma-separated columns to exclude (can be used with --include-cols to exclude geometry/bbox)

### Spatial Filtering

- `--bbox BBOX` - Bounding box filter: `xmin,ymin,xmax,ymax`
- `--geometry GEOM` - Geometry filter: GeoJSON, WKT, @filepath, or - for stdin
- `--use-first-geometry` - Use first geometry if FeatureCollection contains multiple

### SQL Filtering

- `--where CLAUSE` - DuckDB WHERE clause for filtering rows
- `--limit N` - Maximum number of rows to extract

### Output Options

--8<-- "_includes/common-cli-options.md"

## Examples

```bash
# Extract all data
gpio extract input.parquet output.parquet

# Extract specific columns
gpio extract data.parquet output.parquet --include-cols id,name,area

# Exclude columns
gpio extract data.parquet output.parquet --exclude-cols internal_id,temp

# Filter by bounding box
gpio extract data.parquet output.parquet --bbox -122.5,37.5,-122.0,38.0

# Filter by geometry from file
gpio extract data.parquet output.parquet --geometry @boundary.geojson

# SQL WHERE filter
gpio extract data.parquet output.parquet --where "population > 10000"

# Combined filters
gpio extract data.parquet output.parquet \
  --include-cols id,name \
  --bbox -122.5,37.5,-122.0,38.0 \
  --where "status = 'active'"

# Extract from remote file
gpio extract s3://bucket/data.parquet output.parquet --bbox 0,0,10,10

# Preview query with dry run
gpio extract data.parquet output.parquet \
  --where "name LIKE '%Hotel%'" \
  --dry-run
```

## Column Selection Behavior

- **include-cols only**: Select specified columns + geometry + bbox (if exists)
- **exclude-cols only**: Select all columns except specified
- **Both**: Select include-cols, but exclude-cols can remove geometry/bbox
- Geometry and bbox always included unless explicitly excluded

## Spatial Filtering Details

- `--bbox`: Uses bbox column for fast filtering when available (bbox covering), otherwise calculates from geometry
- `--geometry`: Supports inline GeoJSON/WKT, file reference (@filepath), or stdin (-)
- CRS warning: Tool warns if bbox looks like lat/long but data uses projected CRS

## WHERE Clause Notes

- Accepts any valid DuckDB SQL WHERE expression
- Column names with special characters need double quotes in SQL: `"crop:name"`
- Shell escaping varies by platform - see [User Guide](../guide/extract.md) for examples
- Dangerous SQL keywords (DROP, DELETE, etc.) are blocked for safety
