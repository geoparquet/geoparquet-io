# Extracting Data

The `extract` command allows you to filter and subset GeoParquet files by columns, spatial extent, and attribute values. It's useful for creating smaller datasets, extracting regions of interest, or selecting specific attributes.

## Basic Usage

```bash
# Extract all data (useful for format conversion or compression change)
gpio extract input.parquet output.parquet

# Extract with different compression
gpio extract input.parquet output.parquet --compression GZIP
```

## Column Selection

### Including Specific Columns

Select only the columns you need. The geometry column and bbox column (if present) are automatically included unless explicitly excluded.

```bash
# Extract only id and name columns (plus geometry and bbox)
gpio extract places.parquet subset.parquet --include-cols id,name

# Extract multiple attribute columns
gpio extract buildings.parquet subset.parquet --include-cols height,building_type,address
```

### Excluding Columns

Remove unwanted columns from the output:

```bash
# Exclude large or unnecessary columns
gpio extract data.parquet output.parquet --exclude-cols raw_data,metadata_json

# Exclude multiple columns
gpio extract data.parquet output.parquet --exclude-cols temp_id,internal_notes,debug_info
```

### Combining Include and Exclude

You can combine both to control exactly which columns appear, including removing geometry or bbox columns:

```bash
# Include specific columns but exclude geometry (for non-spatial export)
gpio extract data.parquet output.parquet \
  --include-cols id,name,population \
  --exclude-cols geometry

# Include columns but exclude bbox to save space
gpio extract data.parquet output.parquet \
  --include-cols id,name,area \
  --exclude-cols bbox
```

## Spatial Filtering

### Bounding Box Filter

Filter features by a rectangular bounding box. The bbox is specified as `xmin,ymin,xmax,ymax` in the same coordinate system as your data.

```bash
# Extract features in San Francisco area (WGS84 coordinates)
gpio extract places.parquet sf_places.parquet \
  --bbox -122.5,37.7,-122.3,37.8

# Extract from remote FIBOA dataset (projected coordinates)
gpio extract https://data.source.coop/fiboa/data/si/si-2024.parquet slovenia_subset.parquet \
  --bbox 450000,50000,500000,100000

# Extract from S3 building dataset (WGS84 coordinates)
gpio extract s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/country_iso=AGO/2017612633061982208.parquet angola_subset.parquet \
  --bbox 13.0,-9.0,14.0,-8.0
```

**CRS Awareness**: The tool detects coordinate system mismatches. If your bbox looks like lat/long coordinates but the data uses a projected CRS, you'll get a helpful warning showing the data's actual bounds.

### Geometry Filter

Filter features by intersection with any geometry, not just rectangles.

```bash
# Filter by inline WKT polygon
gpio extract data.parquet subset.parquet \
  --geometry "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))"

# Filter by inline GeoJSON
gpio extract data.parquet subset.parquet \
  --geometry '{"type":"Polygon","coordinates":[[[0,0],[0,10],[10,10],[10,0],[0,0]]]}'

# Filter by geometry from file
gpio extract data.parquet subset.parquet --geometry @boundary.geojson

# Filter by geometry from stdin (useful in pipelines)
cat boundary.geojson | gpio extract data.parquet subset.parquet --geometry -

# Extract buildings within city boundary
gpio extract buildings.parquet city_buildings.parquet \
  --geometry @city_boundary.geojson
```

**FeatureCollection Handling**: If your GeoJSON file contains multiple features, use `--use-first-geometry`:

```bash
gpio extract data.parquet subset.parquet \
  --geometry @regions.geojson \
  --use-first-geometry
```

## Attribute Filtering with WHERE

Use SQL WHERE clauses to filter by attribute values. This uses DuckDB SQL syntax.

### Simple WHERE Examples

```bash
# Filter by numeric value
gpio extract data.parquet output.parquet --where "population > 10000"

# Filter by string equality
gpio extract data.parquet output.parquet --where "status = 'active'"

# Filter by string pattern
gpio extract data.parquet output.parquet --where "name LIKE '%Hotel%'"

# Filter by multiple conditions
gpio extract data.parquet output.parquet \
  --where "population > 10000 AND area_km2 < 500"

# Filter with IN clause
gpio extract data.parquet output.parquet \
  --where "category IN ('restaurant', 'cafe', 'bar')"

# Filter by date
gpio extract data.parquet output.parquet \
  --where "updated_at >= '2024-01-01'"

# Filter with NULL check
gpio extract data.parquet output.parquet \
  --where "description IS NOT NULL"
```

### WHERE with Special Column Names

Column names containing special characters (like `:`, `-`, `.`) need to be quoted with double quotes in SQL. The shell escaping varies by platform.

**Simple approach (works in bash/zsh):**

```bash
# Column name with colon - use single quotes around the whole WHERE clause
gpio extract data.parquet output.parquet \
  --where '"crop:name" = '\''wheat'\'''

# Column name with dash
gpio extract data.parquet output.parquet \
  --where '"building-type" = '\''residential'\'''

# Column name with dot
gpio extract data.parquet output.parquet \
  --where '"height.meters" > 50'
```

**Alternative escaping (more portable):**

```bash
# Use backslash escaping
gpio extract data.parquet output.parquet \
  --where "\"crop:name\" = 'wheat'"

# Multiple conditions with special column names
gpio extract data.parquet output.parquet \
  --where "\"crop:name\" = 'wheat' AND \"farm:organic\" = true"
```

**Real-world examples with the FIBOA dataset:**

```bash
# Extract wheat fields from Slovenia FIBOA data
gpio extract https://data.source.coop/fiboa/data/si/si-2024.parquet wheat_fields.parquet \
  --where '"crop:name" = '\''wheat'\'''

# Extract large organic farms
gpio extract https://data.source.coop/fiboa/data/si/si-2024.parquet organic_farms.parquet \
  --where '"farm:organic" = true AND area > 50000'

# Extract specific crop types in a region
gpio extract https://data.source.coop/fiboa/data/si/si-2024.parquet crop_subset.parquet \
  --bbox 450000,50000,500000,100000 \
  --where '"crop:name" IN ('\''wheat'\'', '\''corn'\'', '\''barley'\'')'
```

**Tips for WHERE clause escaping:**

1. **Single quotes for strings in SQL**: `'wheat'`, `'active'`
2. **Double quotes for column names in SQL**: `"crop:name"`, `"farm:organic"`
3. **Shell escaping**: Use `'\''` to escape single quotes within single-quoted strings
4. **Test with --dry-run**: Preview the query before executing

### WHERE with Numeric and Boolean Columns

```bash
# Numeric comparisons
gpio extract data.parquet output.parquet --where "area > 1000"
gpio extract data.parquet output.parquet --where "height BETWEEN 10 AND 50"

# Boolean columns
gpio extract data.parquet output.parquet --where "is_validated = true"
gpio extract data.parquet output.parquet --where "active = false OR pending = true"

# Null checks
gpio extract data.parquet output.parquet --where "notes IS NULL"
gpio extract data.parquet output.parquet --where "updated_at IS NOT NULL"
```

### Complex WHERE Examples

```bash
# Combine multiple conditions
gpio extract data.parquet output.parquet \
  --where "population > 5000 AND (status = 'active' OR priority = 'high')"

# String functions
gpio extract data.parquet output.parquet \
  --where "LOWER(name) LIKE '%park%'"

# Math operations
gpio extract data.parquet output.parquet \
  --where "area_km2 / population < 0.001"

# Case-insensitive search
gpio extract data.parquet output.parquet \
  --where "name ILIKE '%hotel%'"
```

## Combining Filters

Combine column selection, spatial filtering, and WHERE clauses:

```bash
# Extract specific columns in a bbox with attribute filter
gpio extract places.parquet hotels.parquet \
  --include-cols name,address,rating \
  --bbox -122.5,37.7,-122.3,37.8 \
  --where "category = 'hotel' AND rating >= 4"

# Extract from remote file with all filter types
gpio extract https://data.source.coop/fiboa/data/si/si-2024.parquet wheat_subset.parquet \
  --bbox 450000,50000,500000,100000 \
  --include-cols id,area,crop:name,farm:organic \
  --where '"crop:name" = '\''wheat'\'' AND area > 10000'

# Extract buildings in area with specific attributes
gpio extract s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/country_iso=AGO/2017612633061982208.parquet large_buildings.parquet \
  --bbox 13.0,-9.0,14.0,-8.0 \
  --where "area_in_meters > 1000"
```

## Limiting Results

Limit the number of rows extracted, useful for testing or sampling:

```bash
# Extract first 1000 matching rows
gpio extract data.parquet sample.parquet --limit 1000

# Extract first 100 hotels in bbox
gpio extract places.parquet hotels_sample.parquet \
  --bbox -122.5,37.7,-122.3,37.8 \
  --where "category = 'hotel'" \
  --limit 100
```

## Working with Remote Files

Extract supports remote files over HTTP/HTTPS and S3:

```bash
# Extract from HTTP URL
gpio extract https://data.source.coop/fiboa/data/si/si-2024.parquet subset.parquet \
  --bbox 450000,50000,500000,100000

# Extract from S3 (uses AWS credentials)
gpio extract s3://my-bucket/data.parquet output.parquet \
  --where "category = 'important'"

# Extract from S3 with specific profile
gpio extract s3://my-bucket/data.parquet output.parquet \
  --profile my-aws-profile \
  --bbox 0,0,10,10
```

## Working with Partitioned Input Data

The `extract` command can read from partitioned GeoParquet datasets, including directories containing multiple parquet files and hive-style partitions.

### Reading from Directories

```bash
# Read all parquet files in a directory
gpio extract partitions/ merged.parquet

# Read from glob pattern
gpio extract "data/*.parquet" merged.parquet

# Read nested directories
gpio extract "data/**/*.parquet" merged.parquet
```

### Hive-Style Partitions

Files organized with `key=value` directory structures are automatically detected:

```bash
# Read hive-style partitions (auto-detected)
gpio extract country_partitions/ merged.parquet

# Explicitly enable hive partitioning (adds partition columns to data)
gpio extract partitions/ merged.parquet --hive-input
```

### Schema Merging

When combining files with different schemas, use `--allow-schema-diff`:

```bash
# Merge files with different columns (fills NULL for missing columns)
gpio extract partitions/ merged.parquet --allow-schema-diff
```

### Applying Filters to Partitioned Data

All filters work with partitioned input:

```bash
# Spatial filter across partitioned dataset
gpio extract partitions/ filtered.parquet --bbox -122.5,37.5,-122.0,38.0

# WHERE filter across partitions
gpio extract "data/*.parquet" filtered.parquet --where "population > 10000"

# Combined filters with schema merging
gpio extract partitions/ subset.parquet \
  --bbox 0,0,10,10 \
  --where "status = 'active'" \
  --allow-schema-diff
```

## Dry Run and Debugging

Preview the SQL query that will be executed:

```bash
# See the SQL query without executing
gpio extract data.parquet output.parquet \
  --where "population > 10000" \
  --dry-run

# Show SQL during execution
gpio extract data.parquet output.parquet \
  --where "population > 10000" \
  --show-sql

# Verbose output with detailed progress
gpio extract data.parquet output.parquet \
  --bbox -122.5,37.7,-122.3,37.8 \
  --verbose
```

## Compression Options

Control output file compression:

--8<-- "_includes/compression-options.md"

```bash
# Use GZIP for wider compatibility
gpio extract data.parquet output.parquet \
  --compression GZIP \
  --compression-level 9

# Maximize compression with ZSTD
gpio extract data.parquet output.parquet \
  --compression ZSTD \
  --compression-level 22

# Fast compression with LZ4
gpio extract data.parquet output.parquet \
  --compression LZ4
```

## Row Group Sizing

Control row group size for optimal query performance:

```bash
# Target row groups of 256MB
gpio extract data.parquet output.parquet --row-group-size-mb 256

# Exact row count per row group
gpio extract data.parquet output.parquet --row-group-size 100000
```

## Performance Tips

1. **Use bbox column**: Files with bbox columns filter much faster than geometric intersection
2. **Column selection**: Only extract columns you need to reduce file size and processing time
3. **Spatial before attribute**: Spatial filters (bbox/geometry) are applied first, then WHERE clause
4. **Limit for testing**: Use `--limit` and `--dry-run` when developing complex queries
5. **Remote files**: Filters are pushed down to minimize data transfer

## Common Patterns

### Extract Sample Data

```bash
# Get a small sample for testing
gpio extract large_file.parquet sample.parquet --limit 1000

# Get sample from specific area
gpio extract large_file.parquet sample.parquet \
  --bbox 0,0,1,1 \
  --limit 100
```

### Extract by Category

```bash
# Extract all features of a specific type
gpio extract data.parquet restaurants.parquet \
  --where "category = 'restaurant'"

# Extract multiple categories
gpio extract data.parquet food_places.parquet \
  --where "category IN ('restaurant', 'cafe', 'bakery')"
```

### Extract Recent Data

```bash
# Extract data updated this year
gpio extract data.parquet recent.parquet \
  --where "updated_at >= '2024-01-01'"

# Extract data from specific time range
gpio extract data.parquet range.parquet \
  --where "created_at BETWEEN '2024-01-01' AND '2024-06-30'"
```

### Extract Non-Spatial Subset

```bash
# Extract as attribute table (no geometry)
gpio extract data.parquet attributes.parquet \
  --include-cols id,name,category,population \
  --exclude-cols geometry,bbox
```

## Error Handling

### Empty Results

If no features match your filters, the tool creates an empty file and shows a warning:

```bash
gpio extract data.parquet output.parquet --bbox 1000,1000,1001,1001
# Warning: No rows match the specified filters.
# Extracted 0 rows to output.parquet
```

### Column Not Found

If you specify a non-existent column, you'll get a clear error:

```bash
gpio extract data.parquet output.parquet --include-cols invalid_column
# Error: Columns not found in schema (--include-cols): invalid_column
# Available columns: id, name, geometry, bbox, ...
```

### Invalid WHERE Clause

SQL syntax errors are reported with details:

```bash
gpio extract data.parquet output.parquet --where "invalid syntax here"
# Error: Parser Error: syntax error at or near "here"
```

### Dangerous SQL Keywords

For safety, certain SQL keywords are blocked in WHERE clauses:

```bash
gpio extract data.parquet output.parquet --where "population > 1000; DROP TABLE users"
# Error: WHERE clause contains potentially dangerous SQL keywords: DROP
```

## See Also

- [CLI Reference](../cli/extract.md) - Complete option reference
- [Remote Files Guide](remote-files.md) - Working with S3 and HTTP files
- [Inspect Guide](inspect.md) - Examine file structure and metadata
- [Partition Guide](partition.md) - Split files into partitions
