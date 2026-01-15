# Extracting Data

The `extract` command allows you to filter and subset GeoParquet files by columns, spatial extent, and attribute values. It's useful for creating smaller datasets, extracting regions of interest, or selecting specific attributes.

## Basic Usage

=== "CLI"

    ```bash
    # Extract all data (useful for format conversion or compression change)
    gpio extract input.parquet output.parquet

    # Extract with different compression
    gpio extract input.parquet output.parquet --compression GZIP
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Extract all data (useful for format conversion or compression change)
    gpio.read('input.parquet').write('output.parquet')

    # Extract with different compression
    gpio.read('input.parquet').write('output.parquet', compression='GZIP')
    ```

## Column Selection

### Including Specific Columns

Select only the columns you need. The geometry column and bbox column (if present) are automatically included unless explicitly excluded.

=== "CLI"

    ```bash
    # Extract only id and name columns (plus geometry and bbox)
    gpio extract places.parquet subset.parquet --include-cols id,name

    # Extract multiple attribute columns
    gpio extract buildings.parquet subset.parquet --include-cols height,building_type,address
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Extract only id and name columns (plus geometry and bbox)
    gpio.read('places.parquet').extract(columns=['id', 'name']).write('subset.parquet')

    # Extract multiple attribute columns
    gpio.read('buildings.parquet').extract(columns=['height', 'building_type', 'address']).write('subset.parquet')
    ```

### Excluding Columns

Remove unwanted columns from the output:

=== "CLI"

    ```bash
    # Exclude large or unnecessary columns
    gpio extract data.parquet output.parquet --exclude-cols raw_data,metadata_json

    # Exclude multiple columns
    gpio extract data.parquet output.parquet --exclude-cols temp_id,internal_notes,debug_info
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Exclude large or unnecessary columns
    gpio.read('data.parquet').extract(exclude_columns=['raw_data', 'metadata_json']).write('output.parquet')

    # Exclude multiple columns
    gpio.read('data.parquet').extract(exclude_columns=['temp_id', 'internal_notes', 'debug_info']).write('output.parquet')
    ```

### Combining Include and Exclude

You can combine both to control exactly which columns appear, including removing geometry or bbox columns:

=== "CLI"

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

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Include specific columns but exclude geometry (for non-spatial export)
    gpio.read('data.parquet').extract(
        columns=['id', 'name', 'population'],
        exclude_columns=['geometry']
    ).write('output.parquet')

    # Include columns but exclude bbox to save space
    gpio.read('data.parquet').extract(
        columns=['id', 'name', 'area'],
        exclude_columns=['bbox']
    ).write('output.parquet')
    ```

## Spatial Filtering

### Bounding Box Filter

Filter features by a rectangular bounding box. The bbox is specified as `xmin,ymin,xmax,ymax` in the same coordinate system as your data.

=== "CLI"

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

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Extract features in San Francisco area (WGS84 coordinates)
    gpio.read('places.parquet').extract(bbox=(-122.5, 37.7, -122.3, 37.8)).write('sf_places.parquet')

    # Extract from remote FIBOA dataset (projected coordinates)
    gpio.read('https://data.source.coop/fiboa/data/si/si-2024.parquet').extract(
        bbox=(450000, 50000, 500000, 100000)
    ).write('slovenia_subset.parquet')
    ```

!!! note "Remote file support"
    S3, GCS, Azure, and HTTPS URLs are supported via DuckDB's httpfs extension. See the [Remote Files guide](remote-files.md) for credential configuration.

**CRS Awareness**: The tool detects coordinate system mismatches. If your bbox looks like lat/long coordinates but the data uses a projected CRS, you'll get a helpful warning showing the data's actual bounds.

### Geometry Filter

Filter features by intersection with any geometry, not just rectangles.

!!! note "CLI Only"
    Geometry filtering with arbitrary shapes is currently only available via the CLI.
    For rectangular regions, use the `bbox` parameter in Python.

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

=== "CLI"

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

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Filter by numeric value
    gpio.read('data.parquet').extract(where="population > 10000").write('output.parquet')

    # Filter by string equality
    gpio.read('data.parquet').extract(where="status = 'active'").write('output.parquet')

    # Filter by multiple conditions
    gpio.read('data.parquet').extract(where="population > 10000 AND area_km2 < 500").write('output.parquet')

    # Filter with IN clause
    gpio.read('data.parquet').extract(where="category IN ('restaurant', 'cafe', 'bar')").write('output.parquet')
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

=== "CLI"

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

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Extract specific columns in a bbox with attribute filter
    gpio.read('places.parquet').extract(
        columns=['name', 'address', 'rating'],
        bbox=(-122.5, 37.7, -122.3, 37.8),
        where="category = 'hotel' AND rating >= 4"
    ).write('hotels.parquet')

    # Extract from remote file with all filter types
    gpio.read('https://data.source.coop/fiboa/data/si/si-2024.parquet').extract(
        columns=['id', 'area', 'crop:name', 'farm:organic'],
        bbox=(450000, 50000, 500000, 100000),
        where='"crop:name" = \'wheat\' AND area > 10000'
    ).write('wheat_subset.parquet')
    ```

## Limiting Results

Limit the number of rows extracted, useful for testing or sampling:

=== "CLI"

    ```bash
    # Extract first 1000 matching rows
    gpio extract data.parquet sample.parquet --limit 1000

    # Extract first 100 hotels in bbox
    gpio extract places.parquet hotels_sample.parquet \
      --bbox -122.5,37.7,-122.3,37.8 \
      --where "category = 'hotel'" \
      --limit 100
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Extract first 1000 matching rows
    gpio.read('data.parquet').extract(limit=1000).write('sample.parquet')

    # Extract first 100 hotels in bbox
    gpio.read('places.parquet').extract(
        bbox=(-122.5, 37.7, -122.3, 37.8),
        where="category = 'hotel'",
        limit=100
    ).write('hotels_sample.parquet')
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

## Extracting from BigQuery

Extract data directly from BigQuery tables to GeoParquet. BigQuery `GEOGRAPHY` columns are automatically converted to GeoParquet geometry with spherical edges.

### Basic Usage

=== "CLI"

    ```bash
    # Extract entire table
    gpio extract bigquery myproject.geodata.buildings output.parquet

    # Extract with row limit
    gpio extract bigquery myproject.geodata.buildings output.parquet --limit 10000
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Read from BigQuery
    table = gpio.Table.from_bigquery('myproject.geodata.buildings')
    table.write('output.parquet')

    # With limit
    table = gpio.Table.from_bigquery('myproject.geodata.buildings', limit=10000)
    ```

### Filtering Data

Apply filters that are pushed down to BigQuery for efficient querying:

=== "CLI"

    ```bash
    # WHERE filter (BigQuery SQL syntax)
    gpio extract bigquery myproject.geodata.buildings output.parquet \
      --where "area_sqm > 1000 AND building_type = 'commercial'"

    # Select specific columns
    gpio extract bigquery myproject.geodata.buildings output.parquet \
      --include-cols "id,name,geography,area_sqm"

    # Combined filters
    gpio extract bigquery myproject.geodata.buildings output.parquet \
      --include-cols "id,name,geography" \
      --where "updated_date > '2024-01-01'" \
      --limit 50000
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # With filtering
    table = gpio.Table.from_bigquery(
        'myproject.geodata.buildings',
        where="area_sqm > 1000",
        columns=['id', 'name', 'geography', 'area_sqm'],
        limit=50000
    )
    ```

### Spatial Filtering with Bounding Box

Filter data spatially using a bounding box:

=== "CLI"

    ```bash
    # Filter to San Francisco area
    gpio extract bigquery myproject.geodata.buildings output.parquet \
      --bbox -122.52,37.70,-122.35,37.82
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Filter to San Francisco area
    table = gpio.Table.from_bigquery(
        'myproject.geodata.buildings',
        bbox="-122.52,37.70,-122.35,37.82"
    )
    ```

### Bbox Filtering Mode: Server vs Local

When you specify a `--bbox`, the spatial filter can be applied in two places:

1. **Server-side (BigQuery)**: The filter is pushed to BigQuery using `ST_INTERSECTS()`, so only matching rows are transferred
2. **Local (DuckDB)**: All data is fetched from BigQuery, then filtered locally in DuckDB

The `--bbox-mode` option controls this behavior:

| Mode | Description |
|------|-------------|
| `auto` | (Default) Automatically chooses based on table size |
| `server` | Always push spatial filter to BigQuery |
| `local` | Always filter locally in DuckDB |

#### Understanding the Tradeoffs

**Server-side filtering** is better for large tables because:

- Only matching rows are transferred, reducing data movement
- BigQuery's spatial indexing can accelerate the query
- Less memory usage locally

**Local filtering** is better for smaller tables because:

- Avoids the overhead of BigQuery's spatial function execution
- Uses DuckDB's efficient geometry routines once data is local
- More predictable performance for small datasets

The `--bbox-threshold` option sets the row count where `auto` mode switches from local to server filtering (default: 500,000 rows).

#### How Auto Mode Works

In `auto` mode, gpio checks the table's row count from BigQuery metadata:

- Tables **below** the threshold use local filtering
- Tables **at or above** the threshold use server-side filtering

This heuristic balances the overhead of spatial function execution against data transfer costs.

#### Examples

=== "CLI"

    ```bash
    # Force server-side filtering (good for very large tables)
    gpio extract bigquery myproject.geodata.global_buildings output.parquet \
      --bbox -122.52,37.70,-122.35,37.82 \
      --bbox-mode server

    # Force local filtering (good for small tables with complex geometries)
    gpio extract bigquery myproject.geodata.city_parks output.parquet \
      --bbox -122.52,37.70,-122.35,37.82 \
      --bbox-mode local

    # Adjust the threshold for auto mode (use server for tables > 100K rows)
    gpio extract bigquery myproject.geodata.buildings output.parquet \
      --bbox -122.52,37.70,-122.35,37.82 \
      --bbox-threshold 100000

    # Higher threshold (use server only for very large tables > 1M rows)
    gpio extract bigquery myproject.geodata.buildings output.parquet \
      --bbox -122.52,37.70,-122.35,37.82 \
      --bbox-threshold 1000000
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Force server-side filtering
    table = gpio.Table.from_bigquery(
        'myproject.geodata.global_buildings',
        bbox="-122.52,37.70,-122.35,37.82",
        bbox_mode="server"
    )

    # Force local filtering
    table = gpio.Table.from_bigquery(
        'myproject.geodata.city_parks',
        bbox="-122.52,37.70,-122.35,37.82",
        bbox_mode="local"
    )

    # Custom threshold
    table = gpio.Table.from_bigquery(
        'myproject.geodata.buildings',
        bbox="-122.52,37.70,-122.35,37.82",
        bbox_threshold=100000
    )
    ```

#### When to Change the Defaults

Consider using `--bbox-mode server` when:

- Your table has millions of rows
- You're filtering to a small geographic area (high selectivity)
- Network bandwidth is limited

Consider using `--bbox-mode local` when:

- Your table has fewer than 500K rows
- You're filtering to a large area (low selectivity)
- The table contains complex geometries that are slow to test server-side

Consider adjusting `--bbox-threshold` when:

- You consistently work with tables of a certain size
- You've benchmarked and found a different crossover point for your data
- Your BigQuery pricing tier or network conditions differ from typical

### Authentication

The command uses Google Cloud credentials in this order:

1. **--credentials-file**: Explicit service account JSON file
2. **GOOGLE_APPLICATION_CREDENTIALS**: Environment variable pointing to JSON file
3. **gcloud auth**: Application default credentials from `gcloud auth application-default login`

```bash
# Using service account file
gpio extract bigquery myproject.geodata.table output.parquet \
  --credentials-file /path/to/service-account.json

# Using environment variable
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
gpio extract bigquery myproject.geodata.table output.parquet

# Using gcloud auth (for development)
gcloud auth application-default login
gpio extract bigquery myproject.geodata.table output.parquet
```

### GEOGRAPHY Column Handling

BigQuery `GEOGRAPHY` columns are automatically converted to GeoParquet geometry:

- GEOGRAPHY data is returned in WGS84 (EPSG:4326)
- The geometry column is auto-detected by common names (`geography`, `geom`, `geometry`)
- Use `--geography-column` to specify explicitly if needed

```bash
# Explicit geography column
gpio extract bigquery myproject.geodata.parcels output.parquet \
  --geography-column "parcel_boundary"
```

### Spherical Edges

BigQuery GEOGRAPHY uses **spherical geodesic edges** (S2-based), meaning lines between points follow the shortest path on a sphere rather than planar straight lines. This is automatically reflected in the output GeoParquet metadata:

```json
{
  "columns": {
    "geometry": {
      "edges": "spherical",
      "orientation": "counterclockwise"
    }
  }
}
```

This ensures downstream tools correctly interpret the geometry edges. Most GIS tools assume planar edges by default, so the `edges: "spherical"` metadata is important for accurate analysis.

### Limitations

!!! warning "Important Limitations"

    **Views and External Tables Not Supported**

    The BigQuery Storage Read API cannot read from:

    - Logical views
    - Materialized views
    - External tables (e.g., tables backed by Cloud Storage)

    You must extract from native BigQuery tables. If you need data from a view,
    create a table from the view first:

    ```sql
    CREATE TABLE mydataset.mytable AS SELECT * FROM mydataset.myview;
    ```

Other limitations:

- **BIGNUMERIC columns**: Not supported (76-digit precision exceeds DuckDB's 38-digit limit)
- **Large results**: Consider using `--limit` and `--where` to reduce data transfer

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

=== "CLI"

    ```bash
    # Get a small sample for testing
    gpio extract large_file.parquet sample.parquet --limit 1000

    # Get sample from specific area
    gpio extract large_file.parquet sample.parquet \
      --bbox 0,0,1,1 \
      --limit 100
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Get a small sample for testing
    gpio.read('large_file.parquet').extract(limit=1000).write('sample.parquet')

    # Get sample from specific area
    gpio.read('large_file.parquet').extract(bbox=(0, 0, 1, 1), limit=100).write('sample.parquet')
    ```

### Extract by Category

=== "CLI"

    ```bash
    # Extract all features of a specific type
    gpio extract data.parquet restaurants.parquet \
      --where "category = 'restaurant'"

    # Extract multiple categories
    gpio extract data.parquet food_places.parquet \
      --where "category IN ('restaurant', 'cafe', 'bakery')"
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Extract all features of a specific type
    gpio.read('data.parquet').extract(where="category = 'restaurant'").write('restaurants.parquet')

    # Extract multiple categories
    gpio.read('data.parquet').extract(where="category IN ('restaurant', 'cafe', 'bakery')").write('food_places.parquet')
    ```

### Extract Recent Data

=== "CLI"

    ```bash
    # Extract data updated this year
    gpio extract data.parquet recent.parquet \
      --where "updated_at >= '2024-01-01'"

    # Extract data from specific time range
    gpio extract data.parquet range.parquet \
      --where "created_at BETWEEN '2024-01-01' AND '2024-06-30'"
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Extract data updated this year
    gpio.read('data.parquet').extract(where="updated_at >= '2024-01-01'").write('recent.parquet')

    # Extract data from specific time range
    gpio.read('data.parquet').extract(
        where="created_at BETWEEN '2024-01-01' AND '2024-06-30'"
    ).write('range.parquet')
    ```

### Extract Non-Spatial Subset

=== "CLI"

    ```bash
    # Extract as attribute table (no geometry)
    gpio extract data.parquet attributes.parquet \
      --include-cols id,name,category,population \
      --exclude-cols geometry,bbox
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Extract as attribute table (no geometry)
    gpio.read('data.parquet').extract(
        columns=['id', 'name', 'category', 'population'],
        exclude_columns=['geometry', 'bbox']
    ).write('attributes.parquet')
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
