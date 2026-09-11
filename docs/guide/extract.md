# Extracting Data

The `extract` command allows you to filter and subset GeoParquet files by columns, spatial extent, and attribute values. It's useful for creating smaller datasets, extracting regions of interest, or selecting specific attributes.

## Basic Usage

=== "CLI"

    <!-- doctest: menu -->
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
    # Extract only the id and name columns (plus geometry and bbox)
    gpio extract places.parquet subset.parquet --include-cols fsq_place_id,name
    ```

    ```bash
    # Extract multiple attribute columns
    gpio extract places.parquet subset.parquet --include-cols name,address,placemaker_url
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Extract only the id and name columns (plus geometry and bbox)
    gpio.read('places.parquet').extract(columns=['fsq_place_id', 'name']).write('subset.parquet')

    # Extract multiple attribute columns
    gpio.read('places.parquet').extract(
        columns=['name', 'address', 'placemaker_url']
    ).write('attributes.parquet')
    ```

Unknown column names are rejected up front: `extract()` names the missing column
rather than dropping it silently.

### Excluding Columns

Remove unwanted columns from the output:

=== "CLI"

    ```bash
    # Exclude a large or unnecessary column
    gpio extract data.parquet output.parquet --exclude-cols placemaker_url

    # Exclude multiple columns
    gpio extract data.parquet output2.parquet --exclude-cols placemaker_url,address
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Exclude a large or unnecessary column
    gpio.read('data.parquet').extract(exclude_columns=['placemaker_url']).write('output.parquet')

    # Exclude multiple columns
    gpio.read('data.parquet').extract(
        exclude_columns=['placemaker_url', 'address']
    ).write('output2.parquet')
    ```

### Combining Include and Exclude

You can combine both to control exactly which columns appear, including removing geometry or bbox columns:

=== "CLI"

    ```bash
    # Include specific columns but exclude geometry (for non-spatial export).
    # Exclude bbox too: it is a covering of the geometry that was just dropped,
    # and is otherwise added back automatically.
    gpio extract data.parquet output.parquet \
      --include-cols fsq_place_id,name,address \
      --exclude-cols geometry,bbox

    # Include columns but exclude bbox to save space
    gpio extract data.parquet output2.parquet \
      --include-cols fsq_place_id,name \
      --exclude-cols bbox
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Include specific columns but exclude geometry (for non-spatial export).
    # Exclude bbox too: it is a covering of the geometry that was just dropped,
    # and is otherwise added back automatically.
    gpio.read('data.parquet').extract(
        columns=['fsq_place_id', 'name', 'address'],
        exclude_columns=['geometry', 'bbox']
    ).write('output.parquet')

    # Include columns but exclude bbox to save space
    gpio.read('data.parquet').extract(
        columns=['fsq_place_id', 'name'],
        exclude_columns=['bbox']
    ).write('output2.parquet')
    ```

## Spatial Filtering

### Bounding Box Filter

Filter features by a rectangular bounding box. The bbox is specified as `xmin,ymin,xmax,ymax` in the same coordinate system as your data.

=== "CLI"

    ```bash
    # Extract features in San Francisco area (WGS84 coordinates)
    gpio extract places.parquet sf_places.parquet \
      --bbox -122.5,37.7,-122.3,37.8
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Extract from remote FIBOA dataset (projected coordinates)
    gpio extract https://data.source.coop/fiboa/data/si/si-2024.parquet slovenia_subset.parquet \
      --bbox 450000,50000,500000,100000

    # Extract from S3 building dataset (WGS84 coordinates)
    gpio extract s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/geoparquet/by_country_s2/country_iso=AGO/2017612633061982208.parquet angola_subset.parquet \
      --bbox 13.0,-9.0,14.0,-8.0
    ```

=== "Python"

    <!-- doctest: skip="reads an example URL that is an illustration, not a live file" -->
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
    S3, GCS, Azure, and HTTPS URLs are supported via DuckDB's httpfs extension. A URL is used exactly as you paste it — gpio takes it as already percent-encoded and never re-encodes it. See the [Remote Files guide](remote-files.md) for credential configuration.

**CRS Awareness**: The tool detects coordinate system mismatches. If your bbox looks like lat/long coordinates but the data uses a projected CRS, you'll get a helpful warning showing the data's actual bounds.

!!! note "Metadata `bbox` and `geometry_types` describe the output"
    The GeoParquet metadata `bbox` and `geometry_types` are derived from the
    data, so `extract` recomputes them whenever the output is not a faithful
    copy of a single input file:

    - **A row filter** (`--bbox`, `--geometry`, `--where`, `--limit`) — both are
      recomputed over only the surviving rows rather than the full input extent.
      A filter that keeps zero rows omits `bbox` entirely (it is optional per
      spec).
    - **A glob or directory input** — the merged output describes *all* the
      inputs, not just the first file whose footer was read. (Carrying the first
      file's `bbox` would under-cover the merge, and conformant readers skip
      data outside a declared bbox.)
    - **A geometry repair** (`--repair-geometry`, on by default) that actually
      fixed a row — `ST_MakeValid` can change a geometry's type, so a repaired
      self-intersecting `Polygon` comes back a `MultiPolygon` and the input's
      `geometry_types` would under-declare the output. A repair pass that found
      nothing invalid changes no row and keeps the carried stats.
    - **`--exclude-cols`** — metadata that references a dropped column is
      pruned: excluding the `bbox` column drops the `covering` entry, and
      excluding the geometry column writes plain Parquet with no `geo` metadata
      at all.

    An unfiltered, unrepaired, single-file column-only extract preserves the
    input's `bbox`/`geometry_types` unchanged.

    Recomputing costs one extra aggregate pass over the (already filtered)
    query — on the order of 30% of a plain copy for a few million rows. Use
    `--verbose` to see when it runs.

### Geometry Filter

Filter features by intersection with any geometry, not just rectangles.

!!! note "CLI Only"
    Geometry filtering with arbitrary shapes is currently only available via the CLI.
    For rectangular regions, use the `bbox` parameter in Python.

<!-- doctest: menu -->
```bash
# Filter by inline WKT polygon
gpio extract data.parquet subset.parquet \
  --geometry "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))"

# Filter by inline GeoJSON
gpio extract data.parquet subset.parquet \
  --geometry '{"type":"Polygon","coordinates":[[[0,0],[0,10],[10,10],[10,0],[0,0]]]}'
```

<!-- doctest: skip="needs boundary.geojson, which the harness does not seed" -->
```bash
# Filter by geometry from file
gpio extract data.parquet subset.parquet --geometry @boundary.geojson

# Filter by geometry from stdin (useful in pipelines)
cat boundary.geojson | gpio extract data.parquet subset.parquet --geometry -

# Extract buildings within city boundary
gpio extract buildings.parquet city_buildings.parquet \
  --geometry @city_boundary.geojson
```

**FeatureCollection Handling**: If your GeoJSON file contains multiple features, use `--use-first-geometry`:

<!-- doctest: skip="needs regions.geojson, which the harness does not seed" -->
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
    # Filter by string pattern
    gpio extract data.parquet output.parquet --where "name LIKE '%Hotel%'"
    ```

    <!-- doctest: skip="filters on 'population', a column the sample data does not have" -->
    ```bash
    # Filter by numeric value
    gpio extract data.parquet output.parquet --where "population > 10000"

    # Filter by string equality
    gpio extract data.parquet output.parquet --where "status = 'active'"

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
    ```

    <!-- doctest: skip="filters on 'population', a column the sample data does not have" -->
    ```python
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

<!-- doctest: skip="filters on 'crop:name', a column the sample data does not have" -->
```bash
# Column name with colon - use single quotes around the whole WHERE clause
gpio extract data.parquet output.parquet \
  --where '"crop:name" = '\''wheat'\'''
```

<!-- doctest: skip="filters on 'building-type', a column the sample data does not have" -->
```bash
# Column name with dash
gpio extract data.parquet output.parquet \
  --where '"building-type" = '\''residential'\'''

# Column name with dot
gpio extract data.parquet output.parquet \
  --where '"height.meters" > 50'
```

**Alternative escaping (more portable):**

<!-- doctest: skip="filters on 'crop:name', a column the sample data does not have" -->
```bash
# Use backslash escaping
gpio extract data.parquet output.parquet \
  --where "\"crop:name\" = 'wheat'"

# Multiple conditions with special column names
gpio extract data.parquet output.parquet \
  --where "\"crop:name\" = 'wheat' AND \"farm:organic\" = true"
```

**Real-world examples with the FIBOA dataset:**

<!-- doctest: network -->
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

<!-- doctest: skip="filters on 'area', a column the sample data does not have" -->
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

<!-- doctest: menu -->
```bash
# String functions
gpio extract data.parquet output.parquet \
  --where "LOWER(name) LIKE '%park%'"

# Case-insensitive search
gpio extract data.parquet output.parquet \
  --where "name ILIKE '%hotel%'"
```

<!-- doctest: skip="filters on 'population', a column the sample data does not have" -->
```bash
# Combine multiple conditions
gpio extract data.parquet output.parquet \
  --where "population > 5000 AND (status = 'active' OR priority = 'high')"

# Math operations
gpio extract data.parquet output.parquet \
  --where "area_km2 / population < 0.001"
```

## Combining Filters

Combine column selection, spatial filtering, and WHERE clauses:

=== "CLI"

    <!-- doctest: skip="names columns the sample data does not have (rating)" -->
    ```bash
    # Extract specific columns in a bbox with attribute filter
    gpio extract places.parquet hotels.parquet \
      --include-cols name,address,rating \
      --bbox -122.5,37.7,-122.3,37.8 \
      --where "category = 'hotel' AND rating >= 4"
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
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

    <!-- doctest: skip="uses attribute columns the sample dataset does not carry" -->
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
    ```

    <!-- doctest: skip="filters on 'category', a column the sample data does not have" -->
    ```bash
    # Extract first 100 hotels in bbox
    gpio extract places.parquet hotels_sample.parquet \
      --bbox -122.5,37.7,-122.3,37.8 \
      --where "category = 'hotel'" \
      --limit 100
    ```

=== "Python"

    <!-- doctest: skip="filters on 'category', a column the sample data does not have" -->
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

<!-- doctest: skip="needs cloud credentials" -->
```bash
# Extract from HTTP URL
gpio extract https://data.source.coop/fiboa/data/si/si-2024.parquet subset.parquet \
  --bbox 450000,50000,500000,100000

# Extract from S3 (uses AWS credentials)
gpio extract s3://my-bucket/data.parquet output.parquet \
  --where "category = 'important'"

# Extract from S3 with specific profile
gpio extract s3://my-bucket/data.parquet output.parquet \
  --aws-profile my-aws-profile \
  --bbox 0,0,10,10
```

## Extracting from BigQuery

Extract data directly from BigQuery tables to GeoParquet. BigQuery `GEOGRAPHY` columns are automatically converted to GeoParquet geometry with spherical edges.

### Basic Usage

=== "CLI"

    <!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
    ```bash
    # Extract entire table
    gpio extract bigquery myproject.geodata.buildings output.parquet

    # Extract with row limit
    gpio extract bigquery myproject.geodata.buildings output.parquet --limit 10000
    ```

=== "Python"

    <!-- doctest: skip="needs cloud credentials" -->
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

    <!-- doctest: skip="needs cloud credentials" -->
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

    <!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
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

    <!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
    ```bash
    # Filter to San Francisco area
    gpio extract bigquery myproject.geodata.buildings output.parquet \
      --bbox -122.52,37.70,-122.35,37.82
    ```

=== "Python"

    <!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
    ```python
    import geoparquet_io as gpio

    # Filter to San Francisco area
    table = gpio.Table.from_bigquery(
        'myproject.geodata.buildings',
        bbox="-122.52,37.70,-122.35,37.82"
    )
    ```

!!! note "Bbox format differences"
    `Table.from_bigquery()` accepts `bbox` as a string (e.g., `"-122.52,37.70,-122.35,37.82"`), while `Table.extract()` expects a tuple (e.g., `(-122.52, 37.70, -122.35, 37.82)`).

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

    <!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
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

    <!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
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

<!-- doctest: skip="needs cloud credentials" -->
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

### Geometry Column Handling

#### Native GEOGRAPHY Columns

BigQuery `GEOGRAPHY` columns (native spatial type) are automatically detected and converted to GeoParquet geometry:

- GEOGRAPHY data is returned in WGS84 (EPSG:4326)
- Only native GEOGRAPHY-typed columns are auto-detected
- Use `--geography-column` to specify explicitly if the column has an unusual name

<!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
```bash
# Explicit geography column
gpio extract bigquery myproject.geodata.parcels output.parquet \
  --geography-column "parcel_boundary"
```

#### VARCHAR Columns (WKT/GeoJSON)

If your geometry is stored as text in a VARCHAR column (WKT or GeoJSON format), use `--geography-column` with `--geometry-format`:

=== "CLI"

    <!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
    ```bash
    # Parse WKT geometry from VARCHAR column
    gpio extract bigquery myproject.dataset.table output.parquet \
      --geography-column geometry --geometry-format wkt

    # Parse GeoJSON geometry from VARCHAR column
    gpio extract bigquery myproject.dataset.table output.parquet \
      --geography-column geojson_col --geometry-format geojson
    ```

=== "Python"

    <!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
    ```python
    import geoparquet_io as gpio

    # Parse WKT geometry from VARCHAR column
    table = gpio.Table.from_bigquery(
        'myproject.dataset.table',
        geography_column='geometry',
        geometry_format='wkt'
    )
    table.write('output.parquet')
    ```

#### No Geometry Column

If no geometry column is found, the output is plain Parquet (not GeoParquet):

<!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
```bash
# Extract table without geometry - outputs plain Parquet
gpio extract bigquery myproject.dataset.plain_table output.parquet
```

### Spherical Edges

BigQuery GEOGRAPHY uses **spherical geodesic edges** (S2-based), meaning lines between points follow the shortest path on a sphere rather than planar straight lines. This is automatically reflected in the output GeoParquet metadata for native GEOGRAPHY columns:

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

**Edge interpretation defaults:**

| Geometry Source | Default Edges |
|-----------------|---------------|
| Native GEOGRAPHY column | `spherical` |
| VARCHAR column (WKT/GeoJSON) | `planar` (no edges metadata) |

You can override the default with `--edges`:

<!-- doctest: skip="queries a table that exists only in the reader's own warehouse" -->
```bash
# Force spherical edges for VARCHAR geometry
gpio extract bigquery myproject.dataset.table output.parquet \
  --geography-column geometry --geometry-format wkt --edges spherical

# Force planar edges for native GEOGRAPHY (unusual)
gpio extract bigquery myproject.dataset.table output.parquet \
  --edges planar
```

This ensures downstream tools correctly interpret the geometry edges. Most GIS tools assume planar edges by default, so the `edges: "spherical"` metadata is important for accurate analysis with native GEOGRAPHY data.

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
- **`--write-memory` bounds the scan, not the write**: this command writes through
  PyArrow, so the value is applied as DuckDB's `memory_limit` on the connection
  running the BigQuery scan. It is not a cap on the command's peak memory — see
  [Write Strategies](write-strategies.md#explicit-memory-limits)

## Extracting from ArcGIS Feature Services

Extract data directly from ArcGIS REST Feature Services to GeoParquet. Features are downloaded with server-side filtering; CLI outputs default to ZSTD compression, bbox metadata, and Hilbert spatial ordering, while Python defaults are described below under Output Optimization.

### Basic Usage

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Extract from public ArcGIS Feature Service
    gpio extract arcgis https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Counties/FeatureServer/0 counties.parquet

    # Extract with row limit
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet --limit 1000
    ```

=== "Python"

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    import geoparquet_io as gpio

    # Read from ArcGIS Feature Service
    table = gpio.extract_arcgis(
        service_url='https://services.arcgis.com/.../FeatureServer/0'
    )
    table.write('output.parquet')
    ```

### Server-Side Filtering

Filters are pushed to the ArcGIS server for efficient querying—only matching data is downloaded:

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # WHERE filter (server-side)
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet \
      --where "STATE_NAME = 'California'"

    # Bounding box filter (server-side)
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet \
      --bbox -122.5,37.5,-122.0,38.0

    # Select specific columns (server-side)
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet \
      --include-cols NAME,POPULATION,STATE_NAME

    # Combined filters
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet \
      --bbox -122.5,37.5,-122.0,38.0 \
      --where "POPULATION > 100000" \
      --include-cols NAME,POPULATION \
      --limit 500
    ```

=== "Python"

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    import geoparquet_io as gpio

    # WHERE filter (server-side)
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        where="STATE_NAME = 'California'"
    )
    table.write("output.parquet")

    # Bounding box filter (server-side)
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        bbox=(-122.5, 37.5, -122.0, 38.0)
    )
    table.write("output.parquet")

    # Select specific columns (server-side)
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        include_cols="NAME,POPULATION,STATE_NAME"
    )
    table.write("output.parquet")

    # Combined filters
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        bbox=(-122.5, 37.5, -122.0, 38.0),
        where="POPULATION > 100000",
        include_cols="NAME,POPULATION",
        limit=500
    )
    table.write("output.parquet")
    ```

### Authentication

For protected services, provide credentials:

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Using direct token
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet \
      --token "your_arcgis_token"

    # Using token file
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet \
      --token-file /path/to/token.txt

    # Using username/password (generates token via ArcGIS REST API)
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet \
      --username myuser \
      --password mypassword

    # Enterprise portal authentication
    gpio extract arcgis https://enterprise.example.com/arcgis/rest/services/.../FeatureServer/0 output.parquet \
      --username myuser \
      --password mypassword \
      --portal-url https://enterprise.example.com/portal
    ```

=== "Python"

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    import geoparquet_io as gpio

    # Using direct token
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        token="your_arcgis_token"
    )
    table.write("output.parquet")

    # Using token file
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        token_file="/path/to/token.txt"
    )
    table.write("output.parquet")

    # Using username/password (generates token via ArcGIS REST API)
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        username="myuser",
        password="mypassword"
    )
    table.write("output.parquet")

    # Enterprise portal authentication
    table = gpio.extract_arcgis(
        service_url="https://enterprise.example.com/arcgis/rest/services/.../FeatureServer/0",
        username="myuser",
        password="mypassword",
        portal_url="https://enterprise.example.com/portal"
    )
    table.write("output.parquet")
    ```

### Output Optimization

By default, ArcGIS extracts include bbox metadata and Hilbert spatial ordering for optimal query performance:

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Skip Hilbert ordering (faster extraction, less optimal queries)
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet \
      --skip-hilbert

    # Skip bbox column (smaller file, slower spatial filtering)
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet \
      --skip-bbox

    # Custom compression
    gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet \
      --compression GZIP \
      --compression-level 6
    ```

=== "Python"

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    import geoparquet_io as gpio

    # Skip Hilbert ordering (faster extraction, less optimal queries)
    # Python API does not apply Hilbert sorting by default - just don't chain .sort_hilbert()
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0"
    )
    table.write("output.parquet")

    # Skip bbox column (smaller file, slower spatial filtering)
    # Python API does not add bbox by default - just don't chain .add_bbox()
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0"
    )
    table.write("output.parquet")

    # Custom compression (equivalent to --compression GZIP --compression-level 6)
    # Pass compression and compression_level to table.write()
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0"
    )
    table.write("output.parquet", compression="GZIP", compression_level=6)
    ```

    !!! note "CLI vs Python API Defaults"
        The CLI applies Hilbert sorting and bbox by default (use `--skip-hilbert` and `--skip-bbox` to disable).
        The Python API does NOT apply these by default—chain `.sort_hilbert()` and `.add_bbox()` explicitly if needed.

### Output CRS

By default, `gpio extract arcgis` fetches GeoJSON from the server and outputs WGS84 (CRS84). Use `--output-crs` to preserve the layer's native coordinate reference system or request a specific CRS instead.

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Preserve the layer's native CRS (fetches EsriJSON with the layer's advertised SR)
    gpio extract arcgis "https://services.arcgis.com/.../FeatureServer/0" out.parquet \
      --output-crs native

    # Request a specific CRS (fetches EsriJSON with outSR set to EPSG:25830)
    gpio extract arcgis "https://services.arcgis.com/.../FeatureServer/0" out.parquet \
      --output-crs EPSG:25830
    ```

=== "Python"

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    import geoparquet_io as gpio

    # Preserve the layer's native CRS
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        output_crs="native"
    )
    table.write("out.parquet")

    # Request a specific CRS
    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        output_crs="EPSG:25830"
    )
    table.write("out.parquet")
    ```

The output GeoParquet is tagged with the CRS the server actually returned. If the server returns a different CRS than the one requested, a warning is emitted and the file is tagged with the actual CRS. Legacy Esri codes are normalized to their EPSG equivalents (102100 becomes 3857), and layers that advertise an Esri-specific WKID with no EPSG equivalent (for example 102039) are tagged with the `ESRI` authority so the CRS stays resolvable. A WKID that resolves to neither an EPSG nor an ESRI definition is left untagged rather than labeled with a code no reader can resolve.

With `--output-crs native`, the layer's spatial reference is taken from its advertised WKID, or recovered from its WKT when no WKID is published. If the WKT maps to no EPSG code, the extraction fails with a clear message so you can pass an explicit `--output-crs EPSG:<code>` instead.

| Value | Behavior |
|-------|----------|
| *(omitted)* | Fetches GeoJSON, outputs WGS84 (default) |
| `native` | Fetches EsriJSON with the layer's advertised spatial reference |
| `EPSG:<code>` | Fetches EsriJSON with `outSR` set to the requested EPSG code |

### Geometry generalization

Some layers have very large or vertex-dense polygons that are slow to download or that overwhelm a server. Use `--max-allowable-offset` to ask the server to simplify each geometry before it is sent (the ArcGIS `maxAllowableOffset` parameter, a Douglas-Peucker tolerance). This reduces the vertex count per feature, which is different from `--limit`, which only caps the number of features.

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Generalize heavy polygons to a 0.005 unit tolerance in the output CRS
    gpio extract arcgis "https://services.arcgis.com/.../FeatureServer/0" out.parquet \
      --output-crs native --max-allowable-offset 0.005
    ```

=== "Python"

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    import geoparquet_io as gpio

    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        output_crs="native",
        max_allowable_offset=0.005,
    )
    table.write("out.parquet")
    ```

The tolerance is in the units of the request's output CRS. With `--output-crs` set, that is the chosen CRS's unit, degrees for a geographic CRS and the projected unit (often meters) for a projected CRS. Without `--output-crs` the request is anchored to WGS84, so the tolerance is in degrees. The value must be positive, a bad value fails fast before any network call.

### Slow servers and timeouts

Each HTTP request defaults to a 60-second timeout. FeatureServer layers with very large or vertex-dense polygons (e.g. national or regional boundaries with detailed coastlines) can take the server several minutes to serialize a single page, which fails with an HTTP timeout. Use `--timeout` to raise the per-request limit:

=== "CLI"

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # Allow up to 5 minutes per request for heavy polygon layers
    gpio extract arcgis "https://services.arcgis.com/.../FeatureServer/0" out.parquet \
      --timeout 300
    ```

=== "Python"

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    import geoparquet_io as gpio

    table = gpio.extract_arcgis(
        service_url="https://services.arcgis.com/.../FeatureServer/0",
        timeout=300,
    )
    table.write("out.parquet")
    ```

Combine `--timeout` with `--batch-size` (smaller pages) or `--max-allowable-offset` (fewer vertices per feature) when a layer is both slow and heavy.

### Finding Service URLs

ArcGIS Feature Service URLs follow this pattern:

    https://<server>/arcgis/rest/services/<folder>/<service>/FeatureServer/<layer_id>

To find service URLs:

1. Go to the ArcGIS REST Services Directory (usually `https://server/arcgis/rest/services`)
2. Navigate to the feature service
3. Click on a specific layer (0, 1, 2, etc.)
4. Copy the URL from your browser

!!! note "Layer ID Required"
    The URL must include the layer ID (e.g., `/FeatureServer/0`). Services often have multiple layers—use the REST directory to find the correct one.

## Extracting from WFS Services

Web Feature Service (WFS) is an OGC standard for serving vector geospatial data over HTTP. Many government agencies and organizations publish data via WFS. gpio streams each response to disk and parses it locally with DuckDB, making extraction very fast.

### Basic Usage

=== "CLI"
    <!-- doctest: skip="illustrative endpoint" -->
    ```bash
    # List available layers
    gpio extract wfs https://geo.example.com/wfs

    # Extract a layer to GeoParquet
    gpio extract wfs https://geo.example.com/wfs cities output.parquet

    # Extract with limit
    gpio extract wfs https://geo.example.com/wfs cities output.parquet --limit 1000

    # Verbose mode shows progress
    gpio extract wfs https://geo.example.com/wfs cities output.parquet --verbose
    ```

=== "Python"
    <!-- doctest: skip="illustrative endpoint" -->
    ```python
    from geoparquet_io.api import Table

    # Extract and chain operations
    Table.from_wfs('https://geo.example.com/wfs', 'cities', limit=1000) \
        .add_bbox() \
        .sort_hilbert() \
        .write('cities.parquet')
    ```

### Bbox Filtering

Spatial filtering can be applied server-side (pushed to WFS) or locally (after download):

=== "CLI"
    <!-- doctest: skip="illustrative endpoint" -->
    ```bash
    # Server-side bbox filter (default for WFS)
    gpio extract wfs https://geo.example.com/wfs cities output.parquet \
        --bbox -122.5,37.5,-122.0,38.0

    # Explicitly choose server-side
    gpio extract wfs https://geo.example.com/wfs cities output.parquet \
        --bbox -122.5,37.5,-122.0,38.0 --bbox-mode server

    # Force local filtering (download all, then filter)
    gpio extract wfs https://geo.example.com/wfs cities output.parquet \
        --bbox -122.5,37.5,-122.0,38.0 --bbox-mode local
    ```

=== "Python"
    <!-- doctest: skip="illustrative endpoint" -->
    ```python
    from geoparquet_io.api import Table

    table = Table.from_wfs(
        'https://geo.example.com/wfs',
        'cities',
        bbox=(-122.5, 37.5, -122.0, 38.0)
    )
    ```

### Parallel Fetching for Large Datasets

For datasets with 1 million+ features, use parallel pagination to avoid server timeouts:

=== "CLI"
    <!-- doctest: skip="illustrative endpoint" -->
    ```bash
    # Parallel extraction with 4 workers
    gpio extract wfs https://geo.example.com/wfs large_layer output.parquet \
        --workers 4

    # Extract multiple layers in parallel to a directory
    gpio extract wfs https://geo.example.com/wfs layer1,layer2,layer3 ./output/ \
        --parallel-layers 3

    # For most datasets under 100K features, single-stream is faster
    gpio extract wfs https://geo.example.com/wfs cities output.parquet
    ```

=== "Python"
    <!-- doctest: skip="illustrative endpoint" -->
    ```python
    from geoparquet_io.api import Table

    # Parallel extraction
    table = Table.from_wfs(
        'https://geo.example.com/wfs',
        'large_layer',
        max_workers=4
    )

    # Extract multiple layers (tune defaults for large datasets)
    from geoparquet_io.api import ops
    results = ops.from_wfs_layers(
        'https://geo.example.com/wfs',
        ['layer1', 'layer2', 'layer3'],
        './output/',
        parallel_layers=3,
        max_workers=2,        # parallel page fetches per layer
        page_size=100000,     # features per WFS request page
        auto_tile=True        # subdivide bbox when the server caps responses
    )
    ```

| Option | Default | Description |
|--------|---------|-------------|
| `--workers` | 1 | Number of parallel requests (1-10) |
| `--page-size` | 100000 | Features per WFS request page |
| `--parallel-layers` | 1 | Concurrent layer extraction for multi-layer requests |
| `--auto-tile` | enabled | Auto-subdivide bbox when server caps response |

!!! tip "Auto-Tiling for Capped Servers"
    Many WFS servers impose maxFeatures limits (e.g., 1M features). When gpio detects a capped response, it automatically subdivides the bbox and fetches all features. Disable with `--no-auto-tile` if you want the capped response.

!!! tip "When to use parallel"
    - **Single stream (default)**: Fastest for datasets under ~100K features
    - **Parallel (`--workers 2-4`)**: For 1M+ feature datasets where timeouts occur

### Output Optimization

By default, WFS extracts include Hilbert spatial ordering and bbox columns:

<!-- doctest: skip="illustrative endpoint" -->
```bash
# Skip optimizations for faster extraction
gpio extract wfs https://geo.example.com/wfs cities output.parquet \
    --skip-hilbert \
    --skip-bbox

# Custom compression
gpio extract wfs https://geo.example.com/wfs cities output.parquet \
    --compression GZIP \
    --compression-level 6
```

### WFS Version

gpio supports WFS 1.0.0, 1.1.0, and 2.0.0. By default, auto-negotiation tries the newest version first:

<!-- doctest: skip="illustrative endpoint" -->
```bash
# Auto-negotiate (default) - tries 2.0.0, then 1.1.0, then 1.0.0
gpio extract wfs https://geo.example.com/wfs cities output.parquet

# Force specific version
gpio extract wfs https://geo.example.com/wfs cities output.parquet \
    --wfs-version 1.1.0
```

### Response Formats

gpio reads the formats a service advertises in its `GetCapabilities` response
and asks for the best one it can parse: GeoJSON first, because it is the
fastest to read, then GML. Nothing needs to be configured — a GeoJSON service
and a GML-only service are extracted with the same command.

Older WFS 1.0.0/1.1.0 deployments frequently offer GML and nothing else, and
some servers answer with GML even when a different format was requested. gpio
decides how to parse a response from the `Content-Type` the server actually
sent, so either case works. GML is read through GDAL, which resolves the
geometry, the attributes and the CRS the server used.

If a service advertises formats but none is one gpio can read, gpio omits
`outputFormat` from the request entirely and takes the server's own default,
rather than demanding a format the service has just said it does not offer. If
a service advertises nothing at all, gpio asks for GeoJSON as it always has.

### Axis Order

WFS 1.1.0+ with URN-format CRS (e.g., `urn:ogc:def:crs:EPSG::4326`) uses lat,lon axis order per OGC spec. gpio auto-detects this, but you can override:

<!-- doctest: skip="illustrative endpoint" -->
```bash
# Force lon,lat (XY) axis order
gpio extract wfs https://geo.example.com/wfs cities output.parquet \
    --bbox 4.82,50.44,4.92,50.48 \
    --axis-order xy

# Force lat,lon axis order
gpio extract wfs https://geo.example.com/wfs cities output.parquet \
    --axis-order latlon
```

### CRS Handling

gpio automatically negotiates the coordinate reference system with the WFS server:

<!-- doctest: skip="illustrative endpoint" -->
```bash
# Request specific CRS from server
gpio extract wfs https://geo.example.com/wfs cities output.parquet \
    --output-crs EPSG:3857
```

When a server reports the CRS it actually used (GeoServer and most WFS servers
echo it in the GeoJSON `crs` member, and a GML response carries an `srsName` on
its geometries), gpio trusts that declaration verbatim —
it never second-guesses a server-honored CRS by inspecting coordinates. This
matters for projected systems: a bounding box alone cannot distinguish, say,
EPSG:22174 (Argentine Gauss-Krüger) from EPSG:3857 (Web Mercator), so guessing
would silently mislabel the output and corrupt any later reprojection (#499).

If a server ignores `srsName` and returns a *different* CRS than requested,
gpio honors the real CRS rather than the one you asked for:

- with `--output-crs`, it reprojects from the server's actual CRS to your target;
- without `--output-crs`, it labels the output with the server's actual CRS and warns;
- with `--strict-crs`, it fails instead of proceeding.

<!-- doctest: skip="illustrative endpoint" -->
```bash
# Fail instead of warn when the server returns a different CRS than requested
gpio extract wfs https://geo.example.com/wfs cities output.parquet \
    --strict-crs
```

### Common Public WFS Services

- **Transport for Cairo**: `https://data.transportforcairo.com/geoserver/geonode/ows`
- **GeoServer Demo**: `https://demo.geoserver.org/geoserver/wfs`
- **NextGIS Demo** (GML-only): `https://demo.nextgis.com/api/resource/9748/wfs`
- State GIS portals (varies by state)
- Municipal open data portals

## Extracting from Carto SQL API

Extract data from Carto SQL API endpoints directly to GeoParquet. Carto's SQL API provides access to PostgreSQL/PostGIS tables via HTTP, commonly used by municipal open data portals.

### Basic Usage

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # Extract entire table
    gpio extract carto https://phl.carto.com/api/v2/sql \
        opa_properties_public output.parquet

    # Extract with row limit
    gpio extract carto https://phl.carto.com/api/v2/sql \
        opa_properties_public output.parquet --limit 10000
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    from geoparquet_io.api import ops

    # Extract from Carto
    table = ops.from_carto(
        'https://phl.carto.com/api/v2/sql',
        'opa_properties_public',
        limit=10000
    )
    ```

### Server-Side Filtering

Filters are pushed to the Carto server for efficient querying:

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # WHERE filter (PostgreSQL syntax)
    gpio extract carto https://phl.carto.com/api/v2/sql \
        opa_properties_public output.parquet \
        --where "category_code_description = 'SINGLE FAMILY'"

    # Bounding box filter (uses ST_Intersects on server)
    gpio extract carto https://phl.carto.com/api/v2/sql \
        opa_properties_public output.parquet \
        --bbox "-75.2,39.9,-75.1,40.0"

    # Select specific columns
    gpio extract carto https://phl.carto.com/api/v2/sql \
        opa_properties_public output.parquet \
        --include-cols "parcel_number,market_value,the_geom"

    # Combined filters
    gpio extract carto https://phl.carto.com/api/v2/sql \
        opa_properties_public output.parquet \
        --bbox "-75.2,39.9,-75.1,40.0" \
        --where "market_value > 100000" \
        --limit 5000
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    from geoparquet_io.api import ops

    # With filters
    table = ops.from_carto(
        'https://phl.carto.com/api/v2/sql',
        'opa_properties_public',
        where="category_code_description = 'SINGLE FAMILY'",
        bbox=(-75.2, 39.9, -75.1, 40.0),
        limit=5000
    )
    ```

### URL Format

The Carto SQL API URL follows this pattern:

    https://<account>.carto.com/api/v2/sql

You can provide either the full API URL or just the base domain:

<!-- doctest: network -->
```bash
# Full URL (explicit)
gpio extract carto https://phl.carto.com/api/v2/sql opa_properties_public output.parquet

# Base domain (api/v2/sql is appended automatically)
gpio extract carto https://phl.carto.com opa_properties_public output.parquet
```

### Geometry Column

Carto tables typically have geometry in a column named `the_geom` (WGS84). This is automatically renamed to `geometry` in the output for consistency with other geoparquet-io extractors.

### Non-Geometry (Tabular) Tables

Carto accounts often include geometry-less lookup or demographics tables. `gpio` auto-detects these and extracts them as **plain Parquet** (no GeoParquet `geo` metadata key) — the same behavior as converting a non-spatial file. Hilbert ordering, the bbox column, and `--bbox` filtering are skipped (they require geometry); `--where`, `--limit`, `--include-cols`, and `--exclude-cols` are still honored.

!!! note "How detection works"
    Carto attaches a `the_geom` column (type `geometry`) to nearly every managed table — even purely tabular ones, where it is entirely `NULL`. So inspecting the schema alone isn't enough. `gpio` first checks the schema for a geometry-typed column, then runs a fast `WHERE the_geom IS NOT NULL LIMIT 1` probe to confirm the column actually holds geometry before choosing the GeoParquet path.

Use `--geometry` / `--no-geometry` to override auto-detection:

=== "CLI"

    <!-- doctest: network -->
    ```bash
    # Auto-detect (default): plain Parquet if the table has no geometry
    gpio extract carto https://phl.carto.com/api/v2/sql \
        my_lookup_table output.parquet

    # Force tabular extraction (plain Parquet, no geo metadata)
    gpio extract carto https://phl.carto.com/api/v2/sql \
        my_lookup_table output.parquet \
        --no-geometry

    # Force geometry extraction (GeoParquet)
    gpio extract carto https://phl.carto.com/api/v2/sql \
        opa_properties_public output.parquet \
        --geometry
    ```

=== "Python"

    <!-- doctest: network -->
    ```python
    from geoparquet_io.api import ops

    # Auto-detect (default): returns a plain table for geometry-less sources
    table = ops.from_carto(
        'https://phl.carto.com/api/v2/sql',
        'my_lookup_table',
    )

    # Force tabular extraction
    table = ops.from_carto(
        'https://phl.carto.com/api/v2/sql',
        'my_lookup_table',
        geometry=False,
    )
    ```

### Output Optimization

By default, Carto extracts include Hilbert spatial ordering and bbox columns:

<!-- doctest: network -->
```bash
# Skip optimizations for faster extraction
gpio extract carto https://phl.carto.com/api/v2/sql \
    opa_properties_public output.parquet \
    --skip-hilbert \
    --skip-bbox

# Custom compression
gpio extract carto https://phl.carto.com/api/v2/sql \
    opa_properties_public output.parquet \
    --compression GZIP
```

### Large Tables and Timeouts

For large tables, Carto's SQL API may time out or return errors. Use filters to reduce the result set:

<!-- doctest: network -->
```bash
# Use --limit for sampling or testing
gpio extract carto https://phl.carto.com/api/v2/sql \
    opa_properties_public output.parquet \
    --limit 50000

# Use --bbox to extract a geographic subset
gpio extract carto https://phl.carto.com/api/v2/sql \
    opa_properties_public output.parquet \
    --bbox "-75.2,39.9,-75.1,40.0"

# Increase timeout for slower connections (default: 120s)
gpio extract carto https://phl.carto.com/api/v2/sql \
    opa_properties_public output.parquet \
    --timeout 300
```

!!! warning "Large Table Limitations"
    Carto's SQL API has response size limits (~4MB by default). For tables with
    hundreds of thousands of rows, always use `--limit`, `--where`, or `--bbox`
    to reduce the result set. The extractor includes retry logic for transient
    failures, but very large unbounded queries will likely fail.

### Authentication

For private Carto tables or enterprise endpoints, set the `CARTO_API_KEY` environment variable:

<!-- doctest: network -->
```bash
export CARTO_API_KEY="your_api_key_here"
gpio extract carto https://your-org.carto.com/api/v2/sql \
    private_table output.parquet
```

Or in Python:

<!-- doctest: skip="continues a Python session an earlier block started" -->
```python
import os
os.environ["CARTO_API_KEY"] = "your_api_key_here"

table = ops.from_carto(
    'https://your-org.carto.com/api/v2/sql',
    'private_table'
)

# Or pass directly:
table = ops.from_carto(
    'https://your-org.carto.com/api/v2/sql',
    'private_table',
    api_key='your_api_key_here'
)
```

### Common Public Carto Endpoints

- **Philadelphia**: `https://phl.carto.com/api/v2/sql`
  - Tables: `opa_properties_public`, `public_cases_fc`, many more
- **Los Angeles**: `https://lahub.maps.arcgis.com` (ArcGIS - use `gpio extract arcgis`)
- Other municipal open data portals using Carto

!!! tip "Finding Available Tables"
    Check the city's open data portal documentation for available table names.
    For Philadelphia, see [cityofphiladelphia.github.io/carto-api-explorer](https://cityofphiladelphia.github.io/carto-api-explorer/).

## Working with Partitioned Input Data

The `extract` command can read from partitioned GeoParquet datasets, including directories containing multiple parquet files and hive-style partitions.

### Reading from Directories

<!-- doctest: skip="needs partitions/, which the harness does not seed" -->
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

<!-- doctest: skip="needs country_partitions/, which the harness does not seed" -->
```bash
# Read hive-style partitions (auto-detected)
gpio extract country_partitions/ merged.parquet

# Explicitly enable hive partitioning (adds partition columns to data)
gpio extract partitions/ merged.parquet --hive-input
```

### Schema Merging

When combining files with different schemas, use `--allow-schema-diff`:

<!-- doctest: setup="gpio partition quadkey input.parquet partitions/ --resolution 6 --partition-resolution 2" -->
```bash
# Merge files with different columns (fills NULL for missing columns)
gpio extract partitions/ merged.parquet --allow-schema-diff
```

### Applying Filters to Partitioned Data

All filters work with partitioned input:

<!-- doctest: skip="needs partitions/, which the harness does not seed" -->
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

<!-- doctest: menu -->
```bash
# See the SQL query without executing
gpio extract data.parquet output.parquet \
  --where "population > 10000" \
  --dry-run

# Verbose output with detailed progress
gpio extract data.parquet output.parquet \
  --bbox -122.5,37.7,-122.3,37.8 \
  --verbose
```

<!-- doctest: skip="filters on 'population', a column the sample data does not have" -->
```bash
# Show SQL during execution
gpio extract data.parquet output.parquet \
  --where "population > 10000" \
  --show-sql
```

## Compression Options

Control output file compression:

--8<-- "_includes/compression-options.md"

<!-- doctest: menu -->
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

<!-- doctest: menu -->
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

    <!-- doctest: menu -->
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

    <!-- doctest: skip="filters on 'category', a column the sample data does not have" -->
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
    ```

    <!-- doctest: skip="filters on 'category', a column the sample data does not have" -->
    ```python
    # Extract all features of a specific type
    gpio.read('data.parquet').extract(where="category = 'restaurant'").write('restaurants.parquet')

    # Extract multiple categories
    gpio.read('data.parquet').extract(where="category IN ('restaurant', 'cafe', 'bakery')").write('food_places.parquet')
    ```

### Extract Recent Data

=== "CLI"

    <!-- doctest: skip="filters on 'updated_at', a column the sample data does not have" -->
    ```bash
    # Extract data updated this year
    gpio extract data.parquet recent.parquet \
      --where "updated_at >= '2024-01-01'"

    # Extract data from specific time range
    gpio extract data.parquet range.parquet \
      --where "created_at BETWEEN '2024-01-01' AND '2024-06-30'"
    ```

=== "Python"

    <!-- doctest: skip="filters on 'updated_at', a column the sample data does not have" -->
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

    <!-- doctest: skip="names columns the sample data does not have (category, id, population)" -->
    ```bash
    # Extract as attribute table (no geometry)
    gpio extract data.parquet attributes.parquet \
      --include-cols id,name,category,population \
      --exclude-cols geometry,bbox
    ```

=== "Python"

    <!-- doctest: skip="excludes the geometry column, and the writer then requires it (KeyError in write_strategies/duckdb_kv.py)" -->
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

<!-- doctest: skip="demonstrates the error for an unknown --include-cols column on purpose", demonstrates-error -->
```bash
gpio extract data.parquet output.parquet --include-cols invalid_column
# Error: Columns not found in schema (--include-cols): invalid_column
# Available columns: id, name, geometry, bbox, ...
```

The same check runs on `gpio extract bigquery`, `carto` and `arcgis`, against
the schema each already reads to plan the extraction — so an unknown name fails
locally instead of becoming a backend error or, for ArcGIS, an `outFields`
entry the server quietly ignores. Names are matched case-insensitively and
resolved to the source's own spelling.

A **blank entry** in the list — `--include-cols id,,name`, or `--include-cols '   '` —
is a usage error and is rejected before anything is opened or contacted. That
holds for the Python API too (`ops.from_carto`, `ops.from_arcgis`,
`ops.read_bigquery`, `ops.extract`, `ops.create_pmtiles`), which never goes
through the CLI's option parsing. A **wholly empty** value is not a blank entry:
`--include-cols ""` means "option not given", so `--include-cols "$COLS"` keeps
working when `COLS` is unset.

### Invalid WHERE Clause

SQL syntax errors are reported with details:

<!-- doctest: skip="demonstrates the error message for an invalid --where on purpose", demonstrates-error -->
```bash
gpio extract data.parquet output.parquet --where "invalid syntax here"
# Error: Parser Error: syntax error at or near "here"
```

### Dangerous SQL Keywords

For safety, certain SQL keywords are blocked in WHERE clauses:

<!-- doctest: skip="demonstrates the SQL-injection guard rejecting a --where on purpose", demonstrates-error -->
```bash
gpio extract data.parquet output.parquet --where "population > 1000; DROP TABLE users"
# Error: WHERE clause contains potentially dangerous SQL keywords: DROP
```

A `;` statement separator is rejected on its own, even when no blocked keyword follows it, because DuckDB executes multi-statement strings:

<!-- doctest: skip="demonstrates the error message for an invalid --where on purpose", demonstrates-error -->
```bash
gpio extract data.parquet output.parquet --where "1=1; COPY (SELECT 42) TO 'x.csv'"
# Error: WHERE clause contains a ';' statement separator outside a quoted string
```

Semicolons inside string literals or quoted identifiers are fine (`--where "name = 'a;b'"`).

## Large File Handling

gpio efficiently handles larger-than-memory files using streaming write strategies. The default strategy uses constant memory regardless of file size.

### Basic Usage

For most files, no special configuration is needed:

=== "CLI"

    ```bash
    # Process a 50GB file on a machine with 4GB RAM - just works
    gpio extract huge_dataset.parquet filtered.parquet --bbox -122.5,37.5,-122.0,38.0
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Large files work automatically
    gpio.read('huge_dataset.parquet') \
        .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
        .write('filtered.parquet')
    ```

### Memory Configuration

For containerized environments or when you need explicit control:

=== "CLI"

    <!-- doctest: menu -->
    ```bash
    # Limit memory usage for Docker/Kubernetes
    gpio extract input.parquet output.parquet --write-memory 512MB

    # Use a different write strategy
    gpio extract input.parquet output.parquet --write-strategy streaming
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Memory limit is auto-detected; select a write strategy if needed
    gpio.read('input.parquet').write('output.parquet', write_strategy='streaming')
    ```

For detailed information on write strategies, memory configuration, and container environments, see the [Write Strategies Guide](write-strategies.md).

## See Also

- [CLI Reference](../cli/extract.md) - Complete option reference
- [Write Strategies Guide](write-strategies.md) - Large file handling and memory configuration
- [Remote Files Guide](remote-files.md) - Working with S3 and HTTP files
- [Inspect Guide](inspect.md) - Examine file structure and metadata
- [Partition Guide](partition.md) - Split files into partitions
