# extract Command

For detailed usage and examples, see the [Extract User Guide](../guide/extract.md).

## Quick Reference

```bash
gpio extract --help
```

This will show all available options for the `extract` command.

## Subcommands

The `extract` command supports multiple data sources:

### extract geoparquet (default)

Extract from GeoParquet files. This is the default when no subcommand is specified.

```bash
gpio extract input.parquet output.parquet --bbox -122,37,-121,38
gpio extract geoparquet input.parquet output.parquet  # Explicit
```

### extract bigquery

Extract from BigQuery tables to GeoParquet.

```bash
gpio extract bigquery PROJECT.DATASET.TABLE output.parquet
```

**Options:**

- `--project` - GCP project ID (overrides project in TABLE_ID)
- `--credentials-file` - Path to service account JSON file
- `--include-cols` - Comma-separated columns to include
- `--exclude-cols` - Comma-separated columns to exclude
- `--where` - SQL WHERE clause (BigQuery SQL syntax)
- `--bbox` - Bounding box filter: `minx,miny,maxx,maxy`
- `--bbox-mode` - Filter mode: `auto` (default), `server`, or `local`
- `--bbox-threshold` - Row count threshold for auto mode (default: 500000)
- `--limit` - Maximum rows to extract
- `--geography-column` - GEOGRAPHY column name (auto-detected if not set)
- `--dry-run` - Show SQL without executing
- `--show-sql` - Print SQL during execution

**Authentication (in order of precedence):**

1. `--credentials-file`: Path to service account JSON
2. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
3. `gcloud auth application-default` credentials

**Bbox Filtering Modes:**

The `--bbox-mode` option controls where spatial filtering occurs:

- `auto` (default): Uses table row count to decide. Tables ≥ threshold use server-side, smaller tables use local
- `server`: Always push filter to BigQuery using `ST_INTERSECTS()` - best for large tables
- `local`: Always filter locally in DuckDB after fetch - best for small tables

The `--bbox-threshold` sets the row count where `auto` switches to server-side filtering. Default is 500,000 rows. See the [User Guide](../guide/extract.md#bbox-filtering-mode-server-vs-local) for detailed tradeoff analysis.

!!! warning "Limitations"
    **Cannot read BigQuery views or external tables** - this is a limitation of the BigQuery Storage Read API. BIGNUMERIC columns are not supported (exceeds DuckDB's precision).

### extract arcgis

Extract from ArcGIS Feature Services to GeoParquet.

```bash
gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet
```

**Options:**

- `--token` - Direct ArcGIS authentication token
- `--token-file` - Path to file containing authentication token
- `--username` - ArcGIS Online/Enterprise username (requires --password)
- `--password` - ArcGIS Online/Enterprise password (requires --username)
- `--portal-url` - Enterprise portal URL for token generation
- `--where` - SQL WHERE clause (pushed to server, default: `1=1`)
- `--bbox` - Bounding box filter: `xmin,ymin,xmax,ymax` in WGS84 (pushed to server)
- `--include-cols` - Comma-separated columns to include (pushed to server)
- `--exclude-cols` - Comma-separated columns to exclude (applied after download)
- `--limit` - Maximum number of features to extract
- `--output-crs` - Output CRS such as `EPSG:25830`, or `native` for the layer's advertised SR (default reprojects to WGS84)
- `--max-allowable-offset` - Server-side geometry generalization tolerance in output CRS units
- `--workers` - Parallel requests for large datasets (1-10, default: 1)
- `--batch-size` - Features per request (default: server's maxRecordCount). The starting rung of the adaptive page-size ladder: a page the server refuses is retried at the same offset with a smaller size, and the smaller size sticks
- `--timeout` - Per-request HTTP timeout in seconds (default: 60). Increase for layers with very large/complex geometries that are slow to serialize
- `--skip-hilbert` - Skip Hilbert spatial ordering
- `--skip-bbox` - Skip adding bbox column

**Authentication (in order of precedence):**

1. `--token`: Direct token string
2. `--token-file`: Path to file containing token
3. `--username`/`--password`: Generate token via ArcGIS REST API

**Examples:**

```bash
# Public service (no auth needed)
gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 output.parquet

# With bounding box filter (server-side)
gpio extract arcgis https://... output.parquet --bbox -122.5,37.5,-122.0,38.0

# With SQL WHERE filter (server-side)
gpio extract arcgis https://... output.parquet --where "state='CA'"

# Select specific columns (server-side)
gpio extract arcgis https://... output.parquet --include-cols name,population

# With authentication
gpio extract arcgis https://... output.parquet --username user --password pass
```

!!! note "Server-Side Filtering"
    The `--where`, `--bbox`, `--include-cols`, and `--limit` options are pushed to the ArcGIS server for efficient filtering. Only matching data is downloaded.

### extract wfs

Extract from WFS (Web Feature Service) to GeoParquet.

```bash
gpio extract wfs https://geo.example.com/wfs layer_name output.parquet
```

**Options:**

- `--version` - WFS protocol version: `1.0.0` or `1.1.0` (default)
- `--bbox` - Bounding box filter: `xmin,ymin,xmax,ymax` in WGS84
- `--bbox-mode` - Filter mode: `auto` (default), `server`, or `local`
- `--limit` - Maximum features to extract
- `--output-crs` - Request specific CRS from server (e.g., `EPSG:4326`)
- `--workers` - Parallel requests for large datasets (1-10, default: 1)
- `--page-size` - Features per page (default: 100000, max: 500000)
- `--auto-tile/--no-auto-tile` - Auto-subdivide bbox when server caps response (default: enabled)
- `--parallel-layers` - Concurrent layer extraction for multi-layer requests (1-10, default: 1)
- `--skip-hilbert` - Skip Hilbert spatial ordering
- `--skip-bbox` - Skip adding bbox column

**Examples:**

```bash
# List available layers
gpio extract wfs https://geo.example.com/wfs

# Extract entire layer
gpio extract wfs https://geo.example.com/wfs cities output.parquet

# With bbox filter
gpio extract wfs https://geo.example.com/wfs cities output.parquet \
    --bbox -122.5,37.5,-122.0,38.0

# With CRS and limit
gpio extract wfs https://geo.example.com/wfs cities output.parquet \
    --output-crs EPSG:4326 --limit 10000

# Parallel extraction for large datasets (1M+ features)
gpio extract wfs https://geo.example.com/wfs large_layer output.parquet \
    --workers 4

# Extract multiple layers in parallel to a directory
gpio extract wfs https://geo.example.com/wfs layer1,layer2,layer3 ./output/ \
    --parallel-layers 3
```

!!! tip "Auto-Tiling"
    When a WFS server caps responses (e.g., maxFeatures=1M), gpio automatically subdivides the bbox and fetches all features. Disable with `--no-auto-tile` if you want the capped response.

!!! tip "Performance"
    For datasets under ~100K features, the default single-stream mode is fastest. Use `--workers 2-4` for very large datasets (1M+ features) where server timeouts occur.

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

# Extract from S3-compatible storage
gpio --s3-endpoint minio.local:9000 --s3-no-ssl \
  extract geoparquet s3://bucket/input.parquet output.parquet --bbox -122,37,-121,38

# Extract from source.coop
gpio --s3-endpoint data.source.coop \
  extract geoparquet s3://bucket/data.parquet output.parquet --limit 1000

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
