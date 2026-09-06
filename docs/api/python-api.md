# Python API

gpio provides a fluent Python API for GeoParquet transformations. This API offers the best performance by keeping data in memory as Arrow tables, avoiding file I/O entirely.

## Installation

=== "CLI"

    ```bash
    uv tool install geoparquet-io
    ```

=== "Python"

    ```bash
    uv add geoparquet-io
    ```

## Quick Start

```python
import geoparquet_io as gpio

# Read, transform, and write in a fluent chain
gpio.read('input.parquet') \
    .add_bbox() \
    .add_quadkey(resolution=12) \
    .sort_hilbert() \
    .write('output.parquet')
```

## Reading Data

Use `gpio.read()` to load a GeoParquet file:

```python
import geoparquet_io as gpio

# Read a file
table = gpio.read('places.parquet')

# Access properties
print(f"Rows: {table.num_rows}")
print(f"Columns: {table.column_names}")
print(f"Geometry column: {table.geometry_column}")
```

### S3-Compatible Storage

Read from MinIO, Cloudflare R2, source.coop, and other S3-compatible services:

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Read from source.coop
    table = gpio.read_partition(
        's3://bucket/data/*.parquet',
        s3_endpoint='data.source.coop'
    )

    # Read from MinIO (no SSL)
    table = gpio.read_partition(
        's3://bucket/data/',
        s3_endpoint='minio.local:9000',
        s3_use_ssl=False,
        aws_profile='minio-dev'
    )

    # Environment variables also work
    import os
    os.environ['AWS_ENDPOINT_URL'] = 'https://data.source.coop'
    table = gpio.read_partition('s3://bucket/data/')
    ```

=== "CLI"

    ```bash
    # Read from source.coop
    gpio inspect summary s3://bucket/data/*.parquet \
      --s3-endpoint data.source.coop

    # Read from MinIO (no SSL)
    gpio inspect summary s3://bucket/data/ \
      --s3-endpoint minio.local:9000 \
      --s3-no-ssl \
      --aws-profile minio-dev

    # Environment variables also work
    export AWS_ENDPOINT_URL=https://data.source.coop
    gpio inspect summary s3://bucket/data/
    ```

For uploads to S3-compatible storage:

=== "Python"

    ```python
    table.upload(
        's3://bucket/output.parquet',
        s3_endpoint='minio.local:9000',
        s3_use_ssl=False
    )
    ```

=== "CLI"

    ```bash
    gpio publish upload local.parquet s3://bucket/output.parquet \
      --s3-endpoint minio.local:9000 \
      --s3-no-ssl
    ```

### Reading from BigQuery

Use `Table.from_bigquery()` to read directly from BigQuery tables. The `table_id` parameter accepts fully-qualified `"project.dataset.table"` format or `"dataset.table"` when a separate `project` argument is provided (or when using your default gcloud project). When using `bbox`, provide coordinates as `"minx,miny,maxx,maxy"` representing longitude,latitude in EPSG:4326 degrees (e.g., `"-122.52,37.70,-122.35,37.82"`).

```python
import geoparquet_io as gpio

# Basic read
table = gpio.Table.from_bigquery('myproject.geodata.buildings')

# With filtering
table = gpio.Table.from_bigquery(
    'myproject.geodata.buildings',
    where="area_sqm > 1000",
    columns=['id', 'name', 'geography'],
    limit=10000
)

# With spatial filtering (bbox)
table = gpio.Table.from_bigquery(
    'myproject.geodata.buildings',
    bbox="-122.52,37.70,-122.35,37.82"
)

# With explicit credentials
table = gpio.Table.from_bigquery(
    'myproject.geodata.buildings',
    credentials_file='/path/to/service-account.json'
)

# Chain with other operations
gpio.Table.from_bigquery('myproject.geodata.buildings', limit=10000) \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')
```

**Bbox filtering modes:**

When using `bbox`, control where filtering happens with `bbox_mode`:

```python
# Server-side filtering (best for large tables)
table = gpio.Table.from_bigquery(
    'myproject.geodata.global_buildings',
    bbox="-122.52,37.70,-122.35,37.82",
    bbox_mode="server"
)

# Local filtering (best for small tables)
table = gpio.Table.from_bigquery(
    'myproject.geodata.city_parks',
    bbox="-122.52,37.70,-122.35,37.82",
    bbox_mode="local"
)

# Custom threshold for auto mode (default: 500000)
table = gpio.Table.from_bigquery(
    'myproject.geodata.buildings',
    bbox="-122.52,37.70,-122.35,37.82",
    bbox_threshold=100000  # Use server for tables > 100K rows
)
```

See the [Extract Guide](../guide/extract.md#bbox-filtering-mode-server-vs-local) for detailed tradeoff analysis.

!!! warning "BigQuery Limitations"
    - **Cannot read views or external tables** (Storage Read API limitation)
    - BIGNUMERIC columns are not supported

### Reading from ArcGIS Feature Services

Use `gpio.extract_arcgis()` to download features from ArcGIS REST Feature Services. Server-side filtering is applied for efficient data transfer.

```python
import geoparquet_io as gpio

# Basic read from public service
table = gpio.extract_arcgis(
    'https://services.arcgis.com/.../FeatureServer/0'
)

# With server-side filtering
table = gpio.extract_arcgis(
    'https://services.arcgis.com/.../FeatureServer/0',
    where="STATE_NAME = 'California'",
    bbox=(-122.5, 37.5, -122.0, 38.0),
    include_cols='NAME,POPULATION,STATE_NAME',
    limit=10000
)

# With authentication
table = gpio.extract_arcgis(
    'https://services.arcgis.com/.../FeatureServer/0',
    token='your_arcgis_token'
)

# With username/password authentication
table = gpio.extract_arcgis(
    'https://services.arcgis.com/.../FeatureServer/0',
    username='myuser',
    password='mypassword'
)

# Chain with other operations
gpio.extract_arcgis(
    'https://services.arcgis.com/.../FeatureServer/0',
    limit=10000
).add_bbox().sort_hilbert().write('output.parquet')
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `service_url` | str | ArcGIS Feature Service URL with layer ID |
| `token` | str | Direct authentication token |
| `token_file` | str | Path to file containing token |
| `username` | str | ArcGIS Online/Enterprise username |
| `password` | str | ArcGIS password (requires username) |
| `portal_url` | str | Enterprise portal URL for token generation |
| `where` | str | SQL WHERE clause (default: "1=1" = all) |
| `bbox` | tuple | Bounding box filter (xmin, ymin, xmax, ymax) in WGS84 |
| `include_cols` | str | Comma-separated columns to include |
| `exclude_cols` | str | Comma-separated columns to exclude |
| `limit` | int | Maximum number of features |
| `max_workers` | int | Number of parallel fetch workers (default: 1) |
| `output_crs` | str | Output CRS (e.g. `EPSG:25830`) or `native`; default reprojects to WGS84 |
| `max_allowable_offset` | float | Server-side geometry generalization tolerance in output CRS units |
| `timeout` | float | Per-request HTTP timeout in seconds (default: 60); increase for slow, heavy-geometry layers |

!!! note "No automatic Hilbert sorting"
    Unlike the CLI `gpio extract arcgis` command, the Python API does NOT apply Hilbert sorting by default. Chain `.sort_hilbert()` explicitly if you want spatial ordering.

### Reading from WFS Services

Use `Table.from_wfs()` to read from OGC Web Feature Services. WFS is widely used by government agencies and organizations for publishing geospatial data.

#### Table.from_wfs()

Create a Table from a WFS layer:

```python
from geoparquet_io.api import Table

table = Table.from_wfs(
    'https://geo.example.com/wfs',
    'cities',
    version='auto',       # WFS version (auto, 2.0.0, 1.1.0, 1.0.0)
    bbox=(-122.5, 37.5, -122.0, 38.0),  # Optional filter
    limit=1000,           # Max features
    max_workers=2,        # Parallel requests
    axis_order='auto',    # Bbox axis order (auto, xy, latlon)
)
```

#### ops.from_wfs()

Functional API returning PyArrow Table:

```python
from geoparquet_io.api import ops

table = ops.from_wfs('https://geo.example.com/wfs', 'cities', limit=100)
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `service_url` | str | WFS service URL |
| `typename` | str | Layer name to extract |
| `version` | str | WFS version: `auto` (default), `2.0.0`, `1.1.0`, `1.0.0`. Auto tries 2.0.0 first. |
| `bbox` | tuple | Bounding box filter (xmin, ymin, xmax, ymax) |
| `limit` | int | Maximum number of features |
| `max_workers` | int | Number of parallel fetch workers (default: 1) |
| `page_size` | int | Features per WFS request page (default: 100000) |
| `axis_order` | str | Bbox axis order: `auto` (default), `xy`, `latlon`. Auto detects from CRS format. |
| `auto_tile` | bool | Auto-subdivide bbox when server caps response (default: True) |
| `strict_crs` | bool | Fail when the server returns a different CRS than requested (default: False, warns and uses the server's actual CRS instead). gpio trusts the CRS the server declares in its GeoJSON response and never guesses from coordinates. |

!!! note "No automatic Hilbert sorting"
    Like other Python API extraction methods, `from_wfs()` does NOT apply Hilbert sorting by default. Chain `.sort_hilbert()` explicitly if needed.

#### ops.aggregate_a5()

Functional API for A5 grid aggregation. Returns a PyArrow Table.

```python
from geoparquet_io.api import ops
import pyarrow.parquet as pq

table = pq.read_table('fields.parquet')
result = ops.aggregate_a5(
    table,
    resolution=8,
    metric="sum:area_ha,avg:yield",
    breakdown="crop_type",
)
```

#### ops.aggregate_h3()

Functional API for H3 grid aggregation (resolution 0–15). Returns a PyArrow Table.

```python
from geoparquet_io.api import ops
import pyarrow.parquet as pq

table = pq.read_table('fields.parquet')
result = ops.aggregate_h3(
    table,
    resolution=8,
    metric="sum:area_ha",
    breakdown="crop_type",
)
```

#### ops.aggregate_admin()

Functional API for admin region aggregation. Returns a PyArrow Table.

```python
from geoparquet_io.api import ops
import pyarrow.parquet as pq

table = pq.read_table('fields.parquet')
result = ops.aggregate_admin(table, level="country", metric="sum:area_ha")
```

#### ops.from_wfs_layers()

Extract multiple WFS layers in parallel to a directory:

```python
from geoparquet_io.api import ops

results = ops.from_wfs_layers(
    'https://geo.example.com/wfs',
    ['cities', 'roads', 'buildings'],
    './output/',
    parallel_layers=3,
    max_workers=2
)
# Returns: {'cities': Path('output/cities.parquet'), ...}
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `service_url` | str | WFS service URL |
| `typenames` | list[str] | Layer names to extract |
| `output_dir` | str \| Path | Output directory for parquet files |
| `parallel_layers` | int | Concurrent layer extraction (default: 1) |
| `max_workers` | int | Parallel fetch workers per layer (default: 1) |
| `page_size` | int | Features per page (default: 100000) |
| `auto_tile` | bool | Auto-subdivide bbox when server caps (default: True) |

## Table Class

The `Table` class wraps a PyArrow Table and provides chainable transformation methods.

### Properties

| Property | Description |
|----------|-------------|
| `num_rows` | Number of rows in the table |
| `column_names` | List of column names |
| `geometry_column` | Name of the geometry column |
| `crs` | CRS as PROJJSON dict or string (None = OGC:CRS84 default) |
| `bounds` | Bounding box tuple (xmin, ymin, xmax, ymax) |
| `schema` | PyArrow Schema object |
| `geoparquet_version` | GeoParquet version string (e.g., "1.1") |

```python
table = gpio.read('data.parquet')

# Get CRS
print(table.crs)  # e.g., {'id': {'authority': 'EPSG', 'code': 4326}, ...}

# Get bounds
print(table.bounds)  # e.g., (-122.5, 37.5, -122.0, 38.0)

# Get schema
for field in table.schema:
    print(f"{field.name}: {field.type}")
```

### Methods

#### `info(verbose=True)`

Print or return summary information about the table.

```python
# Print formatted summary
table.info()
# Table: 766 rows, 6 columns
# Geometry: geometry
# CRS: EPSG:4326
# Bounds: [-122.500000, 37.500000, -122.000000, 38.000000]
# GeoParquet: 1.1

# Get as dictionary
info_dict = table.info(verbose=False)
print(info_dict['rows'])  # 766
print(info_dict['crs'])   # None or CRS dict
```

#### `head(n=10)` / `tail(n=10)`

Get the first or last N rows.

```python
# First 10 rows (default)
first_rows = table.head()

# First 50 rows
first_50 = table.head(50)

# Last 10 rows (default)
last_rows = table.tail()

# Last 5 rows
last_5 = table.tail(5)

# Chain with other operations
preview = table.head(100).add_bbox()
```

#### `stats()`

Calculate column statistics.

```python
stats = table.stats()

# Access stats for a column
print(stats['population']['min'])     # Minimum value
print(stats['population']['max'])     # Maximum value
print(stats['population']['nulls'])   # Null count
print(stats['population']['unique'])  # Approximate unique count

# Geometry columns have only null counts
print(stats['geometry']['nulls'])
```

#### `metadata(include_parquet_metadata=False)`

Get GeoParquet and schema metadata.

```python
meta = table.metadata()

# Access metadata
print(meta['geoparquet_version'])  # e.g., '1.1.0'
print(meta['geometry_column'])     # e.g., 'geometry'
print(meta['crs'])                 # CRS dict or None
print(meta['bounds'])              # (xmin, ymin, xmax, ymax)
print(meta['columns'])             # List of column info dicts

# Full geo metadata from 'geo' key
geo_meta = meta.get('geo_metadata', {})

# Include raw Parquet schema metadata
full_meta = table.metadata(include_parquet_metadata=True)
```

#### `to_geojson(output_path=None, precision=7, write_bbox=False, id_field=None)`

Convert to GeoJSON.

```python
# Write to file
table.to_geojson('output.geojson')

# With options
table.to_geojson('output.geojson', precision=5, write_bbox=True)

# Get as string (no file output)
geojson_str = table.to_geojson()
```

#### `add_bbox(column_name='bbox')`

Add a bounding box struct column computed from geometry.

```python
table = gpio.read('input.parquet').add_bbox()
# or with custom name
table = gpio.read('input.parquet').add_bbox(column_name='bounds')
```

#### `add_quadkey(column_name='quadkey', resolution=13, use_centroid=False)`

Add a quadkey column based on geometry location.

```python
# Default resolution (13)
table = gpio.read('input.parquet').add_quadkey()

# Custom resolution
table = gpio.read('input.parquet').add_quadkey(resolution=10)

# Force centroid calculation even if bbox exists
table = gpio.read('input.parquet').add_quadkey(use_centroid=True)
```

#### `add_h3(column_name='h3_cell', resolution=9)`

Add an H3 hexagonal cell column based on geometry location.

```python
# Default resolution (9, ~100m cells)
table = gpio.read('input.parquet').add_h3()

# Lower resolution for larger cells
table = gpio.read('input.parquet').add_h3(resolution=6)

# Custom column name
table = gpio.read('input.parquet').add_h3(column_name='hex_id', resolution=8)
```

#### `add_s2(column_name='s2_cell', level=13)`

Add an S2 spherical cell column based on geometry location.

```python
# Default level (13, ~1.2 km² cells)
table = gpio.read('input.parquet').add_s2()

# Lower level for larger cells
table = gpio.read('input.parquet').add_s2(level=10)

# Custom column name
table = gpio.read('input.parquet').add_s2(column_name='s2_index', level=15)
```

#### `add_a5(column_name='a5_cell', resolution=15)`

Add an A5 cell column based on geometry location.

```python
# Default resolution (15)
table = gpio.read('input.parquet').add_a5()

# Lower resolution for larger cells
table = gpio.read('input.parquet').add_a5(resolution=10)

# Custom column name
table = gpio.read('input.parquet').add_a5(column_name='a5_index', resolution=12)
```

#### `add_geometry_metrics(vecorel=True)`

Add geodesic area (m²) and perimeter (m) columns using WGS84 spheroid calculations.

```python
# Add metrics with Vecorel metadata (default)
table = gpio.read('input.parquet').add_geometry_metrics()

# Without Vecorel metadata
table = gpio.read('input.parquet').add_geometry_metrics(vecorel=False)
```

Adds `metrics:area` and `metrics:perimeter` columns. With `vecorel=True` (default), also writes Vecorel schema metadata and ensures `id`/`geometry` are present and non-nullable.

#### `add_admin_divisions(dataset='gaul', levels=None, vecorel=False, prefix=None)`

Add administrative division columns via spatial join with remote boundary datasets.
Defaults match the CLI (`gpio add admin-divisions`).

```python
# Every level the dataset provides -- GAUL continent, country, department
table = gpio.read('input.parquet').add_admin_divisions()

# A single level from the default GAUL dataset
table = gpio.read('input.parquet').add_admin_divisions(levels=['country'])

# Overture dataset with multiple levels
table = gpio.read('input.parquet').add_admin_divisions(
    dataset='overture',
    levels=['country', 'region']
)

# Vecorel-compliant output (forces Overture with country,region)
table = gpio.read('input.parquet').add_admin_divisions(vecorel=True)
```

**Parameters:**

- `dataset` (str): Boundaries dataset — `"gaul"` (default) or `"overture"`, or a custom URL
- `levels` (list[str] | None): Levels to add. `None` adds every level the dataset
  provides — `["continent", "country", "department"]` for GAUL,
  `["country", "region"]` for Overture — matching the CLI with no `--levels`
- `vecorel` (bool): Emit Vecorel-compliant columns; forces Overture with country,region
- `prefix` (str | None): Column name prefix, as with the CLI's `--prefix`. `None` uses the
  dataset's own name, so columns are `gaul_country` under the default dataset and
  `overture_country` under Overture. Pass `prefix='overture'` to keep the pre-1.4 names

#### `add_kdtree(column_name='kdtree_cell', iterations=None, sample_size=100000, *, auto=False, target_rows=120000)`

Add a KD-tree cell column for data-adaptive spatial partitioning.

Name an `iterations` count, or pass `auto=True` to size the tree from the row
count the way `gpio add kdtree` does. A call that gives neither -- or both --
raises `InvalidParameterError`.

```python
# Auto: sized from the row count, targeting ~120k rows per cell
table = gpio.read('input.parquet').add_kdtree(auto=True)

# Auto with a different target
table = gpio.read('input.parquet').add_kdtree(auto=True, target_rows=50000)

# An explicit count
table = gpio.read('input.parquet').add_kdtree(iterations=6)  # 64 partitions

# More partitions with larger sample
table = gpio.read('input.parquet').add_kdtree(iterations=12, sample_size=500000)
```

#### `sort_hilbert()`

Reorder rows using Hilbert curve ordering for better spatial locality.

```python
table = gpio.read('input.parquet').sort_hilbert()
```

#### `sort_str(tile_size=50000)`

Reorder rows with Sort-Tile-Recursive packing: X strips, each sorted on Y with
alternating direction. `tile_size` only selects the number of strips, as
`ceil(sqrt(num_rows / tile_size))`, so set it to roughly the rows you intend to
put in a row group and expect a coarse response - see
[the sort guide](../guide/sort.md#what-row-group-size-does-here).

```python
table = gpio.read('input.parquet').sort_str(tile_size=50000)
table.write('output.parquet', row_group_rows=50000)
```

#### `sort_column(column_name, descending=False)`

Sort rows by a specified column.

```python
# Sort by name ascending
table = gpio.read('input.parquet').sort_column('name')

# Sort by population descending
table = gpio.read('input.parquet').sort_column('population', descending=True)
```

#### `sort_quadkey(column_name='quadkey', resolution=13, use_centroid=False, remove_column=False)`

Sort rows by quadkey for spatial locality. If no quadkey column exists, one is added automatically.

```python
# Sort by quadkey (auto-adds column if needed)
table = gpio.read('input.parquet').sort_quadkey()

# Sort and remove the quadkey column afterward
table = gpio.read('input.parquet').sort_quadkey(remove_column=True)

# Use existing quadkey column
table = gpio.read('input.parquet').sort_quadkey(column_name='my_quadkey')
```

#### `reproject(target_crs='EPSG:4326', source_crs=None, assume_crs84=False)`

Reproject geometry to a different coordinate reference system.

```python
# Reproject to WGS84 (auto-detects source CRS from metadata)
table = gpio.read('input.parquet').reproject(target_crs='EPSG:4326')

# Reproject with explicit source CRS
table = gpio.read('input.parquet').reproject(
    target_crs='EPSG:3857',
    source_crs='EPSG:4326'
)
```

A geometry column's `crs` may be **omitted** (defaults to OGC:CRS84) or set to
**`null`** (CRS is *unknown*) — these mean different things in the GeoParquet
spec. If a file declares `crs: null` but the coordinates are really lon/lat
WGS84, use `assume_crs84=True` to treat them as OGC:CRS84 and write the default
(the `crs` key is omitted on output, no coordinates are changed):

```python
table = gpio.read('unknown_crs.parquet').reproject(assume_crs84=True)
```

#### `extract(columns=None, exclude_columns=None, bbox=None, where=None, limit=None)`

Filter columns and rows.

Names in `columns` and `exclude_columns` are checked against the schema:
an unknown name raises `InvalidParameterError` naming it, rather than being
silently ignored. A column may not appear in both lists, except for the
geometry and bbox columns.

```python
# Select specific columns
table = gpio.read('input.parquet').extract(columns=['name', 'address'])

# Exclude columns
table = gpio.read('input.parquet').extract(exclude_columns=['placemaker_url'])

# Limit rows
table = gpio.read('input.parquet').extract(limit=1000)

# Spatial filter
table = gpio.read('input.parquet').extract(bbox=(-122.5, 37.5, -122.0, 38.0))

# SQL WHERE clause
table = gpio.read('input.parquet').extract(where="population > 10000")
```

#### `write(path, format=None, compression='ZSTD', compression_level=None, row_group_size_mb=None, row_group_rows=None, geoparquet_version=None, write_strategy='duckdb-kv', profile=None, verbose=False, ...)`

Write the table to a GeoParquet file. Returns the output `Path` for chaining or confirmation.

```python
# Basic write
path = table.write('output.parquet')
print(f"Wrote to {path}")

# With compression options
table.write('output.parquet', compression='GZIP', compression_level=6)

# With row group size
table.write('output.parquet', row_group_size_mb=128)
```

**GeoParquet Version**

Control the GeoParquet spec version written into the file metadata:

```python
# Write GeoParquet 2.0
table.write('output.parquet', geoparquet_version='2.0')
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `geoparquet_version` | str | Spec version to write: `1.0`, `1.1`, `1.1-geoarrow`, `2.0`, or `parquet-geo-only`. Defaults to `None`, which lets gpio select the version (normally `1.1`). |

**Write Strategy Options**

For large files, choose a write strategy to control memory behavior:

```python
# Use streaming strategy (constant memory usage)
table.write('output.parquet', write_strategy='streaming')
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `write_strategy` | str | Write strategy: `duckdb-kv` (default), `streaming`, `disk-rewrite`, or `in-memory` |

DuckDB's memory limit is auto-detected and respects container cgroup limits. To
set it explicitly, use the CLI `gpio extract --write-memory` flag; the Python
API does not expose a memory-limit argument.

See the [Write Strategies Guide](../guide/write-strategies.md) for detailed information on each strategy.

**GeoParquet Version Options**

Control the GeoParquet encoding version when writing:

=== "Python"

    ```python
    # GeoParquet 1.1 with native GeoArrow nested-coordinate encoding
    # (no bbox column; incompatible mixed-geometry columns fall back to WKB)
    table.write('output.parquet', geoparquet_version='1.1-geoarrow')

    # GeoParquet 1.0 with WKB encoding
    table.write('output.parquet', geoparquet_version='1.0')

    # GeoParquet 1.1 with WKB encoding (default)
    table.write('output.parquet', geoparquet_version='1.1')
    ```

=== "CLI"

    ```bash
    # GeoParquet 1.1 with native GeoArrow nested-coordinate encoding
    gpio convert input.geojson output.parquet --geoparquet-version 1.1-geoarrow

    # GeoParquet 1.0 with WKB encoding
    gpio convert input.shp output.parquet --geoparquet-version 1.0
    ```

`1.1-geoarrow` converts geometry from any input to native GeoArrow (nested-coordinate) types.
Compatible geometry type mixes are promoted (e.g. Polygon + MultiPolygon → MultiPolygon).
Incompatible mixes (e.g. Point + Polygon) fall back to WKB. No bbox column is added.

#### `to_arrow()`

Get the underlying PyArrow Table for interop with other Arrow-based tools.

```python
arrow_table = table.to_arrow()
```

#### Spatial Partitioning Methods

Each method below has an `ops.partition_by_*` twin taking a `pa.Table` plus the output directory, for callers who are not using the fluent wrapper -- `ops.partition_by_h3(table, 'output/', resolution=7)` does exactly what `Table.partition_by_h3('output/', resolution=7)` does. See [Pure Functions](#pure-functions-ops-module).

Every partition method -- spatial, string and admin alike, through `Table` or `ops` -- returns the same dict: `{'output_dir': str, 'file_count': int, 'hive': bool}`, where `file_count` is the number of `.parquet` files written under `output_dir`.

All spatial partitioning methods need to be told how finely to split. Give them an explicit resolution (or `level`), or pass `auto=True` to size one from the data -- the same choice `gpio partition` offers, and the same calculation behind it. A call that gives neither raises `InvalidParameterError` rather than picking a default for you. Under `auto=True`, `target_rows` (default 100,000) sets the rows you want per partition and `max_partitions` (default 10,000) caps how many are created; passing `auto=True` together with an explicit resolution is an error.

!!! warning "Breaking change: the implicit resolutions are gone"
    These methods used to fall back to a hardcoded resolution when you gave
    them none, so a call that named no resolution still wrote partitions --
    just not the ones `gpio partition` would have written from the same bytes.
    The same call now raises `InvalidParameterError`. To keep the output you
    had, pass the old default explicitly:

    | Method | Old implicit default |
    |--------|----------------------|
    | `partition_by_h3` | `resolution=9` |
    | `partition_by_quadkey` | `resolution=13, partition_resolution=6` |
    | `partition_by_s2` | `level=13` |
    | `partition_by_a5` | `resolution=15` |
    | `partition_by_kdtree` | `iterations=9` |
    | `add_kdtree` | `iterations=9` |

    To get what the CLI gives you instead, pass `auto=True`. Under `auto=True`
    the KD-tree methods take `target_rows` (default 120,000) and no
    `max_partitions`, matching `gpio add kdtree --auto N` and
    `gpio partition kdtree --auto N`.

    One workflow is exempt: `partition_by_kdtree` on a table that already
    carries the `kdtree_cell` column (say from `add_kdtree()`) needs no sizing
    parameter, and never fell back to `iterations=9` — the existing cells drive
    the partition, before and after this change.

#### `partition_by_quadkey(output_dir, resolution=None, partition_resolution=None, auto=False, target_rows=100000, max_partitions=10000, compression='ZSTD', hive=False, keep_quadkey_column=None, overwrite=False)`

Partition the table into a directory by quadkey. Pass `hive=True` for Hive-style `key=value/` subdirectories (matches CLI `--hive`).

With `hive=False` the partition value lives only in the file name, so the generated `quadkey` column is dropped from the output. Pass `keep_quadkey_column=True` to keep it (mirrors the CLI's `--keep-*-column`).

```python
# Partition to a directory
stats = table.partition_by_quadkey('output/', resolution=12, partition_resolution=6)
print(f"Created {stats['file_count']} files")

# Let gpio size both resolutions from the data
stats = table.partition_by_quadkey('output/', auto=True)

# With custom options
stats = table.partition_by_quadkey(
    'output/',
    resolution=13,
    partition_resolution=4,
    compression='SNAPPY',
    overwrite=True
)
```

#### `partition_by_h3(output_dir, resolution=None, auto=False, target_rows=100000, max_partitions=10000, compression='ZSTD', hive=False, keep_h3_column=None, overwrite=False)`

Partition the table into a directory by H3 cell. Pass `hive=True` for Hive-style `key=value/` subdirectories (matches CLI `--hive`).

With `hive=False` the partition value lives only in the file name, so the generated `h3_cell` column is dropped from the output. Pass `keep_h3_column=True` to keep it (mirrors the CLI's `--keep-*-column`).

```python
# Partition by H3
stats = table.partition_by_h3('output/', resolution=6)
print(f"Created {stats['file_count']} files")

# Or let gpio size the resolution from the data
stats = table.partition_by_h3('output/', auto=True, target_rows=50000)
```

#### `partition_by_s2(output_dir, level=None, auto=False, target_rows=100000, max_partitions=10000, compression='ZSTD', hive=False, keep_s2_column=None, overwrite=False)`

Partition the table into a directory by S2 cell. Pass `hive=True` for Hive-style `key=value/` subdirectories (matches CLI `--hive`).

With `hive=False` the partition value lives only in the file name, so the generated `s2_cell` column is dropped from the output. Pass `keep_s2_column=True` to keep it (mirrors the CLI's `--keep-*-column`).

```python
# Partition by S2
stats = table.partition_by_s2('output/', level=10)
print(f"Created {stats['file_count']} files")

# Or let gpio size the level from the data
stats = table.partition_by_s2('output/', auto=True)
```

#### `partition_by_a5(output_dir, resolution=None, auto=False, target_rows=100000, max_partitions=10000, compression='ZSTD', hive=False, keep_a5_column=None, overwrite=False)`

Partition the table into a directory by A5 cell. Pass `hive=True` for Hive-style `key=value/` subdirectories (matches CLI `--hive`).

With `hive=False` the partition value lives only in the file name, so the generated `a5_cell` column is dropped from the output. Pass `keep_a5_column=True` to keep it (mirrors the CLI's `--keep-*-column`).

```python
# Partition by A5
stats = table.partition_by_a5('output/', resolution=12)
print(f"Created {stats['file_count']} files")

# Or let gpio size the resolution from the data
stats = table.partition_by_a5('output/', auto=True)
```

#### `partition_by_string(output_dir, column, chars=None, hive=False, overwrite=False)`

Partition by string column values or prefixes.

```python
# Partition by full column values
stats = table.partition_by_string('output/', column='category')

# Partition by first 2 characters
stats = table.partition_by_string('output/', column='mgrs_code', chars=2)
```

#### `partition_by_kdtree(output_dir, iterations=None, auto=False, target_rows=120000, hive=False, keep_kdtree_column=None, overwrite=False)`

Partition by KD-tree spatial cells.

With `hive=False` the partition value lives only in the file name, so the generated
`kdtree_cell` column is dropped from the output. Pass `keep_kdtree_column=True` to
keep it (mirrors the CLI's `--keep-kdtree-column`).

Name an `iterations` count, or pass `auto=True` to size the tree from the row
count the way `gpio partition kdtree` does. A call that gives neither -- or both
-- raises `InvalidParameterError`, unless the table already carries the
`kdtree_cell` column (say from `add_kdtree()`): then no sizing parameter is
needed and the existing cells drive the partition.

```python
# Auto: sized from the row count, targeting ~120k rows per partition
stats = table.partition_by_kdtree('output/', auto=True)

# 64 partitions (2^6)
stats = table.partition_by_kdtree('output/', iterations=6)
```

#### `partition_by_admin(output_dir, dataset='gaul', levels=None, hive=False, overwrite=False, vecorel=False)`

Partition by administrative boundaries.

Set `vecorel=True` to write Vecorel-compliant admin columns
(`admin:country_code`, `admin:subdivision_code`) with schema metadata into each
partition. This forces the Overture dataset with `country,region` levels.

```python
# Partition by country using GAUL dataset
stats = table.partition_by_admin('output/', dataset='gaul', levels=['country'])

# Multi-level hierarchical
stats = table.partition_by_admin(
    'output/',
    dataset='gaul',
    levels=['continent', 'country', 'department'],
    hive=True
)

# Vecorel-compliant partitions (forces Overture country,region)
stats = table.partition_by_admin('output/', vecorel=True)
```

### Aggregation Methods {#aggregation}

#### `aggregate_a5(resolution, metric=None, breakdown=None, breakdown_limit=20, out_geometry='polygon', where=None, metric_nodata=None, bucket_point='geometry', bbox_column=None)`

Aggregate features into A5 grid cells with per-cell statistics for low-zoom visualization.

```python
import geoparquet_io as gpio

# Basic cell count at resolution 8
result = gpio.read('fields.parquet').aggregate_a5(resolution=8)
result.write('cells.parquet')

# Numeric rollups + category breakdown
result = gpio.read('fields.parquet').aggregate_a5(
    resolution=8,
    metric="sum:area_ha,avg:yield",
    breakdown="crop_type",
    breakdown_limit=15,
)
result.write('cells.parquet')

# Aggregate only a subset of rows
result = gpio.read('fields.parquet').aggregate_a5(
    resolution=8,
    where="\"crop:name\" = 'wheat'",
)
result.write('wheat_cells.parquet')

# No geometry — plain Parquet (re-join a5_cell to geometry later)
result = gpio.read('fields.parquet').aggregate_a5(
    resolution=8,
    metric="sum:area_ha",
    out_geometry="none",
)
result.write('cells_stats.parquet')
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `resolution` | int | required | A5 resolution level (0–30) |
| `metric` | str | None | Numeric rollups: `"sum:col,avg:col"`. Bare column = sum. |
| `breakdown` | str | None | Categorical column to pivot into `count_<value>` columns |
| `breakdown_limit` | int | 20 | Max categories; remainder goes into `count_other` |
| `out_geometry` | str | `"polygon"` | Geometry per cell: `"polygon"`, `"centroid"`, `"both"`, or `"none"` |
| `where` | str | None | DuckDB WHERE clause filtering input rows before aggregation |
| `metric_nodata` | str | None | NoData sentinel value(s) mapped to NULL in metric columns, e.g. `"-999"` or `"-999,-9999"` (`"nan"` matches NaN) |
| `bucket_point` | str | `"geometry"` | Keying point source: `"geometry"` (centroid), `"bbox"` (bbox covering column center, skips reading geometry), or a point column name |
| `bbox_column` | str | None | Bbox covering column for `bucket_point="bbox"` (auto-detected when omitted) |

Every output row carries `a5_cell` (UBIGINT) as the bucket identifier.

#### `aggregate_h3(resolution, metric=None, breakdown=None, breakdown_limit=20, out_geometry='polygon', where=None, metric_nodata=None, bucket_point='geometry', bbox_column=None)`

Aggregate features into H3 hexagonal grid cells. Same options as `aggregate_a5`,
but the resolution range is **0–15** and the bucket id column is `h3_cell` (a
string).

```python
import geoparquet_io as gpio

result = gpio.read('fields.parquet').aggregate_h3(
    resolution=8,
    metric="sum:area_ha",
    breakdown="crop_type",
)
result.write('cells.parquet')
```

Every output row carries `h3_cell` (string) as the bucket identifier.

#### `aggregate_admin(level='country', metric=None, breakdown=None, breakdown_limit=20, out_geometry='polygon', where=None, metric_nodata=None, bucket_point='geometry', bbox_column=None)`

Aggregate features into administrative regions (Overture Maps) with per-region statistics.

```python
import geoparquet_io as gpio

# Country-level aggregation
result = gpio.read('fields.parquet').aggregate_admin(level="country")
result.write('by_country.parquet')

# Region-level with rollups and breakdown
result = gpio.read('fields.parquet').aggregate_admin(
    level="region",
    metric="sum:area_ha,avg:yield",
    breakdown="crop_type",
)
result.write('by_region.parquet')
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `level` | str | `"country"` | Admin level: `"country"` or `"region"` |
| `metric` | str | None | Numeric rollups: `"sum:col,avg:col"`. Bare column = sum. |
| `breakdown` | str | None | Categorical column to pivot into `count_<value>` columns |
| `breakdown_limit` | int | 20 | Max categories; remainder goes into `count_other` |
| `out_geometry` | str | `"polygon"` | Geometry per region: `"polygon"`, `"centroid"`, `"both"`, or `"none"` |
| `where` | str | None | DuckDB WHERE clause filtering input rows before aggregation |

Every output row carries `admin_code` and `admin_name` bucket identifiers. Features outside all regions go into an `unassigned` bucket.

!!! note "Known limitation"
    `admin_name` currently equals the ISO code (same as `admin_code`).

#### `overview(level, cell_column=None)`

Roll an aggregate table up to a coarser overview level by true cell hierarchy
(`a5_cell_to_parent` / `h3_cell_to_parent`; admin region codes collapse to
their ISO country prefix). The table must be a `process aggregate` output.

```python
import geoparquet_io as gpio

# Grid aggregate: roll res-10 cells up to res 6
coarse = gpio.read('cells.parquet').overview(6)
coarse.write('cells_r6.parquet')

# Region-level admin aggregate: roll up to country
by_country = gpio.read('by_region.parquet').overview('country')
by_country.write('by_region_country.parquet')
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `level` | int or str | required | Coarser grid resolution, or `"country"` for admin |
| `cell_column` | str | None | Cell id column when auto-detection fails |
| `scheme` | str | None | Bucketing scheme (`a5`/`h3`/`admin`) when inference is ambiguous, e.g. H3 ids stored as integers |

`count`, `sum_*`, `min_*`, `max_*`, and breakdown `count_*` columns roll up
exactly; `avg_*` is count-weighted (exact when the metric had no NULLs). For
file-based batch building of several levels (with auto level selection), use
`ops.create_overviews`.

### Sub-Partitioning Utilities

For working with directories of partitioned files, gpio provides utilities to find and sub-partition large files.

#### `find_large_files(directory, min_size_bytes, recursive=True)`

Find parquet files exceeding a size threshold.

```python
from geoparquet_io.core.sub_partition import find_large_files

# Find files over 100MB
large_files = find_large_files('/data/partitions/', min_size_bytes=100 * 1024 * 1024)
print(f"Found {len(large_files)} large files")
for file_path in large_files:
    print(f"  {file_path}")
```

**Parameters:**
- `directory` (str): Directory to search
- `min_size_bytes` (int): Minimum file size in bytes
- `recursive` (bool): Search subdirectories (default: True)

**Returns:** List of file paths sorted by size (largest first)

#### `sub_partition_directory(directory, partition_type, min_size_bytes, resolution=None, level=None, in_place=False, hive=False, overwrite=False, verbose=False, force=False, skip_analysis=True, compression='ZSTD', compression_level=None, auto=False, target_rows=100000, max_partitions=10000, partition_resolution=None)`

Sub-partition large files in a directory using spatial indexing.

```python
from geoparquet_io.core.sub_partition import sub_partition_directory

# Sub-partition all H3-partitioned files over 100MB
result = sub_partition_directory(
    directory='/data/h3_partitions/',
    partition_type='h3',
    min_size_bytes=100 * 1024 * 1024,
    resolution=4,
    in_place=True,  # Replace originals
    verbose=True
)

print(f"Processed: {result['processed']}")
print(f"Errors: {len(result['errors'])}")

# Sub-partition S2 files with auto-resolution
result = sub_partition_directory(
    directory='/data/s2_partitions/',
    partition_type='s2',
    min_size_bytes=50 * 1024 * 1024,
    auto=True,
    target_rows=50000,
    skip_analysis=True  # Skip per-file analysis for speed
)

# Sub-partition quadkey files
result = sub_partition_directory(
    directory='/data/quadkey_partitions/',
    partition_type='quadkey',
    min_size_bytes=200 * 1024 * 1024,
    resolution=8,
    hive=True
)
```

**Parameters:**
- `directory` (str): Directory containing parquet files
- `partition_type` (str): Type of partition ("h3", "a5", "s2", "quadkey")
- `min_size_bytes` (int): Minimum file size to process
- `resolution` (int | None): Resolution for H3/quadkey (0-15 for H3)
- `partition_resolution` (int | None): Quadkey partition prefix length (0-23, no greater than `resolution`); required with explicit quadkey `resolution`
- `level` (int | None): Level for S2 (alias for resolution)
- `in_place` (bool): Delete originals after successful sub-partition (default: False)
- `hive` (bool): Use Hive-style partitioning (default: False)
- `overwrite` (bool): Overwrite existing output directories (default: False)
- `verbose` (bool): Print verbose output (default: False)
- `force` (bool): Force operation even with warnings (default: False)
- `skip_analysis` (bool): Skip partition analysis for performance (default: True)
- `compression` (str): Compression codec (default: "ZSTD")
- `compression_level` (int | None): Compression level (default: None — the codec picks its own; a fixed value is rejected by codecs whose range excludes it, e.g. GZIP is 1-9)
- `auto` (bool): Auto-calculate resolution (default: False)
- `target_rows` (int): Target rows per partition for auto mode (default: 100000)
- `max_partitions` (int): Max partitions for auto mode (default: 10000)

**Returns:** Dictionary with keys:
- `processed` (int): Number of files successfully processed
- `skipped` (int): Number of files skipped (below threshold)
- `errors` (list): List of dicts with keys `file` and `error`

**Note:** When `auto=True`, the function automatically calculates the best resolution based on data distribution. Use `skip_analysis=True` for faster batch processing when you trust the resolution settings.

#### `add_bbox_metadata(bbox_column='bbox')`

Add bbox covering metadata to the table schema.

```python
# Add bbox column and metadata in one chain
table_with_bbox = table.add_bbox().add_bbox_metadata()

# Or add metadata to existing bbox column
table_with_meta = table.add_bbox_metadata()
```

Raises `ValueError` when the table declares GeoParquet 1.0: the `covering` key was
introduced in 1.1, so writing it at 1.0 would produce a file that fails validation.
Write the table at 1.1 first (`table.write(path, geoparquet_version='1.1')`).

Raises `GeoParquetError` when the table carries no GeoParquet `geo` metadata at all:
`covering` describes a geometry column that `encoding` and `geometry_types` define, so
a table read from plain Parquet is refused here exactly as `gpio add bbox-metadata`
refuses the file. Convert it first: `gpio convert geoparquet in.parquet out.parquet`.
`ops.add_bbox_metadata(table)` is the function form and applies the same rules.

#### `check()` / `check_spatial()` / `check_compression()` / `check_bbox()` / `check_row_groups()`

Run best-practice checks on the table.

```python
# Run all checks
result = table.check()
if result.passed():
    print("All checks passed!")
else:
    for failure in result.failures():
        print(f"Failed: {failure}")

# Individual checks
spatial_result = table.check_spatial()
compression_result = table.check_compression()
bbox_result = table.check_bbox()
row_group_result = table.check_row_groups()

# Access results as dictionary
details = result.to_dict()
```

#### `check_optimization()`

Evaluate spatial query optimization across five factors (native geo types, geo bbox stats, spatial sorting, row group size, ZSTD compression). Returns a score from 0 to 5.

```python
result = table.check_optimization()
details = result.to_dict()
print(f"Score: {details['score']}/5 - {details['level']}")
```

#### `check_spatial_pushdown()`

Check spatial filter pushdown readiness by analyzing row group bounding box overlap.

```python
result = table.check_spatial_pushdown()
details = result.to_dict()
print(f"Skip rate: {details['estimated_skip_rate']}")
print(f"Bbox area ratio: {details['avg_bbox_area_ratio']}")
```

#### `check_bloom_filters()`

Check bloom filter presence across columns.

```python
result = table.check_bloom_filters()
details = result.to_dict()
```

#### `Table.explain_analyze(file_path, query=None)`

Run DuckDB EXPLAIN ANALYZE on a query against a Parquet file. This is a classmethod, not an instance method.

```python
result = Table.explain_analyze('data.parquet')
print(result)

# Custom query
result = Table.explain_analyze(
    'data.parquet',
    query="SELECT * FROM read_parquet('{file}') WHERE id > 10"
)
```

#### `validate(version=None)`

Validate against GeoParquet specification.

```python
result = table.validate()
if result.passed():
    print(f"Valid GeoParquet {table.geoparquet_version}")

# Validate against specific version
result = table.validate(version='1.1')
```

#### `upload(destination, compression='ZSTD', compression_level=None, row_group_size_mb=None, row_group_rows=None, geoparquet_version=None, profile=None, s3_endpoint=None, ...)`

Write and upload the table to cloud object storage (S3, GCS, Azure).

```python
# Upload to S3
gpio.read('input.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .upload('s3://bucket/data.parquet')

# Upload with AWS profile
table.upload('s3://bucket/data.parquet', profile='my-aws-profile')

# Upload to S3-compatible storage (MinIO, source.coop)
table.upload(
    's3://bucket/data.parquet',
    s3_endpoint='minio.example.com:9000',
    s3_use_ssl=False
)

# Upload to GCS
table.upload('gs://bucket/data.parquet')

# Upload as GeoParquet 2.0
table.upload('s3://bucket/data.parquet', geoparquet_version='2.0')
```

## Converting Other Formats

### Reading Other Formats (to GeoParquet)

Use `gpio.convert()` to load GeoPackage, Shapefile, GeoJSON, FlatGeobuf, or CSV files:

```python
import geoparquet_io as gpio

# Convert GeoPackage
table = gpio.convert('data.gpkg')

# Convert Shapefile
table = gpio.convert('data.shp')

# Convert GeoJSON
table = gpio.convert('data.geojson')

# Convert CSV with WKT geometry
table = gpio.convert('data.csv', wkt_column='geometry')

# Convert CSV with lat/lon columns
table = gpio.convert('data.csv', lat_column='latitude', lon_column='longitude')

# Convert from S3 with authentication
table = gpio.convert('s3://bucket/data.gpkg', profile='my-aws')
```

Unlike the CLI `convert` command, the Python API does NOT apply Hilbert sorting by default. Chain `.sort_hilbert()` explicitly if you want spatial ordering:

```python
# Full conversion workflow
gpio.convert('data.shp') \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')
```

### Writing to Other Formats (from GeoParquet)

The `Table.write()` method supports multiple output formats with automatic format detection:

```python
import geoparquet_io as gpio

# Read GeoParquet
table = gpio.read('data.parquet')

# Write to different formats (auto-detected from extension)
table.write('output.gpkg')      # GeoPackage
table.write('output.fgb')       # FlatGeobuf
table.write('output.csv')       # CSV with WKT
table.write('output.shp')       # Shapefile
table.write('output.geojson')   # GeoJSON

# Or specify format explicitly
table.write('output.dat', format='csv')
```

#### Format-Specific Options

**GeoPackage:**

```python
table.write('output.gpkg',
           layer_name='buildings',  # Custom layer name
           overwrite=True)          # Overwrite existing file
```

**Shapefile:**

```python
table.write('output.shp',
           encoding='ISO-8859-1',  # Custom encoding (default: UTF-8)
           overwrite=True)
```

!!! warning "Shapefile Limitations"
    Shapefiles have significant limitations:

    - Column names truncated to 10 characters
    - File size limit of 2GB
    - Limited data type support
    - Creates multiple files (.shp, .shx, .dbf, .prj)

    Consider using GeoPackage or FlatGeobuf for new projects.

**CSV:**

```python
table.write('output.csv',
           include_wkt=True,    # Include WKT geometry (default)
           include_bbox=False)  # Exclude bbox column
```

**GeoJSON:**

```python
table.write('output.geojson',
           precision=5,             # Coordinate precision (default: 7)
           write_bbox=True,         # Include bbox for each feature
           id_field='osm_id',       # Use field as feature ID
           pretty=True,             # Pretty-print JSON
           keep_crs=False)          # Reproject to WGS84 (default)
```

#### Using ops Functions for Format Conversion

For functional-style programming, use `ops.convert_to_*()` functions:

```python
from geoparquet_io import ops
import pyarrow.parquet as pq

# Read Arrow table
table = pq.read_table('data.parquet')

# Convert to various formats
ops.convert_to_geopackage(table, 'output.gpkg', layer_name='features')
ops.convert_to_flatgeobuf(table, 'output.fgb')
ops.convert_to_csv(table, 'output.csv', include_wkt=True)
ops.convert_to_shapefile(table, 'output.shp', encoding='UTF-8')
ops.convert_to_geojson(table, 'output.geojson', precision=7)
```

## Reading Partitioned Data

Use `gpio.read_partition()` to read Hive-partitioned datasets:

```python
import geoparquet_io as gpio

# Read from a partitioned directory
table = gpio.read_partition('partitioned_output/')

# Read with glob pattern
table = gpio.read_partition('data/quadkey=*/*.parquet')

# Allow schema differences across partitions
table = gpio.read_partition('output/', allow_schema_diff=True)
```

## Method Chaining

All transformation methods return a new `Table`, enabling fluent chains:

```python
result = gpio.read('input.parquet') \
    .extract(limit=10000) \
    .add_bbox() \
    .add_quadkey(resolution=12) \
    .sort_hilbert()

result.write('output.parquet')
```

## Pure Functions (ops module)

For integration with other Arrow workflows, use the `ops` module which provides pure functions:

```python
import pyarrow.parquet as pq
from geoparquet_io.api import ops

# Read with PyArrow
table = pq.read_table('input.parquet')

# Apply transformations
table = ops.add_bbox(table)
table = ops.add_quadkey(table, resolution=12)
table = ops.sort_hilbert(table)

# Write with PyArrow
pq.write_table(table, 'output.parquet')
```

> **Note:** `pq.write_table()` may not preserve all GeoParquet metadata (such as the `geo` key with CRS and geometry column info). For proper metadata preservation, wrap the result in `Table(table).write('output.parquet')` or use `write_parquet_with_metadata()` from `geoparquet_io.core.common`. The fluent API's `.write()` method is recommended.

Partitioning is the exception to the `table in -> table out` shape: like the CLI it
writes a *directory*, so `ops.partition_by_*` takes the table plus an output
directory and returns the run's statistics -- the same
`{'output_dir': str, 'file_count': int, 'hive': bool}` dict from every scheme.

```python
import pyarrow.parquet as pq
from geoparquet_io.api import ops

table = pq.read_table('input.parquet')

# An explicit resolution, or auto=True -- gpio never guesses one for you
stats = ops.partition_by_h3(table, 'output/', resolution=7)
stats = ops.partition_by_a5(table, 'output/', auto=True, target_rows=50000)

# Non-spatial schemes return the same dict
stats = ops.partition_by_string(table, 'output/', column='region', hive=True)
stats = ops.partition_by_admin(table, 'output/', levels=['country'])

print(f"Created {stats['file_count']} files")
```

### Sub-partitioning a directory

`gpio partition <index> <dir>/ --min-size` is the other half of the partition
commands: it walks a *directory*, splits every file over a size threshold into a
sibling `<file>_<index>/` directory, and with `--in-place` removes each original
once its sub-partitions hold every row it had. That is the step you reach for
when partitioning by country or by a string column has left a few oversized
files.

Its unit of work is a directory on disk, not an in-memory table, so it is not a
`Table` method. It is an `ops` function over a path — one per index, named after
the command it mirrors:

```python
from geoparquet_io.api import ops

# Split every file over 100MB into H3 sub-partitions, removing the originals
result = ops.sub_partition_by_h3(
    'by_country/', min_size='100MB', resolution=7, in_place=True
)
print(f"{result['processed']} file(s) sub-partitioned")

# Or see what would happen, writing and deleting nothing
plan = ops.sub_partition_by_a5('by_country/', min_size='100MB', auto=True, preview=True)
for candidate in plan['candidates']:
    print(f"{candidate['path']} -> {candidate['output_dir']}/")
```

The return value is a dict with `processed`, `skipped`, `errors`, `candidates`
(the files the threshold selected, with sizes and destinations) and `preview`.

- `min_size` takes the CLI's `'100MB'` spelling or a plain byte count.
- A file whose sub-partitions do not hold all of its rows keeps its original —
  rows with a NULL or empty geometry get a NULL index cell and are dropped by
  partitioning — and is reported in `errors`.
- Any per-file failure raises `PartitionError` once the run finishes, so a
  partial run is never read as a complete one. `exc.result` carries the run dict,
  including the files that succeeded.
- `column_name=` and `output_dir=` are refused: in directory mode each file gets
  its own sibling directory and the default index column name. Partition a single
  file with `ops.partition_by_<index>` if you need to control those.
- `ops.sub_partition_by_quadkey` accepts both `resolution` for column precision and
  `partition_resolution` for the output prefix length (0-23, no greater than
  `resolution`), or `auto=True` to calculate both. Directory mode forwards these
  to the same validated partitioner as single-file mode.
- `ops.sub_partition_by_s2` uses the `geography` DuckDB community extension,
  which gpio installs on first use; where that extension cannot load it raises
  `ExtensionUnavailableError` before touching a file, like every other S2
  entry point.

### Available Functions

| Function | Description |
|----------|-------------|
| `ops.add_bbox(table, column_name='bbox', geometry_column=None)` | Add bounding box column |
| `ops.add_bbox_metadata(table, bbox_column='bbox', geometry_column=None)` | Add `covering` metadata for an existing bbox column |
| `ops.add_quadkey(table, column_name='quadkey', resolution=13, use_centroid=False, geometry_column=None)` | Add quadkey column |
| `ops.add_h3(table, column_name='h3_cell', resolution=9, geometry_column=None)` | Add H3 cell column |
| `ops.add_a5(table, column_name='a5_cell', resolution=15, geometry_column=None)` | Add A5 cell column |
| `ops.add_s2(table, column_name='s2_cell', level=13, geometry_column=None)` | Add S2 cell column |
| `ops.add_geometry_metrics(table, vecorel=True)` | Add geodesic area and perimeter columns |
| `ops.add_admin_divisions(table, dataset='gaul', levels=None, vecorel=False)` | Add admin division columns via spatial join |
| `ops.add_kdtree(table, column_name='kdtree_cell', iterations=None, sample_size=100000, geometry_column=None, auto=False, target_rows=120000)` | Add KD-tree cell column |
| `ops.sort_hilbert(table, geometry_column=None)` | Reorder by Hilbert curve |
| `ops.sort_str(table, geometry_column=None, tile_size=50000)` | Reorder with Sort-Tile-Recursive ordering (`tile_size` picks the strip count) |
| `ops.sort_column(table, column, descending=False)` | Sort by column(s) |
| `ops.sort_quadkey(table, column_name='quadkey', resolution=13, use_centroid=False, remove_column=False)` | Sort by quadkey |
| `ops.reproject(table, target_crs='EPSG:4326', source_crs=None, geometry_column=None, assume_crs84=False)` | Reproject geometry (`assume_crs84` treats an unknown/null CRS as OGC:CRS84) |
| `ops.extract(table, columns=None, exclude_columns=None, bbox=None, where=None, limit=None, geometry_column=None)` | Filter columns/rows |
| `ops.read_bigquery(table_id, project=None, credentials_file=None, where=None, bbox=None, bbox_mode='auto', bbox_threshold=500000, limit=None, columns=None, exclude_columns=None)` | Read BigQuery table |
| `ops.from_arcgis(service_url, token=None, where='1=1', bbox=None, include_cols=None, exclude_cols=None, limit=None)` | Fetch ArcGIS Feature Service |
| `ops.convert_to_geojson(table, output, precision=7, write_bbox=False, id_field=None)` | Convert to GeoJSON |
| `ops.convert_to_geopackage(table, output, layer_name='features', overwrite=False)` | Convert to GeoPackage |
| `ops.convert_to_flatgeobuf(table, output)` | Convert to FlatGeobuf |
| `ops.convert_to_csv(table, output, include_wkt=True, include_bbox=True)` | Convert to CSV |
| `ops.convert_to_shapefile(table, output, encoding='UTF-8', overwrite=False)` | Convert to Shapefile |
| `ops.from_wfs(service_url, typename, version='auto', bbox=None, limit=None, max_workers=1, page_size=100000, auto_tile=True, ...)` | Fetch from WFS service |
| `ops.from_wfs_layers(service_url, typenames, output_dir, parallel_layers=1, max_workers=1, page_size=100000, ...)` | Fetch multiple WFS layers to directory |
| `ops.create_overviews(input_parquet, levels=None, max_tile_kb=500, bytes_per_cell=None, cell_column=None, scheme=None, output_dir=None, force=False, ...)` | Build coarser overview levels from an aggregate file |
| `ops.create_pmtiles_pyramid(input_path, output_path, levels=None, max_tile_kb=500, layer_mode='grouped', include_features=False, features_source=None, max_zoom=None, ...)` | Build a zoom-banded multi-level PMTiles archive from an aggregate file (requires tippecanoe + tile-join) |
| `ops.partition_by_h3(table, output_dir, resolution=None, auto=False, target_rows=100000, max_partitions=10000, compression='ZSTD', hive=False, keep_h3_column=None, overwrite=False, geometry_column=None)` | Partition into a directory by H3 cell |
| `ops.partition_by_a5(table, output_dir, resolution=None, auto=False, target_rows=100000, max_partitions=10000, compression='ZSTD', hive=False, keep_a5_column=None, overwrite=False, geometry_column=None)` | Partition into a directory by A5 cell |
| `ops.partition_by_s2(table, output_dir, level=None, auto=False, target_rows=100000, max_partitions=10000, compression='ZSTD', hive=False, keep_s2_column=None, overwrite=False, geometry_column=None)` | Partition into a directory by S2 cell |
| `ops.partition_by_quadkey(table, output_dir, resolution=None, partition_resolution=None, auto=False, target_rows=100000, max_partitions=10000, compression='ZSTD', hive=False, keep_quadkey_column=None, overwrite=False, geometry_column=None)` | Partition into a directory by quadkey |
| `ops.partition_by_kdtree(table, output_dir, iterations=None, auto=False, target_rows=120000, hive=False, keep_kdtree_column=None, overwrite=False, compression='ZSTD', compression_level=None, geometry_column=None)` | Partition into a directory by KD-tree cell |
| `ops.partition_by_string(table, output_dir, column, chars=None, hive=False, overwrite=False, compression='ZSTD', compression_level=None, geometry_column=None)` | Partition into a directory by string column value |
| `ops.partition_by_admin(table, output_dir, dataset='gaul', levels=None, hive=False, overwrite=False, vecorel=False, compression='ZSTD', compression_level=None, geometry_column=None)` | Partition into a directory by administrative boundaries |
| `ops.sub_partition_by_h3(directory, min_size, resolution=None, auto=False, in_place=False, preview=False, hive=False, overwrite=False, force=False, skip_analysis=False, compression='ZSTD', compression_level=None, ...)` | Split every file in a directory over `min_size` into H3 sub-partitions |
| `ops.sub_partition_by_a5(directory, min_size, resolution=None, auto=False, in_place=False, preview=False, ...)` | Split every file in a directory over `min_size` into A5 sub-partitions |
| `ops.sub_partition_by_quadkey(directory, min_size, resolution=None, partition_resolution=None, auto=False, in_place=False, preview=False, ...)` | Split every file in a directory over `min_size` into quadkey sub-partitions (pass both resolutions or use `auto=True`) |
| `ops.sub_partition_by_s2(directory, min_size, level=None, auto=False, in_place=False, preview=False, ...)` | Split every file in a directory over `min_size` into S2 sub-partitions |
| `ops.get_row_group_geo_stats(parquet_file)` | Per-row-group geo bbox statistics |
| `ops.compression_stats(path)` | Per-column compression ratios |
| `ops.explain_analyze(file_path, query=None)` | DuckDB EXPLAIN ANALYZE query plan |

## Pipeline Composition

Use `pipe()` to create reusable transformation pipelines:

```python
from geoparquet_io.api import pipe, read

# Define a reusable pipeline
preprocess = pipe(
    lambda t: t.add_bbox(),
    lambda t: t.add_quadkey(resolution=12),
    lambda t: t.sort_hilbert(),
)

# Apply to any table
result = preprocess(read('input.parquet'))
result.write('output.parquet')

# Or with ops functions
from geoparquet_io.api import ops

transform = pipe(
    lambda t: ops.add_bbox(t),
    lambda t: ops.add_quadkey(t, resolution=10),
    lambda t: ops.extract(t, limit=1000),
)

import pyarrow.parquet as pq
table = pq.read_table('input.parquet')
result = transform(table)
```

## Performance

The Python API provides the best performance because:

1. **No file I/O**: Data stays in memory as Arrow tables
2. **Zero-copy**: Arrow's columnar format enables efficient operations
3. **DuckDB backend**: Spatial operations use DuckDB's optimized engine

Benchmark comparison (75MB file, 400K rows):

| Approach | Time | Speedup |
|----------|------|---------|
| File-based CLI | 34s | baseline |
| Piped CLI | 16s | 53% faster |
| Python API | 7s | 78% faster |

## Integration with PyArrow

The API integrates seamlessly with PyArrow:

```python
import pyarrow.parquet as pq
import geoparquet_io as gpio
from geoparquet_io.api import Table

# From PyArrow Table
arrow_table = pq.read_table('input.parquet')
table = Table(arrow_table)
result = table.add_bbox().sort_hilbert()

# To PyArrow Table
arrow_result = result.to_arrow()

# Use with PyArrow operations
filtered = arrow_result.filter(arrow_result['population'] > 1000)
```

## Advanced: Direct Core Function Access

For power users who need direct access to core functions (e.g., for custom pipelines or when you need file-based operations without the Table wrapper):

```python
from geoparquet_io.core.add.bbox import add_bbox_column
from geoparquet_io.core.hilbert_order import hilbert_order

# File-based operations
add_bbox_column(
    input_parquet="input.parquet",
    output_parquet="output.parquet",
    bbox_column_name="bbox",
    verbose=True
)

hilbert_order(
    input_parquet="input.parquet",
    output_parquet="sorted.parquet",
    geometry_column="geometry",
    add_bbox_flag=True,
    verbose=True
)
```

See the sections below for all available functions.

> **Note:** The fluent API (`gpio.read()...`) is recommended for most use cases as it provides better ergonomics and in-memory performance. The core API is primarily useful for:
>
> - Integrating with existing file-based pipelines
> - When you need fine-grained control over function parameters
> - Building custom tooling around gpio

## Standalone Functions

### STAC Generation

Generate and validate STAC (SpatioTemporal Asset Catalog) metadata:

```python
from geoparquet_io import generate_stac, validate_stac

# Generate STAC Item for a single file
stac_path = generate_stac(
    'data.parquet',
    bucket='s3://my-bucket/data/'
)

# Generate STAC Collection for a directory
stac_path = generate_stac(
    'partitioned/',
    bucket='s3://my-bucket/data/',
    collection_id='my-dataset'
)

# With all options
stac_path = generate_stac(
    'data.parquet',
    output_path='custom.json',
    bucket='s3://my-bucket/data/',
    item_id='my-item',
    public_url='https://data.example.com/',
    overwrite=True,
    verbose=True
)

# Validate STAC
result = validate_stac('collection.json')
if result.passed():
    print("Valid STAC!")
else:
    for failure in result.failures():
        print(f"Issue: {failure}")
```

### CheckResult Class

All check and validate methods return a `CheckResult` object:

```python
from geoparquet_io import CheckResult

# Methods
result.passed()          # Returns True if all checks passed
result.failures()        # List of failure messages
result.warnings()        # List of warning messages
result.recommendations() # List of recommendations
result.to_dict()         # Full results as dictionary

# Can be used as boolean
if result:
    print("Passed!")
```

### Row Group Geo Statistics

Inspect per-row-group bounding box statistics to verify spatial locality:

```python
from geoparquet_io.api.ops import get_row_group_geo_stats

# Get stats for each row group
stats = get_row_group_geo_stats('hilbert_sorted.parquet')

for rg in stats:
    print(f"Row group {rg['row_group_id']}: "
          f"{rg['num_rows']} rows, "
          f"bbox=[{rg['xmin']:.2f}, {rg['ymin']:.2f}, "
          f"{rg['xmax']:.2f}, {rg['ymax']:.2f}]")

# Works with both:
# - Native Parquet geo stats (GeoParquet 2.0, parquet-geo-only)
# - Bbox column statistics (files with a bbox column)
```

## See Also

- [Command Piping](../guide/piping.md) - CLI piping for shell workflows
- [Examples](../examples/basic.md) - Python usage examples
- [Spatial Performance Guide](../concepts/spatial-indices.md) - Understanding bbox, sorting, and partitioning
