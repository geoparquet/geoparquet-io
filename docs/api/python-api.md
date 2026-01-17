# Python API

gpio provides a fluent Python API for GeoParquet transformations. This API offers the best performance by keeping data in memory as Arrow tables, avoiding file I/O entirely.

## Installation

```bash
pip install geoparquet-io
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

!!! note "No automatic Hilbert sorting"
    Unlike the CLI `gpio extract arcgis` command, the Python API does NOT apply Hilbert sorting by default. Chain `.sort_hilbert()` explicitly if you want spatial ordering.

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

#### `add_kdtree(column_name='kdtree_cell', iterations=9, sample_size=100000)`

Add a KD-tree cell column for data-adaptive spatial partitioning.

```python
# Default settings (512 partitions = 2^9)
table = gpio.read('input.parquet').add_kdtree()

# Fewer partitions
table = gpio.read('input.parquet').add_kdtree(iterations=6)  # 64 partitions

# More partitions with larger sample
table = gpio.read('input.parquet').add_kdtree(iterations=12, sample_size=500000)
```

#### `sort_hilbert()`

Reorder rows using Hilbert curve ordering for better spatial locality.

```python
table = gpio.read('input.parquet').sort_hilbert()
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

#### `reproject(target_crs='EPSG:4326', source_crs=None)`

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

#### `extract(columns=None, exclude_columns=None, bbox=None, where=None, limit=None)`

Filter columns and rows.

```python
# Select specific columns
table = gpio.read('input.parquet').extract(columns=['name', 'address'])

# Exclude columns
table = gpio.read('input.parquet').extract(exclude_columns=['temp_id'])

# Limit rows
table = gpio.read('input.parquet').extract(limit=1000)

# Spatial filter
table = gpio.read('input.parquet').extract(bbox=(-122.5, 37.5, -122.0, 38.0))

# SQL WHERE clause
table = gpio.read('input.parquet').extract(where="population > 10000")
```

#### `write(path, compression='ZSTD', compression_level=None, row_group_size_mb=None, row_group_rows=None)`

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

#### `to_arrow()`

Get the underlying PyArrow Table for interop with other Arrow-based tools.

```python
arrow_table = table.to_arrow()
```

#### `partition_by_quadkey(output_dir, resolution=13, partition_resolution=6, compression='ZSTD', hive=True, overwrite=False)`

Partition the table into a Hive-partitioned directory by quadkey.

```python
# Partition to a directory
stats = table.partition_by_quadkey('output/', resolution=12)
print(f"Created {stats['file_count']} files")

# With custom options
stats = table.partition_by_quadkey(
    'output/',
    partition_resolution=4,
    compression='SNAPPY',
    overwrite=True
)
```

#### `partition_by_h3(output_dir, resolution=9, compression='ZSTD', hive=True, overwrite=False)`

Partition the table into a Hive-partitioned directory by H3 cell.

```python
# Partition by H3
stats = table.partition_by_h3('output/', resolution=6)
print(f"Created {stats['file_count']} files")
```

#### `partition_by_string(output_dir, column, chars=None, hive=True, overwrite=False)`

Partition by string column values or prefixes.

```python
# Partition by full column values
stats = table.partition_by_string('output/', column='category')

# Partition by first 2 characters
stats = table.partition_by_string('output/', column='mgrs_code', chars=2)
```

#### `partition_by_kdtree(output_dir, iterations=9, hive=True, overwrite=False)`

Partition by KD-tree spatial cells.

```python
# Default (512 partitions = 2^9)
stats = table.partition_by_kdtree('output/')

# 64 partitions (2^6)
stats = table.partition_by_kdtree('output/', iterations=6)
```

#### `partition_by_admin(output_dir, dataset='gaul', levels=None, hive=True, overwrite=False)`

Partition by administrative boundaries.

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
```

#### `add_admin_divisions(dataset='overture', levels=None, country_filter=None, use_centroid=False)`

Add administrative division columns via spatial join.

```python
# Add country codes
enriched = table.add_admin_divisions(
    dataset='overture',
    levels=['country']
)

# Add multiple levels with country filter
enriched = table.add_admin_divisions(
    dataset='gaul',
    levels=['continent', 'country', 'department'],
    country_filter='US'
)
```

#### `add_bbox_metadata(bbox_column='bbox')`

Add bbox covering metadata to the table schema.

```python
# Add bbox column and metadata in one chain
table_with_bbox = table.add_bbox().add_bbox_metadata()

# Or add metadata to existing bbox column
table_with_meta = table.add_bbox_metadata()
```

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

#### `validate(version=None)`

Validate against GeoParquet specification.

```python
result = table.validate()
if result.passed():
    print(f"Valid GeoParquet {table.geoparquet_version}")

# Validate against specific version
result = table.validate(version='1.1')
```

#### `upload(destination, compression='ZSTD', profile=None, s3_endpoint=None, ...)`

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

### Available Functions

| Function | Description |
|----------|-------------|
| `ops.add_bbox(table, column_name='bbox', geometry_column=None)` | Add bounding box column |
| `ops.add_quadkey(table, column_name='quadkey', resolution=13, use_centroid=False, geometry_column=None)` | Add quadkey column |
| `ops.add_h3(table, column_name='h3_cell', resolution=9, geometry_column=None)` | Add H3 cell column |
| `ops.add_kdtree(table, column_name='kdtree_cell', iterations=9, sample_size=100000, geometry_column=None)` | Add KD-tree cell column |
| `ops.sort_hilbert(table, geometry_column=None)` | Reorder by Hilbert curve |
| `ops.sort_column(table, column, descending=False)` | Sort by column(s) |
| `ops.sort_quadkey(table, column_name='quadkey', resolution=13, use_centroid=False, remove_column=False)` | Sort by quadkey |
| `ops.reproject(table, target_crs='EPSG:4326', source_crs=None, geometry_column=None)` | Reproject geometry |
| `ops.extract(table, columns=None, exclude_columns=None, bbox=None, where=None, limit=None, geometry_column=None)` | Filter columns/rows |
| `ops.read_bigquery(table_id, project=None, credentials_file=None, where=None, bbox=None, bbox_mode='auto', bbox_threshold=500000, limit=None, columns=None, exclude_columns=None)` | Read BigQuery table |
| `ops.from_arcgis(service_url, token=None, where='1=1', bbox=None, include_cols=None, exclude_cols=None, limit=None)` | Fetch ArcGIS Feature Service |

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
from geoparquet_io.core.add_bbox_column import add_bbox_column
from geoparquet_io.core.hilbert_order import hilbert_order

# File-based operations
add_bbox_column(
    input_parquet="input.parquet",
    output_parquet="output.parquet",
    bbox_name="bbox",
    verbose=True
)

hilbert_order(
    input_parquet="input.parquet",
    output_parquet="sorted.parquet",
    geometry_column="geometry",
    add_bbox=True,
    verbose=True
)
```

See [Core Functions Reference](core.md) for all available functions.

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

## See Also

- [Command Piping](../guide/piping.md) - CLI piping for shell workflows
- [Core API Reference](core.md) - Low-level function reference
- [Spatial Performance Guide](../concepts/spatial-indices.md) - Understanding bbox, sorting, and partitioning
