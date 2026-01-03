# Basic Usage Examples

Python API examples demonstrating common operations.

## Reading and Inspecting Files

```python
import geoparquet_io as gpio

# Read a GeoParquet file
table = gpio.read('data.parquet')

# Get basic info
print(f"Rows: {table.num_rows}")
print(f"Columns: {table.column_names}")
print(f"Geometry: {table.geometry_column}")
print(f"CRS: {table.crs}")
print(f"Bounds: {table.bounds}")

# Print formatted summary
table.info()
```

## Adding Bounding Boxes

```python
import geoparquet_io as gpio

# Add bbox column
gpio.read('input.parquet') \
    .add_bbox() \
    .write('output.parquet')

# With custom column name
gpio.read('input.parquet') \
    .add_bbox(column_name='bounds') \
    .write('output.parquet')
```

## Hilbert Curve Sorting

```python
import geoparquet_io as gpio

# Sort for spatial locality
gpio.read('input.parquet') \
    .sort_hilbert() \
    .write('sorted.parquet')

# Add bbox and sort in one chain
gpio.read('input.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .write('optimized.parquet')
```

## Adding Spatial Indices

```python
import geoparquet_io as gpio

# Add H3 hexagonal cells
gpio.read('input.parquet') \
    .add_h3(resolution=9) \
    .write('with_h3.parquet')

# Add quadkey tiles
gpio.read('input.parquet') \
    .add_quadkey(resolution=12) \
    .write('with_quadkey.parquet')

# Add multiple indices
gpio.read('input.parquet') \
    .add_bbox() \
    .add_h3(resolution=9) \
    .add_quadkey(resolution=12) \
    .sort_hilbert() \
    .write('enriched.parquet')
```

## Filtering Data

```python
import geoparquet_io as gpio

# Limit rows
table = gpio.read('input.parquet').extract(limit=1000)

# Select specific columns
table = gpio.read('input.parquet').extract(columns=['name', 'geometry'])

# Exclude columns
table = gpio.read('input.parquet').extract(exclude_columns=['temp_id'])

# Spatial filter by bounding box
table = gpio.read('input.parquet').extract(
    bbox=(-122.5, 37.5, -122.0, 38.0)
)

# SQL WHERE clause
table = gpio.read('input.parquet').extract(
    where="population > 10000"
)

# Combined filtering
gpio.read('input.parquet') \
    .extract(
        bbox=(-122.5, 37.5, -122.0, 38.0),
        columns=['name', 'population', 'geometry'],
        where="population > 1000",
        limit=5000
    ) \
    .write('filtered.parquet')
```

## Converting from Other Formats

```python
import geoparquet_io as gpio

# Convert GeoPackage
gpio.convert('data.gpkg') \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')

# Convert Shapefile
gpio.convert('data.shp').write('output.parquet')

# Convert CSV with lat/lon
gpio.convert('data.csv', lat_column='lat', lon_column='lon') \
    .write('output.parquet')

# Convert CSV with WKT geometry
gpio.convert('data.csv', wkt_column='geometry') \
    .write('output.parquet')
```

## Partitioning Data

```python
import geoparquet_io as gpio

# Partition by H3 cells
stats = gpio.read('input.parquet') \
    .add_h3(resolution=9) \
    .partition_by_h3('output/', resolution=6)
print(f"Created {stats['file_count']} files")

# Partition by quadkey
stats = gpio.read('input.parquet') \
    .add_quadkey() \
    .partition_by_quadkey('output/', partition_resolution=4)
print(f"Created {stats['file_count']} files")
```

## Reading Partitioned Data

```python
import geoparquet_io as gpio

# Read from partitioned directory
table = gpio.read_partition('partitioned_output/')

# Read with glob pattern
table = gpio.read_partition('data/quadkey=*/*.parquet')

# Allow schema differences across partitions
table = gpio.read_partition('output/', allow_schema_diff=True)
```

## Compression Options

Available compression formats:

--8<-- "_includes/compression-options.md"

Example usage:

```python
import geoparquet_io as gpio

# ZSTD (recommended, default)
gpio.read('input.parquet') \
    .add_bbox() \
    .write('output.parquet', compression='ZSTD')

# GZIP for wide compatibility
gpio.read('input.parquet') \
    .add_bbox() \
    .write('output.parquet', compression='GZIP', compression_level=6)

# LZ4 for fast decompression
gpio.read('input.parquet') \
    .add_bbox() \
    .write('output.parquet', compression='LZ4')
```

## Cloud Storage

```python
import geoparquet_io as gpio

# Upload to S3
gpio.read('input.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .upload('s3://bucket/data.parquet')

# With AWS profile
gpio.read('input.parquet') \
    .upload('s3://bucket/data.parquet', profile='my-aws-profile')

# S3-compatible storage (MinIO, source.coop)
gpio.read('input.parquet') \
    .upload(
        's3://bucket/data.parquet',
        s3_endpoint='minio.example.com:9000',
        s3_use_ssl=False
    )
```

## Advanced: Direct Core Function Access

For file-based operations or when you need fine-grained control:

```python
from geoparquet_io.core.add_bbox_column import add_bbox_column
from geoparquet_io.core.hilbert_order import hilbert_order

# Add bounding box (file-based)
add_bbox_column(
    input_parquet="input.parquet",
    output_parquet="output.parquet",
    bbox_name="bbox",
    verbose=True,
    compression="ZSTD",
    compression_level=15
)

# Hilbert sorting (file-based)
hilbert_order(
    input_parquet="input.parquet",
    output_parquet="sorted.parquet",
    geometry_column="geometry",
    add_bbox=True,
    verbose=True
)
```

See the [Core API Reference](../api/core.md) for all available functions.

## Next Steps

- [Batch Processing Examples](batch.md) - Processing multiple files
- [Workflow Examples](workflows.md) - Complete end-to-end workflows
- [Python API Reference](../api/python-api.md) - Full API documentation
