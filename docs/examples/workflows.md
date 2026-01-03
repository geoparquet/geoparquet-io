# Workflow Examples

Complete end-to-end workflows for common GeoParquet tasks.

## Workflow: Optimize a Shapefile for Cloud Queries

Convert a Shapefile to an optimized GeoParquet file ready for cloud analytics.

### CLI Version

```bash
# Single command with all optimizations
gpio convert buildings.shp optimized.parquet

# Or step by step for more control
gpio convert buildings.shp temp.parquet --skip-hilbert
gpio add bbox temp.parquet | gpio sort hilbert - optimized.parquet
rm temp.parquet

# Verify the result
gpio check all optimized.parquet
```

### Python Version

```python
import geoparquet_io as gpio

# Convert with all optimizations
gpio.convert('buildings.shp') \
    .add_bbox() \
    .sort_hilbert() \
    .write('optimized.parquet')

# Verify
table = gpio.read('optimized.parquet')
table.info()
```

---

## Workflow: Create a Partitioned Dataset for Analytics

Split a large dataset into partitioned files for efficient querying in BigQuery, Athena, or DuckDB.

### CLI Version

```bash
# Preview the partition strategy
gpio partition h3 buildings.parquet --resolution 6 --preview

# Create partitions
gpio partition h3 buildings.parquet output_dir/ --resolution 6

# Or partition by country
gpio add admin-divisions buildings.parquet | \
    gpio partition admin - output_dir/ --column country_code
```

### Python Version

```python
import geoparquet_io as gpio

# Partition by H3 cells
gpio.read('buildings.parquet') \
    .add_h3(resolution=9) \
    .partition_by_h3('output/', resolution=6)

# Partition by quadkey
gpio.read('buildings.parquet') \
    .add_quadkey(resolution=12) \
    .partition_by_quadkey('output/', partition_resolution=4)
```

---

## Workflow: Filter and Upload to Cloud Storage

Extract a spatial subset and upload to S3 for sharing.

### CLI Version

```bash
# Filter by bounding box, optimize, and save locally
gpio extract --bbox "-122.5,37.5,-122.0,38.0" buildings.parquet | \
    gpio add bbox - | \
    gpio sort hilbert - sf_buildings.parquet

# Upload to S3
gpio publish upload sf_buildings.parquet s3://my-bucket/data/sf_buildings.parquet
```

### Python Version

```python
import geoparquet_io as gpio

# Filter, optimize, and upload in one chain
gpio.read('buildings.parquet') \
    .extract(bbox=(-122.5, 37.5, -122.0, 38.0)) \
    .add_bbox() \
    .sort_hilbert() \
    .upload('s3://my-bucket/data/sf_buildings.parquet')
```

---

## Workflow: Enrich Data with Spatial Indices

Add multiple spatial indices for different query patterns.

### CLI Version

```bash
# Add H3, quadkey, and bbox indices
gpio add bbox input.parquet | \
    gpio add h3 --resolution 9 - | \
    gpio add quadkey --resolution 12 - | \
    gpio sort hilbert - enriched.parquet

# Verify indices were added
gpio inspect enriched.parquet
```

### Python Version

```python
import geoparquet_io as gpio

gpio.read('input.parquet') \
    .add_bbox() \
    .add_h3(resolution=9) \
    .add_quadkey(resolution=12) \
    .sort_hilbert() \
    .write('enriched.parquet')
```

---

## Workflow: Validate and Fix GeoParquet Files

Check a file against best practices and automatically fix issues.

### CLI Version

```bash
# Check current state
gpio check all input.parquet

# Auto-fix issues
gpio check all input.parquet --fix --fix-output fixed.parquet

# Verify fixes
gpio check all fixed.parquet
```

### Python Version

```python
import geoparquet_io as gpio
import subprocess

# Check using CLI (no Python API for check yet)
result = subprocess.run(
    ['gpio', 'check', 'all', 'input.parquet'],
    capture_output=True,
    text=True
)
print(result.stdout)

# Manual fix using Python API
gpio.read('input.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .write('fixed.parquet')
```

---

## Workflow: Process Remote Files

Read from HTTP/S3/GCS and write to cloud storage.

### CLI Version

```bash
# Download, optimize, and upload
gpio add bbox https://example.com/data.parquet | \
    gpio sort hilbert - | \
    gpio publish upload - s3://my-bucket/optimized.parquet

# S3 to S3 processing
gpio add bbox s3://source-bucket/data.parquet \
    s3://dest-bucket/optimized.parquet --profile my-aws
```

### Python Version

```python
import geoparquet_io as gpio

# Note: gpio.read() doesn't support remote files directly yet
# Use the CLI for remote reads, or download first

# For S3 uploads:
gpio.read('local_data.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .upload('s3://my-bucket/data.parquet', profile='my-aws')
```

---

## Workflow: Merge Partitioned Data

Read a partitioned dataset and write as a single file.

### CLI Version

```bash
# Read partitions and write single file (requires Python API or DuckDB)
python -c "
import geoparquet_io as gpio
gpio.read_partition('partitioned_dir/') \
    .sort_hilbert() \
    .write('merged.parquet')
"
```

### Python Version

```python
import geoparquet_io as gpio

# Read all partitions
table = gpio.read_partition('partitioned_dir/')

# Optionally re-sort
table.sort_hilbert().write('merged.parquet')

# With schema differences allowed
table = gpio.read_partition('partitioned_dir/', allow_schema_diff=True)
table.write('merged.parquet')
```

---

## Workflow: Convert CSV with Coordinates

Convert a CSV with lat/lon columns to GeoParquet.

### CLI Version

```bash
# Auto-detect lat/lon columns
gpio convert points.csv output.parquet --lat-column latitude --lon-column longitude

# With optimization
gpio convert points.csv | \
    gpio add bbox - | \
    gpio sort hilbert - output.parquet
```

### Python Version

```python
import geoparquet_io as gpio

# Convert with lat/lon columns
gpio.convert('points.csv', lat_column='latitude', lon_column='longitude') \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')

# Or with WKT geometry column
gpio.convert('data.csv', wkt_column='geom') \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')
```

---

## Workflow: Create Reusable Pipeline

Define a standard processing pipeline for consistent data preparation.

### Python Version

```python
from geoparquet_io.api import pipe, read

# Define standard preprocessing pipeline
optimize = pipe(
    lambda t: t.add_bbox(),
    lambda t: t.add_h3(resolution=9),
    lambda t: t.sort_hilbert(),
)

# Apply to multiple files
for input_file in ['file1.parquet', 'file2.parquet', 'file3.parquet']:
    output_file = input_file.replace('.parquet', '_optimized.parquet')
    optimize(read(input_file)).write(output_file)
```

---

## See Also

- [Basic Usage Examples](basic.md) - Simple operations
- [Batch Processing](batch.md) - Processing multiple files
- [Python API Reference](../api/python-api.md) - Full API documentation
- [Command Piping](../guide/piping.md) - CLI piping guide
