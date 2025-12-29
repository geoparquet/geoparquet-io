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

## Table Class

The `Table` class wraps a PyArrow Table and provides chainable transformation methods.

### Properties

| Property | Description |
|----------|-------------|
| `num_rows` | Number of rows in the table |
| `column_names` | List of column names |
| `geometry_column` | Name of the geometry column |

### Methods

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

#### `sort_hilbert()`

Reorder rows using Hilbert curve ordering for better spatial locality.

```python
table = gpio.read('input.parquet').sort_hilbert()
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

Write the table to a GeoParquet file.

```python
table.write('output.parquet')

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
| `ops.sort_hilbert(table, geometry_column=None)` | Reorder by Hilbert curve |
| `ops.extract(table, columns=None, exclude_columns=None, bbox=None, where=None, limit=None, geometry_column=None)` | Filter columns/rows |

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

## See Also

- [Command Piping](../guide/piping.md) - CLI piping for shell workflows
- [Core API Reference](core.md) - Low-level function reference
