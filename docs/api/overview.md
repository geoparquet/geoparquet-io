# Python API Overview

geoparquet-io provides a powerful Python API for programmatic access to all functionality. The API offers the best performance by keeping data in memory as Arrow tables.

## Quick Example

```python
import geoparquet_io as gpio

# Read, transform, and write in a fluent chain
gpio.read('input.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')
```

## API Options

The Python API offers three ways to work with GeoParquet data:

### 1. Fluent Table API (Recommended)

The primary API for most users. Provides chainable methods on a `Table` object:

```python
import geoparquet_io as gpio

# Chain operations fluently
result = gpio.read('input.parquet') \
    .extract(limit=10000) \
    .add_bbox() \
    .add_h3(resolution=9) \
    .sort_hilbert()

result.write('output.parquet')
```

See [Python API Reference](python-api.md) for full documentation.

### 2. Pure Functions (ops module)

For integration with existing PyArrow workflows:

```python
import pyarrow.parquet as pq
from geoparquet_io.api import ops

table = pq.read_table('input.parquet')
table = ops.add_bbox(table)
table = ops.sort_hilbert(table)
```

See [Python API Reference - ops module](python-api.md#pure-functions-ops-module) for details.

### 3. Pipeline Composition

Build reusable transformation pipelines:

```python
from geoparquet_io.api import pipe, read

preprocess = pipe(
    lambda t: t.add_bbox(),
    lambda t: t.add_h3(resolution=9),
    lambda t: t.sort_hilbert(),
)

result = preprocess(read('input.parquet'))
result.write('output.parquet')
```

See [Python API Reference - Pipeline Composition](python-api.md#pipeline-composition) for details.

## Key Functions

| Function | Description |
|----------|-------------|
| `gpio.read(path)` | Read a GeoParquet file into a Table |
| `gpio.read_partition(path)` | Read a Hive-partitioned dataset |
| `gpio.convert(path)` | Convert Shapefile/GeoJSON/GeoPackage/CSV to Table |
| `gpio.pipe(*funcs)` | Create a reusable transformation pipeline |

## Table Methods

| Method | Description |
|--------|-------------|
| `.add_bbox()` | Add bounding box column |
| `.add_h3(resolution)` | Add H3 hexagonal cell column |
| `.add_quadkey(resolution)` | Add quadkey tile column |
| `.add_kdtree()` | Add KD-tree partition column |
| `.sort_hilbert()` | Sort by Hilbert space-filling curve |
| `.sort_quadkey()` | Sort by quadkey |
| `.sort_column(name)` | Sort by any column |
| `.extract(...)` | Filter columns and rows |
| `.reproject(target_crs)` | Reproject to different CRS |
| `.write(path)` | Write to GeoParquet file |
| `.upload(url)` | Upload to cloud storage |
| `.partition_by_h3()` | Partition into H3-based files |
| `.partition_by_quadkey()` | Partition into quadkey-based files |

## Performance

The Python API provides the best performance:

| Approach | Time (75MB, 400K rows) | Notes |
|----------|------------------------|-------|
| CLI (file-based) | 34s | Each command writes intermediate file |
| CLI (piped) | 16s | Arrow IPC streaming between commands |
| **Python API** | **7s** | In-memory, no I/O overhead |

## Advanced: Core Module Access

For power users who need direct access to file-based functions:

```python
from geoparquet_io.core.add_bbox_column import add_bbox_column
from geoparquet_io.core.hilbert_order import hilbert_order

add_bbox_column(
    input_parquet="input.parquet",
    output_parquet="output.parquet",
    bbox_name="bbox",
    verbose=True
)
```

See [Core Functions Reference](core.md) for all available functions.

## Next Steps

- [Python API Reference](python-api.md) - Complete method documentation
- [Examples](../examples/basic.md) - Usage patterns and examples
- [Spatial Performance Guide](../concepts/spatial-indices.md) - Understanding bbox, sorting, and partitioning
