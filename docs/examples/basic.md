# Basic Usage Examples

Python API examples demonstrating common operations.

## Adding Bounding Boxes

```python
from geoparquet_io.core.add_bbox_column import add_bbox_column

add_bbox_column(
    input_parquet="input.parquet",
    output_parquet="output.parquet",
    bbox_name="bbox",
    verbose=True,
    compression="ZSTD",
    compression_level=15
)
```

## Hilbert Curve Sorting

```python
from geoparquet_io.core.hilbert_order import hilbert_order

hilbert_order(
    input_parquet="input.parquet",
    output_parquet="sorted.parquet",
    geometry_column="geometry",
    add_bbox=True,
    verbose=True
)
```

## Checking File Quality

```python
from geoparquet_io.core.check_parquet_structure import check_all

check_all(parquet_file="input.parquet", verbose=True)
```

## Calculating Dataset Bounds

```python
from geoparquet_io.core.common import get_dataset_bounds

bounds = get_dataset_bounds(
    parquet_file="input.parquet",
    geometry_column="geometry",
    verbose=True
)

if bounds:
    xmin, ymin, xmax, ymax = bounds
    print(f"Bounds: ({xmin}, {ymin}, {xmax}, {ymax})")
```

## Compression Options

Available compression formats:

--8<-- "_includes/compression-options.md"

Example usage:

```python
from geoparquet_io.core.add_bbox_column import add_bbox_column

# ZSTD (recommended)
add_bbox_column(..., compression="ZSTD", compression_level=15)

# GZIP (widely compatible)
add_bbox_column(..., compression="GZIP", compression_level=6)
```

## Complete Example Script

See the [examples/basic_usage.py](https://github.com/cholmes/geoparquet-io/blob/main/examples/basic_usage.py) file in the repository for a complete working example.

## Next Steps

- [Batch Processing Examples](batch.md)
- [API Reference](../api/overview.md)
