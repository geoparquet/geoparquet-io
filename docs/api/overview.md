# Python API Overview

In addition to the CLI, geoparquet-io provides a Python API for programmatic access to all functionality.

## Quick Example

```python
from geoparquet_io.core.add_bbox_column import add_bbox_column
from geoparquet_io.core.hilbert_order import hilbert_order
from geoparquet_io.core.partition_by_h3 import partition_by_h3

# Add bounding box column
add_bbox_column(
    input_parquet="input.parquet",
    output_parquet="with_bbox.parquet",
    bbox_name="bbox",
    verbose=True
)

# Sort by Hilbert curve
hilbert_order(
    input_parquet="input.parquet",
    output_parquet="sorted.parquet",
    geometry_column="geometry",
    add_bbox=True,
    verbose=True
)

# Partition by H3
partition_by_h3(
    input_parquet="input.parquet",
    output_folder="output/",
    resolution=9,
    hive=False,
    verbose=True
)
```

## Module Structure

The API is organized into modules under `geoparquet_io.core`:

### Adding Columns

- `add_bbox_column` - Add bounding box struct column
- `add_h3_column` - Add H3 hexagonal cell IDs
- `add_kdtree_column` - Add KD-tree partition IDs
- `add_country_codes` - Add country ISO codes

### Spatial Operations

- `hilbert_order` - Sort by Hilbert curve
- `add_bbox_metadata` - Update bbox metadata

### Partitioning

- `partition_by_string` - Partition by string column
- `partition_by_h3` - Partition by H3 cells
- `partition_by_kdtree` - Partition by KD-tree
- `split_by_country` - Split by country code

### Validation

- `check_parquet_structure` - Validate structure and metadata
- `check_spatial_order` - Check spatial ordering

### Utilities

- `common` - Shared utility functions
- `inspect_utils` - File inspection helpers

## API Documentation

For detailed API documentation with all parameters and options, see the [Core Functions Reference](core.md).

## Examples

See the [Examples section](../examples/basic.md) for real-world usage patterns.
