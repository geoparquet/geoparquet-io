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
