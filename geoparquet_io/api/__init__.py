"""
Python API for GeoParquet transformations.

Provides a fluent API for chaining GeoParquet operations:

    import geoparquet_io as gpio

    # Read existing GeoParquet
    gpio.read('input.parquet') \\
        .add_bbox() \\
        .add_quadkey(resolution=12) \\
        .sort_hilbert() \\
        .write('output.parquet')

    # Convert other formats to GeoParquet
    gpio.convert('data.gpkg') \\
        .sort_hilbert() \\
        .upload('s3://bucket/data.parquet')

Also provides pure table-centric functions:

    from geoparquet_io.api import ops

    table = pq.read_table('input.parquet')
    table = ops.add_bbox(table)
    table = ops.sort_hilbert(table)
"""

from geoparquet_io.api import ops
from geoparquet_io.api.lazy import (
    LazyTable,
    convert_lazy,
    from_arrow,
    from_relation,
    from_table,
    read_lazy,
)
from geoparquet_io.api.ops import read_bigquery
from geoparquet_io.api.pipeline import pipe
from geoparquet_io.api.table import Table, convert, extract_arcgis, read, read_partition

__all__ = [
    # Eager API
    "Table",
    "read",
    "read_partition",
    "read_bigquery",
    "convert",
    "extract_arcgis",
    "pipe",
    "ops",
    # Lazy API
    "LazyTable",
    "read_lazy",
    "convert_lazy",
    "from_table",
    "from_relation",
    "from_arrow",
]
