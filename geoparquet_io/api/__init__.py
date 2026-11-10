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

# `read_bigquery` is the deprecated spelling of `from_bigquery` (#1436): kept
# importable from here, and warning once per process, until it is removed in the
# next minor release.
from geoparquet_io.api.ops import from_bigquery, read_bigquery
from geoparquet_io.api.pipeline import pipe
from geoparquet_io.api.table import Table, convert, extract_arcgis, read, read_partition
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.layers import list_layers

__all__ = [
    "InvalidParameterError",
    "Table",
    "read",
    "read_partition",
    "from_bigquery",
    "read_bigquery",
    "convert",
    "extract_arcgis",
    "list_layers",
    "pipe",
    "ops",
]
