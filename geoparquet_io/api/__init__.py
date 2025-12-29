"""
Python API for GeoParquet transformations.

Provides a fluent API for chaining GeoParquet operations:

    import geoparquet_io as gpio

    gpio.read('input.parquet') \\
        .add_bbox() \\
        .add_quadkey(resolution=12) \\
        .sort_hilbert() \\
        .write('output.parquet')

Also provides pure table-centric functions:

    from geoparquet_io.api import ops

    table = pq.read_table('input.parquet')
    table = ops.add_bbox(table)
    table = ops.sort_hilbert(table)
"""

from geoparquet_io.api.pipeline import pipe
from geoparquet_io.api.table import Table, read

__all__ = ["Table", "read", "pipe"]
