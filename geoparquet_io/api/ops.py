"""
Pure table-centric operations for GeoParquet transformations.

These functions accept and return PyArrow Tables, making them easy to
compose and integrate with other Arrow-based workflows.

Example:
    import pyarrow.parquet as pq
    from geoparquet_io.api import ops

    table = pq.read_table('input.parquet')
    table = ops.add_bbox(table)
    table = ops.add_quadkey(table, resolution=12)
    table = ops.sort_hilbert(table)
    pq.write_table(table, 'output.parquet')
"""

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.add_bbox_column import add_bbox_table
from geoparquet_io.core.add_quadkey_column import add_quadkey_table
from geoparquet_io.core.extract import extract_table
from geoparquet_io.core.hilbert_order import hilbert_order_table


def add_bbox(
    table: pa.Table,
    column_name: str = "bbox",
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a bounding box struct column to a table.

    Args:
        table: Input PyArrow Table
        column_name: Name for the bbox column (default: 'bbox')
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with bbox column added
    """
    return add_bbox_table(
        table,
        bbox_column_name=column_name,
        geometry_column=geometry_column,
    )


def add_quadkey(
    table: pa.Table,
    column_name: str = "quadkey",
    resolution: int = 13,
    use_centroid: bool = False,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a quadkey column based on geometry location.

    Args:
        table: Input PyArrow Table
        column_name: Name for the quadkey column (default: 'quadkey')
        resolution: Quadkey zoom level 0-23 (default: 13)
        use_centroid: Force centroid even if bbox exists
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with quadkey column added
    """
    return add_quadkey_table(
        table,
        quadkey_column_name=column_name,
        resolution=resolution,
        use_centroid=use_centroid,
        geometry_column=geometry_column,
    )


def sort_hilbert(
    table: pa.Table,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Reorder table rows using Hilbert curve ordering.

    Args:
        table: Input PyArrow Table
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with rows reordered by Hilbert curve
    """
    return hilbert_order_table(
        table,
        geometry_column=geometry_column,
    )


def extract(
    table: pa.Table,
    columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    where: str | None = None,
    limit: int | None = None,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Extract columns and rows with optional filtering.

    Args:
        table: Input PyArrow Table
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude
        bbox: Bounding box filter (xmin, ymin, xmax, ymax)
        where: SQL WHERE clause
        limit: Maximum rows to return
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        Filtered table
    """
    return extract_table(
        table,
        columns=columns,
        exclude_columns=exclude_columns,
        bbox=bbox,
        where=where,
        limit=limit,
        geometry_column=geometry_column,
    )
