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
from geoparquet_io.core.add_h3_column import add_h3_table
from geoparquet_io.core.add_kdtree_column import add_kdtree_table
from geoparquet_io.core.add_quadkey_column import add_quadkey_table
from geoparquet_io.core.extract import extract_table
from geoparquet_io.core.hilbert_order import hilbert_order_table
from geoparquet_io.core.reproject import reproject_table
from geoparquet_io.core.sort_by_column import sort_by_column_table
from geoparquet_io.core.sort_quadkey import sort_by_quadkey_table


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


def add_h3(
    table: pa.Table,
    column_name: str = "h3_cell",
    resolution: int = 9,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an H3 cell column based on geometry location.

    Args:
        table: Input PyArrow Table
        column_name: Name for the H3 column (default: 'h3_cell')
        resolution: H3 resolution level 0-15 (default: 9)
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with H3 column added
    """
    return add_h3_table(
        table,
        h3_column_name=column_name,
        resolution=resolution,
        geometry_column=geometry_column,
    )


def add_kdtree(
    table: pa.Table,
    column_name: str = "kdtree_cell",
    iterations: int = 9,
    sample_size: int = 100000,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a KD-tree cell column based on geometry location.

    Args:
        table: Input PyArrow Table
        column_name: Name for the KD-tree column (default: 'kdtree_cell')
        iterations: Number of recursive splits 1-20 (default: 9)
        sample_size: Number of points to sample for boundaries (default: 100000)
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with KD-tree column added
    """
    return add_kdtree_table(
        table,
        kdtree_column_name=column_name,
        iterations=iterations,
        sample_size=sample_size,
        geometry_column=geometry_column,
    )


def sort_column(
    table: pa.Table,
    column: str | list[str],
    descending: bool = False,
) -> pa.Table:
    """
    Sort table rows by the specified column(s).

    Args:
        table: Input PyArrow Table
        column: Column name or list of column names to sort by
        descending: Sort in descending order (default: False)

    Returns:
        New table with rows sorted by the column(s)
    """
    return sort_by_column_table(
        table,
        columns=column,
        descending=descending,
    )


def sort_quadkey(
    table: pa.Table,
    column_name: str = "quadkey",
    resolution: int = 13,
    use_centroid: bool = False,
    remove_column: bool = False,
) -> pa.Table:
    """
    Sort table rows by quadkey column.

    If the quadkey column doesn't exist, it will be auto-added.

    Args:
        table: Input PyArrow Table
        column_name: Name of the quadkey column (default: 'quadkey')
        resolution: Quadkey resolution for auto-adding (0-23, default: 13)
        use_centroid: Use geometry centroid when auto-adding
        remove_column: Remove the quadkey column after sorting

    Returns:
        New table with rows sorted by quadkey
    """
    return sort_by_quadkey_table(
        table,
        quadkey_column_name=column_name,
        resolution=resolution,
        use_centroid=use_centroid,
        remove_quadkey_column=remove_column,
    )


def reproject(
    table: pa.Table,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Reproject geometry to a different coordinate reference system.

    Args:
        table: Input PyArrow Table
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Source CRS. If None, detected from metadata.
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with reprojected geometry
    """
    return reproject_table(
        table,
        target_crs=target_crs,
        source_crs=source_crs,
        geometry_column=geometry_column,
    )


def convert_to_geojson(
    table: pa.Table,
    output_path: str | None = None,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
) -> str | None:
    """
    Convert a GeoParquet table to GeoJSON.

    Writes to file if output_path is provided, otherwise streams to stdout.

    Args:
        table: Input PyArrow Table with geometry column
        output_path: Output file path, or None to stream to stdout
        rs: Include RFC 8142 record separators (streaming mode only)
        precision: Coordinate decimal precision (default 7 per RFC 7946).
            Note: Very low precision values (e.g., 3) may collapse small
            geometries since coordinates are snapped to a grid.
        write_bbox: Include bbox property for each feature
        id_field: Field to use as feature 'id' member

    Returns:
        Output path if writing to file, None if streaming to stdout
    """
    import tempfile
    import uuid
    from pathlib import Path

    from geoparquet_io.core.geojson_stream import (
        convert_to_geojson as convert_to_geojson_impl,
    )

    if not isinstance(table, pa.Table):
        raise TypeError(f"Expected pa.Table, got {type(table).__name__}")

    # Write table to temp parquet file for processing
    temp_dir = Path(tempfile.gettempdir())
    temp_input = temp_dir / f"gpio_geojson_{uuid.uuid4()}.parquet"

    try:
        import pyarrow.parquet as pq

        pq.write_table(table, str(temp_input))

        # Call core function
        convert_to_geojson_impl(
            input_path=str(temp_input),
            output_path=output_path,
            rs=rs,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
        )

        return output_path

    finally:
        # Clean up temp file
        if temp_input.exists():
            temp_input.unlink()
