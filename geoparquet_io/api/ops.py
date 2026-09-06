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

from typing import TYPE_CHECKING

import pyarrow as pa

from geoparquet_io.core.add.a5 import add_a5_table
from geoparquet_io.core.add.bbox import add_bbox_table
from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata_table
from geoparquet_io.core.add.h3 import add_h3_table
from geoparquet_io.core.add.kdtree import DEFAULT_KDTREE_TARGET_ROWS, add_kdtree_table
from geoparquet_io.core.add.quadkey import add_quadkey_table
from geoparquet_io.core.add.s2 import add_s2_table
from geoparquet_io.core.extract import extract_table
from geoparquet_io.core.hilbert_order import hilbert_order_table
from geoparquet_io.core.reproject import reproject_table
from geoparquet_io.core.sort_by_column import sort_by_column_table
from geoparquet_io.core.sort_quadkey import sort_by_quadkey_table
from geoparquet_io.core.str_order import DEFAULT_STR_TILE_SIZE, str_order_table
from geoparquet_io.core.wfs import DEFAULT_WFS_PAGE_SIZE

if TYPE_CHECKING:
    from pathlib import Path


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


def add_bbox_metadata(
    table: pa.Table,
    bbox_column: str = "bbox",
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add bbox covering metadata for an existing bbox column.

    Mirrors `gpio add bbox-metadata`: it writes the GeoParquet `covering` key and
    nothing else, so the table must already carry `geo` metadata describing the
    geometry column. A table read from plain Parquet is refused rather than given
    an invented `geo` block (#713).

    Args:
        table: Input PyArrow Table, carrying GeoParquet `geo` metadata
        bbox_column: Name of the existing bbox column (default: 'bbox')
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table whose `geo` metadata carries the covering key

    Raises:
        GeoParquetError: If the table carries no GeoParquet metadata
        ValueError: If the geometry or bbox column is missing, or the declared
            version predates GeoParquet 1.1
    """
    return add_bbox_metadata_table(
        table,
        bbox_column=bbox_column,
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


def add_geometry_metrics(
    table: pa.Table,
    vecorel: bool = True,
) -> pa.Table:
    """
    Add geodesic area (m²) and perimeter (m) columns.

    Uses WGS84 spheroid-based calculations. Adds metrics:area
    and metrics:perimeter columns.

    Args:
        table: Input PyArrow Table
        vecorel: Add Vecorel schema metadata (default: True)

    Returns:
        New table with geometry metrics added

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = pq.read_table('input.parquet')
        >>> table = ops.add_geometry_metrics(table)
    """
    from geoparquet_io.core.add.geometry_metrics import (
        add_geometry_metrics as _add_geometry_metrics,
    )

    return _file_round_trip(table, _add_geometry_metrics, vecorel=vecorel)


def add_admin_divisions(
    table: pa.Table,
    *,
    dataset: str = "gaul",
    levels: list[str] | None = None,
    vecorel: bool = False,
    prefix: str | None = None,
) -> pa.Table:
    """
    Add administrative division columns via spatial join.

    Args:
        table: Input PyArrow Table
        dataset: Boundaries dataset ("gaul", "overture"). Default matches the
            CLI (`gpio add admin-divisions --dataset`).
        levels: Admin levels to add (e.g., ["country", "region"]). None adds
            every level the dataset provides, matching the CLI with no
            ``--levels``: ``["continent", "country", "department"]`` for GAUL,
            ``["country", "region"]`` for Overture.
        vecorel: Output Vecorel-compliant columns (default: False)
        prefix: Column name prefix, as with the CLI's ``--prefix``. None uses
            the dataset's own name (``gaul_country``, ``overture_country``);
            "admin" produces ``admin:country``.

    Returns:
        New table with admin division columns added

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = pq.read_table('input.parquet')
        >>> table = ops.add_admin_divisions(table, vecorel=True)
    """
    from geoparquet_io.core.add.admin_divisions import add_admin_divisions_multi
    from geoparquet_io.core.admin_datasets import default_admin_levels

    if vecorel:
        dataset = "overture"
        levels = ["country", "region"]

    return _file_round_trip(
        table,
        add_admin_divisions_multi,
        dataset_name=dataset,
        levels=levels or default_admin_levels(dataset),
        vecorel=vecorel,
        prefix=prefix,
        verbose=False,
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


def sort_str(
    table: pa.Table,
    geometry_column: str | None = None,
    tile_size: int = DEFAULT_STR_TILE_SIZE,
) -> pa.Table:
    """Reorder table rows using Sort-Tile-Recursive packing.

    Args:
        table: Input PyArrow Table
        geometry_column: Geometry column name (auto-detected if None)
        tile_size: Roughly the rows you intend to put in a row group. STR uses
            it only to choose the number of X strips, as
            ``ceil(sqrt(num_rows / tile_size))``, so it is a coarse control
            rather than an exact tile capacity (default: 50,000)

    Returns:
        New table with rows reordered into spatially compact tiles
    """
    return str_order_table(
        table,
        geometry_column=geometry_column,
        tile_size=tile_size,
    )


def extract(
    table: pa.Table,
    columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    where: str | None = None,
    limit: int | None = None,
    geometry_column: str | None = None,
    repair_geometry: bool = True,
) -> pa.Table:
    """
    Extract columns and rows with optional filtering.

    Column names are validated against the schema: an unknown name raises rather
    than being silently ignored, and a name may not appear in both ``columns``
    and ``exclude_columns`` (geometry and bbox excepted).

    Args:
        table: Input PyArrow Table
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude
        bbox: Bounding box filter (xmin, ymin, xmax, ymax)
        where: SQL WHERE clause
        limit: Maximum rows to return
        geometry_column: Geometry column name (auto-detected if None)
        repair_geometry: Repair invalid geometry with ST_MakeValid (default: True)

    Returns:
        Filtered table. Excluding every geometry column yields an attribute
        table whose ``geo`` metadata is dropped, since it is no longer
        GeoParquet.

    Raises:
        InvalidParameterError: If a requested column does not exist, if a column
            is in both lists, or if ``bbox`` is used on a table with no geometry
            column.
    """
    return extract_table(
        table,
        columns=columns,
        exclude_columns=exclude_columns,
        bbox=bbox,
        where=where,
        limit=limit,
        geometry_column=geometry_column,
        repair_geometry=repair_geometry,
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


def add_a5(
    table: pa.Table,
    column_name: str = "a5_cell",
    resolution: int = 15,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an A5 cell column based on geometry location.

    Args:
        table: Input PyArrow Table
        column_name: Name for the A5 column (default: 'a5_cell')
        resolution: A5 resolution level 0-30 (default: 15)
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with A5 column added
    """
    return add_a5_table(
        table,
        a5_column_name=column_name,
        resolution=resolution,
        geometry_column=geometry_column,
    )


def aggregate_a5(
    table,
    resolution: int,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    geometry_column: str | None = None,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = "geometry",
    bbox_column: str | None = None,
) -> pa.Table:
    """
    Aggregate an Arrow table into A5 grid cells with per-cell statistics.

    Args:
        table: Input PyArrow Table with geometry column
        resolution: A5 resolution level 0-30
        metric: Aggregation metric, e.g. "sum:area" or "mean:value"
        breakdown: Column name to pivot into per-category count columns
        breakdown_limit: Max number of breakdown categories (default: 20)
        out_geometry: Output geometry type: "polygon", "centroid", "both", or "none"
        geometry_column: Geometry column name (defaults to "geometry")
        where: DuckDB WHERE clause filtering input rows before aggregation
        metric_nodata: NoData sentinel value(s) mapped to NULL in metric columns,
            e.g. "-999" or "-999,-9999"
        bucket_point: Keying point source: "geometry" (centroid, default),
            "bbox" (center of a bbox covering column), or a point column name
        bbox_column: Bbox covering column for bucket_point="bbox" (auto-detected
            when omitted)

    Returns:
        New PyArrow Table with one row per A5 cell
    """
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_a5_table

    return aggregate_a5_table(
        table,
        resolution=resolution,
        metric=metric,
        breakdown=breakdown,
        breakdown_limit=breakdown_limit,
        out_geometry=out_geometry,
        geometry_column=geometry_column,
        where=where,
        metric_nodata=metric_nodata,
        bucket_point=bucket_point,
        bbox_column=bbox_column,
    )


def aggregate_h3(
    table,
    resolution: int,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    geometry_column: str | None = None,
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = "geometry",
    bbox_column: str | None = None,
) -> pa.Table:
    """
    Aggregate an Arrow table into H3 grid cells with per-cell statistics.

    Args:
        table: Input PyArrow Table with geometry column
        resolution: H3 resolution level 0-15
        metric: Aggregation metric, e.g. "sum:area" or "mean:value"
        breakdown: Column name to pivot into per-category count columns
        breakdown_limit: Max number of breakdown categories (default: 20)
        out_geometry: Output geometry type: "polygon", "centroid", "both", or "none"
        geometry_column: Geometry column name (defaults to "geometry")
        where: DuckDB WHERE clause filtering input rows before aggregation
        metric_nodata: NoData sentinel value(s) mapped to NULL in metric columns,
            e.g. "-999" or "-999,-9999"
        bucket_point: Keying point source: "geometry" (centroid, default),
            "bbox" (center of a bbox covering column), or a point column name
        bbox_column: Bbox covering column for bucket_point="bbox" (auto-detected
            when omitted)

    Returns:
        New PyArrow Table with one row per H3 cell
    """
    from geoparquet_io.core.process.aggregate.by_h3 import aggregate_h3_table

    return aggregate_h3_table(
        table,
        resolution=resolution,
        metric=metric,
        breakdown=breakdown,
        breakdown_limit=breakdown_limit,
        out_geometry=out_geometry,
        geometry_column=geometry_column,
        where=where,
        metric_nodata=metric_nodata,
        bucket_point=bucket_point,
        bbox_column=bbox_column,
    )


def aggregate_admin(
    table: pa.Table,
    level: str = "country",
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    where: str | None = None,
    metric_nodata: str | None = None,
    bucket_point: str = "geometry",
    bbox_column: str | None = None,
) -> pa.Table:
    """
    Aggregate an Arrow table into administrative regions with per-region statistics.

    Args:
        table: Input PyArrow Table with geometry column
        level: Admin level to aggregate by ("country", "region", "subregion")
        metric: Aggregation metric, e.g. "sum:area" or "mean:value"
        breakdown: Column name to pivot into per-category count columns
        breakdown_limit: Max number of breakdown categories (default: 20)
        out_geometry: Output geometry type: "polygon", "centroid", "both", or "none"
        where: DuckDB WHERE clause filtering input rows before aggregation
        metric_nodata: NoData sentinel value(s) mapped to NULL in metric columns,
            e.g. "-999" or "-999,-9999"
        bucket_point: Join-point source: "geometry" (centroid, default),
            "bbox" (center of a bbox covering column), or a point column name
        bbox_column: Bbox covering column for bucket_point="bbox" (auto-detected
            when omitted)

    Returns:
        New PyArrow Table with one row per admin region

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = pq.read_table('input.parquet')
        >>> table = ops.aggregate_admin(table, level="country")
    """
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    return _file_round_trip(
        table,
        aggregate_by_admin,
        level=level,
        metric=metric,
        breakdown=breakdown,
        breakdown_limit=breakdown_limit,
        out_geometry=out_geometry,
        where=where,
        metric_nodata=metric_nodata,
        bucket_point=bucket_point,
        bbox_column=bbox_column,
    )


def create_overviews(
    input_parquet: str,
    *,
    levels: str | list[int | str] | None = None,
    max_tile_kb: int = 500,
    bytes_per_cell: float | None = None,
    cell_column: str | None = None,
    scheme: str | None = None,
    output_dir: str | None = None,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    geoparquet_version: str | None = None,
    force: bool = False,
    verbose: bool = False,
    show_sql: bool = False,
) -> list[tuple[int | str, str]]:
    """
    Build coarser overview levels from an aggregate GeoParquet file.

    Detects the aggregate's scheme (a5/h3/admin) and base level, rolls up by
    true cell hierarchy, and writes one GeoParquet sibling per coarser level
    (``cells.parquet`` -> ``cells_r4.parquet``; admin ->
    ``by_region_country.parquet``). Counts, sums, mins, maxes, and breakdown
    counts roll up exactly; averages are count-weighted (exact when the
    metric had no NULLs).

    Args:
        input_parquet: Path to a `gpio process aggregate` output
        levels: Explicit levels (comma string or list; admin: "country").
            Default: auto-select against max_tile_kb
        max_tile_kb: Tile-size budget in KB for auto level selection (default: 500)
        bytes_per_cell: Override the estimated compressed bytes per cell
        cell_column: Cell id column when auto-detection fails
        scheme: Bucketing scheme (a5/h3/admin) when inference is ambiguous,
            e.g. H3 ids stored as integers
        output_dir: Directory for overview files (default: beside the input)
        compression: Parquet compression codec (default: ZSTD)
        compression_level: Optional compression level
        geoparquet_version: GeoParquet version to write
        force: Overwrite existing overview output files
        verbose: Enable verbose output
        show_sql: Log the rollup SQL

    Returns:
        List of (level, output_path) tuples, coarse to fine

    Example:
        >>> from geoparquet_io.api import ops
        >>> ops.create_overviews('cells.parquet', levels=[4, 7])
        [(4, 'cells_r4.parquet'), (7, 'cells_r7.parquet')]
    """
    from geoparquet_io.core.process.overview import create_overviews as _create_overviews

    return _create_overviews(
        input_parquet,
        levels=levels,
        max_tile_kb=max_tile_kb,
        bytes_per_cell=bytes_per_cell,
        cell_column=cell_column,
        scheme=scheme,
        output_dir=output_dir,
        compression=compression,
        compression_level=compression_level,
        geoparquet_version=geoparquet_version,
        force=force,
        verbose=verbose,
        show_sql=show_sql,
    )


def add_kdtree(
    table: pa.Table,
    column_name: str = "kdtree_cell",
    iterations: int | None = None,
    sample_size: int = 100000,
    geometry_column: str | None = None,
    *,
    auto: bool = False,
    target_rows: int = DEFAULT_KDTREE_TARGET_ROWS,
) -> pa.Table:
    """
    Add a KD-tree cell column based on geometry location.

    Function form of `Table.add_kdtree`, mirroring `gpio add kdtree`. Like both
    of them it refuses to guess: pass ``iterations``, or pass ``auto=True`` to
    size the tree from the row count.

    Args:
        table: Input PyArrow Table
        column_name: Name for the KD-tree column (default: 'kdtree_cell')
        iterations: Number of recursive splits 1-20, giving ``2 ** iterations``
            cells. Required unless ``auto=True``
        sample_size: Number of points to sample for boundaries (default: 100000)
        geometry_column: Geometry column name (auto-detected if None)
        auto: Size the tree from the row count (default: False); mutually
            exclusive with ``iterations``
        target_rows: Target rows per cell when ``auto=True`` (default: 120000)

    Returns:
        New table with KD-tree column added

    Raises:
        InvalidParameterError: If both ``iterations`` and ``auto`` were given, or
            neither was

    Example:
        >>> from geoparquet_io.api import ops
        >>> sized = ops.add_kdtree(table, iterations=6)
        >>> auto = ops.add_kdtree(table, auto=True)
    """
    return add_kdtree_table(
        table,
        kdtree_column_name=column_name,
        iterations=iterations,
        sample_size=sample_size,
        geometry_column=geometry_column,
        auto_target_rows=("rows", target_rows) if auto else None,
    )


def add_s2(
    table: pa.Table,
    column_name: str = "s2_cell",
    level: int = 13,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an S2 cell column based on geometry location.

    Uses Google's S2 spherical geometry library to compute cell IDs
    from geometry centroids. Cell IDs are stored as hex tokens for portability.

    Args:
        table: Input PyArrow Table
        column_name: Name for the S2 column (default: 's2_cell')
        level: S2 level 0-30 (default: 13, ~1.2 km² cells)
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with S2 column added

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = pq.read_table('input.parquet')
        >>> table = ops.add_s2(table, level=13)
        >>> pq.write_table(table, 'output.parquet')
    """
    return add_s2_table(
        table,
        s2_column_name=column_name,
        level=level,
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
    assume_crs84: bool = False,
) -> pa.Table:
    """
    Reproject geometry to a different coordinate reference system.

    Args:
        table: Input PyArrow Table
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Source CRS. If None, detected from metadata.
        geometry_column: Geometry column name (auto-detected if None)
        assume_crs84: Treat an unknown/null input CRS as OGC:CRS84 instead of
            whatever detection finds (no coordinate change).

    Returns:
        New table with reprojected geometry
    """
    return reproject_table(
        table,
        target_crs=target_crs,
        source_crs=source_crs,
        geometry_column=geometry_column,
        assume_crs84=assume_crs84,
    )


# --------------------------------------------------------------------------
# Partitioning
#
# Partitioning writes a *directory* rather than returning a table, so these
# functions break the `table in -> table out` shape the rest of this module
# has: they take the table plus an output directory and return the same stats
# dict the `Table` methods do. They exist so a caller holding a plain
# `pa.Table` has the same front door for partitioning that `add`, `sort`,
# `convert`, `extract` and `process` already give them (#799).
# --------------------------------------------------------------------------


def _partition(
    table: pa.Table,
    output_dir: str | Path,
    method: str,
    geometry_column: str | None,
    **kwargs,
) -> dict:
    """Delegate to the matching ``Table.partition_by_*`` method.

    The partition core functions read a file, and `Table` already owns the
    temp-file machinery that bridges an in-memory table to them
    (`_run_partition_with_temp_file`). Delegating keeps both front doors on one
    code path instead of a second copy that can drift out of step.

    Imported inside the function: `geoparquet_io.api.__init__` imports `ops`
    before `table`, so a module-level import here would be circular.
    """
    from geoparquet_io.api.table import Table

    wrapper = Table(table, geometry_column=geometry_column)
    return getattr(wrapper, method)(output_dir, **kwargs)


def partition_by_h3(
    table: pa.Table,
    output_dir: str | Path,
    *,
    resolution: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
    compression: str = "ZSTD",
    hive: bool = False,
    keep_h3_column: bool | None = None,
    overwrite: bool = False,
    geometry_column: str | None = None,
) -> dict:
    """
    Partition a table into a directory of files split by H3 cell.

    Function form of `Table.partition_by_h3`, mirroring `gpio partition h3`.
    Like both of them it refuses to guess: pass a ``resolution``, or pass
    ``auto=True`` to size one from the data.

    Args:
        table: Input PyArrow Table with a geometry column
        output_dir: Output directory path
        resolution: H3 resolution level 0-15. Required unless ``auto=True``
        auto: Calculate the resolution from the data (default: False);
            mutually exclusive with ``resolution``
        target_rows: Target rows per partition when ``auto=True`` (default: 100000)
        max_partitions: Maximum partitions when ``auto=True`` (default: 10000)
        compression: Compression codec (default: ZSTD)
        hive: Use Hive-style ``h3_cell=value/`` directories (default: False)
        keep_h3_column: Keep the generated ``h3_cell`` column. None (default)
            follows ``hive``
        overwrite: Overwrite an existing output directory
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the same
        dict every ``partition_by_*`` function returns, where ``file_count``
        counts the ``.parquet`` files under ``output_dir``.

    Example:
        >>> from geoparquet_io.api import ops
        >>> stats = ops.partition_by_h3(table, 'output/', resolution=6)
        >>> stats = ops.partition_by_h3(table, 'output/', auto=True)
    """
    return _partition(
        table,
        output_dir,
        "partition_by_h3",
        geometry_column,
        resolution=resolution,
        auto=auto,
        target_rows=target_rows,
        max_partitions=max_partitions,
        compression=compression,
        hive=hive,
        keep_h3_column=keep_h3_column,
        overwrite=overwrite,
    )


def partition_by_a5(
    table: pa.Table,
    output_dir: str | Path,
    *,
    resolution: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
    compression: str = "ZSTD",
    hive: bool = False,
    keep_a5_column: bool | None = None,
    overwrite: bool = False,
    geometry_column: str | None = None,
) -> dict:
    """
    Partition a table into a directory of files split by A5 cell.

    Function form of `Table.partition_by_a5`, mirroring `gpio partition a5`.
    Pass a ``resolution``, or ``auto=True`` to size one from the data.

    Args:
        table: Input PyArrow Table with a geometry column
        output_dir: Output directory path
        resolution: A5 resolution level 0-30. Required unless ``auto=True``
        auto: Calculate the resolution from the data (default: False);
            mutually exclusive with ``resolution``
        target_rows: Target rows per partition when ``auto=True`` (default: 100000)
        max_partitions: Maximum partitions when ``auto=True`` (default: 10000)
        compression: Compression codec (default: ZSTD)
        hive: Use Hive-style ``a5_cell=value/`` directories (default: False)
        keep_a5_column: Keep the generated ``a5_cell`` column. None (default)
            follows ``hive``
        overwrite: Overwrite an existing output directory
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the same
        dict every ``partition_by_*`` function returns, where ``file_count``
        counts the ``.parquet`` files under ``output_dir``.

    Example:
        >>> from geoparquet_io.api import ops
        >>> stats = ops.partition_by_a5(table, 'output/', resolution=12)
    """
    return _partition(
        table,
        output_dir,
        "partition_by_a5",
        geometry_column,
        resolution=resolution,
        auto=auto,
        target_rows=target_rows,
        max_partitions=max_partitions,
        compression=compression,
        hive=hive,
        keep_a5_column=keep_a5_column,
        overwrite=overwrite,
    )


def partition_by_s2(
    table: pa.Table,
    output_dir: str | Path,
    *,
    level: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
    compression: str = "ZSTD",
    hive: bool = False,
    keep_s2_column: bool | None = None,
    overwrite: bool = False,
    geometry_column: str | None = None,
) -> dict:
    """
    Partition a table into a directory of files split by S2 cell.

    Function form of `Table.partition_by_s2`, mirroring `gpio partition s2`.
    Pass a ``level``, or ``auto=True`` to size one from the data.

    Args:
        table: Input PyArrow Table with a geometry column
        output_dir: Output directory path
        level: S2 level 0-30 (13 is ~1.2 km² cells). Required unless ``auto=True``
        auto: Calculate the level from the data (default: False); mutually
            exclusive with ``level``
        target_rows: Target rows per partition when ``auto=True`` (default: 100000)
        max_partitions: Maximum partitions when ``auto=True`` (default: 10000)
        compression: Compression codec (default: ZSTD)
        hive: Use Hive-style ``s2_cell=value/`` directories (default: False)
        keep_s2_column: Keep the generated ``s2_cell`` column. None (default)
            follows ``hive``
        overwrite: Overwrite an existing output directory
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the same
        dict every ``partition_by_*`` function returns, where ``file_count``
        counts the ``.parquet`` files under ``output_dir``.

    Example:
        >>> from geoparquet_io.api import ops
        >>> stats = ops.partition_by_s2(table, 'output/', level=10)
    """
    return _partition(
        table,
        output_dir,
        "partition_by_s2",
        geometry_column,
        level=level,
        auto=auto,
        target_rows=target_rows,
        max_partitions=max_partitions,
        compression=compression,
        hive=hive,
        keep_s2_column=keep_s2_column,
        overwrite=overwrite,
    )


def partition_by_quadkey(
    table: pa.Table,
    output_dir: str | Path,
    *,
    resolution: int | None = None,
    partition_resolution: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
    compression: str = "ZSTD",
    hive: bool = False,
    keep_quadkey_column: bool | None = None,
    overwrite: bool = False,
    geometry_column: str | None = None,
) -> dict:
    """
    Partition a table into a directory of files split by quadkey.

    Function form of `Table.partition_by_quadkey`, mirroring
    `gpio partition quadkey`. Pass both ``resolution`` and
    ``partition_resolution``, or ``auto=True`` to size them from the data.

    Args:
        table: Input PyArrow Table with a geometry column
        output_dir: Output directory path
        resolution: Quadkey resolution for sorting (0-23). Required unless
            ``auto=True``
        partition_resolution: Resolution for partition boundaries (0-23).
            Required unless ``auto=True``
        auto: Calculate both resolutions from the data (default: False);
            mutually exclusive with the two above
        target_rows: Target rows per partition when ``auto=True`` (default: 100000)
        max_partitions: Maximum partitions when ``auto=True`` (default: 10000)
        compression: Compression codec (default: ZSTD)
        hive: Use Hive-style ``quadkey=value/`` directories (default: False)
        keep_quadkey_column: Keep the generated ``quadkey`` column. None
            (default) follows ``hive``
        overwrite: Overwrite an existing output directory
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the same
        dict every ``partition_by_*`` function returns, where ``file_count``
        counts the ``.parquet`` files under ``output_dir``.

    Example:
        >>> from geoparquet_io.api import ops
        >>> stats = ops.partition_by_quadkey(
        ...     table, 'output/', resolution=13, partition_resolution=6
        ... )
    """
    return _partition(
        table,
        output_dir,
        "partition_by_quadkey",
        geometry_column,
        resolution=resolution,
        partition_resolution=partition_resolution,
        auto=auto,
        target_rows=target_rows,
        max_partitions=max_partitions,
        compression=compression,
        hive=hive,
        keep_quadkey_column=keep_quadkey_column,
        overwrite=overwrite,
    )


def partition_by_kdtree(
    table: pa.Table,
    output_dir: str | Path,
    *,
    iterations: int | None = None,
    auto: bool = False,
    target_rows: int = DEFAULT_KDTREE_TARGET_ROWS,
    hive: bool = False,
    keep_kdtree_column: bool | None = None,
    overwrite: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    geometry_column: str | None = None,
) -> dict:
    """
    Partition a table into a directory of files split by KD-tree cell.

    Function form of `Table.partition_by_kdtree`, mirroring
    `gpio partition kdtree`. Recursively splits the data spatially, producing
    ``2 ** iterations`` balanced partitions. Like both of them it refuses to
    guess: pass ``iterations``, or pass ``auto=True`` to size the tree from the
    row count. A table already carrying a ``kdtree_cell`` column needs neither
    -- the existing cells drive the partition and no tree is built.

    Args:
        table: Input PyArrow Table with a geometry column
        output_dir: Output directory path
        iterations: Number of KD-tree splits. Required unless ``auto=True``
        auto: Size the tree from the row count (default: False); mutually
            exclusive with ``iterations``
        target_rows: Target rows per partition when ``auto=True`` (default: 120000)
        hive: Use Hive-style ``kdtree_cell=value/`` directories (default: False)
        keep_kdtree_column: Keep the generated ``kdtree_cell`` column. None
            (default) follows ``hive``
        overwrite: Overwrite an existing output directory
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level. None lets the codec pick its own
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the same
        dict every ``partition_by_*`` function returns, where ``file_count``
        counts the ``.parquet`` files under ``output_dir``.

    Raises:
        InvalidParameterError: If both ``iterations`` and ``auto`` were given, or
            neither was and the table does not already carry the column

    Example:
        >>> from geoparquet_io.api import ops
        >>> stats = ops.partition_by_kdtree(table, 'output/', iterations=6)
        >>> auto = ops.partition_by_kdtree(table, 'output/', auto=True)
    """
    return _partition(
        table,
        output_dir,
        "partition_by_kdtree",
        geometry_column,
        iterations=iterations,
        auto=auto,
        target_rows=target_rows,
        hive=hive,
        keep_kdtree_column=keep_kdtree_column,
        overwrite=overwrite,
        compression=compression,
        compression_level=compression_level,
    )


def partition_by_string(
    table: pa.Table,
    output_dir: str | Path,
    column: str,
    *,
    chars: int | None = None,
    hive: bool = False,
    overwrite: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    geometry_column: str | None = None,
) -> dict:
    """
    Partition a table into a directory of files split by a string column.

    Function form of `Table.partition_by_string`, mirroring
    `gpio partition string`.

    Args:
        table: Input PyArrow Table
        output_dir: Output directory path
        column: Column name to partition by
        chars: Use the first N characters as the partition key (None: the whole value)
        hive: Use Hive-style ``column=value/`` directories (default: False)
        overwrite: Overwrite an existing output directory
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level. None lets the codec pick its own
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the same
        dict every ``partition_by_*`` function returns, where ``file_count``
        counts the ``.parquet`` files under ``output_dir``.

    Example:
        >>> from geoparquet_io.api import ops
        >>> stats = ops.partition_by_string(table, 'output/', 'country_code', hive=True)
    """
    return _partition(
        table,
        output_dir,
        "partition_by_string",
        geometry_column,
        column=column,
        chars=chars,
        hive=hive,
        overwrite=overwrite,
        compression=compression,
        compression_level=compression_level,
    )


def partition_by_admin(
    table: pa.Table,
    output_dir: str | Path,
    *,
    dataset: str = "gaul",
    levels: list[str] | None = None,
    hive: bool = False,
    overwrite: bool = False,
    vecorel: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    geometry_column: str | None = None,
) -> dict:
    """
    Partition a table into a directory of files split by administrative boundaries.

    Function form of `Table.partition_by_admin`, mirroring
    `gpio partition admin`. Spatially joins against an administrative
    boundaries dataset, so it downloads that dataset on first use.

    Args:
        table: Input PyArrow Table with a geometry column
        output_dir: Output directory path
        dataset: Boundaries dataset ("gaul", "overture", or a custom URL)
        levels: Admin levels to partition by (e.g. ["country", "department"]
            for GAUL, ["country", "region"] for Overture). Defaults to ["country"]
        hive: Use Hive-style ``level=value/`` directories (default: False)
        overwrite: Overwrite an existing output directory
        vecorel: Emit Vecorel-compliant admin columns; forces the Overture
            dataset with country,region levels (default: False)
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level. None lets the codec pick its own
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the same
        dict every ``partition_by_*`` function returns, where ``file_count``
        counts the ``.parquet`` files under ``output_dir``.
        The ``int`` partition count that ``partition_by_admin_hierarchical``
        reports is not passed through (#822); ``file_count`` is counted off
        the output directory instead.

    Example:
        >>> from geoparquet_io.api import ops
        >>> stats = ops.partition_by_admin(table, 'output/', levels=['country'])
    """
    return _partition(
        table,
        output_dir,
        "partition_by_admin",
        geometry_column,
        dataset=dataset,
        levels=levels,
        hive=hive,
        overwrite=overwrite,
        vecorel=vecorel,
        compression=compression,
        compression_level=compression_level,
    )


# --------------------------------------------------------------------------
# Sub-partitioning a directory
#
# `gpio partition <index>` is two operations behind one command: partition a
# file, or walk a *directory* and split every file over --min-size into a
# sibling `<file>_<index>/`. The second is what you reach for after partitioning
# by country or by a string column has left a handful of oversized files.
#
# It had no Python front door at all (#811). It cannot be a `Table` method --
# the unit of work is a directory of files on disk, not an in-memory table --
# and it does not fit `ops.partition_by_*` either, which take one table. So it
# is its own family of `ops` functions over a path, one per index, named after
# the CLI command each mirrors.
# --------------------------------------------------------------------------

_SUB_PARTITION_RESOLUTION_ARG = {"s2": "level"}


def _parse_min_size(min_size: str | int) -> int:
    """Bytes from either the CLI's '100MB' spelling or a plain byte count."""
    from geoparquet_io.core.common import parse_size_string
    from geoparquet_io.core.exceptions import InvalidParameterError

    if isinstance(min_size, bool) or not isinstance(min_size, (str, int)):
        raise InvalidParameterError(
            "min_size", f"expected a size string or byte count, got {min_size!r}"
        )
    if isinstance(min_size, int):
        if min_size < 0:
            raise InvalidParameterError("min_size", f"byte count cannot be negative: {min_size}")
        return min_size
    try:
        return parse_size_string(min_size)
    except ValueError as exc:
        raise InvalidParameterError("min_size", str(exc)) from None


def _sub_partition(
    directory: str | Path,
    partition_type: str,
    *,
    min_size: str | int,
    resolution: int | None = None,
    partition_resolution: int | None = None,
    level: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
    in_place: bool = False,
    preview: bool = False,
    hive: bool = False,
    overwrite: bool = False,
    force: bool = False,
    skip_analysis: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    verbose: bool = False,
    column_name: str | None = None,
    output_dir: str | Path | None = None,
) -> dict:
    """Shared body of the four `sub_partition_by_*` functions.

    Validates what the CLI validates -- and refuses what it refuses -- then hands
    the run to `core.sub_partition.sub_partition_directory`, which is the same
    function `gpio partition <index> <dir> --min-size` reaches.
    """
    # `Path` is a TYPE_CHECKING-only import in this module, so the runtime one is
    # aliased rather than shadowing the annotation name.
    from pathlib import Path as _Path

    from geoparquet_io.core import sub_partition as core_sub_partition
    from geoparquet_io.core.exceptions import InvalidParameterError, PartitionError

    if not _Path(directory).is_dir():
        raise InvalidParameterError(
            "directory",
            f"not a directory: {directory}. Sub-partitioning walks a directory of "
            f"parquet files; use ops.partition_by_{partition_type}() to partition "
            "a single table.",
        )

    offending = core_sub_partition.offending_single_file_only_options(
        partition_type,
        column_name,
        output_dir,
        column_label="column_name",
        output_label="output_dir",
    )
    if offending:
        raise InvalidParameterError(
            offending[0],
            core_sub_partition.single_file_only_option_message(partition_type, offending),
        )

    min_size_bytes = _parse_min_size(min_size)

    if preview:
        # The CLI's --preview branch plans and stops before any resolution is
        # needed, so a preview without resolution/auto must succeed here too.
        return {
            "preview": True,
            "candidates": core_sub_partition.plan_sub_partition(
                str(directory), partition_type, min_size_bytes
            ),
            "processed": 0,
            "skipped": 0,
            "errors": [],
        }

    argument = _SUB_PARTITION_RESOLUTION_ARG.get(partition_type, "resolution")
    if not auto and (resolution if resolution is not None else level) is None:
        raise InvalidParameterError(
            argument,
            f"pass {argument}=<int> or auto=True -- gpio does not guess a "
            f"{partition_type} {argument}",
        )

    candidates = core_sub_partition.plan_sub_partition(
        str(directory), partition_type, min_size_bytes
    )

    result = core_sub_partition.sub_partition_directory(
        directory=str(directory),
        partition_type=partition_type,
        min_size_bytes=min_size_bytes,
        resolution=resolution,
        partition_resolution=partition_resolution,
        level=level,
        in_place=in_place,
        hive=hive,
        overwrite=overwrite,
        verbose=verbose,
        force=force,
        skip_analysis=skip_analysis,
        compression=compression.upper(),
        compression_level=compression_level or 15,
        auto=auto,
        target_rows=target_rows,
        max_partitions=max_partitions,
    )

    run = {"preview": False, "candidates": candidates, **result}
    if run["errors"]:
        # Per-file failures are collected rather than raised, so without this a
        # caller who never inspected `errors` would read a partial run as a
        # complete one -- the API's version of the exit-0 bug in #778.
        failed = len(run["errors"])
        detail = "\n".join(f"  {err['file']}: {err['error']}" for err in run["errors"])
        raise PartitionError(
            f"{failed} of {failed + run['processed']} file(s) failed to sub-partition "
            f"by {partition_type}:\n{detail}",
            result=run,
        )
    return run


def sub_partition_by_h3(
    directory: str | Path,
    *,
    min_size: str | int,
    resolution: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
    in_place: bool = False,
    preview: bool = False,
    hive: bool = False,
    overwrite: bool = False,
    force: bool = False,
    skip_analysis: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    verbose: bool = False,
    column_name: str | None = None,
    output_dir: str | Path | None = None,
) -> dict:
    """
    Split every file in a directory over ``min_size`` into H3 sub-partitions.

    Function form of `gpio partition h3 <dir>/ --min-size ...`. Each file over
    the threshold is partitioned into a sibling ``<file>_h3/`` directory; files
    under it are left alone. Pass a ``resolution``, or ``auto=True``.

    Args:
        directory: Directory of parquet files, searched recursively
        min_size: Size threshold -- ``'100MB'`` or a byte count
        resolution: H3 resolution level 0-15. Required unless ``auto=True``
        auto: Size the resolution from each file (default: False)
        target_rows: Target rows per partition when ``auto=True`` (default: 100000)
        max_partitions: Maximum partitions when ``auto=True`` (default: 10000)
        in_place: Delete each original once its sub-partitions hold every row it
            had (default: False)
        preview: List the candidate files and stop, writing and deleting nothing
        hive: Use Hive-style ``h3_cell=value/`` directories (default: False)
        overwrite: Overwrite existing output directories
        force: Partition even when the analysis warns
        skip_analysis: Skip the partition strategy analysis
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level. None uses the ZSTD default of 15
        verbose: Log each file as it is processed
        column_name: Rejected unless it is the default ``h3_cell``: directory
            mode writes one sibling directory per file with the default column
            name. Partition a single file to control it
        output_dir: Rejected for the same reason -- output directories are
            derived per file, not shared

    Returns:
        dict with ``processed``, ``skipped``, ``errors``, the ``candidates`` the
        threshold selected, and ``preview``

    Raises:
        InvalidParameterError: For a path that is not a directory, an
            unparsable ``min_size``, a missing resolution, or an argument
            directory mode cannot honour
        PartitionError: If any file failed to sub-partition. Its ``result``
            attribute carries the run dict, including the files that succeeded

    Example:
        >>> from geoparquet_io.api import ops
        >>> ops.sub_partition_by_h3('by_country/', min_size='100MB', resolution=7,
        ...                         in_place=True)
    """
    return _sub_partition(
        directory,
        "h3",
        min_size=min_size,
        resolution=resolution,
        auto=auto,
        target_rows=target_rows,
        max_partitions=max_partitions,
        in_place=in_place,
        preview=preview,
        hive=hive,
        overwrite=overwrite,
        force=force,
        skip_analysis=skip_analysis,
        compression=compression,
        compression_level=compression_level,
        verbose=verbose,
        column_name=column_name,
        output_dir=output_dir,
    )


def sub_partition_by_a5(
    directory: str | Path,
    *,
    min_size: str | int,
    resolution: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
    in_place: bool = False,
    preview: bool = False,
    hive: bool = False,
    overwrite: bool = False,
    force: bool = False,
    skip_analysis: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    verbose: bool = False,
    column_name: str | None = None,
    output_dir: str | Path | None = None,
) -> dict:
    """
    Split every file in a directory over ``min_size`` into A5 sub-partitions.

    Function form of `gpio partition a5 <dir>/ --min-size ...`. Each file over
    the threshold is partitioned into a sibling ``<file>_a5/`` directory; files
    under it are left alone. Pass a ``resolution``, or ``auto=True``.

    Args:
        directory: Directory of parquet files, searched recursively
        min_size: Size threshold -- ``'100MB'`` or a byte count
        resolution: A5 resolution level 0-30. Required unless ``auto=True``
        auto: Size the resolution from each file (default: False)
        target_rows: Target rows per partition when ``auto=True`` (default: 100000)
        max_partitions: Maximum partitions when ``auto=True`` (default: 10000)
        in_place: Delete each original once its sub-partitions hold every row it
            had (default: False)
        preview: List the candidate files and stop, writing and deleting nothing
        hive: Use Hive-style ``a5_cell=value/`` directories (default: False)
        overwrite: Overwrite existing output directories
        force: Partition even when the analysis warns
        skip_analysis: Skip the partition strategy analysis
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level. None uses the ZSTD default of 15
        verbose: Log each file as it is processed
        column_name: Rejected unless it is the default ``a5_cell`` (see
            `sub_partition_by_h3`)
        output_dir: Rejected -- output directories are derived per file

    Returns:
        dict with ``processed``, ``skipped``, ``errors``, ``candidates`` and
        ``preview``

    Raises:
        InvalidParameterError: For a path that is not a directory, an
            unparsable ``min_size``, a missing resolution, or an argument
            directory mode cannot honour
        PartitionError: If any file failed to sub-partition; ``result`` carries
            the run dict

    Example:
        >>> from geoparquet_io.api import ops
        >>> ops.sub_partition_by_a5('by_country/', min_size='100MB', resolution=10,
        ...                         in_place=True)
    """
    return _sub_partition(
        directory,
        "a5",
        min_size=min_size,
        resolution=resolution,
        auto=auto,
        target_rows=target_rows,
        max_partitions=max_partitions,
        in_place=in_place,
        preview=preview,
        hive=hive,
        overwrite=overwrite,
        force=force,
        skip_analysis=skip_analysis,
        compression=compression,
        compression_level=compression_level,
        verbose=verbose,
        column_name=column_name,
        output_dir=output_dir,
    )


def sub_partition_by_quadkey(
    directory: str | Path,
    *,
    min_size: str | int,
    resolution: int | None = None,
    partition_resolution: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
    in_place: bool = False,
    preview: bool = False,
    hive: bool = False,
    overwrite: bool = False,
    force: bool = False,
    skip_analysis: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    verbose: bool = False,
    column_name: str | None = None,
    output_dir: str | Path | None = None,
) -> dict:
    """
    Split every file in a directory over ``min_size`` into quadkey sub-partitions.

    Function form of `gpio partition quadkey <dir>/ --min-size ...`. Each file
    over the threshold is partitioned into a sibling ``<file>_quadkey/``
    directory; files under it are left alone.

    Pass both ``resolution`` (column precision) and ``partition_resolution``
    (partition prefix length), or use ``auto=True`` to size both from the data.
    The partition resolution cannot exceed the column resolution, matching
    single-file partitioning.

    Args:
        directory: Directory of parquet files, searched recursively
        min_size: Size threshold -- ``'100MB'`` or a byte count
        resolution: Quadkey column resolution 0-23
        partition_resolution: Quadkey partition prefix length 0-23, at most resolution
        auto: Size the resolution from each file (default: False)
        target_rows: Target rows per partition when ``auto=True`` (default: 100000)
        max_partitions: Maximum partitions when ``auto=True`` (default: 10000)
        in_place: Delete each original once its sub-partitions hold every row it
            had (default: False)
        preview: List the candidate files and stop, writing and deleting nothing
        hive: Use Hive-style ``quadkey=value/`` directories (default: False)
        overwrite: Overwrite existing output directories
        force: Partition even when the analysis warns
        skip_analysis: Skip the partition strategy analysis
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level. None uses the ZSTD default of 15
        verbose: Log each file as it is processed
        column_name: Rejected unless it is the default ``quadkey`` (see
            `sub_partition_by_h3`)
        output_dir: Rejected -- output directories are derived per file

    Returns:
        dict with ``processed``, ``skipped``, ``errors``, ``candidates`` and
        ``preview``

    Raises:
        InvalidParameterError: For a path that is not a directory, an
            unparsable ``min_size``, a missing resolution, or an argument
            directory mode cannot honour
        PartitionError: If any file failed to sub-partition; ``result`` carries
            the run dict

    Example:
        >>> from geoparquet_io.api import ops
        >>> ops.sub_partition_by_quadkey('by_country/', min_size='100MB', auto=True,
        ...                              in_place=True)
    """
    return _sub_partition(
        directory,
        "quadkey",
        min_size=min_size,
        resolution=resolution,
        partition_resolution=partition_resolution,
        auto=auto,
        target_rows=target_rows,
        max_partitions=max_partitions,
        in_place=in_place,
        preview=preview,
        hive=hive,
        overwrite=overwrite,
        force=force,
        skip_analysis=skip_analysis,
        compression=compression,
        compression_level=compression_level,
        verbose=verbose,
        column_name=column_name,
        output_dir=output_dir,
    )


def sub_partition_by_s2(
    directory: str | Path,
    *,
    min_size: str | int,
    level: int | None = None,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
    in_place: bool = False,
    preview: bool = False,
    hive: bool = False,
    overwrite: bool = False,
    force: bool = False,
    skip_analysis: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    verbose: bool = False,
    column_name: str | None = None,
    output_dir: str | Path | None = None,
) -> dict:
    """
    Split every file in a directory over ``min_size`` into S2 sub-partitions.

    Function form of `gpio partition s2 <dir>/ --min-size ...`.

    Args:
        directory: Directory of parquet files, searched recursively
        min_size: Size threshold -- ``'100MB'`` or a byte count
        level: S2 level 0-30. Required unless ``auto=True``
        auto: Size the level from each file (default: False)
        target_rows: Target rows per partition when ``auto=True`` (default: 100000)
        max_partitions: Maximum partitions when ``auto=True`` (default: 10000)
        in_place: Delete each original once its sub-partitions hold every row it
            had (default: False)
        preview: List the candidate files and stop, writing and deleting nothing
        hive: Use Hive-style ``s2_cell=value/`` directories (default: False)
        overwrite: Overwrite existing output directories
        force: Partition even when the analysis warns
        skip_analysis: Skip the partition strategy analysis
        compression: Compression codec (default: ZSTD)
        compression_level: Compression level. None uses the ZSTD default of 15
        verbose: Log each file as it is processed
        column_name: Rejected unless it is the default ``s2_cell`` (see
            `sub_partition_by_h3`)
        output_dir: Rejected -- output directories are derived per file

    Returns:
        dict with ``processed``, ``skipped``, ``errors``, ``candidates`` and
        ``preview``

    Raises:
        ExtensionUnavailableError: Always, in this release -- see above
        InvalidParameterError: For a path that is not a directory, an
            unparsable ``min_size``, a missing level, or an argument directory
            mode cannot honour
        PartitionError: If any file failed to sub-partition; ``result`` carries
            the run dict

    Example:
        >>> from geoparquet_io.api import ops
        >>> ops.sub_partition_by_s2('by_country/', min_size='100MB', level=10)
    """
    return _sub_partition(
        directory,
        "s2",
        min_size=min_size,
        level=level,
        auto=auto,
        target_rows=target_rows,
        max_partitions=max_partitions,
        in_place=in_place,
        preview=preview,
        hive=hive,
        overwrite=overwrite,
        force=force,
        skip_analysis=skip_analysis,
        compression=compression,
        compression_level=compression_level,
        verbose=verbose,
        column_name=column_name,
        output_dir=output_dir,
    )


def read_bigquery(
    table_id: str,
    *,
    project: str | None = None,
    credentials_file: str | None = None,
    where: str | None = None,
    bbox: str | None = None,
    bbox_mode: str = "auto",
    bbox_threshold: int = 500000,
    limit: int | None = None,
    columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    geography_column: str | None = None,
    geometry_format: str = "wkt",
    edges: str | None = None,
    repair_geometry: bool = True,
) -> pa.Table:
    """
    Read data from a BigQuery table.

    Uses DuckDB's BigQuery extension with the Storage Read API for
    efficient Arrow-based scanning with filter pushdown.

    Native GEOGRAPHY columns are automatically detected and converted
    with spherical edges. VARCHAR columns containing WKT or GeoJSON
    can be parsed by specifying geography_column and geometry_format.

    Args:
        table_id: Fully qualified BigQuery table ID (project.dataset.table)
        project: GCP project ID (overrides project in table_id if set)
        credentials_file: Path to service account JSON file
        where: SQL WHERE clause for filtering (BigQuery SQL syntax)
        bbox: Bounding box for spatial filter as "minx,miny,maxx,maxy"
        bbox_mode: Filtering mode - "auto" (default), "server", or "local"
        bbox_threshold: Row count threshold for auto mode (default: 500000)
        limit: Maximum rows to extract
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude
        geography_column: Column containing geometry data. Auto-detected
            for native GEOGRAPHY columns. Specify to parse VARCHAR columns.
        geometry_format: Format of VARCHAR geometry ("wkt" or "geojson")
        edges: Edge interpretation ("spherical" or "planar"). Native
            GEOGRAPHY columns default to "spherical", VARCHAR to "planar".

    Returns:
        PyArrow Table with BigQuery data

    Raises:
        FileNotFoundError: If credentials_file doesn't exist
        RuntimeError: If BigQuery query fails

    Note:
        **Cannot read BigQuery views or external tables** - this is a
        limitation of the BigQuery Storage Read API.

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = ops.read_bigquery('myproject.geodata.buildings')
        >>> table = ops.add_bbox(table)
        >>> pq.write_table(table, 'output.parquet')

        >>> # Parse VARCHAR column as WKT geometry
        >>> table = ops.read_bigquery(
        ...     'myproject.dataset.table',
        ...     geography_column='geometry',
        ...     geometry_format='wkt'
        ... )
    """
    from geoparquet_io.core.extract_bigquery import extract_bigquery

    # Convert columns list to comma-separated string for the core function
    include_cols = ",".join(columns) if columns else None
    exclude_cols = ",".join(exclude_columns) if exclude_columns else None

    # Validate bbox_mode
    valid_bbox_modes = {"auto", "server", "local"}
    if bbox_mode not in valid_bbox_modes:
        raise ValueError(
            f"Invalid bbox_mode '{bbox_mode}' for table '{table_id}'. "
            f"Must be one of: {', '.join(sorted(valid_bbox_modes))}"
        )

    # Validate bbox_threshold
    if not isinstance(bbox_threshold, int) or bbox_threshold < 0:
        raise ValueError(
            f"Invalid bbox_threshold '{bbox_threshold}' for table '{table_id}'. "
            "Must be an integer >= 0."
        )

    # Get PyArrow table (don't write to file)
    arrow_table = extract_bigquery(
        table_id=table_id,
        output_parquet=None,  # Return table instead of writing
        project=project,
        credentials_file=credentials_file,
        where=where,
        bbox=bbox,
        bbox_mode=bbox_mode,
        bbox_threshold=bbox_threshold,
        limit=limit,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        geography_column=geography_column,
        geometry_format=geometry_format,
        edges=edges,
        verbose=False,
        repair_geometry=repair_geometry,
    )

    if arrow_table is None:
        raise RuntimeError(f"Failed to read from BigQuery table: {table_id}")

    return arrow_table


def convert_to_geojson(
    table: pa.Table,
    output_path: str | None = None,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    repair_geometry: bool = True,
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
            repair_geometry=repair_geometry,
        )

        return output_path

    finally:
        # Clean up temp file
        if temp_input.exists():
            temp_input.unlink()


def _file_round_trip(
    table: pa.Table,
    func,
    **kwargs,
) -> pa.Table:
    """
    Execute an input->output file transformation on an in-memory table.

    Writes table to a temp input file, calls func(input_parquet, output_parquet, **kwargs),
    reads the output back, and cleans up both temp files.
    """
    import tempfile
    import uuid
    from pathlib import Path

    import pyarrow.parquet as pq

    temp_dir = Path(tempfile.gettempdir())
    temp_input = temp_dir / f"gpio_in_{uuid.uuid4()}.parquet"
    temp_output = temp_dir / f"gpio_out_{uuid.uuid4()}.parquet"

    try:
        pq.write_table(table, str(temp_input))
        func(
            input_parquet=str(temp_input),
            output_parquet=str(temp_output),
            **kwargs,
        )
        return pq.read_table(str(temp_output))
    finally:
        for f in (temp_input, temp_output):
            if f.exists():
                f.unlink()


def _table_to_temp_parquet_and_convert(
    table: pa.Table,
    output_path: str,
    writer_func,
    prefix: str,
    **writer_kwargs,
) -> str:
    """
    Helper function to convert PyArrow Table to a format via temp parquet file.

    Eliminates repetitive temp file handling across all conversion functions.

    Args:
        table: Input PyArrow Table
        output_path: Output file path
        writer_func: Writer function from format_writers module
        prefix: Prefix for temp file name
        **writer_kwargs: Keyword arguments to pass to writer function

    Returns:
        Output path

    Raises:
        TypeError: If table is not a PyArrow Table
    """
    import tempfile
    import uuid
    from pathlib import Path

    if not isinstance(table, pa.Table):
        raise TypeError(f"Expected pa.Table, got {type(table).__name__}")

    # Write table to temp parquet file for processing
    temp_dir = Path(tempfile.gettempdir())
    temp_input = temp_dir / f"gpio_{prefix}_{uuid.uuid4()}.parquet"

    try:
        import pyarrow.parquet as pq

        pq.write_table(table, str(temp_input))

        # Call writer function
        writer_func(
            input_path=str(temp_input),
            output_path=output_path,
            verbose=False,
            **writer_kwargs,
        )

        return output_path

    finally:
        # Clean up temp file
        if temp_input.exists():
            temp_input.unlink()


def convert_to_geopackage(
    table: pa.Table,
    output_path: str,
    overwrite: bool = False,
    layer_name: str = "features",
) -> str:
    """
    Convert a GeoParquet table to GeoPackage format.

    Writes to file and creates spatial index automatically.

    Args:
        table: Input PyArrow Table with geometry column
        output_path: Output file path (must be local, not cloud URL)
        overwrite: Overwrite existing file (default: False)
        layer_name: Layer name in GeoPackage (default: 'features')

    Returns:
        Output path
    """
    from geoparquet_io.core.format_writers import write_geopackage

    return _table_to_temp_parquet_and_convert(
        table,
        output_path,
        write_geopackage,
        "geopackage",
        overwrite=overwrite,
        layer_name=layer_name,
    )


def convert_to_flatgeobuf(
    table: pa.Table,
    output_path: str,
) -> str:
    """
    Convert a GeoParquet table to FlatGeobuf format.

    Writes to file and creates spatial index automatically.

    Args:
        table: Input PyArrow Table with geometry column
        output_path: Output file path (must be local, not cloud URL)

    Returns:
        Output path
    """
    from geoparquet_io.core.format_writers import write_flatgeobuf

    return _table_to_temp_parquet_and_convert(
        table,
        output_path,
        write_flatgeobuf,
        "flatgeobuf",
    )


def convert_to_csv(
    table: pa.Table,
    output_path: str,
    include_wkt: bool = True,
    include_bbox: bool = True,
) -> str:
    """
    Convert a GeoParquet table to CSV format.

    Converts geometry to WKT text representation.
    Complex types (STRUCT, LIST, MAP) are JSON-encoded.

    Args:
        table: Input PyArrow Table with geometry column
        output_path: Output file path (must be local, not cloud URL)
        include_wkt: Include WKT geometry column (default: True)
        include_bbox: Include bbox column if present (default: True)

    Returns:
        Output path
    """
    from geoparquet_io.core.format_writers import write_csv

    return _table_to_temp_parquet_and_convert(
        table,
        output_path,
        write_csv,
        "csv",
        include_wkt=include_wkt,
        include_bbox=include_bbox,
    )


def convert_to_shapefile(
    table: pa.Table,
    output_path: str,
    overwrite: bool = False,
    encoding: str = "UTF-8",
) -> str:
    """
    Convert a GeoParquet table to Shapefile format.

    Note: Shapefiles have significant limitations:
    - Column names truncated to 10 characters
    - File size limit of 2GB
    - Limited data type support
    - Creates multiple files (.shp, .shx, .dbf, .prj)

    Args:
        table: Input PyArrow Table with geometry column
        output_path: Output file path (must be local, not cloud URL)
        overwrite: Overwrite existing file (default: False)
        encoding: Character encoding (default: 'UTF-8')

    Returns:
        Output path
    """
    from geoparquet_io.core.format_writers import write_shapefile

    return _table_to_temp_parquet_and_convert(
        table,
        output_path,
        write_shapefile,
        "shapefile",
        overwrite=overwrite,
        encoding=encoding,
    )


def from_arcgis(
    service_url: str,
    token: str | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    limit: int | None = None,
    max_workers: int = 1,
    output_crs: str | None = None,
    max_allowable_offset: float | None = None,
    repair_geometry: bool = True,
    timeout: float = 60.0,
) -> pa.Table:
    """
    Fetch ArcGIS Feature Service as a PyArrow Table.

    Lower-level function for users who want direct Arrow table access.
    Supports server-side filtering for efficient data transfer.

    Args:
        service_url: ArcGIS Feature Service URL with layer ID
        token: Optional authentication token
        where: SQL WHERE clause to filter features (default: "1=1" = all)
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        include_cols: Comma-separated column names to include (server-side)
        exclude_cols: Comma-separated column names to exclude (client-side)
        limit: Maximum number of features to return
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)
        output_crs: Preserve native CRS. 'native' uses the layer's advertised SR,
            or pass an EPSG code (e.g. EPSG:25830). Default None reprojects to WGS84.
        max_allowable_offset: Server-side geometry generalization tolerance, in
            output-CRS units (degrees on the default WGS84 path).
        timeout: Per-request HTTP timeout in seconds (default 60). Increase for
            layers with very large/complex geometries that are slow to serialize.

    Returns:
        PyArrow Table with WKB geometry column

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = ops.from_arcgis('https://services.arcgis.com/.../FeatureServer/0')
        >>> table = ops.add_bbox(table)
        >>> table = ops.sort_hilbert(table)
        >>>
        >>> # With server-side filtering
        >>> table = ops.from_arcgis(url, bbox=(-122.5, 37.5, -122.0, 38.0), limit=1000)
        >>>
        >>> # With parallel fetching for large datasets
        >>> table = ops.from_arcgis(url, limit=100000, max_workers=3)
    """
    from geoparquet_io.core.arcgis import ArcGISAuth, arcgis_to_table

    auth = ArcGISAuth(token=token) if token else None
    return arcgis_to_table(
        service_url,
        auth=auth,
        where=where,
        bbox=bbox,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        limit=limit,
        max_workers=max_workers,
        output_crs=output_crs,
        max_allowable_offset=max_allowable_offset,
        verbose=False,
        repair_geometry=repair_geometry,
        timeout=timeout,
    )


def from_wfs(
    service_url: str,
    typename: str,
    version: str = "auto",
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    max_workers: int = 1,
    page_size: int = DEFAULT_WFS_PAGE_SIZE,
    axis_order: str = "auto",
    strict_crs: bool = False,
    auto_tile: bool = True,
    repair_geometry: bool = True,
) -> pa.Table:
    """
    Fetch WFS layer as PyArrow Table.

    Uses DuckDB's native HTTP streaming for fast extraction. For very large
    datasets (1M+ features), use max_workers > 1 to enable parallel pagination.

    Args:
        service_url: WFS service URL
        typename: Feature type name (e.g., 'cities' or 'ns:cities')
        version: WFS version ('auto', '2.0.0', '1.1.0', '1.0.0'). Default 'auto'
            tries 2.0.0, then 1.1.0, then 1.0.0.
        bbox: Optional bounding box filter (xmin, ymin, xmax, ymax)
        limit: Maximum features to fetch
        max_workers: Parallel requests for large datasets (default: 1)
        page_size: Features per page when using parallel mode (default: 100000)
        axis_order: Bbox axis order ('auto', 'xy', 'latlon'). 'auto' detects from
            CRS format - URN CRS with WFS 1.1.0+ uses lat,lon per OGC spec.
        strict_crs: If True, fail when the server returns a different CRS than
            requested. If False (default), warn and use the server's actual CRS.
            The CRS the server declares in its GeoJSON response is authoritative;
            gpio never guesses from coordinates when the server states it (#499).
        auto_tile: Automatically subdivide into spatial tiles when the server
            caps responses (maxFeatures or startIndex limits). Matches the CLI
            default; setting it False accepts silently truncated results
            (default: True)

    Returns:
        PyArrow Table with geometry column

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = ops.from_wfs('https://geo.example.com/wfs', 'cities', limit=100)
        >>> # Accept whatever a capped server returns, without tiling:
        >>> table = ops.from_wfs('https://geo.example.com/wfs', 'parcels', auto_tile=False)
    """
    from geoparquet_io.core.wfs import wfs_to_table

    return wfs_to_table(
        service_url,
        typename,
        version=version,
        bbox=bbox,
        limit=limit,
        max_workers=max_workers,
        page_size=page_size,
        axis_order=axis_order,
        strict_crs=strict_crs,
        auto_tile=auto_tile,
        repair_geometry=repair_geometry,
    )


def from_wfs_layers(
    service_url: str,
    typenames: list[str],
    output_dir: str,
    version: str = "auto",
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    max_workers: int = 1,
    page_size: int = DEFAULT_WFS_PAGE_SIZE,
    parallel_layers: int = 1,
    axis_order: str = "auto",
    strict_crs: bool = False,
    auto_tile: bool = True,
    skip_hilbert: bool = False,
    skip_bbox: bool = False,
    compression: str = "ZSTD",
    overwrite: bool = False,
    repair_geometry: bool = True,
) -> dict[str, str]:
    """
    Extract multiple WFS layers in parallel to a directory.

    Each layer is saved as a separate GeoParquet file named after the typename.

    Args:
        service_url: WFS service URL
        typenames: List of layer typenames to extract
        output_dir: Directory to write output files
        version: WFS version ('auto', '2.0.0', '1.1.0', '1.0.0'). Default 'auto'
        bbox: Optional bounding box filter (xmin, ymin, xmax, ymax) applied to all layers
        limit: Maximum features per layer
        max_workers: Per-layer pagination workers (default: 1)
        page_size: Features per page (default: 100000)
        parallel_layers: Number of layers to extract concurrently (default: 1)
        axis_order: Bbox axis order ('auto', 'xy', 'latlon')
        strict_crs: If True, fail when the server returns a different CRS than requested
        auto_tile: Automatically subdivide into spatial tiles when the server
            caps responses (maxFeatures or startIndex limits). Matches the CLI
            default; setting it False accepts silently truncated results
            (default: True)
        skip_hilbert: Skip Hilbert curve sorting (default: False)
        skip_bbox: Skip adding bbox column (default: False)
        compression: Compression algorithm (default: 'ZSTD')
        overwrite: Overwrite existing files (default: False)

    Returns:
        Dict mapping typename to output file path (only successful extractions)

    Example:
        >>> from geoparquet_io.api import ops
        >>> results = ops.from_wfs_layers(
        ...     'https://geo.example.com/wfs',
        ...     ['roads', 'buildings', 'parcels'],
        ...     './output/',
        ...     parallel_layers=3,
        ...     max_workers=2
        ... )
        >>> print(results)  # {'roads': './output/roads.parquet', ...}
    """
    from geoparquet_io.core.wfs import convert_wfs_layers_to_directory

    results = convert_wfs_layers_to_directory(
        service_url=service_url,
        typenames=typenames,
        output_dir=output_dir,
        parallel_layers=parallel_layers,
        max_workers=max_workers,
        page_size=page_size,
        version=version,
        bbox=bbox,
        limit=limit,
        axis_order=axis_order,
        strict_crs=strict_crs,
        skip_hilbert=skip_hilbert,
        skip_bbox=skip_bbox,
        compression=compression,
        overwrite=overwrite,
        auto_tile=auto_tile,
        repair_geometry=repair_geometry,
    )
    # Convert Path to str for simpler API
    return {k: str(v) for k, v in results.items()}


def from_carto(
    url: str,
    table_name: str,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    api_key: str | None = None,
    timeout: float = 120.0,
    repair_geometry: bool = True,
    geometry: bool | None = None,
) -> pa.Table:
    """
    Fetch Carto SQL API table as PyArrow Table.

    Geometry tables are parsed via DuckDB's ST_Read (GeoJSON) and carry
    GeoParquet metadata. Geometry-less (tabular) tables are fetched as CSV and
    returned as a plain table with no ``geo`` metadata. Filters are pushed to
    the server for optimal performance.

    Args:
        url: Carto SQL API URL (e.g., 'https://phl.carto.com/api/v2/sql')
            or base domain (e.g., 'https://phl.carto.com')
        table_name: Name of the table to query
        where: SQL WHERE clause for filtering
        bbox: Optional bounding box filter (xmin, ymin, xmax, ymax) in WGS84.
            Ignored for geometry-less tables.
        limit: Maximum rows to fetch
        include_cols: Comma-separated columns to include
        exclude_cols: Comma-separated columns to exclude
        api_key: API key for authenticated requests (or set CARTO_API_KEY env var)
        timeout: Request timeout in seconds (default: 120)
        repair_geometry: Repair invalid geometry with ST_MakeValid (geometry
            tables only; default: True)
        geometry: Extraction mode. ``None`` (default) auto-detects from the
            table schema; ``True`` forces geometry extraction; ``False`` forces
            plain/tabular extraction (no ``geo`` metadata).

    Returns:
        PyArrow Table. For geometry tables, a 'geometry' column with GeoParquet
        metadata; for tabular tables, a plain table with no ``geo`` metadata.

    Note:
        The Carto geometry column 'the_geom' is renamed to 'geometry'
        for consistency with other geoparquet-io extractors.

        For authenticated endpoints, either pass api_key or set the
        CARTO_API_KEY environment variable.

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = ops.from_carto(
        ...     'https://phl.carto.com/api/v2/sql',
        ...     'opa_properties_public',
        ...     limit=100
        ... )
        >>> # With filters:
        >>> table = ops.from_carto(
        ...     'https://phl.carto.com/api/v2/sql',
        ...     'opa_properties_public',
        ...     where="category_code_description = 'SINGLE FAMILY'",
        ...     bbox=(-75.2, 39.9, -75.1, 40.0)
        ... )
    """
    from geoparquet_io.core.carto import carto_to_table

    return carto_to_table(
        url=url,
        table_name=table_name,
        where=where,
        bbox=bbox,
        limit=limit,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        api_key=api_key,
        timeout=timeout,
        repair_geometry=repair_geometry,
        geometry=geometry,
    )


def get_row_group_geo_stats(parquet_file: str) -> list[dict]:
    """
    Get per-row-group geo_bbox statistics from a GeoParquet file.

    Returns a list of dicts with row_group_id, num_rows, xmin, ymin,
    xmax, ymax for each row group. Useful for verifying spatial locality
    after Hilbert sorting.

    Tries native Parquet geo stats first (GeoParquet 2.0 / parquet-geo-only),
    then falls back to bbox column statistics if no native stats are available.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        List of dicts with per-row-group bbox statistics.
        Empty list if no geo stats are available.
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_file_metadata,
        get_per_row_group_bbox_stats,
        get_per_row_group_native_geo_stats,
        has_bbox_column,
    )
    from geoparquet_io.core.metadata_utils import (
        _get_num_rows_per_row_group,
        _merge_row_counts,
    )

    # Try native geo stats first (GeoParquet 2.0 / parquet-geo-only)
    rg_stats = get_per_row_group_native_geo_stats(parquet_file)

    # Fall back to bbox column if no native stats
    if not rg_stats:
        has_bbox, bbox_col_name = has_bbox_column(parquet_file)
        if has_bbox and bbox_col_name:
            rg_stats = get_per_row_group_bbox_stats(parquet_file, bbox_col_name)

    if not rg_stats:
        return []

    file_meta = get_file_metadata(parquet_file)
    num_rows_per_rg = _get_num_rows_per_row_group(parquet_file, file_meta)

    return _merge_row_counts(rg_stats, num_rows_per_rg)


def compression_stats(path: str) -> list[dict]:
    """
    Get per-column compression ratios for a Parquet file.

    Queries Parquet metadata to compute compressed and uncompressed sizes
    per column, along with the compression ratio.

    Args:
        path: Path or URL to the Parquet file

    Returns:
        List of dicts ordered by compressed_bytes descending, each with:
        - column_name: Column path in schema
        - compression: Compression codec name
        - compressed_bytes: Total compressed size in bytes
        - uncompressed_bytes: Total uncompressed size in bytes
        - ratio: Compression ratio (uncompressed / compressed)

    Example:
        >>> from geoparquet_io.api import ops
        >>> stats = ops.compression_stats('data.parquet')
        >>> for col in stats:
        ...     print(f"{col['column_name']}: {col['ratio']}x")
    """
    from geoparquet_io.core.duckdb_metadata import get_compression_stats

    return get_compression_stats(str(path))


def explain_analyze(
    file_path: str,
    query: str | None = None,
) -> dict:
    """
    Run EXPLAIN ANALYZE on a DuckDB query against a Parquet file.

    Shows per-operator timing, cardinality, filter pushdown detection,
    and row group pruning analysis.

    Args:
        file_path: Path to the input Parquet file.
        query: Optional SQL query. Use {file} as placeholder for the file path.
               Defaults to SELECT * FROM read_parquet('{file}').

    Returns:
        Dictionary with operators, timing, and analysis results.

    Example:
        >>> from geoparquet_io.api import ops
        >>> result = ops.explain_analyze('input.parquet')
        >>> for op in result['operators']:
        ...     print(f"{op['name']}: {op['timing']:.6f}s")
        >>> # With a custom query:
        >>> result = ops.explain_analyze(
        ...     'input.parquet',
        ...     query="SELECT * FROM read_parquet('{file}') WHERE id > 10"
        ... )
    """
    from geoparquet_io.core.benchmark import explain_analyze as _explain_analyze

    return _explain_analyze(
        file_path=file_path,
        query=query,
    )


def create_pmtiles(
    input_path: str,
    output_path: str,
    *,
    layer: str | None = None,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    bbox: str | None = None,
    where: str | None = None,
    include_cols: str | None = None,
    precision: int = 6,
    verbose: bool = False,
    profile: str | None = None,
    src_crs: str | None = None,
    attribution: str | None = None,
    layer_by_column: str | None = None,
    simplify_only_low_zooms: bool = True,
    no_simplification_of_shared_nodes: bool = True,
    no_tile_size_limit: bool = True,
    drop_densest_as_needed: bool = True,
    maximum_tile_bytes: int | None = None,
    force: bool = False,
    repair_geometry: bool = True,
) -> None:
    """
    Create PMTiles from a GeoParquet file using tippecanoe.

    Streams GeoParquet through gpio and tippecanoe to generate PMTiles.
    Requires tippecanoe to be installed and available in PATH.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path for output PMTiles file
        layer: Layer name in PMTiles (defaults to output filename)
        min_zoom: Minimum zoom level (optional)
        max_zoom: Maximum zoom level (optional, auto-detected if not set)
        bbox: Bounding box filter as "minx,miny,maxx,maxy"
        where: SQL WHERE clause for filtering
        include_cols: Comma-separated list of columns to include
        precision: Coordinate decimal precision (default: 6)
        verbose: Enable verbose output
        profile: AWS profile name for S3 files
        src_crs: Source CRS for reprojection to WGS84
        attribution: Attribution HTML for the tiles
        layer_by_column: Split tiles into layers grouped by the values of this column
        simplify_only_low_zooms: Pass --simplify-only-low-zooms (default: True)
        no_simplification_of_shared_nodes: Pass --no-simplification-of-shared-nodes (default: True)
        no_tile_size_limit: Pass --no-tile-size-limit, removing the tile size
            cap (default: True). Set False to respect tippecanoe's size limit so
            that drop_densest_as_needed actually drops features on dense data.
        drop_densest_as_needed: Pass --drop-densest-as-needed (default: True).
            Only takes effect when there is a tile size limit to drop against.
        maximum_tile_bytes: Set an explicit per-tile byte cap via
            --maximum-tile-bytes. Takes precedence over no_tile_size_limit.
        force: Pass --force to overwrite the output file if it already exists.

    Raises:
        TippecanoeNotFoundError: If tippecanoe is not in PATH
        ValueError: If paths contain shell metacharacters
        RuntimeError: If any subprocess fails

    Example:
        >>> from geoparquet_io.api import ops
        >>> ops.create_pmtiles('buildings.parquet', 'buildings.pmtiles')
        >>> # With options:
        >>> ops.create_pmtiles(
        ...     'data.parquet',
        ...     'tiles.pmtiles',
        ...     layer='buildings',
        ...     max_zoom=14,
        ...     bbox='-122.5,37.5,-122.0,38.0'
        ... )
    """
    from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

    create_pmtiles_from_geoparquet(
        input_path=input_path,
        output_path=output_path,
        layer=layer,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        bbox=bbox,
        where=where,
        include_cols=include_cols,
        precision=precision,
        verbose=verbose,
        profile=profile,
        src_crs=src_crs,
        attribution=attribution,
        layer_by_column=layer_by_column,
        simplify_only_low_zooms=simplify_only_low_zooms,
        no_simplification_of_shared_nodes=no_simplification_of_shared_nodes,
        no_tile_size_limit=no_tile_size_limit,
        drop_densest_as_needed=drop_densest_as_needed,
        maximum_tile_bytes=maximum_tile_bytes,
        force=force,
        repair_geometry=repair_geometry,
    )


def create_pmtiles_pyramid(
    input_path: str,
    output_path: str,
    *,
    levels: str | list[int | str] | None = None,
    max_tile_kb: int = 500,
    bytes_per_cell: float | None = None,
    layer_mode: str = "grouped",
    include_features: bool = False,
    features_source: str | None = None,
    features_min_zoom: int | None = None,
    max_zoom: int | None = None,
    attribution: str | None = None,
    force: bool = False,
    verbose: bool = False,
) -> None:
    """
    Create a banded multi-level PMTiles pyramid from an aggregate file.

    Detects the aggregate's scheme (a5/h3/admin) and base level, assigns each
    level a zoom band that fits the tile budget, runs tippecanoe once per band,
    and merges everything into one archive with tile-join. Existing overview
    siblings (from `gpio process overview` / `ops.create_overviews`) are
    reused; missing levels are built automatically. Bands are recorded in the
    PMTiles metadata under ``gpio:pyramid``.

    Requires tippecanoe and tile-join (ships with tippecanoe) in PATH.

    Args:
        input_path: Path to a `gpio process aggregate` output (GeoParquet)
        output_path: Path for the output PMTiles archive
        levels: Explicit overview levels (comma string or list; admin:
            "country"). Default: auto-select against max_tile_kb
        max_tile_kb: Tile-size budget in KB for band selection (default: 500)
        bytes_per_cell: Override the estimated compressed bytes per cell
        layer_mode: "single", "grouped" (default), or "per-level"
        include_features: Append the original features as the final zoom band
        features_source: GeoParquet source for the features band
        features_min_zoom: First zoom of the features band (default: base
            band max zoom + 1)
        max_zoom: Max zoom of the base aggregate band
        attribution: Attribution HTML for the tiles
        force: Overwrite the output archive if it exists
        verbose: Enable verbose output

    Raises:
        TippecanoeNotFoundError: If tippecanoe is not in PATH
        TileJoinNotFoundError: If tile-join is not in PATH
        RuntimeError: If a tippecanoe or tile-join run fails

    Example:
        >>> from geoparquet_io.api import ops
        >>> ops.create_pmtiles_pyramid('cells.parquet', 'cells.pmtiles')
        >>> ops.create_pmtiles_pyramid(
        ...     'cells.parquet',
        ...     'pyramid.pmtiles',
        ...     include_features=True,
        ...     features_source='buildings.parquet',
        ...     max_zoom=8,
        ... )
    """
    from geoparquet_io.core.pmtiles_pyramid import (
        create_pmtiles_pyramid as _create_pmtiles_pyramid,
    )

    _create_pmtiles_pyramid(
        input_path,
        output_path,
        levels=levels,
        max_tile_kb=max_tile_kb,
        bytes_per_cell=bytes_per_cell,
        layer_mode=layer_mode,
        include_features=include_features,
        features_source=features_source,
        features_min_zoom=features_min_zoom,
        max_zoom=max_zoom,
        attribution=attribution,
        force=force,
        verbose=verbose,
    )
