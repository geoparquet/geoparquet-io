"""Preview which row groups a bbox-filtered read would touch, before reading (#1310).

Given a file and a query bbox, :func:`preview_bbox_filter` answers from the
Parquet footer alone: which row groups can match, and how many rows and
compressed bytes those groups hold. A consumer (the QGIS download plugins, a
dry run) gets a real denominator for a progress bar and a size warning before
the first data byte moves.

The numbers are derived only from per-row-group spatial statistics the file
actually carries -- native Parquet geo statistics first, then a bbox covering
column's chunk statistics, the same two sources ``gpio check`` and the Python
API's ``get_row_group_geo_stats`` read. A file with neither gets a first-class
"no estimate" (:attr:`BboxFilterPreview.has_estimate` False, with the reason),
never a guess. The estimate is conservative the way DuckDB's own pruning is: a
row group whose statistics are absent cannot be ruled out, so it is counted.

Everything duckdb-flavoured is imported inside the function bodies: this module
belongs to the embeddable set (tracker #1311) and must import without pulling
duckdb into the process.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from geoparquet_io.core.bbox_parse import BBox, parse_bbox

#: The one reason a preview carries no estimate.
NO_STATS_REASON = (
    "no usable per-row-group spatial statistics: the file has neither native "
    "Parquet geo statistics nor a bbox covering column with statistics"
)


@dataclass(frozen=True)
class RowGroupEstimate:
    """One row group a bbox-filtered read would touch, and what touching it costs."""

    row_group_id: int
    num_rows: int
    #: Compressed bytes of every column chunk in the group: what a full-width
    #: read of the group downloads.
    compressed_bytes: int


@dataclass(frozen=True)
class BboxFilterPreview:
    """What a bbox-filtered read of one file would touch, estimated from its footer.

    ``has_estimate`` is the first thing to read: when it is False the file
    exposed no usable per-row-group spatial statistics, ``reason`` says so, and
    ``row_groups``/``est_rows``/``est_bytes`` are empty/None rather than a
    guess. The ``total_*`` fields are facts of the file either way, so a caller
    can still warn about whole-file size when there is no estimate.
    """

    has_estimate: bool
    #: Why there is no estimate; None when there is one.
    reason: str | None
    #: Which statistics backed the estimate; None when there is no estimate.
    stats_source: Literal["native", "bbox_struct"] | None
    #: The row groups the read would touch, ordered by row_group_id.
    row_groups: tuple[RowGroupEstimate, ...]
    #: Upper-bound row count of the touched groups; 0 is a real "nothing matches".
    est_rows: int | None
    #: Compressed bytes of the touched groups.
    est_bytes: int | None
    total_row_groups: int
    total_rows: int
    total_bytes: int


def preview_bbox_filter(
    parquet_file: str,
    bbox: Sequence[float],
    geometry_column: str | None = None,
) -> BboxFilterPreview:
    """Preview the row groups a bbox-filtered read of ``parquet_file`` would touch.

    Args:
        parquet_file: Path or URL of ONE (Geo)Parquet file. A glob or
            partition directory is resolved to its first file, so the preview
            (totals included) describes that file alone, not the dataset.
        bbox: Query bounds as ``(xmin, ymin, xmax, ymax)`` in the file's CRS.
        geometry_column: Geometry column whose statistics to read;
            auto-detected from the ``geo`` metadata when None.

    Returns:
        A :class:`BboxFilterPreview`. Check ``has_estimate`` before using the
        estimates: a file without usable statistics reports why instead of
        guessing.

    Raises:
        InvalidParameterError: If ``bbox`` is not four finite numbers with
            ``xmin <= xmax`` and ``ymin <= ymax``, or if ``geometry_column``
            names a column the file does not have.
    """
    query_bbox = parse_bbox(bbox)

    from geoparquet_io.core.duckdb_metadata import (
        _get_connection_for_file,
        _materialize_row_group_metadata,
        get_row_group_sizes,
    )

    con, _ = _get_connection_for_file(parquet_file)
    try:
        # A cursor owns its TEMP tables. Materialize this footer only for this
        # preview, so repeated statistics queries cannot fetch it again.
        _materialize_row_group_metadata(parquet_file, con, "_gpio_preview_metadata")
        sizes = get_row_group_sizes(parquet_file, con=con, metadata_table="_gpio_preview_metadata")
        totals = {
            "total_row_groups": len(sizes),
            "total_rows": sum(size["num_rows"] for size in sizes),
            "total_bytes": sum(size["compressed_bytes"] for size in sizes),
        }

        stats, source = _row_group_spatial_stats(
            parquet_file, geometry_column, con=con, metadata_table="_gpio_preview_metadata"
        )
        if not stats:
            return BboxFilterPreview(
                has_estimate=False,
                reason=NO_STATS_REASON,
                stats_source=None,
                row_groups=(),
                est_rows=None,
                est_bytes=None,
                **totals,
            )

        matched = _matching_row_groups(stats, sizes, query_bbox)
        return BboxFilterPreview(
            has_estimate=True,
            reason=None,
            stats_source=source,
            row_groups=matched,
            est_rows=sum(group.num_rows for group in matched),
            est_bytes=sum(group.compressed_bytes for group in matched),
            **totals,
        )

    finally:
        con.close()


def _row_group_spatial_stats(
    parquet_file: str, geometry_column: str | None, con, *, metadata_table: str
) -> tuple[list[dict], Literal["native", "bbox_struct"] | None]:
    """Resolve the geometry and covering once, using this preview's footer rows."""
    from geoparquet_io.core.bbox_structure import (
        bbox_covering_column_for,
        bbox_structure_from_metadata,
    )
    from geoparquet_io.core.duckdb_metadata import (
        get_per_row_group_native_geo_stats,
        get_row_group_bbox_stats,
        get_schema_info,
    )
    from geoparquet_io.core.geo_metadata import raw_geo_of_file
    from geoparquet_io.core.geometry_detection import primary_geometry_column
    from geoparquet_io.core.parquet_schema import root_schema_columns

    schema = get_schema_info(parquet_file, con=con)
    raw_geo = raw_geo_of_file(parquet_file, con=con)
    geo = raw_geo.block
    columns = [(col["name"], col.get("logical_type")) for col in root_schema_columns(schema)]
    primary = primary_geometry_column(columns, geo=raw_geo.value)
    selected = geometry_column or primary

    def native():
        if not selected:
            return []
        return get_per_row_group_native_geo_stats(
            parquet_file,
            selected,
            con=con,
            strict=geometry_column is not None,
            metadata_table=metadata_table,
        )

    def bbox_column():
        if geometry_column is None or geometry_column == primary:
            return bbox_structure_from_metadata(schema, geo).get("bbox_column_name")
        return bbox_covering_column_for(geo, geometry_column)

    return get_row_group_bbox_stats(
        parquet_file,
        native=native,
        bbox_column=bbox_column,
        con=con,
        metadata_table=metadata_table,
    )


def _matching_row_groups(
    stats: list[dict],
    sizes: list[dict],
    bbox: tuple[float, float, float, float],
) -> tuple[RowGroupEstimate, ...]:
    """The row groups the read touches: bbox-intersecting ones, plus unvouched ones.

    A group with no statistics row cannot be pruned -- the real read touches it
    -- so it is counted. That keeps the preview an upper bound: a group it
    leaves out contributes no rows to the real read.
    """
    bboxes_by_group = {stat["row_group_id"]: stat for stat in stats}
    return tuple(
        RowGroupEstimate(
            row_group_id=size["row_group_id"],
            num_rows=size["num_rows"],
            compressed_bytes=size["compressed_bytes"],
        )
        for size in sizes
        if (stat := bboxes_by_group.get(size["row_group_id"])) is None or _intersects(stat, bbox)
    )


def _intersects(stat: dict, bbox: tuple[float, float, float, float]) -> bool:
    """Whether a row group's bbox statistics intersect the query bbox (edges count).

    Either box may wrap the antimeridian: the query bbox, and a group's
    statistics, which Parquet permits to have xmin > xmax (see
    aggregate_native_geo_stats). Each is split into its lobes by the owner
    (``BBox.pieces``) and they intersect when any two lobes do; pruning a
    wrapped group as an empty interval would drop rows the real read returns.
    """
    group = BBox(stat["xmin"], stat["ymin"], stat["xmax"], stat["ymax"])
    return any(_boxes_meet(g, q) for g in group.pieces() for q in BBox(*bbox).pieces())


def _boxes_meet(a: BBox, b: BBox) -> bool:
    """Whether two non-wrapping boxes intersect (edges count)."""
    return not (a.xmax < b.xmin or a.xmin > b.xmax or a.ymax < b.ymin or a.ymin > b.ymax)
