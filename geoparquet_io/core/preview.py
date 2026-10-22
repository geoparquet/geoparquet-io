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

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from geoparquet_io.core.exceptions import InvalidParameterError

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
    query_bbox = _validated_bbox(bbox)

    from geoparquet_io.core.duckdb_metadata import get_row_group_sizes

    sizes = get_row_group_sizes(parquet_file)
    totals = {
        "total_row_groups": len(sizes),
        "total_rows": sum(size["num_rows"] for size in sizes),
        "total_bytes": sum(size["compressed_bytes"] for size in sizes),
    }

    stats, source = _row_group_spatial_stats(parquet_file, geometry_column)
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


def _validated_bbox(bbox: Sequence[float]) -> tuple[float, float, float, float]:
    """``bbox`` as four floats, or the InvalidParameterError saying what it must be."""
    try:
        xmin, ymin, xmax, ymax = (float(value) for value in bbox)
    except (TypeError, ValueError) as exc:
        raise InvalidParameterError(
            "bbox", "must be four numbers: (xmin, ymin, xmax, ymax)"
        ) from exc
    if not all(math.isfinite(value) for value in (xmin, ymin, xmax, ymax)):
        # NaN compares False against everything, so letting it through would
        # silently "match" every row group instead of estimating anything.
        raise InvalidParameterError("bbox", f"must be finite numbers: {bbox!r}")
    if xmin > xmax or ymin > ymax:
        raise InvalidParameterError(
            "bbox", f"xmin > xmax or ymin > ymax: ({xmin}, {ymin}, {xmax}, {ymax})"
        )
    return (xmin, ymin, xmax, ymax)


def _row_group_spatial_stats(
    parquet_file: str, geometry_column: str | None
) -> tuple[list[dict], Literal["native", "bbox_struct"] | None]:
    """The file's per-row-group bboxes and where they came from, or ``([], None)``.

    Native Parquet geo statistics first, then a bbox covering column's chunk
    statistics -- the same order and owners as ``ops.get_row_group_geo_stats``.
    An explicit ``geometry_column`` is honored on both rungs: a name the file
    does not have raises instead of reading as "the file has no statistics",
    and the covering fallback only uses a covering that column itself declares
    -- never the primary column's (#1171: a secondary's read must not be
    pruned on the primary's extents).
    """
    stats = _native_stats(parquet_file, geometry_column)
    if stats:
        return stats, "native"

    from geoparquet_io.core.duckdb_metadata import get_per_row_group_bbox_stats

    bbox_column = _covering_bbox_column(parquet_file, geometry_column)
    if bbox_column:
        stats = get_per_row_group_bbox_stats(parquet_file, bbox_column)
        if stats:
            return stats, "bbox_struct"
    return [], None


def _native_stats(parquet_file: str, geometry_column: str | None) -> list[dict]:
    """Per-row-group native geo statistics; a typo'd explicit column raises."""
    from geoparquet_io.core.duckdb_metadata import (
        get_native_geo_stats_by_row_group,
        get_per_row_group_native_geo_stats,
    )

    if geometry_column is None:
        return get_per_row_group_native_geo_stats(parquet_file)

    chunks = get_native_geo_stats_by_row_group(parquet_file, geometry_column)
    if chunks is None:
        raise InvalidParameterError(
            "geometry_column", f"the file has no column named {geometry_column!r}"
        )
    return [
        {key: chunk[key] for key in ("row_group_id", "xmin", "ymin", "xmax", "ymax")}
        for chunk in chunks
        if chunk["xmin"] is not None
    ]


def _covering_bbox_column(parquet_file: str, geometry_column: str | None) -> str | None:
    """The bbox covering column the preview may prune on, or ``None``.

    With no explicit column (or the primary named explicitly) that is the
    primary's covering, conventional-name fallback included. Any other column
    may only be pruned on a covering its own ``geo`` entry declares.
    """
    from geoparquet_io.core.bbox_structure import bbox_covering_column_for, check_bbox_structure

    if geometry_column is None or geometry_column == _primary_geometry_column(parquet_file):
        return check_bbox_structure(parquet_file).get("bbox_column_name")

    from geoparquet_io.core.duckdb_metadata import get_geo_metadata

    return bbox_covering_column_for(get_geo_metadata(parquet_file), geometry_column)


def _primary_geometry_column(parquet_file: str) -> str | None:
    from geoparquet_io.core.duckdb_metadata import find_primary_geometry_column_duckdb

    try:
        return find_primary_geometry_column_duckdb(parquet_file)
    except Exception:
        return None


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
    """Whether a row group's bbox statistics intersect the query bbox (edges count)."""
    xmin, ymin, xmax, ymax = bbox
    if stat["ymax"] < ymin or stat["ymin"] > ymax:
        return False
    if stat["xmin"] > stat["xmax"]:
        # The Parquet geospatial statistics permit xmin > xmax for a group
        # wrapping the antimeridian (see aggregate_native_geo_stats): the X
        # range is the two lobes [xmin, 180] and [-180, xmax], not an empty
        # interval. Pruning it as empty would drop rows the real read returns.
        return bool(stat["xmin"] <= xmax or stat["xmax"] >= xmin)
    return not (stat["xmax"] < xmin or stat["xmin"] > xmax)
