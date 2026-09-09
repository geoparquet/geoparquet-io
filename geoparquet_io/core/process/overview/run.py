#!/usr/bin/env python3
"""Orchestration for `gpio process overview`: build coarser aggregate levels.

Reads an existing `gpio process aggregate` output (small), rolls it up to one
or more coarser levels -- explicit via ``levels`` or auto-selected against a
tile-size budget -- and writes one GeoParquet sibling per level.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.logging_config import info as log_info
from geoparquet_io.core.partition.auto_resolution import _register_quadkey_udf
from geoparquet_io.core.process.aggregate.common import (
    antimeridian_aware_bbox,
    geometry_to_geom_expr,
)
from geoparquet_io.core.process.overview.detect import (
    AggregateInfo,
    aggregate_connection,
    detect_aggregate_info,
)
from geoparquet_io.core.process.overview.levels import (
    DEFAULT_MAX_TILE_KB,
    MAX_PROBE_ZOOM,
    Band,
    estimate_bytes_per_cell,
    probe_worst_tile_counts,
    select_bands,
)
from geoparquet_io.core.process.overview.rollup import (
    GRID_PARENT_TEMPLATES,
    GRID_SCHEMES,
    admin_parent_expr,
    build_level_sql,
    validate_level,
)


def overview_output_path(
    input_parquet: str, scheme: str, level: int | str, output_dir: str | None = None
) -> str:
    """Sibling path for one overview level.

    Grid: ``cells.parquet`` -> ``cells_r7.parquet``. Admin: ``by_region.parquet``
    -> ``by_region_country.parquet``.
    """
    path = Path(input_parquet)
    directory = Path(output_dir) if output_dir else path.parent
    suffix = path.suffix or ".parquet"
    if scheme == "admin":
        return str(directory / f"{path.stem}_{level}{suffix}")
    return str(directory / f"{path.stem}_r{level}{suffix}")


def parse_levels(levels: str | Sequence[int | str], info: AggregateInfo) -> list[int | str]:
    """Normalize an explicit ``levels`` parameter (comma string or list)."""
    if isinstance(levels, str):
        raw: list = [part.strip() for part in levels.split(",") if part.strip()]
    else:
        raw = list(levels)
    if not raw:
        raise InvalidParameterError("levels", "no levels given")
    parsed = [validate_level(info, level) for level in raw]
    if len(set(parsed)) != len(parsed):
        raise InvalidParameterError("levels", f"duplicate levels in {parsed}")
    if info.scheme == "admin":
        return parsed
    return sorted(parsed)


def _grid_cells_probe_sql(info: AggregateInfo, source_sql: str, level: int) -> str:
    """One lon/lat row per distinct level-``level`` parent cell of a grid input."""
    qcol = quote_identifier(info.cell_column)
    if level == info.base_level:
        parent = qcol
    else:
        parent = GRID_PARENT_TEMPLATES[info.scheme].format(cell=qcol, level=level)
    # a5_cell_to_lonlat returns [lon, lat]; h3_cell_to_latlng returns [lat, lng].
    if info.scheme == "a5":
        lonlat = "a5_cell_to_lonlat(__parent)"
        lon, lat = "__ll[1]", "__ll[2]"
    else:
        lonlat = "h3_cell_to_latlng(__parent)"
        lon, lat = "__ll[2]", "__ll[1]"
    return (
        f"SELECT {lon} AS lon, {lat} AS lat FROM ("
        f"SELECT {lonlat} AS __ll FROM ("
        f"SELECT DISTINCT {parent} AS __parent FROM ({source_sql}) "
        f"WHERE {qcol} IS NOT NULL))"
    )


def _admin_cells_probe_sql(con, info: AggregateInfo, source_sql: str, level: str) -> str:
    """One lon/lat row per admin bucket at ``level`` ('region' base or 'country')."""
    if info.out_geometry == "none":
        raise InvalidParameterError(
            "levels",
            "cannot auto-select admin overview zoom bands for an aggregate "
            "without geometry; pass explicit levels",
        )
    qcol = quote_identifier(info.cell_column)
    geom_expr = geometry_to_geom_expr(con, f"({source_sql})", "geometry")
    centroids = (
        f"SELECT {qcol}, ST_X(__c) AS lon, ST_Y(__c) AS lat FROM ("
        f"SELECT {qcol}, ST_Centroid({geom_expr}) AS __c FROM ({source_sql}) "
        f"WHERE geometry IS NOT NULL AND {qcol} != 'unassigned')"
    )
    if level == "region":
        return f"SELECT lon, lat FROM ({centroids})"
    # Circular mean of longitudes: antimeridian-spanning countries (US, RU,
    # FJ, NZ) must probe near +/-180, not at the naive AVG(lon) near lon 0.
    lon_expr = "degrees(atan2(AVG(sin(radians(lon))), AVG(cos(radians(lon)))))"
    return (
        f"SELECT {lon_expr} AS lon, AVG(lat) AS lat FROM ({centroids}) "
        f"GROUP BY {admin_parent_expr(info.cell_column)}"
    )


def plan_bands(
    con,
    source_sql: str,
    info: AggregateInfo,
    levels: list[int | str] | None = None,
    max_tile_kb: int = DEFAULT_MAX_TILE_KB,
    bytes_per_cell: float | None = None,
    verbose: bool = False,
    max_probe_zoom: int | None = None,
) -> list[Band]:
    """Probe worst-tile cell counts and select zoom bands for the pyramid.

    ``levels`` (already validated, coarse-to-fine, excluding the base) restricts
    the candidate set; the base level is always the final candidate.
    ``max_probe_zoom`` caps the probed zoom range (e.g. to an archive's max
    zoom) so band transitions never land beyond it.
    """
    if info.scheme == "admin":
        candidates: list[int | str] = ["country", "region"]
    elif levels is not None:
        candidates = [*levels, info.base_level]
    else:
        scheme = GRID_SCHEMES[info.scheme]
        candidates = [*range(scheme.min_resolution, int(info.base_level)), info.base_level]

    bpc = bytes_per_cell or estimate_bytes_per_cell(info.num_attributes, info.out_geometry)
    probe_zoom = MAX_PROBE_ZOOM if max_probe_zoom is None else max(0, max_probe_zoom)
    _register_quadkey_udf(con)
    worst: dict[int | str, dict[int, int]] = {}
    for level in candidates:
        if info.scheme == "admin":
            cells_sql = _admin_cells_probe_sql(con, info, source_sql, str(level))
        else:
            cells_sql = _grid_cells_probe_sql(info, source_sql, int(level))
        worst[level] = probe_worst_tile_counts(con, cells_sql, max_zoom=probe_zoom)
    bands = select_bands(worst, candidates, bpc, max_tile_kb)
    if verbose:
        debug(f"Estimated {bpc:.0f} bytes/cell; selected bands: {bands}")
    return bands


def _auto_levels(
    con,
    source_sql: str,
    info: AggregateInfo,
    max_tile_kb: int,
    bytes_per_cell: float | None,
    verbose: bool,
) -> list[int | str]:
    """Auto-select the overview levels to build (excludes the base level).

    Admin and grid schemes share the probe-based selection, so both return
    ``[]`` when the base level already fits the budget. A geometry-less admin
    aggregate cannot be probed; it falls back to the only coarser level.
    """
    if info.scheme == "admin" and info.out_geometry == "none":
        return ["country"]
    bands = plan_bands(
        con,
        source_sql,
        info,
        max_tile_kb=max_tile_kb,
        bytes_per_cell=bytes_per_cell,
        verbose=verbose,
    )
    return [band.level for band in bands if band.level != info.base_level]


def _write_overview(
    table: pa.Table,
    out_path: str,
    info: AggregateInfo,
    compression: str,
    compression_level: int | None,
    gpq_version: str | None,
    verbose: bool,
    geo_bbox: list[float] | None = None,
) -> None:
    if info.out_geometry == "none":
        kwargs: dict[str, Any] = {"compression": compression}
        if compression_level is not None:
            kwargs["compression_level"] = compression_level
        pq.write_table(table, out_path, **kwargs)
        return
    write_geoparquet_table(
        table,
        out_path,
        geometry_column="geometry",
        compression=compression,
        compression_level=compression_level,
        geoparquet_version=gpq_version,
        verbose=verbose,
        geo_bbox=geo_bbox,
    )


def _overview_geo_bbox(con, table: pa.Table, info: AggregateInfo) -> list[float] | None:
    """RFC 7946 bbox for one rolled-up level, wrap form included.

    A grid rollup regenerates parent geometry through the same seam-repairing
    builder the aggregate uses, so a parent cell can be a MultiPolygon cut at
    the antimeridian and needs the same bbox treatment (see
    ``antimeridian_aware_bbox``). Best-effort: a bbox is metadata, so a failure
    leaves it to the writer rather than losing the level.
    """
    if info.out_geometry == "none":
        return None
    con.register("__overview_result", table)
    try:
        return antimeridian_aware_bbox(con, "__overview_result", "geometry")
    except duckdb.Error as exc:  # pragma: no cover - defensive
        debug(f"Could not compute an antimeridian-aware bbox: {exc}")
        return None
    finally:
        con.unregister("__overview_result")


def create_overviews(
    input_parquet: str,
    *,
    levels: str | list[int | str] | None = None,
    max_tile_kb: int = DEFAULT_MAX_TILE_KB,
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
    """Build coarser overview levels from an aggregate GeoParquet file.

    Args:
        input_parquet: Path to a `gpio process aggregate` output.
        levels: Explicit levels to build (comma string or list). Grid schemes
            take resolutions coarser than the input's base; admin takes
            ``country``. Default: auto-select against ``max_tile_kb``.
        max_tile_kb: Tile-size budget (KB) driving auto level selection.
        bytes_per_cell: Override the estimated compressed bytes per cell.
        cell_column: Cell id column when auto-detection fails.
        scheme: Bucketing scheme (a5/h3/admin) when inference is ambiguous,
            e.g. H3 ids stored as integers.
        output_dir: Directory for overview files (default: beside the input).
        compression: Parquet compression codec (default ZSTD).
        compression_level: Optional compression level.
        geoparquet_version: GeoParquet spec version to write.
        force: Overwrite existing overview output files.
        verbose: Enable verbose debug logging.
        show_sql: Log the rollup SQL.

    Returns:
        List of ``(level, output_path)`` for every overview written,
        coarse to fine.
    """
    configure_verbose(verbose)
    with aggregate_connection(input_parquet, verbose) as (con, relation):
        info = detect_aggregate_info(con, relation, cell_column, scheme)
        source_sql = f"SELECT * FROM {relation}"

        if levels is not None:
            target_levels = parse_levels(levels, info)
        else:
            target_levels = _auto_levels(
                con, source_sql, info, max_tile_kb, bytes_per_cell, verbose
            )
        if not target_levels:
            log_info("Base level fits the tile budget at every zoom; no overview levels needed")
            return []

        # Refuse to silently overwrite derived sibling files the user never
        # named (mirrors the --force gate on gpio pmtiles pyramid).
        existing = [
            path
            for path in (
                overview_output_path(input_parquet, info.scheme, level, output_dir)
                for level in target_levels
            )
            if Path(path).exists()
        ]
        if existing and not force:
            raise InvalidParameterError(
                "output",
                f"overview output already exists: {', '.join(existing)}. Use --force to overwrite.",
            )

        results: list[tuple[int | str, str]] = []
        for level in target_levels:
            sql = build_level_sql(con, info, source_sql, level)
            if show_sql or verbose:
                debug(sql)
            table = con.execute(sql).arrow().read_all()
            out_path = overview_output_path(input_parquet, info.scheme, level, output_dir)
            _write_overview(
                table,
                out_path,
                info,
                compression,
                compression_level,
                geoparquet_version,
                verbose,
                geo_bbox=_overview_geo_bbox(con, table, info),
            )
            success(f"Wrote level {level} overview ({table.num_rows} rows) -> {out_path}")
            results.append((level, out_path))
        return results
