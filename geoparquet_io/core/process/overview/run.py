#!/usr/bin/env python3
"""Orchestration for `gpio process overview`: build coarser aggregate levels.

Reads an existing `gpio process aggregate` output (small), rolls it up to one
or more coarser levels -- explicit via ``levels`` or auto-selected against a
tile-size budget -- and writes one GeoParquet sibling per level.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.duckdb_utils import quote_identifier, sql_path
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import resolve_file_url
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
from geoparquet_io.core.write_funnels import write_geoparquet_table


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
        target_levels = _plan_levels(
            con, relation, info, levels, max_tile_kb, bytes_per_cell, verbose
        )
        if not target_levels:
            log_info("Base level fits the tile budget at every zoom; no overview levels needed")
            return []
        outputs = [
            overview_output_path(input_parquet, info.scheme, level, output_dir)
            for level in target_levels
        ]
        _refuse_existing(outputs, force)
        return _build_levels(
            con,
            relation,
            info,
            target_levels,
            outputs,
            compression=compression,
            compression_level=compression_level,
            geoparquet_version=geoparquet_version,
            verbose=verbose,
            show_sql=show_sql,
        )


def _plan_levels(
    con, relation: str, info: AggregateInfo, levels, max_tile_kb, bytes_per_cell, verbose
) -> list[int | str]:
    """The coarser levels to build: the user's list, or the tile-budget probe."""
    if levels is not None:
        return parse_levels(levels, info)
    return _auto_levels(
        con, f"SELECT * FROM {relation}", info, max_tile_kb, bytes_per_cell, verbose
    )


def _refuse_existing(paths: list[str], force: bool) -> None:
    """Refuse to silently overwrite files the user did not name one by one
    (mirrors the --force gate on gpio pmtiles pyramid)."""
    existing = [path for path in paths if Path(path).exists()]
    if existing and not force:
        raise InvalidParameterError(
            "output",
            f"overview output already exists: {', '.join(existing)}. Use --force to overwrite.",
        )


def _build_levels(
    con,
    relation: str,
    info: AggregateInfo,
    target_levels: list[int | str],
    outputs: list[str],
    *,
    compression: str,
    compression_level: int | None,
    geoparquet_version: str | None,
    verbose: bool,
    show_sql: bool,
) -> list[tuple[int | str, str]]:
    """Roll the input up to each level and write it as a sibling file."""
    source_sql = f"SELECT * FROM {relation}"
    results: list[tuple[int | str, str]] = []
    for out_path in outputs:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    for level, out_path in zip(target_levels, outputs, strict=True):
        sql = build_level_sql(con, info, source_sql, level)
        if show_sql or verbose:
            debug(sql)
        table = con.execute(sql).arrow().read_all()
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


def create_overview_file(
    input_parquet: str,
    overview_out: str,
    *,
    levels: str | list[int | str] | None = None,
    cell_detail: float | None = None,
    explicit_gsd: str | list[float] | None = None,
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
) -> str:
    """Build the level ladder and assemble it into one overview GeoParquet.

    `create_overviews` writes a sibling file per level, which a tiler then has
    to pick zoom bands for. This emits the same ladder as a single levelled
    file that ``tylertoo export-pmtiles`` tiles in one pass, reading the levels
    as written (#1117). The sibling files are written too (under
    ``output_dir``, or beside the input) and left in place.

    Each level's ``gsd`` is its measured cell width -- the median extent of
    the level's own geometries, in metres -- divided by ``cell_detail``, or
    the value ``explicit_gsd`` names for it. The band selection `gpio pmtiles
    pyramid` performs is deliberately not the source: it discards a level
    whose coarser sibling already fits the budget (#1103), which is the very
    behaviour a pre-levelled file exists to remove. Every requested level
    reaches the file with its own GSD.

    Everything that can be checked before the rollups run is checked first:
    the knobs, the output path against the input and the ``level`` column,
    so a bad option costs no minutes of aggregation.
    """
    from geoparquet_io.core.process.overview.overview_file import (
        DEFAULT_CELL_DETAIL,
        LevelInput,
        write_overview_file,
    )

    configure_verbose(verbose)
    cell_detail = DEFAULT_CELL_DETAIL if cell_detail is None else cell_detail
    if not (math.isfinite(cell_detail) and cell_detail > 0):
        raise InvalidParameterError("cell_detail", f"must be a positive number, got {cell_detail}")
    gsds_wanted = _parse_explicit_gsd(explicit_gsd)
    _refuse_overview_out(input_parquet, overview_out, force)

    with aggregate_connection(input_parquet, verbose) as (con, relation):
        info = detect_aggregate_info(con, relation, cell_column, scheme)
        target_levels = _plan_levels(
            con, relation, info, levels, max_tile_kb, bytes_per_cell, verbose
        )
        if gsds_wanted is not None and len(gsds_wanted) != len(target_levels) + 1:
            raise InvalidParameterError(
                "gsd",
                f"expected {len(target_levels) + 1} GSD value(s) -- one per built level "
                f"({len(target_levels)}) plus the base -- got {len(gsds_wanted)}",
            )
        if gsds_wanted is None and info.out_geometry == "none":
            raise InvalidParameterError(
                "gsd",
                "this aggregate carries no geometry, so cell widths cannot be measured; "
                "pass --gsd with one value per level plus the base",
            )
        outputs = [
            overview_output_path(input_parquet, info.scheme, level, output_dir)
            for level in target_levels
        ]
        _refuse_existing(outputs, force)
        built = _build_levels(
            con,
            relation,
            info,
            target_levels,
            outputs,
            compression=compression,
            compression_level=compression_level,
            geoparquet_version=geoparquet_version,
            verbose=verbose,
            show_sql=show_sql,
        )
        # Coarse first: the built ladder, then the base, which is the input
        # itself and always the finest (OVERVIEWS_SPEC 4.2).
        paths = [*(path for _, path in built), input_parquet]
        gsds = gsds_wanted or _measured_gsds(con, paths, cell_detail)

    write_overview_file(
        [LevelInput(path=p, gsd=g) for p, g in zip(paths, gsds, strict=True)],
        overview_out,
        geoparquet_version=geoparquet_version,
        compression=compression,
        compression_level=compression_level,
        verbose=verbose,
    )
    success(f"Wrote {overview_out} with {len(paths)} level(s)")
    return overview_out


def _refuse_overview_out(input_parquet: str, overview_out: str, force: bool) -> None:
    """The levelled file must not be the input (read while written) and, like
    the siblings, is not silently replaced. Its ``level`` column must not
    shadow one the input already has (OVERVIEWS_SPEC 4.1) -- checked on the
    input before any rollup runs."""
    if Path(overview_out).exists():
        if Path(overview_out).samefile(input_parquet):
            raise InvalidParameterError(
                "overview_out", f"{overview_out} is the input; it would be destroyed while read"
            )
        _refuse_existing([overview_out], force)
    names = [n for n in pq.read_schema(input_parquet).names if n.lower() == "level"]
    if names:
        raise InvalidParameterError(
            "input",
            f"{input_parquet} already has a column named {names[0]!r}; an overview file's "
            "`level` column would shadow it in reader predicates (OVERVIEWS_SPEC 4.1). "
            "Rename the source column first.",
        )


def _measured_gsds(con, paths: list[str], cell_detail: float) -> list[float]:
    """Each level's GSD from the median width of its own cells.

    Measured, not derived from extent/count: sqrt(extent / cells) is the
    spacing between cells, which equals the cell width only when cells tile
    the extent densely. Two hundred scattered points aggregated at H3 r7 gave
    a ~20x overestimate and every level the same value. The median extent of
    the level's geometries is the cell width whatever the scheme, converted
    with the flat 111,320 m/degree OVERVIEWS_SPEC 7.1 prescribes.
    """
    from geoparquet_io.core.process.overview.overview_file import METERS_PER_DEGREE

    gsds: list[float] = []
    for path in paths:
        url = resolve_file_url(path)
        row = con.execute(
            "SELECT median(ST_XMax(geometry) - ST_XMin(geometry)), "
            "median(ST_YMax(geometry) - ST_YMin(geometry)) "
            f"FROM read_parquet({sql_path(url)}) WHERE geometry IS NOT NULL"
        ).fetchone()
        dx, dy = (float(v or 0.0) for v in (row or (0.0, 0.0)))
        width_m = math.sqrt(dx * dy) * METERS_PER_DEGREE
        if not width_m > 0:
            raise InvalidParameterError(
                "gsd", f"could not measure a cell width in {path}; pass --gsd explicitly"
            )
        gsds.append(width_m / cell_detail)
    if any(gsds[i] >= gsds[i - 1] for i in range(1, len(gsds))):
        raise InvalidParameterError(
            "gsd",
            f"measured cell widths do not shrink coarse->fine ({gsds}); pass --gsd explicitly",
        )
    return gsds


def _parse_explicit_gsd(spec: str | list[float] | None) -> list[float] | None:
    """An explicit, strictly decreasing GSD ladder, mirroring tylertoo's ``--gsd``.

    Absolute metres, so it overrides the measured sizing entirely. The count
    is checked against the ladder once the levels are known.
    """
    if spec is None:
        return None
    try:
        values = (
            [float(part) for part in spec.split(",") if part.strip()]
            if isinstance(spec, str)
            else [float(v) for v in spec]
        )
    except (TypeError, ValueError):
        raise InvalidParameterError(
            "gsd", f"expected comma-separated metres (e.g. 2000,800,300), got {spec!r}"
        ) from None
    if not values or any(not (math.isfinite(v) and v > 0) for v in values):
        raise InvalidParameterError("gsd", f"GSDs must be positive metres, got {values}")
    if any(values[i] >= values[i - 1] for i in range(1, len(values))):
        raise InvalidParameterError("gsd", f"GSDs must be strictly decreasing, got {values}")
    return values
