"""Simplify GeoParquet geometries with coarsen (``gpio process simplify``).

The geometry op runs in coarsen (Rust, byte-identical to GEOS — ADR-0007)
over shapely arrays; everything around it stays on gpio rails: the carried
``geo`` block is stripped of stats the simplification invalidates so the
write funnel recomputes them, a declared bbox covering column is recomputed
from the simplified geometries, and the write goes through
:func:`~geoparquet_io.core.write_funnels.write_geoparquet_table`.

Docs: docs/guide/process-simplify.md
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.geo_metadata import sanitized_carried_geo
from geoparquet_io.core.logging_config import debug, warn
from geoparquet_io.core.optional_deps import load_module, require_coarsen
from geoparquet_io.core.write_funnels import write_geoparquet_table

#: Above this row count, coverage mode's whole-column materialization gets a
#: memory warning (plain mode processes chunk by chunk).
_COVERAGE_WARN_ROWS = 1_000_000


def strip_stale_geometry_stats(geo_meta: dict) -> dict:
    """Drop per-column ``geometry_types`` and ``bbox`` from a ``geo`` dict.

    A carried block's stats win over recomputation in the write funnel, so a
    transform that changes geometry must remove them or the output lies.
    Everything else (``crs``, ``edges``, ``covering``, ...) is kept. Mutates
    and returns ``geo_meta``; tolerates malformed shapes.
    """
    columns = geo_meta.get("columns")
    if isinstance(columns, dict):
        for col_meta in columns.values():
            if isinstance(col_meta, dict):
                col_meta.pop("geometry_types", None)
                col_meta.pop("bbox", None)
    return geo_meta


def _geometry_column_of(table: pa.Table, override: str | None) -> str:
    """Resolve the geometry column: override, carried primary, or 'geometry'."""
    name = override
    if name is None:
        geo = sanitized_carried_geo(table.schema.metadata)
        name = geo.get("primary_column") or "geometry"
    if name not in table.column_names:
        raise InvalidParameterError(
            "geometry_column", f"geometry column '{name}' not found in table"
        )
    col_type = table.schema.field(name).type
    if col_type not in (pa.binary(), pa.large_binary()):
        raise InvalidParameterError(
            "geometry_column",
            f"column '{name}' is {col_type}, not a WKB binary column",
        )
    return name


def _simplify_values(
    values: list,
    tolerance: float,
    *,
    coverage: bool,
    preserve_topology: bool,
    simplify_boundary: bool,
    threads: int | None,
) -> tuple[list, int]:
    """Simplify a list of WKB values (``None`` passes through).

    Returns the new WKB list and the count of geometries the operation
    collapsed to empty.
    """
    coarsen = require_coarsen()
    shapely = load_module("shapely")

    np = load_module("numpy")

    idx = [i for i, v in enumerate(values) if v is not None]
    if not idx:
        return list(values), 0
    geoms = shapely.from_wkb(np.array([values[i] for i in idx], dtype=object))
    if coverage:
        out = coarsen.coverage_simplify(
            geoms, tolerance, simplify_boundary=simplify_boundary, threads=threads
        )
    else:
        out = coarsen.simplify(geoms, tolerance, preserve_topology, threads=threads)
    collapsed = int(np.sum(shapely.is_empty(out))) - int(np.sum(shapely.is_empty(geoms)))
    result: list = [None] * len(values)
    for i, wkb in zip(idx, shapely.to_wkb(out), strict=True):
        result[i] = wkb
    return result, max(collapsed, 0)


def _covering_bbox_column(geo_meta: dict, geom_col: str) -> str | None:
    """The bbox struct column the carried covering declares for ``geom_col``."""
    covering = geo_meta.get("columns", {}).get(geom_col, {}).get("covering")
    bbox_refs = covering.get("bbox") if isinstance(covering, dict) else None
    if not isinstance(bbox_refs, dict) or "xmin" not in bbox_refs:
        return None
    ref = bbox_refs["xmin"]
    return ref[0] if isinstance(ref, list) and ref else None


def _refresh_bbox_covering(table: pa.Table, geom_col: str) -> pa.Table:
    """Recompute a declared bbox covering column from ``geom_col``'s data."""
    shapely = load_module("shapely")

    np = load_module("numpy")
    geo = sanitized_carried_geo(table.schema.metadata)
    bbox_col = _covering_bbox_column(geo, geom_col)
    if bbox_col is None or bbox_col not in table.column_names:
        return table
    wkb = np.array(table.column(geom_col).to_pylist(), dtype=object)
    bounds = shapely.bounds(shapely.from_wkb(wkb))  # (n, 4); NaN rows for nulls
    null_mask = np.isnan(bounds[:, 0])
    bounds = np.nan_to_num(bounds)  # children under a null parent still need values
    struct_type = table.schema.field(bbox_col).type
    bound_idx = {"xmin": 0, "ymin": 1, "xmax": 2, "ymax": 3}
    children = []
    for field in struct_type:
        values = bounds[:, bound_idx[field.name]]
        if pa.types.is_float32(field.type):
            # Rounding to nearest float32 can shrink the box past the geometry
            # and make a spatial filter skip the row; round outward instead.
            cast = values.astype(np.float32)
            if field.name.endswith("min"):
                cast = np.where(
                    cast.astype(np.float64) > values,
                    np.nextafter(cast, np.float32(-np.inf)),
                    cast,
                )
            else:
                cast = np.where(
                    cast.astype(np.float64) < values,
                    np.nextafter(cast, np.float32(np.inf)),
                    cast,
                )
            values = cast
        children.append(pa.array(values, type=field.type))
    struct = pa.StructArray.from_arrays(
        children,
        fields=list(struct_type),
        mask=pa.array(null_mask) if null_mask.any() else None,
    )
    debug(f"Recomputed bbox covering column '{bbox_col}' from simplified geometry")
    return table.set_column(
        table.column_names.index(bbox_col), table.schema.field(bbox_col), struct
    )


def _with_stripped_geo(table: pa.Table) -> pa.Table:
    """Strip stale per-column stats from the table's carried ``geo`` block."""
    metadata = dict(table.schema.metadata or {})
    geo = sanitized_carried_geo(metadata)
    if not geo:
        return table
    strip_stale_geometry_stats(geo)
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    return table.replace_schema_metadata(metadata)


def simplify_table(
    table: pa.Table,
    tolerance: float,
    *,
    coverage: bool = False,
    preserve_topology: bool = True,
    simplify_boundary: bool = True,
    threads: int | None = None,
    geometry_column: str | None = None,
    verbose: bool = False,
) -> pa.Table:
    """Simplify a table's geometry column; metadata stats are refreshed.

    Plain mode runs coarsen chunk by chunk; coverage mode hands the whole
    column to ``coverage_simplify`` in one call, which is what preserves
    shared edges across the coverage.
    """
    if tolerance < 0:
        raise InvalidParameterError("tolerance", "must be >= 0")
    geom_col = _geometry_column_of(table, geometry_column)
    column = table.column(geom_col)

    def run(values: list) -> tuple[list, int]:
        return _simplify_values(
            values,
            tolerance,
            coverage=coverage,
            preserve_topology=preserve_topology,
            simplify_boundary=simplify_boundary,
            threads=threads,
        )

    collapsed = 0
    if coverage:
        if table.num_rows > _COVERAGE_WARN_ROWS:
            warn(
                f"coverage simplification holds all {table.num_rows:,} geometries "
                "in memory at once to preserve shared edges"
            )
        new_values, collapsed = run(column.to_pylist())
        chunks = [pa.array(new_values, type=column.type)]
    else:
        chunks = []
        for chunk in column.chunks:
            values, n = run(chunk.to_pylist())
            collapsed += n
            chunks.append(pa.array(values, type=column.type))
    if collapsed:
        warn(f"{collapsed} geometries collapsed to empty at tolerance {tolerance}")
    result = table.set_column(
        table.column_names.index(geom_col),
        table.schema.field(geom_col),
        pa.chunked_array(chunks, type=column.type),
    )
    result = _with_stripped_geo(result)
    return _refresh_bbox_covering(result, geom_col)


def simplify_file(
    input_parquet: str,
    output_parquet: str,
    tolerance: float,
    *,
    coverage: bool = False,
    preserve_topology: bool = True,
    simplify_boundary: bool = True,
    threads: int | None = None,
    geometry_column: str | None = None,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
) -> None:
    """Simplify a GeoParquet file's geometries and write the result."""
    table = pq.read_table(input_parquet)
    result = simplify_table(
        table,
        tolerance,
        coverage=coverage,
        preserve_topology=preserve_topology,
        simplify_boundary=simplify_boundary,
        threads=threads,
        geometry_column=geometry_column,
        verbose=verbose,
    )
    write_geoparquet_table(
        result,
        output_parquet,
        geometry_column=geometry_column,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
    )
