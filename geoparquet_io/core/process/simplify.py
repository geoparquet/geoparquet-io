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
import os
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from geoparquet_io.core.bbox_structure import bbox_covering_column_for
from geoparquet_io.core.compression import resolve_codec, resolve_process_compression_level
from geoparquet_io.core.crs_utils import is_geographic_crs
from geoparquet_io.core.edges import set_edges
from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.geo_metadata import (
    GeometryStats,
    geometry_stats_of,
    merged_geometry_stats,
    sanitized_carried_geo,
)
from geoparquet_io.core.geoarrow_encoding import is_wkb_extension_field
from geoparquet_io.core.geometry_detection import primary_geometry_column, require_geometry_column
from geoparquet_io.core.geometry_repair import warn_planar_math, warn_planar_math_for_file
from geoparquet_io.core.logging_config import debug, info, warn
from geoparquet_io.core.optional_deps import load_module, require_coarsen
from geoparquet_io.core.parquet_writer import (
    WriteOptions,
    regroup,
    resolve_output_geoparquet_version,
    resolve_row_group_rows,
    sized_stream,
    without_geometry_dictionary,
)
from geoparquet_io.core.remote import is_remote_url
from geoparquet_io.core.write_funnels import table_nonplanar_edges, write_geoparquet_table

#: Above this row count, coverage mode's whole-column materialization gets a
#: memory warning (plain mode processes chunk by chunk).
_COVERAGE_WARN_ROWS = 1_000_000


def strip_stale_geometry_stats(geo_meta: dict, geometry_column: str) -> dict:
    """Drop ``geometry_types`` and ``bbox`` from ONE column of a ``geo`` dict.

    A carried block's stats win over recomputation in the write funnel, so a
    transform that changes geometry must remove them or the output lies.
    Only the transformed column is stripped: another geometry column's stats
    are still true, and the funnel carries (never recomputes) columns other
    than the one being written. Everything else (``crs``, ``edges``,
    ``covering``, ...) is kept. Mutates and returns ``geo_meta``; tolerates
    malformed shapes.
    """
    columns = geo_meta.get("columns")
    if isinstance(columns, dict):
        col_meta = columns.get(geometry_column)
        if isinstance(col_meta, dict):
            col_meta.pop("geometry_types", None)
            col_meta.pop("bbox", None)
    return geo_meta


def _utm_epsg(lon: float, lat: float) -> int:
    """EPSG code of the UTM zone containing (lon, lat), hemisphere-aware."""
    zone = min(60, max(1, int((lon + 180.0) // 6) + 1))
    return (32600 if lat >= 0 else 32700) + zone


def _coord_func(transformer):
    """A shapely.transform callback over an (n, 2) coordinate array."""

    def apply(coords):
        np = load_module("numpy")
        x, y = transformer.transform(coords[:, 0], coords[:, 1])
        return np.column_stack([x, y])

    return apply


def _simplify_crs_transformers(
    simplify_crs: str,
    src_crs_json: dict | None,
    sample_wkb: bytes | None,
):
    """(forward, inverse) shapely-transform callbacks for ``simplify_crs``.

    ``auto-utm`` derives the UTM zone from a sample geometry's centroid —
    the right answer for per-zone partitioned global data. The source CRS
    comes from the carried geo block, defaulting to OGC:CRS84 like the rest
    of gpio.
    """
    from pyproj import CRS, Transformer
    from pyproj.exceptions import CRSError

    shapely = load_module("shapely")
    try:
        src = CRS.from_json_dict(src_crs_json) if src_crs_json else CRS.from_user_input("OGC:CRS84")
    except CRSError as e:
        raise InvalidParameterError("simplify_crs", f"input CRS unusable: {e}") from e
    if simplify_crs.lower() in ("auto-utm", "utm"):
        if not src.is_geographic:
            raise InvalidParameterError(
                "simplify_crs",
                "'auto-utm' needs geographic (lon/lat) input; give an explicit CRS",
            )
        if sample_wkb is None:
            return None, None  # nothing to transform
        centroid = shapely.from_wkb(sample_wkb).centroid
        target = CRS.from_epsg(_utm_epsg(centroid.x, centroid.y))
    else:
        try:
            target = CRS.from_user_input(simplify_crs)
        except CRSError as e:
            raise InvalidParameterError("simplify_crs", str(e)) from e
    forward = Transformer.from_crs(src, target, always_xy=True)
    inverse = Transformer.from_crs(target, src, always_xy=True)
    return _coord_func(forward), _coord_func(inverse)


def _first_wkb(values_iter) -> bytes | None:
    return next((v for v in values_iter if v is not None), None)


def _first_wkb_of(column) -> bytes | None:
    """The first non-null WKB value in an Arrow array or chunked array.

    Scans chunk by chunk and locates the first valid row with an index kernel
    over the validity bitmap, so a column whose first batches are all null
    costs a bitmap read — not the materialized Python list per chunk that the
    generator over ``to_pylist()`` cost (lazy or not, ``to_pylist`` builds the
    whole list before the generator sees its first element), and not the
    filtered copy of a chunk's non-null bytes that ``drop_null()`` builds just
    to read one element.
    """
    chunks = column.chunks if isinstance(column, pa.ChunkedArray) else [column]
    for chunk in chunks:
        if not len(chunk):
            continue
        i = pc.index(pc.is_valid(chunk), True).as_py()
        if i >= 0:
            value: bytes = chunk[i].as_py()
            return value
    return None


def _geometry_column_of(table: pa.Table, override: str | None) -> str:
    """Resolve the geometry column: the override, else the table's primary."""
    if override is not None:
        name = override
    else:
        name = require_geometry_column(primary_geometry_column(table), "the table")
    if name not in table.column_names:
        raise InvalidParameterError(
            "geometry_column", f"geometry column '{name}' not found in table"
        )
    field = table.schema.field(name)
    if field.type not in (pa.binary(), pa.large_binary()) and not is_wkb_extension_field(field):
        raise InvalidParameterError(
            "geometry_column",
            f"column '{name}' is {field.type}, not a WKB binary column",
        )
    return name


def _with_wkb_storage(table: pa.Table, geom_col: str) -> pa.Table:
    """Unwrap a WKB extension column (#1198) to its plain-WKB storage.

    ``is_wkb_extension_field`` is the single definition of "WKB bytes"
    (geoarrow.wkb and ogc.wkb alike); only the resolved-extension carrier
    needs unwrapping — the metadata-only carrier is already plain binary.
    The output's native 2.0 shape is the write funnel's job (auto version
    resolution preserves the carried 2.0 declaration and converts the
    column back on write).
    """
    column = table.column(geom_col)
    if not isinstance(column.type, pa.ExtensionType) or not is_wkb_extension_field(
        table.schema.field(geom_col)
    ):
        return table
    storage = pa.chunked_array(
        [chunk.storage for chunk in column.chunks], type=column.type.storage_type
    )
    return table.set_column(
        table.column_names.index(geom_col),
        pa.field(geom_col, column.type.storage_type, nullable=True),
        storage,
    )


def _simplify_values(
    column,
    tolerance: float,
    *,
    coverage: bool,
    preserve_topology: bool,
    simplify_boundary: bool,
    threads: int | None,
    forward=None,
    inverse=None,
) -> tuple[Any, int, Any, Any, Any]:
    """Simplify one Arrow WKB array or chunked array (nulls pass through).

    Returns the new WKB values as an object array, the count of geometries the
    operation collapsed to empty, a per-row boolean mask that is True where the
    output geometry is empty (nulls are never marked), the simplified shapely
    geometry array for the non-null rows, and those rows' positions — so
    callers fold stats and bounds without re-parsing the output WKB.

    A chunked column is taken as-is: ``to_numpy`` and ``pc.is_valid`` fold it
    chunk by chunk, so coverage mode never concatenates at the Arrow level —
    on a ``binary`` column past 2 GiB of WKB that concat overflows the int32
    offsets (ArrowInvalid), and below it it holds a second full copy of the
    column (#1285 fixed the same hazard in ``hilbert_order``).

    Everything per-row here is a numpy or Arrow kernel, never a Python loop.
    These costs are flat per row, so on a file of many small geometries they,
    not the Rust kernel, set the pace: in a 1M-building profile
    ``coarsen._core`` was 7% of the time and the loops this replaced were most
    of the rest (#1300). ``column`` is an Arrow (chunked) array rather than a
    list for the same reason — the bytes objects still have to exist for
    shapely, but the per-row Scalar boxing and the second list that
    ``to_pylist`` plus a comprehension built are gone.
    """
    coarsen = require_coarsen()
    shapely = load_module("shapely")

    np = load_module("numpy")

    n_rows = len(column)
    # pc.is_valid is the null mask, read from Arrow's validity bitmap rather
    # than by testing a million values for None.
    valid = pc.is_valid(column).to_numpy(zero_copy_only=False)
    idx = np.flatnonzero(valid)
    wkb_all = column.to_numpy(zero_copy_only=False)
    if idx.size == 0:
        return (
            wkb_all,
            0,
            np.zeros(n_rows, dtype=bool),
            np.empty(0, dtype=object),
            idx,
        )
    try:
        geoms = shapely.from_wkb(wkb_all[idx])
    except Exception as e:
        raise GeoParquetError(f"could not parse a WKB geometry in the input: {e}") from e
    if forward is not None:
        geoms = shapely.transform(geoms, forward)
    out_wkb = None
    if coverage:
        out, out_wkb = _coverage_out(
            coarsen,
            shapely,
            np,
            geoms,
            wkb_all[idx],
            tolerance,
            simplify_boundary=simplify_boundary,
            threads=threads,
            transformed=forward is not None or inverse is not None,
        )
    else:
        out = coarsen.simplify(geoms, tolerance, preserve_topology, threads=threads)
    if inverse is not None:
        out = shapely.transform(out, inverse)
    out_empty = shapely.is_empty(out)
    collapsed = int(np.sum(out_empty)) - int(np.sum(shapely.is_empty(geoms)))
    # Scatter by index instead of walking the rows: nulls keep their None and
    # everything else takes its new WKB in one assignment.
    result = np.full(n_rows, None, dtype=object)
    result[idx] = shapely.to_wkb(out) if out_wkb is None else out_wkb
    empty_mask = np.zeros(n_rows, dtype=bool)
    empty_mask[idx] = out_empty
    return result, max(collapsed, 0), empty_mask, out, idx


def _coverage_out(
    coarsen,
    shapely,
    np,
    geoms,
    wkb_in,
    tolerance: float,
    *,
    simplify_boundary: bool,
    threads: int | None,
    transformed: bool,
) -> tuple[Any, Any]:
    """Coverage-simplify ``geoms``, via coarsen's WKB kernel when it applies.

    ``coarsen.simplify_wkb`` is the coverage kernel ``coverage_simplify``
    wraps, taking and returning WKB directly: handing it the column's own
    bytes skips the wrapper's ``to_wkb`` pass over the inputs, and its output
    lands in the result column without a second serialization (#1223). It is
    undocumented API and skippable by design: when a ``--simplify-crs``
    transform rewrote the geometries (``transformed`` — the column's WKB is
    no longer the kernel's input) or the installed coarsen does not export
    the symbol, the wrapped kernel runs exactly as before.

    Returns ``(out_geoms, out_wkb)``; ``out_wkb`` is None on the wrapped
    path, where the caller serializes ``out_geoms`` itself. The non-polygonal
    pre-check reproduces the wrapper's own refusal — same TypeError, same
    message — because the kernel's is a differently-typed, differently-worded
    ``ValueError`` (pinned in TestCoverageWkbShortcut).
    """
    simplify_wkb = getattr(coarsen, "simplify_wkb", None)
    if transformed or simplify_wkb is None:
        out = coarsen.coverage_simplify(
            geoms, tolerance, simplify_boundary=simplify_boundary, threads=threads
        )
        return out, None
    if not np.isin(shapely.get_type_id(geoms), (3, 6)).all():
        raise TypeError("One of the Geometry inputs is of incorrect geometry type.")
    out_wkb = np.array(
        simplify_wkb(list(wkb_in), tolerance, simplify_boundary=simplify_boundary, threads=threads),
        dtype=object,
    )
    return shapely.from_wkb(out_wkb), out_wkb


def _row_bounds(shapely, np, n_rows: int, geoms: Any, idx: Any) -> Any:
    """Per-row ``(n, 4)`` bounds of the simplified geometries; NaN rows for nulls."""
    bounds = np.full((n_rows, 4), np.nan)
    if len(idx):
        bounds[idx] = shapely.bounds(geoms)
    return bounds


def _refresh_bbox_covering(table: pa.Table, geom_col: str, bounds=None) -> pa.Table:
    """Recompute a declared bbox covering column from ``geom_col``'s data.

    ``bounds`` is the per-row ``(n, 4)`` array the simplify step already
    computed (:func:`_row_bounds`); when omitted it is re-derived from the
    column's WKB. The covering lookup is the owner module's
    (:func:`bbox_covering_column_for`): a partial or dangling covering is
    not a declaration and must not be rewritten.
    """
    shapely = load_module("shapely")

    np = load_module("numpy")
    geo = sanitized_carried_geo(table.schema.metadata)
    bbox_col = bbox_covering_column_for(geo, geom_col)
    if bbox_col is None or bbox_col not in table.column_names:
        return table
    if bounds is None:
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


def _with_stripped_geo(table: pa.Table, geometry_column: str) -> pa.Table:
    """Strip the simplified column's stale stats from the carried ``geo`` block."""
    metadata = dict(table.schema.metadata or {})
    geo = sanitized_carried_geo(metadata)
    if not geo:
        return table
    strip_stale_geometry_stats(geo, geometry_column)
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    return table.replace_schema_metadata(metadata)


def _with_carried_edges(table: pa.Table, geometry_column: str, edges: str | None) -> pa.Table:
    """Keep the edges the input declared, in its type or its block (#1269).

    Unwrapping the column to plain WKB drops its edge type, so the edges move
    to the carried ``geo`` block, where a 1.x input already keeps them. The
    write funnel then restores them on the output. ``edges`` is the answer of
    ``table_nonplanar_edges``, which already let a stated block win (#1288).
    """
    if not edges:
        return table
    metadata = dict(table.schema.metadata or {})
    geo = sanitized_carried_geo(metadata) or {
        "version": "1.1.0",
        "primary_column": geometry_column,
        "columns": {},
    }
    columns = geo.setdefault("columns", {})
    set_edges(columns.setdefault(geometry_column, {"encoding": "WKB"}), edges)
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
    drop_empty: bool = False,
    simplify_crs: str | None = None,
    verbose: bool = False,
) -> pa.Table:
    """Simplify a table's geometry column; metadata stats are refreshed.

    ``simplify_crs`` projects to that CRS for the simplification (tolerance
    in its units — meters for UTM) and projects the result back (#1197);
    ``"auto-utm"`` picks the UTM zone from the data.

    ``drop_empty=True`` drops rows whose geometry is empty after
    simplification (#1199); nulls are kept either way — null is "unknown",
    empty is "nothing", and only the latter is a simplification artifact.

    Plain mode runs coarsen chunk by chunk; coverage mode hands the whole
    column to ``coverage_simplify`` in one call, which is what preserves
    shared edges across the coverage.
    """
    if tolerance < 0:
        raise InvalidParameterError("tolerance", "must be >= 0")
    geom_col = _geometry_column_of(table, geometry_column)
    warn_planar_math("simplify", table, geom_col)
    declared_edges = table_nonplanar_edges(table, geom_col, None).get(geom_col)
    table = _with_wkb_storage(table, geom_col)
    column = table.column(geom_col)
    forward = inverse = None
    if simplify_crs:
        src_crs = (
            sanitized_carried_geo(table.schema.metadata)
            .get("columns", {})
            .get(geom_col, {})
            .get("crs")
        )
        sample = _first_wkb_of(column)
        forward, inverse = _simplify_crs_transformers(simplify_crs, src_crs, sample)

    def run(values) -> tuple[Any, int, Any, Any, Any]:
        return _simplify_values(
            values,
            tolerance,
            coverage=coverage,
            preserve_topology=preserve_topology,
            simplify_boundary=simplify_boundary,
            threads=threads,
            forward=forward,
            inverse=inverse,
        )

    shapely = load_module("shapely")
    np = load_module("numpy")
    collapsed = 0
    flag_parts: list = []
    bounds_parts: list = []
    if coverage:
        if table.num_rows > _COVERAGE_WARN_ROWS:
            warn(
                f"coverage simplification holds all {table.num_rows:,} geometries "
                "in memory at once to preserve shared edges"
            )
        new_values, collapsed, flags, geoms, idx = run(column)
        chunks = [pa.array(new_values, type=column.type)]
        flag_parts.append(flags)
        bounds_parts.append(_row_bounds(shapely, np, len(flags), geoms, idx))
    else:
        chunks = []
        for chunk in column.chunks:
            values, n, flags, geoms, idx = run(chunk)
            collapsed += n
            flag_parts.append(flags)
            chunks.append(pa.array(values, type=column.type))
            bounds_parts.append(_row_bounds(shapely, np, len(flags), geoms, idx))
    empty_flags = np.concatenate(flag_parts) if flag_parts else np.zeros(0, dtype=bool)
    if collapsed:
        warn(f"{collapsed} geometries collapsed to empty at tolerance {tolerance}")
    result = table.set_column(
        table.column_names.index(geom_col),
        table.schema.field(geom_col),
        pa.chunked_array(chunks, type=column.type),
    )
    row_bounds = np.concatenate(bounds_parts) if bounds_parts else np.empty((0, 4))
    if drop_empty and empty_flags.any():
        keep = ~empty_flags
        result = result.filter(pa.array(keep))
        row_bounds = row_bounds[keep]
        info(f"dropped {int(empty_flags.sum())} empty geometries")
    result = _with_carried_edges(_with_stripped_geo(result, geom_col), geom_col, declared_edges)
    return _refresh_bbox_covering(result, geom_col, bounds=row_bounds)


#: Output versions the streaming writer can produce faithfully: plain WKB
#: columns plus a ``geo`` footer key. 2.0/native outputs convert the column
#: to Parquet's geometry logical type, which only the in-memory funnel does.
_STREAMABLE_VERSIONS = ("1.0", "1.1")


def _can_stream(pf: pq.ParquetFile, geom_col: str, resolved_version: str | None) -> bool:
    """Whether the bounded-memory writer reproduces the funnel's output.

    Plain-mode simplification is per-geometry, so the only questions are
    about the *write*: the output version must be a plain-WKB one, and the
    funnel's large-type normalization must have nothing to do.
    """
    if resolved_version not in _STREAMABLE_VERSIONS:
        return False
    for field in pf.schema_arrow:
        if pa.types.is_large_binary(field.type) or pa.types.is_large_string(field.type):
            return False
    return bool(pa.types.is_binary(pf.schema_arrow.field(geom_col).type))


def _footer_kv_via_funnel(
    schema: pa.Schema,
    geom_col: str,
    stats: GeometryStats,
    write: WriteOptions,
) -> dict[str, str]:
    """The streamed file's footer metadata, produced by the real funnel.

    A zero-row table carrying the merged ``geometry_types``/``bbox`` as
    its geo block goes through :func:`write_geoparquet_table` into a scratch
    file; the footer that comes back is exactly what the in-memory path
    would have written (carried stats win over recomputation, so the empty
    table contributes nothing). One assembly of the geo block, one owner.
    """
    import tempfile

    metadata = dict(schema.metadata or {})
    geo = sanitized_carried_geo(metadata)
    if not geo:
        geo = {"version": "1.1.0", "primary_column": geom_col, "columns": {}}
    columns = geo.setdefault("columns", {})
    col_meta = columns.setdefault(geom_col, {"encoding": "WKB"})
    if stats.geometry_types:
        col_meta["geometry_types"] = stats.geometry_types
    else:
        # Zero valid geometries: leave stats to the funnel's own empty-table
        # behavior, same as the in-memory path.
        col_meta.pop("geometry_types", None)
    if stats.bbox is not None:
        col_meta["bbox"] = stats.bbox
    else:
        # Nothing measurable (every geometry NULL or EMPTY): omit the key, as
        # the bounds owner does for the in-memory path.
        col_meta.pop("bbox", None)
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    empty = schema.with_metadata(metadata).empty_table()
    fd, tmp_path = tempfile.mkstemp(suffix=".parquet", prefix="gpio_simplify_footer_")
    os.close(fd)
    try:
        write_geoparquet_table(empty, tmp_path, geometry_column=geom_col, **write.funnel_kwargs())
        footer = pq.ParquetFile(tmp_path).schema_arrow.metadata or {}
    finally:
        os.remove(tmp_path)
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in footer.items()}


def _simplify_file_streaming(
    pf: pq.ParquetFile,
    output_parquet: str,
    tolerance: float,
    *,
    preserve_topology: bool,
    threads: int | None,
    geom_col: str,
    write: WriteOptions,
    drop_empty: bool,
    simplify_crs: str | None,
    verbose: bool,
    drop_columns: list[str] | None = None,
) -> None:
    """Stream-simplify batch by batch: memory is bounded by one row group."""
    from geoparquet_io.core.remote import remote_write_context, upload_if_remote

    shapely = load_module("shapely")
    np = load_module("numpy")
    schema = pf.schema_arrow
    if drop_columns:
        schema = pa.schema(
            [f for f in schema if f.name not in drop_columns], metadata=schema.metadata
        )
    # Batches are read at the facade's row count (an explicit one, else the
    # default). The written groups follow the facade's answer for the whole
    # pair: a byte target is measured on the leading simplified rows by
    # parquet_writer.sized_stream, the facade's one buffering loop.
    read_rows = resolve_row_group_rows(write.row_group_rows, None)
    writer_schema = schema.with_metadata(None)
    batch_stats: list[GeometryStats] = []
    # The output keeps the input's CRS, which decides how the batch extents fold.
    geographic = is_geographic_crs(
        sanitized_carried_geo(schema.metadata).get("columns", {}).get(geom_col, {}).get("crs")
    )
    collapsed = 0
    dropped = 0
    forward = inverse = None
    if simplify_crs:
        src_crs = (
            sanitized_carried_geo(schema.metadata).get("columns", {}).get(geom_col, {}).get("crs")
        )
        # Scan the geometry column alone (bounded memory) until a sample turns
        # up: the first batch can be all nulls, and auto-utm falling back to
        # "nothing to transform" would silently apply a metre tolerance in
        # degrees to the rest of the file. No sample at all means every
        # geometry is null, so there genuinely is nothing to transform —
        # matching the in-memory path's whole-column scan.
        sample = _first_wkb(
            _first_wkb_of(sample_batch.column(geom_col))
            for sample_batch in pf.iter_batches(batch_size=read_rows, columns=[geom_col])
        )
        forward, inverse = _simplify_crs_transformers(simplify_crs, src_crs, sample)
    batches = pf.iter_batches(batch_size=read_rows)

    def simplified():
        """The input's batches, simplified, in order (counts and stats as they go)."""
        nonlocal collapsed, dropped
        for batch in batches:
            table = pa.Table.from_batches([batch])
            if drop_columns:
                table = table.drop_columns(drop_columns)
            table = table.replace_schema_metadata(schema.metadata)
            values, n, empty_flags, geoms, idx = _simplify_values(
                table.column(geom_col),
                tolerance,
                coverage=False,
                preserve_topology=preserve_topology,
                simplify_boundary=True,
                threads=threads,
                forward=forward,
                inverse=inverse,
            )
            collapsed += n
            table = table.set_column(
                table.column_names.index(geom_col),
                table.schema.field(geom_col),
                pa.chunked_array([pa.array(values, type=pa.binary())]),
            )
            row_bounds = _row_bounds(shapely, np, len(empty_flags), geoms, idx)
            if drop_empty and empty_flags.any():
                keep = ~empty_flags
                table = table.filter(pa.array(keep))
                dropped += int(empty_flags.sum())
                row_bounds = row_bounds[keep]
            table = _refresh_bbox_covering(table, geom_col, bounds=row_bounds)
            batch_stats.append(geometry_stats_of(table, geom_col))
            yield table

    with remote_write_context(output_parquet, is_directory=False, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        writer_kwargs = resolve_codec(
            write.compression, write.compression_level, context="write"
        ).pyarrow_kwargs()
        # A local write streams into a pid-scoped sibling and is published by
        # os.replace only once the footer is complete: pyarrow's writer
        # finalizes a readable file even when the loop raises, and a mid-run
        # failure must not leave a truncated, geo-less file at the user's
        # destination. The remote branch already uploads only on success.
        write_target = (
            actual_output if is_remote else f"{actual_output}.stream-{os.getpid()}.tmp.parquet"
        )
        try:
            # store_schema=False: the geo footer arrives via add_key_value_metadata
            # at close (stats are only known then), and an embedded ARROW:schema
            # written at open would shadow it on read. DuckDB-written GeoParquet
            # carries no ARROW:schema either, so readers already live without it.
            with pq.ParquetWriter(
                write_target,
                writer_schema,
                store_schema=False,
                **without_geometry_dictionary(writer_kwargs, schema, [geom_col]),
            ) as writer:
                for group in regroup(
                    *sized_stream(
                        simplified(), write.row_group_rows, write.row_group_size_mb, schema
                    )
                ):
                    writer.write_table(group, row_group_size=group.num_rows)
                writer.add_key_value_metadata(
                    _footer_kv_via_funnel(
                        schema,
                        geom_col,
                        merged_geometry_stats(batch_stats, geographic=geographic),
                        write,
                    )
                )
            if not is_remote:
                os.replace(write_target, actual_output)
        finally:
            if not is_remote and os.path.exists(write_target):
                os.remove(write_target)
        if is_remote:
            upload_if_remote(actual_output, output_parquet, is_directory=False, verbose=verbose)
    if collapsed:
        warn(f"{collapsed} geometries collapsed to empty at tolerance {tolerance}")
    if dropped:
        info(f"dropped {dropped} empty geometries")
    debug(f"streamed simplify in {read_rows}-row batches to {output_parquet}")


def _simplify_file_in_memory(
    input_parquet: str,
    output_parquet: str,
    tolerance: float,
    write: WriteOptions,
    drop_columns: list[str] | None = None,
    **kwargs,
) -> None:
    table = pq.read_table(input_parquet)
    if drop_columns:
        table = table.drop_columns(drop_columns)
    result = simplify_table(
        table,
        tolerance,
        coverage=kwargs["coverage"],
        preserve_topology=kwargs["preserve_topology"],
        simplify_boundary=kwargs["simplify_boundary"],
        threads=kwargs["threads"],
        geometry_column=kwargs["geometry_column"],
        drop_empty=kwargs["drop_empty"],
        simplify_crs=kwargs["simplify_crs"],
        verbose=kwargs["verbose"],
    )
    write_geoparquet_table(
        result,
        output_parquet,
        geometry_column=kwargs["geometry_column"],
        verbose=kwargs["verbose"],
        **write.funnel_kwargs(),
    )


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
    drop_empty: bool = False,
    simplify_crs: str | None = None,
    refresh_metrics: bool = False,
    compression: str | None = None,
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    _drop_columns: list[str] | None = None,
) -> None:
    """Simplify a GeoParquet file's geometries and write the result.

    ``refresh_metrics`` recomputes the vecorel ``metrics:area`` /
    ``metrics:perimeter`` columns from the simplified geometry (#1200) by
    routing the output through the ``add geometry-metrics`` core — one
    owner for the geodesic math. It refreshes, never adds: inputs without
    the columns get a warning and a plain simplify.

    Plain mode streams batch by batch, so peak memory is bounded by one row
    group regardless of file size (a planet-scale file simplifies on a
    laptop). Coverage mode has to see every geometry at once to preserve
    shared edges — splitting a coverage pass measurably introduces gaps and
    overlaps along the seam — so it reads the whole table; partition first
    (``gpio partition``) for coverage simplification at global scale. The
    in-memory path is also taken for native/2.0 outputs. A byte-sized row
    group target streams too: the facade's resolver measures it on the
    input's first row group.
    """
    if not is_remote_url(input_parquet) and not os.path.exists(input_parquet):
        raise FileNotFoundGeoParquetError(input_parquet)
    # Resolved once, here, so every branch below -- streamed, in-memory, or
    # routed through add geometry-metrics -- writes at the same level. The
    # streaming writer would otherwise inherit pyarrow's default of 1 and the
    # funnel the global 15, from one unset argument (#1299).
    compression_level = resolve_process_compression_level(compression, compression_level)
    write = WriteOptions.pick(locals())
    options = {
        "coverage": coverage,
        "preserve_topology": preserve_topology,
        "simplify_boundary": simplify_boundary,
        "threads": threads,
        "geometry_column": geometry_column,
        "drop_empty": drop_empty,
        "simplify_crs": simplify_crs,
        "verbose": verbose,
    }
    if refresh_metrics:
        _simplify_then_refresh_metrics(input_parquet, output_parquet, tolerance, write, options)
        return
    _simplify_file(input_parquet, output_parquet, tolerance, write, options, _drop_columns)


def _simplify_file(
    input_parquet: str,
    output_parquet: str,
    tolerance: float,
    write: WriteOptions,
    kwargs: dict,
    _drop_columns: list[str] | None = None,
) -> None:
    """simplify_file's body, below the level resolution and the refresh dispatch."""
    coverage, geometry_column = kwargs["coverage"], kwargs["geometry_column"]
    preserve_topology, threads = kwargs["preserve_topology"], kwargs["threads"]
    drop_empty, simplify_crs, verbose = (
        kwargs["drop_empty"],
        kwargs["simplify_crs"],
        kwargs["verbose"],
    )
    if coverage:
        _simplify_file_in_memory(
            input_parquet, output_parquet, tolerance, write, drop_columns=_drop_columns, **kwargs
        )
        return
    with pq.ParquetFile(input_parquet, pre_buffer=False) as pf:
        resolved = resolve_output_geoparquet_version(
            write.geoparquet_version,
            input_file=input_parquet,
            original_metadata=pf.schema_arrow.metadata,
            verbose=verbose,
        )
        geom_col = _geometry_column_of(pf.schema_arrow.empty_table(), geometry_column)
        if not _can_stream(pf, geom_col, resolved):
            pf.close()
            _simplify_file_in_memory(
                input_parquet,
                output_parquet,
                tolerance,
                write,
                drop_columns=_drop_columns,
                **kwargs,
            )
            return
        # The in-memory branches run simplify_table, which warns itself.
        warn_planar_math_for_file("simplify", input_parquet, geom_col)
        _simplify_file_streaming(
            pf,
            output_parquet,
            tolerance,
            preserve_topology=preserve_topology,
            threads=threads,
            geom_col=geom_col,
            write=write,
            drop_empty=drop_empty,
            simplify_crs=simplify_crs,
            verbose=verbose,
            drop_columns=_drop_columns,
        )


def _simplify_then_refresh_metrics(
    input_parquet: str,
    output_parquet: str,
    tolerance: float,
    write: WriteOptions,
    options: dict,
) -> None:
    """Simplify, then recompute vecorel metric columns from the result.

    The stale columns are dropped during the simplify pass (streamed or
    in-memory) into a pid-scoped temp sibling of the output — never a
    cwd-relative path, which collides across concurrent jobs — and the
    ``add geometry-metrics`` core computes them fresh while writing the
    final file. Inherits that core's WGS84 geodesic assumption.
    """
    from geoparquet_io.core.add.geometry_metrics import (
        AREA_COLUMN,
        PERIMETER_COLUMN,
        add_geometry_metrics,
    )

    with pq.ParquetFile(input_parquet, pre_buffer=False) as pf:
        present = [c for c in (AREA_COLUMN, PERIMETER_COLUMN) if c in pf.schema_arrow.names]
    if not present:
        warn(
            "--refresh-metrics: input has no metrics:area/metrics:perimeter "
            "columns; nothing to refresh (use 'gpio add geometry-metrics' to add them)"
        )
        _simplify_file(input_parquet, output_parquet, tolerance, write, options)
        return
    tmp_path = f"{output_parquet}.refresh-{os.getpid()}.tmp.parquet"
    try:
        _simplify_file(input_parquet, tmp_path, tolerance, write, options, present)
        add_geometry_metrics(
            tmp_path,
            output_parquet,
            overwrite=True,
            verbose=options["verbose"],
            **write.funnel_kwargs(),
        )
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
