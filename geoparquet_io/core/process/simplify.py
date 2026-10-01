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

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.geo_metadata import sanitized_carried_geo
from geoparquet_io.core.logging_config import debug, info, warn
from geoparquet_io.core.optional_deps import load_module, require_coarsen
from geoparquet_io.core.parquet_writer import (
    resolve_output_geoparquet_version,
    resolve_row_group_rows,
)
from geoparquet_io.core.write_funnels import write_geoparquet_table

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


def _is_wkb_extension(col_type) -> bool:
    """A geoarrow.wkb extension column — what a native GeoParquet 2.0 file's
    geometry reads back as when geoarrow is registered (#1198). Its storage
    is plain WKB bytes."""
    return (
        isinstance(col_type, pa.ExtensionType)
        and getattr(col_type, "extension_name", "") == "geoarrow.wkb"
        and col_type.storage_type in (pa.binary(), pa.large_binary())
    )


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
    if col_type not in (pa.binary(), pa.large_binary()) and not _is_wkb_extension(col_type):
        raise InvalidParameterError(
            "geometry_column",
            f"column '{name}' is {col_type}, not a WKB binary column",
        )
    return name


def _with_wkb_storage(table: pa.Table, geom_col: str) -> pa.Table:
    """Unwrap a geoarrow.wkb extension column to its plain-WKB storage.

    The simplification works on raw WKB; the output's native 2.0 shape is
    the write funnel's job (auto version resolution preserves the carried
    2.0 declaration and converts the column back on write).
    """
    column = table.column(geom_col)
    if not _is_wkb_extension(column.type):
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
    values: list,
    tolerance: float,
    *,
    coverage: bool,
    preserve_topology: bool,
    simplify_boundary: bool,
    threads: int | None,
    forward=None,
    inverse=None,
) -> tuple[list, int, list]:
    """Simplify a list of WKB values (``None`` passes through).

    Returns the new WKB list, the count of geometries the operation
    collapsed to empty, and a per-row mask that is True where the output
    geometry is empty (nulls are never marked).
    """
    coarsen = require_coarsen()
    shapely = load_module("shapely")

    np = load_module("numpy")

    idx = [i for i, v in enumerate(values) if v is not None]
    if not idx:
        return list(values), 0, [False] * len(values)
    try:
        geoms = shapely.from_wkb(np.array([values[i] for i in idx], dtype=object))
    except Exception as e:
        raise GeoParquetError(f"could not parse a WKB geometry in the input: {e}") from e
    if forward is not None:
        geoms = shapely.transform(geoms, forward)
    if coverage:
        out = coarsen.coverage_simplify(
            geoms, tolerance, simplify_boundary=simplify_boundary, threads=threads
        )
    else:
        out = coarsen.simplify(geoms, tolerance, preserve_topology, threads=threads)
    if inverse is not None:
        out = shapely.transform(out, inverse)
    out_empty = shapely.is_empty(out)
    collapsed = int(np.sum(out_empty)) - int(np.sum(shapely.is_empty(geoms)))
    result: list = [None] * len(values)
    empty_mask = [False] * len(values)
    for i, wkb, is_empty in zip(idx, shapely.to_wkb(out), out_empty, strict=True):
        result[i] = wkb
        empty_mask[i] = bool(is_empty)
    return result, max(collapsed, 0), empty_mask


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


def _with_stripped_geo(table: pa.Table, geometry_column: str) -> pa.Table:
    """Strip the simplified column's stale stats from the carried ``geo`` block."""
    metadata = dict(table.schema.metadata or {})
    geo = sanitized_carried_geo(metadata)
    if not geo:
        return table
    strip_stale_geometry_stats(geo, geometry_column)
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
        sample = _first_wkb(v for chunk in column.chunks for v in chunk.to_pylist())
        forward, inverse = _simplify_crs_transformers(simplify_crs, src_crs, sample)

    def run(values: list) -> tuple[list, int, list]:
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

    collapsed = 0
    empty_flags: list[bool] = []
    if coverage:
        if table.num_rows > _COVERAGE_WARN_ROWS:
            warn(
                f"coverage simplification holds all {table.num_rows:,} geometries "
                "in memory at once to preserve shared edges"
            )
        new_values, collapsed, empty_flags = run(column.to_pylist())
        chunks = [pa.array(new_values, type=column.type)]
    else:
        chunks = []
        for chunk in column.chunks:
            values, n, flags = run(chunk.to_pylist())
            collapsed += n
            empty_flags.extend(flags)
            chunks.append(pa.array(values, type=column.type))
    if collapsed:
        warn(f"{collapsed} geometries collapsed to empty at tolerance {tolerance}")
    result = table.set_column(
        table.column_names.index(geom_col),
        table.schema.field(geom_col),
        pa.chunked_array(chunks, type=column.type),
    )
    if drop_empty and any(empty_flags):
        result = result.filter(pa.array([not flag for flag in empty_flags]))
        info(f"dropped {sum(empty_flags)} empty geometries")
    result = _with_stripped_geo(result, geom_col)
    return _refresh_bbox_covering(result, geom_col)


#: Output versions the streaming writer can produce faithfully: plain WKB
#: columns plus a ``geo`` footer key. 2.0/native outputs convert the column
#: to Parquet's geometry logical type, which only the in-memory funnel does.
_STREAMABLE_VERSIONS = (None, "1.0", "1.1")

#: Geometry-type names by shapely type id, spelled as the GeoParquet spec
#: (and the funnel's geoarrow-based computation) spells them.
_TYPE_NAMES = {
    0: "Point",
    1: "LineString",
    2: "LineString",  # LinearRing reads back as a LineString
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}


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


def _accumulate_stats(shapely, np, wkb_values: list, types: set, bbox: list) -> None:
    """Fold one batch's simplified geometries into the running stats."""
    non_null = [v for v in wkb_values if v is not None]
    if not non_null:
        return
    geoms = shapely.from_wkb(np.array(non_null, dtype=object))
    has_z = shapely.has_z(geoms)
    for type_id, z in zip(shapely.get_type_id(geoms), has_z, strict=True):
        name = _TYPE_NAMES.get(int(type_id))
        if name:
            types.add(f"{name} Z" if z else name)
    bounds = shapely.bounds(geoms)
    finite = np.isfinite(bounds[:, 0])
    if finite.any():
        bounds = bounds[finite]
        bbox[0] = min(bbox[0], float(bounds[:, 0].min()))
        bbox[1] = min(bbox[1], float(bounds[:, 1].min()))
        bbox[2] = max(bbox[2], float(bounds[:, 2].max()))
        bbox[3] = max(bbox[3], float(bounds[:, 3].max()))


def _footer_kv_via_funnel(
    schema: pa.Schema,
    geom_col: str,
    types: set,
    bbox: list,
    geoparquet_version: str | None,
    compression: str,
    compression_level: int | None,
) -> dict[str, str]:
    """The streamed file's footer metadata, produced by the real funnel.

    A zero-row table carrying the accumulated ``geometry_types``/``bbox`` as
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
    if types:
        col_meta["geometry_types"] = sorted(types)
        col_meta["bbox"] = bbox
    else:
        # Zero valid geometries: leave stats to the funnel's own empty-table
        # behavior, same as the in-memory path.
        col_meta.pop("geometry_types", None)
        col_meta.pop("bbox", None)
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    empty = schema.with_metadata(metadata).empty_table()
    fd, tmp_path = tempfile.mkstemp(suffix=".parquet", prefix="gpio_simplify_footer_")
    os.close(fd)
    try:
        write_geoparquet_table(
            empty,
            tmp_path,
            geometry_column=geom_col,
            compression=compression,
            compression_level=compression_level,
            geoparquet_version=geoparquet_version,
        )
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
    compression: str,
    compression_level: int | None,
    row_group_rows: int | None,
    geoparquet_version: str | None,
    drop_empty: bool,
    simplify_crs: str | None,
    verbose: bool,
    drop_columns: list[str] | None = None,
) -> None:
    """Stream-simplify batch by batch: memory is bounded by one row group."""
    from geoparquet_io.core.derive_geo_from_file import _rewrite_writer_kwargs
    from geoparquet_io.core.remote import remote_write_context, upload_if_remote

    shapely = load_module("shapely")
    np = load_module("numpy")
    schema = pf.schema_arrow
    if drop_columns:
        schema = pa.schema(
            [f for f in schema if f.name not in drop_columns], metadata=schema.metadata
        )
    rows = resolve_row_group_rows(row_group_rows, None)
    writer_schema = schema.with_metadata(None)
    types: set = set()
    bbox = [float("inf"), float("inf"), float("-inf"), float("-inf")]
    collapsed = 0
    dropped = 0
    from itertools import chain

    batches = pf.iter_batches(batch_size=rows)
    forward = inverse = None
    if simplify_crs:
        first = next(batches, None)
        if first is not None:
            batches = chain([first], batches)
        src_crs = (
            sanitized_carried_geo(schema.metadata).get("columns", {}).get(geom_col, {}).get("crs")
        )
        sample = _first_wkb(iter(first.column(geom_col).to_pylist())) if first is not None else None
        forward, inverse = _simplify_crs_transformers(simplify_crs, src_crs, sample)
    with remote_write_context(output_parquet, is_directory=False, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        writer_kwargs = _rewrite_writer_kwargs(compression, compression_level)
        # store_schema=False: the geo footer arrives via add_key_value_metadata
        # at close (stats are only known then), and an embedded ARROW:schema
        # written at open would shadow it on read. DuckDB-written GeoParquet
        # carries no ARROW:schema either, so readers already live without it.
        with pq.ParquetWriter(
            actual_output, writer_schema, store_schema=False, **writer_kwargs
        ) as writer:
            for batch in batches:
                table = pa.Table.from_batches([batch])
                if drop_columns:
                    table = table.drop_columns(drop_columns)
                table = table.replace_schema_metadata(schema.metadata)
                values, n, empty_flags = _simplify_values(
                    table.column(geom_col).to_pylist(),
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
                if drop_empty and any(empty_flags):
                    keep = [not flag for flag in empty_flags]
                    table = table.filter(pa.array(keep))
                    dropped += sum(empty_flags)
                    values = [v for v, k in zip(values, keep, strict=True) if k]
                table = _refresh_bbox_covering(table, geom_col)
                _accumulate_stats(shapely, np, values, types, bbox)
                writer.write_table(table, row_group_size=rows)
            writer.add_key_value_metadata(
                _footer_kv_via_funnel(
                    schema,
                    geom_col,
                    types,
                    bbox,
                    geoparquet_version,
                    compression,
                    compression_level,
                )
            )
        if is_remote:
            upload_if_remote(actual_output, output_parquet, is_directory=False, verbose=verbose)
    if collapsed:
        warn(f"{collapsed} geometries collapsed to empty at tolerance {tolerance}")
    if dropped:
        info(f"dropped {dropped} empty geometries")
    debug(f"streamed simplify in {rows}-row batches to {output_parquet}")


def _simplify_file_in_memory(
    input_parquet: str,
    output_parquet: str,
    tolerance: float,
    drop_columns: list[str] | None = None,
    **kwargs,
) -> None:
    table = pq.read_table(input_parquet)
    if drop_columns:
        table = table.drop_columns(drop_columns)
    write_args = {
        k: kwargs[k]
        for k in (
            "geometry_column",
            "compression",
            "compression_level",
            "row_group_size_mb",
            "row_group_rows",
            "geoparquet_version",
            "verbose",
        )
    }
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
    write_geoparquet_table(result, output_parquet, **write_args)


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
    compression: str = "ZSTD",
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
    in-memory path is also taken for native/2.0 outputs and byte-sized row
    group targets, which only the funnel resolves.
    """
    if "://" not in input_parquet and not os.path.exists(input_parquet):
        raise FileNotFoundGeoParquetError(input_parquet)
    if refresh_metrics:
        _simplify_then_refresh_metrics(
            input_parquet,
            output_parquet,
            tolerance,
            coverage=coverage,
            preserve_topology=preserve_topology,
            simplify_boundary=simplify_boundary,
            threads=threads,
            geometry_column=geometry_column,
            drop_empty=drop_empty,
            simplify_crs=simplify_crs,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            geoparquet_version=geoparquet_version,
            verbose=verbose,
        )
        return
    kwargs = {
        "coverage": coverage,
        "preserve_topology": preserve_topology,
        "simplify_boundary": simplify_boundary,
        "threads": threads,
        "geometry_column": geometry_column,
        "drop_empty": drop_empty,
        "simplify_crs": simplify_crs,
        "compression": compression,
        "compression_level": compression_level,
        "row_group_size_mb": row_group_size_mb,
        "row_group_rows": row_group_rows,
        "geoparquet_version": geoparquet_version,
        "verbose": verbose,
    }
    if coverage or row_group_size_mb is not None:
        _simplify_file_in_memory(
            input_parquet, output_parquet, tolerance, drop_columns=_drop_columns, **kwargs
        )
        return
    with pq.ParquetFile(input_parquet, pre_buffer=False) as pf:
        resolved = resolve_output_geoparquet_version(
            geoparquet_version,
            input_file=input_parquet,
            original_metadata=pf.schema_arrow.metadata,
            verbose=verbose,
        )
        geom_col = _geometry_column_of(pf.schema_arrow.empty_table(), geometry_column)
        if not _can_stream(pf, geom_col, resolved):
            pf.close()
            _simplify_file_in_memory(
                input_parquet, output_parquet, tolerance, drop_columns=_drop_columns, **kwargs
            )
            return
        _simplify_file_streaming(
            pf,
            output_parquet,
            tolerance,
            preserve_topology=preserve_topology,
            threads=threads,
            geom_col=geom_col,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            geoparquet_version=geoparquet_version,
            drop_empty=drop_empty,
            simplify_crs=simplify_crs,
            verbose=verbose,
            drop_columns=_drop_columns,
        )


def _simplify_then_refresh_metrics(
    input_parquet: str,
    output_parquet: str,
    tolerance: float,
    *,
    geoparquet_version: str | None,
    verbose: bool,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    **simplify_kwargs,
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
        simplify_file(
            input_parquet,
            output_parquet,
            tolerance,
            geoparquet_version=geoparquet_version,
            verbose=verbose,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            **simplify_kwargs,
        )
        return
    tmp_path = f"{output_parquet}.refresh-{os.getpid()}.tmp.parquet"
    try:
        simplify_file(
            input_parquet,
            tmp_path,
            tolerance,
            geoparquet_version=geoparquet_version,
            verbose=verbose,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            _drop_columns=present,
            **simplify_kwargs,
        )
        add_geometry_metrics(
            tmp_path,
            output_parquet,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            geoparquet_version=geoparquet_version,
            overwrite=True,
            verbose=verbose,
        )
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
