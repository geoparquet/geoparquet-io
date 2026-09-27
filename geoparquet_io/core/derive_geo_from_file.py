"""Deriving a ``geo`` block from a Parquet file that was written without one.

DuckDB can write native ``GEOMETRY``/``GEOGRAPHY`` logical types and per-row-group
statistics while writing no ``geo`` key at all. This module reads those two back
off the finished file -- the logical type string, and the column statistics
pyarrow exposes -- reconstructs the ``geo`` block they imply (geometry types from
the type codes, bbox from the stats, CRS and ``edges`` from the logical type),
and rewrites the file's key-value metadata in place.

Reading a *written file* is the whole job, which is what separates it from
``core/arrow_geo_metadata.py``, whose subject is an in-memory table that has not
been written yet. Split out of ``common.py`` unchanged; every name a caller
imports is still importable from there. The type-code tables it reads are the
single copies in ``geo_metadata``.
"""

import json
import os
import re
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.crs_utils import NULL_CRS_HINT, is_default_crs
from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type, resolve_crs_reference
from geoparquet_io.core.geo_metadata import _DIMENSION_SUFFIXES, _GEOMETRY_TYPE_CODES
from geoparquet_io.core.logging_config import debug, warn


def _geo_code_to_type_name(code: int) -> str | None:
    """Map a Parquet geospatial type code (e.g. 3002) to 'LineString ZM'.

    Reads the one WKB type-code table in ``geo_metadata``. Base 0 is refused
    on purpose: the table maps it to the placeholder ``"Unknown"``, which is
    not a GeoParquet ``geometry_types`` value, and a derived block names only
    the types it can actually read off the file. An unrecognised base or
    dimension falls through to ``None`` and :func:`_geo_col_meta_from_stats`
    drops it.
    """
    dim, base = divmod(code, 1000)
    name = _GEOMETRY_TYPE_CODES.get(base)
    suffix = _DIMENSION_SUFFIXES.get(dim)
    if base == 0 or name is None or suffix is None:
        return None
    return name + suffix


def _geography_edges_from_logical(logical: str) -> str | None:
    """Edges value implied by a native Geography logical type string.

    Returns the declared algorithm (e.g. "spherical", "vincenty"), defaulting
    to "spherical" (the Parquet spec default) when none is spelled out.
    Returns None for non-Geography logical types.
    """
    if not logical.startswith("Geography"):
        return None
    match = re.search(r"algorithm=([A-Za-z_]+)", logical)
    return match.group(1).lower() if match else "spherical"


def _crs_from_geo_logical(logical: str, parquet_file: str) -> tuple[bool, Any]:
    """The CRS a native Geometry/Geography logical type declares.

    Returns ``(present, crs)``. ``present`` is False when the type names no CRS
    or names the OGC:CRS84 / EPSG:4326 default, both of which GeoParquet spells
    by *omitting* the key. Otherwise ``crs`` is the resolved PROJJSON dict, or
    ``None`` for a reference this build cannot resolve -- an explicit
    ``"crs": null`` (CRS unknown), which is the honest reading and never the
    silent CRS84 claim that omitting it would make (#785).
    """
    parsed = parse_geometry_logical_type(logical) or {}
    raw = parsed.get("crs")
    if raw is None:
        return False, None
    crs = resolve_crs_reference(parquet_file, raw)
    if isinstance(crs, dict):
        return (False, None) if is_default_crs(crs) else (True, crs)
    warn(
        f"Native geometry type declares a CRS this build cannot resolve to PROJJSON: {crs!r}. "
        "Writing an explicit null CRS (unknown) rather than claiming the OGC:CRS84 default. "
        + NULL_CRS_HINT
    )
    return True, None


def _geo_col_meta_from_stats(pf, col_index: int, logical: str, parquet_file: str) -> dict:
    """Build one geo column metadata dict from a column's native geo statistics."""
    codes: set[int] = set()
    bbox = None
    zrange = None
    for rg in range(pf.metadata.num_row_groups):
        stats = pf.metadata.row_group(rg).column(col_index).geo_statistics
        if stats is None:
            continue
        codes.update(stats.geospatial_types or [])
        if stats.xmin is not None:
            # Limitation: min/max-merging row-group extents assumes planar
            # edges; geography data crossing the antimeridian can yield an
            # over-wide (though never under-covering) bbox here.
            if bbox is None:
                bbox = [stats.xmin, stats.ymin, stats.xmax, stats.ymax]
            else:
                bbox = [
                    min(bbox[0], stats.xmin),
                    min(bbox[1], stats.ymin),
                    max(bbox[2], stats.xmax),
                    max(bbox[3], stats.ymax),
                ]
        if stats.zmin is not None:
            if zrange is None:
                zrange = [stats.zmin, stats.zmax]
            else:
                zrange = [min(zrange[0], stats.zmin), max(zrange[1], stats.zmax)]

    geometry_types = sorted(t for t in (_geo_code_to_type_name(c) for c in codes) if t is not None)
    col_meta: dict = {"encoding": "WKB", "geometry_types": geometry_types}
    if bbox is not None:
        # RFC 7946 order; 6 values when a Z range exists (M is never
        # part of bbox per spec).
        if zrange is not None:
            col_meta["bbox"] = [bbox[0], bbox[1], zrange[0], bbox[2], bbox[3], zrange[1]]
        else:
            col_meta["bbox"] = bbox
    # The rebuilt block is the file's only geo metadata, so a CRS left behind
    # here is not merely missing: an absent `crs` *means* OGC:CRS84, which
    # relabels projected coordinates as lon/lat while the Parquet logical type
    # still names the real CRS (#785). Mirror the logical type, which is the
    # authority at 2.0.
    crs_present, crs = _crs_from_geo_logical(logical, parquet_file)
    if crs_present:
        col_meta["crs"] = crs
    # A Geography logical type carries edge semantics the geo metadata must
    # not drop: synthesize the matching edges declaration (#588).
    edges = _geography_edges_from_logical(logical)
    if edges:
        col_meta["edges"] = edges
    return col_meta


def _secondary_geometry_names(pf) -> tuple[dict[str, str], dict[str, dict]]:
    """(top-level native columns by logical type string, declared geo-block entries) of a file.

    Only top-level columns count: a GEOMETRY leaf nested in a struct is not a
    column of the table, and keying it by its leaf name would mistake it for a
    top-level column that happens to share the name. A ``geo`` key that does
    not parse leaves the native half standing.
    """
    schema = pf.metadata.schema
    native: dict[str, str] = {}
    for i in range(len(schema)):
        column = schema.column(i)
        if column.path != column.name:
            continue
        logical = str(column.logical_type)
        if logical.startswith(("Geometry", "Geography")):
            native[column.name] = logical
    declared: dict[str, dict] = {}
    raw = (pf.metadata.metadata or {}).get(b"geo")
    try:
        geo_meta = json.loads(raw) if raw else None
    except ValueError:
        geo_meta = None
    columns = geo_meta.get("columns") if isinstance(geo_meta, dict) else None
    if isinstance(columns, dict):
        declared = {name: meta for name, meta in columns.items() if isinstance(meta, dict)}
    return native, declared


def _native_secondary_meta(logical: str, declared: dict | None, input_file: str) -> dict:
    """What a native secondary's own logical type says that its geo entry does not.

    Its OWN ``crs`` and ``edges`` -- never the primary's or the per-file
    witness's (#993/#1000) -- resolved only when the input's ``geo`` block
    does not already state them (a declared value wins the merge anyway, and
    resolving an unresolvable type would warn about a null that is never
    written). A column the block does not describe at all gets the spec's
    "not known" ``geometry_types: []``: every strategy writes a 1.x entry
    without that key into a file DuckDB refuses to open, and duckdb-kv
    recomputes the real list from the rows it writes.
    """
    col_meta: dict = {}
    if declared is None:
        col_meta["geometry_types"] = []
        declared = {}
    if "crs" not in declared:
        try:
            crs_present, crs = _crs_from_geo_logical(logical, input_file)
        except Exception as e:  # noqa: BLE001 - a malformed CRS is "unknown", not a failed write
            debug(f"Could not resolve the CRS of a native secondary column ({logical}): {e}")
            crs_present, crs = True, None
        if crs_present:
            col_meta["crs"] = crs
    if "edges" not in declared:
        edges = _geography_edges_from_logical(logical)
        if edges:
            col_meta["edges"] = edges
    return col_meta


def derive_secondary_geometry_info(
    input_file: str,
    primary_column: str,
    output_columns: list[str] | None = None,
    verbose: bool = False,
) -> dict | None:
    """``geometry_info`` for a rewrite whose caller supplied none, read off the input.

    Every geometry column the input carries beyond ``primary_column`` — native
    Parquet ``GEOMETRY``/``GEOGRAPHY`` logical types, plus anything the input's
    own ``geo`` block declares — becomes a secondary, so
    ``merge_secondary_geometry_metadata`` finally learns it exists (#1000). What
    a native column's type adds is described in :func:`_native_secondary_meta`.
    Derived stats are otherwise absent: for a declared column they flow through
    ``original_metadata``, where the caller's invalidation (#934) has already
    had its say, and re-reading them here would resurrect exactly the stale
    values that invalidation stripped.

    ``output_columns`` limits the answer to columns the write actually emits, so
    a projection cannot come out declaring a column it dropped. Best-effort: an
    unreadable input derives nothing rather than failing the write. The input is
    read with pyarrow, so a remote or multi-file input derives nothing either.
    """
    try:
        with pq.ParquetFile(input_file) as pf:
            native, declared = _secondary_geometry_names(pf)
    except Exception as e:  # noqa: BLE001 - a probe, never the write's failure
        debug(f"Could not derive secondary geometry columns from {input_file}: {e}")
        return None

    metadata: dict[str, dict] = {}
    for name in dict.fromkeys([*native, *declared]):
        if name == primary_column:
            continue
        if output_columns is not None and name not in output_columns:
            continue
        logical = native.get(name)
        metadata[name] = (
            _native_secondary_meta(logical, declared.get(name), input_file) if logical else {}
        )

    if not metadata:
        return None
    secondary = list(metadata)
    if verbose:
        debug(f"Derived secondary geometry columns from {input_file}: {secondary}")
    return {"primary": primary_column, "secondary": secondary, "metadata": metadata}


def _ensure_v2_geo_metadata(
    output_path: str,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    verbose: bool = False,
    primary_column: str | None = None,
) -> None:
    """Attach GeoParquet 2.0 geo metadata if the writer omitted it (#589).

    DuckDB 1.5.4's V2 writer skips the geo KV metadata for M/ZM geometries.
    Rebuild it from the file's native geospatial statistics and logical types,
    mirroring the shape DuckDB writes for XY/XYZ data, and rewrite the file in
    place. ``primary_column`` names the real geometry column for multi-geometry
    files; without it the fallback is "geometry", then alphabetical.
    """
    pf = pq.ParquetFile(output_path)
    try:
        kv = pf.metadata.metadata or {}
        if b"geo" in kv:
            return
        schema = pf.metadata.schema
        geo_cols: dict[int, tuple[str, str]] = {}
        for i in range(len(schema)):
            logical = str(schema.column(i).logical_type)
            if logical.startswith(("Geometry", "Geography")):
                geo_cols[i] = (schema.column(i).name, logical)
        if not geo_cols:
            return

        columns = {
            name: _geo_col_meta_from_stats(pf, i, logical, output_path)
            for i, (name, logical) in geo_cols.items()
        }

        if primary_column and primary_column in columns:
            primary = primary_column
        elif "geometry" in columns:
            primary = "geometry"
        else:
            primary = sorted(columns)[0]
        geo_meta = {"version": "2.0.0", "primary_column": primary, "columns": columns}
    finally:
        # Release the read handle before rewriting (Windows requires it).
        pf.close()

    _rewrite_file_with_geo_metadata(
        output_path, geo_meta, compression, compression_level, row_group_rows
    )
    if verbose:
        debug("Re-attached geo metadata (writer omitted it for M/ZM geometries)")


def _rewrite_writer_kwargs(compression: str, compression_level: int | None) -> dict:
    """ParquetWriter kwargs mirroring the codec the original writer used."""
    # Keys cover every normalized name callers can pass (DuckDB COPY names
    # from _plain_copy_to's compression_map plus pyarrow-style variants).
    codec_map = {
        "ZSTD": "zstd",
        "GZIP": "gzip",
        "BROTLI": "brotli",
        "SNAPPY": "snappy",
        "UNCOMPRESSED": "none",
        "NONE": "none",
        "LZ4": "lz4",
        "LZ4_RAW": "lz4",
    }
    write_kwargs: dict = {"compression": codec_map.get(compression.upper(), "zstd")}
    if compression_level is not None and write_kwargs["compression"] in ("zstd", "gzip", "brotli"):
        write_kwargs["compression_level"] = compression_level
    return write_kwargs


def _copy_row_groups(
    pf: pq.ParquetFile, writer: pq.ParquetWriter, row_group_rows: int | None
) -> None:
    """Copy ``pf`` into ``writer`` one bounded piece at a time.

    An explicit ``row_group_rows`` re-chunks to that size (one group per
    piece, matching what ``write_table(row_group_size=...)`` produced);
    otherwise each existing row group is copied as-is, so the file keeps its
    own layout instead of pyarrow's ~1Mi-row default collapsing the groups.
    The pieces' own schema metadata is irrelevant: the footer's key/value
    metadata (``geo`` included) comes from the schema ``writer`` was opened
    with, and ``write_table`` compares schemas without metadata.
    """
    if row_group_rows:
        for batch in pf.iter_batches(batch_size=row_group_rows):
            writer.write_table(pa.Table.from_batches([batch]), row_group_size=row_group_rows)
        return
    for rg in range(pf.metadata.num_row_groups):
        piece = pf.read_row_group(rg)
        writer.write_table(piece, row_group_size=piece.num_rows or None)


def _rewrite_file_with_geo_metadata(
    output_path: str,
    geo_meta: dict,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
) -> None:
    """Rewrite a parquet file in place with the given geo metadata attached.

    Streams row group by row group into a staged file, so memory is bounded
    by one row group — never the whole file (#1155): this runs right after
    the memory-bounded COPY of an XYM/XYZM 2.0 convert, which may be far
    larger than RAM.
    """
    # geoarrow registration makes pyarrow round-trip the native GEOMETRY/
    # GEOGRAPHY logical types (and their CRS) instead of demoting to binary.
    import geoarrow.pyarrow  # noqa: F401

    write_kwargs = _rewrite_writer_kwargs(compression, compression_level)
    tmp_path = f"{output_path}.geometa.tmp"
    try:
        # Both handles are closed (`with`) before os.replace runs — Windows
        # refuses to replace/unlink a file that is still open. pre_buffer is
        # off because pyarrow's pre-buffer cache keeps every column chunk
        # `iter_batches` has read until the file closes: with it on, the
        # explicit-row_group_rows branch (the default convert path) grew to
        # roughly the whole compressed file instead of one row group.
        with pq.ParquetFile(output_path, pre_buffer=False) as pf:
            new_meta = dict(pf.schema_arrow.metadata or {})
            new_meta[b"geo"] = json.dumps(geo_meta).encode()
            schema = pf.schema_arrow.with_metadata(new_meta)
            with pq.ParquetWriter(tmp_path, schema, **write_kwargs) as writer:
                _copy_row_groups(pf, writer, row_group_rows)
        os.replace(tmp_path, output_path)
    finally:
        # os.replace consumes the tmp file on success; clean it up on failure.
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
