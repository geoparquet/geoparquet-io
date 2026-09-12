"""Deriving a ``geo`` block from a Parquet file that was written without one.

DuckDB can write native ``GEOMETRY``/``GEOGRAPHY`` logical types and per-row-group
statistics while writing no ``geo`` key at all. This module reads those two back
off the finished file -- the logical type string, and the column statistics
pyarrow exposes -- reconstructs the ``geo`` block they imply (geometry types from
the type codes, bbox from the stats, CRS and ``edges`` from the logical type),
and rewrites the file's key-value metadata in place.

Reading a *written file* is the whole job, which is what separates it from
``core/arrow_geo_metadata.py``, whose subject is an in-memory table that has not
been written yet. Split out of ``common.py`` unchanged; every name is still
importable from there.
"""

import json
import os
import re
from typing import Any

import pyarrow.parquet as pq

from geoparquet_io.core.crs_utils import NULL_CRS_HINT, is_default_crs
from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type, resolve_crs_reference
from geoparquet_io.core.logging_config import debug, warn

_GEO_TYPE_CODE_BASES = {
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}
_GEO_TYPE_CODE_SUFFIXES = {0: "", 1: " Z", 2: " M", 3: " ZM"}


def _geo_code_to_type_name(code: int) -> str | None:
    """Map a Parquet geospatial type code (e.g. 3002) to 'LineString ZM'."""
    dim, base = divmod(code, 1000)
    name = _GEO_TYPE_CODE_BASES.get(base)
    suffix = _GEO_TYPE_CODE_SUFFIXES.get(dim)
    if name is None or suffix is None:
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


def _infer_row_group_size(output_path: str) -> int | None:
    """Max rows per existing row group, so a rewrite can mirror the file's layout."""
    pf = pq.ParquetFile(output_path)
    try:
        num_groups = pf.metadata.num_row_groups
        if num_groups == 0:
            return None
        return max(pf.metadata.row_group(i).num_rows for i in range(num_groups))
    finally:
        # Release the read handle before any rewrite (Windows requires it).
        pf.close()


def _rewrite_file_with_geo_metadata(
    output_path: str,
    geo_meta: dict,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
) -> None:
    """Rewrite a parquet file in place with the given geo metadata attached."""
    # geoarrow registration makes pyarrow round-trip the native GEOMETRY/
    # GEOGRAPHY logical types (and their CRS) instead of demoting to binary.
    import geoarrow.pyarrow  # noqa: F401

    # Preserve the file's own row-group layout when the caller didn't specify
    # one — pyarrow's ~1Mi-row default would otherwise collapse the groups.
    if not row_group_rows:
        row_group_rows = _infer_row_group_size(output_path)

    table = pq.read_table(output_path)
    new_meta = dict(table.schema.metadata or {})
    new_meta[b"geo"] = json.dumps(geo_meta).encode()
    table = table.replace_schema_metadata(new_meta)

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
    if row_group_rows:
        write_kwargs["row_group_size"] = row_group_rows

    tmp_path = f"{output_path}.geometa.tmp"
    try:
        pq.write_table(table, tmp_path, **write_kwargs)
        os.replace(tmp_path, output_path)
    finally:
        # os.replace consumes the tmp file on success; clean it up on failure.
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
