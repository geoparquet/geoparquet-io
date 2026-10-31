"""Deriving a ``geo`` block from a Parquet file that was written without one.

DuckDB can write native ``GEOMETRY``/``GEOGRAPHY`` logical types and per-row-group
statistics while writing no ``geo`` key at all. This module reads those two back
off the finished file -- the logical type string, and the column statistics
pyarrow exposes -- reconstructs the ``geo`` block they imply (geometry types from
the type codes, bbox from the stats, CRS and ``edges`` from the logical type).
Writing it into the file is the funnel's (``parquet_footer.patch_or_rewrite``).

Reading a *written file* is the whole job, which is what separates it from
``core/arrow_geo_metadata.py``, whose subject is an in-memory table that has not
been written yet. Split out of ``common.py`` unchanged; every name a caller
imports is still importable from there. The type-code tables it reads are the
single copies in ``geo_metadata``.
"""

from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.crs_utils import (
    NULL_CRS_HINT,
    is_default_crs,
    is_geographic_crs,
    is_unknown_type_crs,
)
from geoparquet_io.core.duckdb_metadata import (
    get_schema_info,
    native_geo_type,
    parse_geometry_logical_type,
    resolve_crs_reference,
)
from geoparquet_io.core.edges import declares_edges, edges_of, logical_type_edges, set_edges
from geoparquet_io.core.geo_metadata import (
    KEEP_CRS,
    GeoBlockOptions,
    SchemaSource,
    compose_geo_block,
    read_raw_geo,
    row_group_geometry_stats,
    sanitized_carried_geo_of_file,
)
from geoparquet_io.core.geometry_detection import primary_geometry_column
from geoparquet_io.core.logging_config import debug, warn
from geoparquet_io.core.parquet_schema import root_schema_columns


def _geography_edges_from_logical(logical: str) -> str | None:
    """Edges value implied by a native Geography logical type string.

    Returns the declared algorithm (e.g. "spherical", "vincenty"), defaulting
    to "spherical" (the Parquet spec default) when none is spelled out
    (:func:`~geoparquet_io.core.edges.logical_type_edges`). Returns None for
    non-Geography logical types.
    """
    if native_geo_type(logical) != "GEOGRAPHY":
        return None
    return logical_type_edges(parse_geometry_logical_type(logical))


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
    if is_unknown_type_crs(raw):
        # `srid:0` is how the type says "unknown", and null is how the block does.
        return True, None
    crs = resolve_crs_reference(parquet_file, raw)
    if isinstance(crs, dict):
        # An empty native CRS is unset, which on this type is the default.
        return (False, None) if not crs or is_default_crs(crs) else (True, crs)
    warn(
        f"Native geometry type declares a CRS this build cannot resolve to PROJJSON: {crs!r}. "
        "Writing an explicit null CRS (unknown) rather than claiming the OGC:CRS84 default. "
        + NULL_CRS_HINT
    )
    return True, None


def _geo_col_meta_from_stats(pf, col_index: int, logical: str, parquet_file: str) -> dict:
    """Build one geo column metadata dict from a column's native geo statistics."""
    chunks = []
    for rg in range(pf.metadata.num_row_groups):
        stats = pf.metadata.row_group(rg).column(col_index).geo_statistics
        if stats is None:
            continue
        chunks.append(
            {
                "xmin": stats.xmin,
                "ymin": stats.ymin,
                "xmax": stats.xmax,
                "ymax": stats.ymax,
                "zmin": stats.zmin,
                "zmax": stats.zmax,
                "geometry_types": list(stats.geospatial_types or []),
            }
        )
    # The one fold (geo_metadata): X merged on the circle, Z over the row
    # groups that carry an extent, the spec's interleaved bbox (M is never part
    # of a bbox), spec type names only.
    # The rebuilt block is the file's only geo metadata, so a CRS left behind
    # here is not merely missing: an absent `crs` *means* OGC:CRS84, which
    # relabels projected coordinates as lon/lat while the Parquet logical type
    # still names the real CRS (#785). Mirror the logical type, which is the
    # authority at 2.0; it also decides how the extents fold.
    crs_present, crs = _crs_from_geo_logical(logical, parquet_file)
    folded = row_group_geometry_stats(chunks, geographic=is_geographic_crs(crs))
    col_meta: dict = {"encoding": "WKB", "geometry_types": folded.geometry_types}
    if folded.bbox is not None:
        col_meta["bbox"] = folded.bbox
    if crs_present:
        col_meta["crs"] = crs
    # A Geography logical type carries edge semantics the geo metadata must
    # not drop: synthesize the matching edges declaration (#588).
    edges = _geography_edges_from_logical(logical)
    if edges:
        set_edges(col_meta, edges)
    return col_meta


def _secondary_geometry_names(input_file: str) -> tuple[dict[str, str], dict[str, dict]]:
    """(top-level native columns by logical type string, declared geo-block entries) of an input.

    Read through ``get_schema_info`` and ``sanitized_carried_geo_of_file`` -- the pair
    ``write_funnels.collect_nonplanar_edges`` already asks this same question of,
    and the pair that resolves a remote URL, a ``dir/*.parquet`` glob and a hive
    directory (taking the first file's footer for a multi-file input, as every
    other metadata reader here does). Reading the input with ``pq.ParquetFile``
    instead meant ``https://``, ``s3://``, globs and directories derived nothing
    at all (#1175).

    Only top-level columns count: a GEOMETRY leaf nested in a struct is not a
    column of the table, and keying it by its leaf name would mistake it for a
    top-level column that happens to share the name -- which ``root_schema_columns``
    (and the top-level-only lookup inside ``get_schema_info``) is what keeps apart.
    A ``geo`` key that does not parse leaves the native half standing.
    """
    native: dict[str, str] = {}
    for column in root_schema_columns(get_schema_info(input_file)):
        logical = column.get("logical_type") or ""
        if native_geo_type(logical) is not None:
            native[column["name"]] = logical
    declared: dict[str, dict] = {}
    try:
        geo_meta = sanitized_carried_geo_of_file(input_file)
    except Exception as e:  # noqa: BLE001 - an unreadable `geo` key leaves the native half
        debug(f"Could not read the geo block of {input_file}: {e}")
        geo_meta = None
    columns = geo_meta.get("columns") if geo_meta else None
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
    if not declares_edges(declared):
        edges = _geography_edges_from_logical(logical)
        if edges:
            set_edges(col_meta, edges)
    return col_meta


def derive_secondary_geometry_info(
    input_file: str | None,
    primary_column: str,
    output_columns: list[str] | None = None,
    verbose: bool = False,
    native_types: dict[str, str] | None = None,
) -> dict | None:
    """``geometry_info`` for a rewrite whose caller supplied none, read off the input.

    Every geometry column the input carries beyond ``primary_column`` — native
    Parquet ``GEOMETRY``/``GEOGRAPHY`` logical types, plus anything the input's
    own ``geo`` block declares — becomes a secondary, so
    the composer's secondary steps finally learn it exists (#1000). What
    a native column's type adds is described in :func:`_native_secondary_meta`.
    Derived stats are otherwise absent: for a declared column they flow through
    ``original_metadata``, where the caller's invalidation (#934) has already
    had its say, and re-reading them here would resurrect exactly the stale
    values that invalidation stripped.

    ``output_columns`` limits the answer to columns the write actually emits, so
    a projection cannot come out declaring a column it dropped. Best-effort: an
    unreadable input derives nothing rather than failing the write. The input is
    read through ``get_schema_info``/``sanitized_carried_geo_of_file``, so a remote URL, a
    glob and a hive directory all answer (#1175).

    ``native_types`` is the same ``{column: logical type string}`` map read from
    somewhere other than the input's footer -- in practice the output query's own
    ``DESCRIBE`` (:func:`geoparquet_io.core.geometry_detection.native_geometry_types_from_query`),
    which is what lets a caller with **no** input witness at all (an Arrow
    stream read from stdin) derive its secondaries. The input's own types win where both speak:
    a logical type carries inline PROJJSON, while ``DESCRIBE`` renders the CRS as
    a bare ``EPSG:3857`` reference. ``input_file`` may be None when only
    ``native_types`` is available.
    """
    native: dict[str, str] = {}
    declared: dict[str, dict] = {}
    if input_file:
        try:
            native, declared = _secondary_geometry_names(input_file)
        except Exception as e:  # noqa: BLE001 - a probe, never the write's failure
            debug(f"Could not derive secondary geometry columns from {input_file}: {e}")
            return None
    for name, logical in (native_types or {}).items():
        native.setdefault(name, logical)

    metadata: dict[str, dict] = {}
    for name in dict.fromkeys([*native, *declared]):
        if name == primary_column:
            continue
        if output_columns is not None and name not in output_columns:
            continue
        logical = native.get(name)
        metadata[name] = (
            _native_secondary_meta(logical, declared.get(name), input_file or "") if logical else {}
        )

    if not metadata:
        return None
    secondary = list(metadata)
    if verbose:
        debug(
            f"Derived secondary geometry columns from {input_file or 'the output query'}: {secondary}"
        )
    return {"primary": primary_column, "secondary": secondary, "metadata": metadata}


def _with_primary_column(raw_geo: bytes, primary_column: str | None, schema=None) -> dict | None:
    """``raw_geo`` composed to name ``primary_column``, or None if it already does.

    None too when the block cannot be read or does not describe
    ``primary_column``: naming a column the block has no entry for would make it
    invalid, and a block this cannot parse is not this repair's to rewrite.
    DuckDB's stats are kept as written; the composer only takes the caller's
    primary (step 2) and runs its gates.
    """
    geo = read_raw_geo({b"geo": raw_geo}).block
    if geo is None or not isinstance(geo.get("columns"), dict):
        return None
    if primary_column not in geo["columns"] or geo.get("primary_column") == primary_column:
        return None
    return compose_geo_block(
        SchemaSource(schema if schema is not None else pa.schema([]), {b"geo": raw_geo}),
        GeoBlockOptions(
            geometry_column=primary_column,
            primary_column=primary_column,
            version="2.0",
            covering=None,
        ),
    )


def derive_v2_geo_metadata(
    output_path: str,
    primary_column: str | None = None,
) -> dict | None:
    """The GeoParquet 2.0 ``geo`` block a written file needs, when its own is wrong.

    DuckDB 1.5.4's V2 writer skips the geo KV metadata for M/ZM geometries, so
    the block has to be rebuilt from the file's native geospatial statistics and
    logical types, mirroring the shape DuckDB writes for XY/XYZ data (#589).
    ``primary_column`` names the real geometry column for multi-geometry files;
    without it ``geometry_detection.primary_geometry_column`` picks among the
    native columns (a standard geometry name, then the alphabetically first).

    When the file does carry a ``geo`` key, the one thing checked is its
    ``primary_column``. DuckDB's generated block names the FIRST geometry column
    in the schema, so a secondary written before the primary (``centroid``
    before ``geometry``) became the primary. A block that describes
    ``primary_column`` but names another column comes back with only that field
    changed; DuckDB's stats are kept as written.

    Returns ``None`` when there is nothing to attach: the file's ``geo`` key
    already names ``primary_column`` (or does not describe it, or is not a block
    this can read), or it has no native geometry column to describe.

    Read-only by design. *Attaching* the block is the caller's job, because the
    cheap way to do it is a footer patch and ``core/parquet_footer`` sits above
    this module in the import layering -- see ``write_funnels``, which owns the
    only caller (#1177).
    """
    pf = pq.ParquetFile(output_path)
    try:
        kv = pf.metadata.metadata or {}
        if b"geo" in kv:
            return _with_primary_column(kv[b"geo"], primary_column, pf.schema_arrow)
        schema = pf.metadata.schema
        geo_cols: dict[int, tuple[str, str]] = {}
        for i in range(len(schema)):
            logical = str(schema.column(i).logical_type)
            if native_geo_type(logical) is not None:
                geo_cols[i] = (schema.column(i).name, logical)
        if not geo_cols:
            return None

        columns = {
            name: _geo_col_meta_from_stats(pf, i, logical, output_path)
            for i, (name, logical) in geo_cols.items()
        }
        geo_cols_logical = dict(geo_cols.values())

        # Every described column is native, so the resolver's last step (the
        # first native-typed column) always answers; the columns go in sorted so
        # that step stays the alphabetical tie-break it has always been.
        primary = primary_geometry_column(
            [(name, geo_cols_logical[name]) for name in sorted(columns)],
            geo={"primary_column": primary_column} if primary_column else None,
        )
        # Each column's entry is what its logical type and statistics say: the
        # primary's goes in as the composer's crs, edges and stats, the
        # secondaries' as their declared metadata. No covering: none is
        # derivable from a logical type.
        declared = columns[primary]
        secondary = [name for name in columns if name != primary]
        return compose_geo_block(
            SchemaSource(pf.schema_arrow),
            GeoBlockOptions(
                geometry_column=primary,
                version="2.0",
                covering=None,
                edges=edges_of(declared) if declares_edges(declared) else None,
                crs=declared["crs"] if "crs" in declared else KEEP_CRS,
                stats={primary: {k: declared.get(k) for k in ("bbox", "geometry_types")}},
                geometry_info={"secondary": secondary, "metadata": columns},
            ),
        )
    finally:
        # Release the read handle before the caller writes to the file (Windows
        # refuses to replace a file that is still open).
        pf.close()
