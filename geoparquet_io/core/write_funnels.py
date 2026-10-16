"""The write funnels: the two front doors every gpio write goes through.

``write_parquet_with_metadata`` takes a DuckDB connection and a query;
``write_geoparquet_table`` takes an Arrow table. Between them they own the
*mechanism* of a write -- picking a strategy, generating and running the
``COPY``, threading key-value metadata through it, pruning carried metadata to
the columns the output actually has, preserving a non-planar ``edges``
declaration across a rewrite that cannot carry it, and uploading a remote
output.

This is deliberately a **sibling** of ``core/parquet_writer.py`` rather than
part of it. That module is the facade #990 introduced, and it owns four
*decisions* -- row-group rows, output version, input CRS, whether a ``geo`` key
is written at all -- that the write paths used to answer for themselves and
disagree about. The funnels here are what *asks* those questions. Folding them
in would put a thousand lines of mechanism behind a name that means "the
decisions", and leave the next write path with no small module to read. The
dependency runs one way and only one way: ``write_funnels`` imports
``parquet_writer``, never the reverse.

Split out of ``common.py`` unchanged; every name is still importable from there.
"""

import copy
import json
from collections.abc import Collection

import pyarrow.parquet as pq

from geoparquet_io.core.arrow_geo_metadata import (
    _apply_geoparquet_metadata,
    _normalize_arrow_large_types,
    _write_table_with_settings,
    nonplanar_edges_from_schema,
    with_planar_edge_types,
)
from geoparquet_io.core.compression import validate_compression_settings
from geoparquet_io.core.crs_utils import (
    _wrap_query_with_crs,
    apply_output_crs,
    extract_crs_from_parquet,
    get_crs_display_name,
    is_geographic_crs,
    is_unknown_type_crs,
    parse_crs_string_to_projjson,
)
from geoparquet_io.core.derive_geo_from_file import (
    _rewrite_file_with_geo_metadata,
    derive_secondary_geometry_info,
    derive_v2_geo_metadata,
)
from geoparquet_io.core.duckdb_metadata import (
    get_geo_metadata,
    get_kv_metadata,
    get_schema_info,
    parse_geometry_logical_type,
)
from geoparquet_io.core.duckdb_utils import (
    _get_query_column_type,
    _get_query_columns,
    sql_path,
    validate_compression_level,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import is_partition_path
from geoparquet_io.core.geo_metadata import (
    GEOGRAPHY_EDGE_ALGORITHMS,
    GEOPARQUET_VERSIONS,
    SELF_EVIDENT_BBOX_COLUMN,
    carried_geometry_column,
    declare_carried_bbox_column,
    gate_illegal_bbox_covering,
    prune_geo_metadata_to_columns,
    sanitized_carried_geo,
    strip_bboxless_covering,
    strip_derived_stats,
    strip_nonplanar_edges,
)
from geoparquet_io.core.geodesic_bbox import (
    ELLIPSOIDAL_ALGORITHMS,
    ColumnBounds,
    ellipsoid_for_crs,
    geography_statistics,
)
from geoparquet_io.core.geometry_detection import (
    _detect_geometry_from_query,
    native_geometry_types_from_query,
)
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.memory_limits import scoped_write_memory_limit
from geoparquet_io.core.parquet_footer import (
    EDGE_ALGORITHMS,
    FooterPatchRaced,
    FooterPatchUnsupported,
    patch_footer,
    patch_footer_kv,
)
from geoparquet_io.core.parquet_schema import root_schema_columns
from geoparquet_io.core.parquet_writer import (
    _CARRIED_SCHEMA_METADATA_KEYS,
    geoparquet_copy_options,
    note_duckdb_copy_rounding,
    resolve_input_crs,
    resolve_output_geoparquet_version,
    resolve_row_group_rows,
)
from geoparquet_io.core.remote import remote_write_context, upload_if_remote
from geoparquet_io.core.write_strategies import (
    WriteStrategy,
    WriteStrategyFactory,
    needs_metadata_rewrite,
)
from geoparquet_io.core.write_strategies.base import (
    has_unknown_secondary,
    merge_secondary_geometry_metadata,
    wrap_query_with_secondary_crs,
)


def collect_nonplanar_edges(input_file: str) -> dict[str, str]:
    """Map geometry column -> non-planar edges value declared by the input.

    The geo block's explicit planar or recognized non-planar declaration wins
    over the native GEOGRAPHY type, just as in ``table_nonplanar_edges``.
    Unrecognized values say nothing, so the type remains the fallback.
    """
    edges_by_col: dict[str, str] = {}
    try:
        for col in get_schema_info(input_file):
            logical = col.get("logical_type") or ""
            if "GeographyType" in logical:
                parsed = parse_geometry_logical_type(logical) or {}
                edges_by_col[col["name"]] = parsed.get("algorithm") or "spherical"
        geo_meta = get_geo_metadata(input_file) or {}
        for name, col_meta in (geo_meta.get("columns") or {}).items():
            edges = col_meta.get("edges") if isinstance(col_meta, dict) else None
            if edges == "planar":
                edges_by_col.pop(name, None)
            elif edges in GEOGRAPHY_EDGE_ALGORITHMS:
                edges_by_col[name] = edges
    except Exception as e:
        # A broken optional geo block must not erase native declarations already
        # read. An empty answer would re-enable destructive planar work (#1287).
        fallback = (
            "keeping known non-planar declarations"
            if edges_by_col
            else "treating its geometry as planar"
        )
        warn(f"Could not read the edges metadata of {input_file}; {fallback}: {e}")
    return edges_by_col


def _edges_for_output_version(edges: str, geo_version: str) -> str:
    """Map an edges value to one representable in the output's spec version.

    GeoParquet 1.x only allows {"planar", "spherical"}; ellipsoidal algorithms
    (vincenty/karney/andoyer/thomas) from 2.0/native inputs degrade to
    "spherical" with a warning. 2.0 outputs keep the algorithm verbatim.
    """
    if not str(geo_version).startswith("1."):
        return edges
    if edges in ("planar", "spherical"):
        return edges
    warn(f"edges algorithm '{edges}' is not representable in GeoParquet 1.x; writing 'spherical'")
    return "spherical"


def _block_edges(original_metadata: dict | None) -> dict[str, object]:
    """Map geometry column -> the ``edges`` value its ``geo`` block states, as written.

    A column whose block has no ``edges`` key is left out: it says nothing.
    An unreadable block reads as empty.
    """
    if not original_metadata:
        return {}
    geo_data = original_metadata.get("geo") or original_metadata.get(b"geo")
    if not geo_data:
        return {}
    try:
        if isinstance(geo_data, bytes):
            geo_data = geo_data.decode("utf-8")
        geo_meta = json.loads(geo_data) if isinstance(geo_data, str) else geo_data
        return {
            name: col_meta["edges"]
            for name, col_meta in (geo_meta.get("columns") or {}).items()
            if isinstance(col_meta, dict) and "edges" in col_meta
        }
    except Exception:
        return {}


def _collect_nonplanar_edges_from_metadata(original_metadata: dict | None) -> dict[str, str]:
    """Map geometry column -> non-planar edges declared in an input KV metadata dict.

    Fallback for write paths that carry the input's KV metadata but not its
    path (the native GEOGRAPHY logical-type half needs the file itself).
    """
    return {
        name: edges
        for name, edges in _block_edges(original_metadata).items()
        if edges and edges != "planar"
    }


def apply_nonplanar_edges(
    edges_by_col: dict[str, str],
    output_file: str,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    verbose: bool = False,
    pending_geo: dict | None = None,
) -> None:
    """Restate non-planar edges in an output's geo block *and* its logical types.

    DuckDB reads a Parquet GEOGRAPHY column as a planar GEOMETRY and has no
    geography type to write back, so every rewrite through it demotes the
    column and drops ``edges`` from the block it regenerates (#588). Losing the
    edge interpretation silently changes the shapes -- great-circle edges read
    as straight lines over the same bytes -- so both carriers are restored:

    * the ``geo`` block's ``edges``, mapped to what the block's version can
      say (:func:`_edges_for_output_version`);
    * the Parquet logical type, retyped from GEOMETRY to GEOGRAPHY with the
      same CRS and the edge algorithm (#999). Restoring only the block left a
      2.0 output whose block said spherical over a planar column, which
      ``check spec`` reports. A 1.x output has no logical type to retype.

    Both go into the footer in one ``patch_footer`` call: the data pages are
    the same WKB either way, so nothing below the footer is re-encoded. The
    block-only rewrite this replaced cost a full pass and lost the compression
    level; it stays as the fallback for a footer that cannot be read.

    A retyped column gets its bounding boxes back (#1272): the row-group
    GeospatialStatistics and the block's ``bbox`` were vertex boxes on a plane,
    which a geodesic edge can leave, so each is replaced with a bound computed
    over the edges (``core/geodesic_bbox``) rather than dropped as #1254 did.
    When nothing can bound a column, the drop remains the fallback, and the
    block's planar ``bbox`` goes with it. A column with non-planar edges that
    is *not* retyped -- every 1.x output, or a 2.0 column that is not a
    top-level GEOMETRY -- gets the geodesic block ``bbox`` only: it has no
    GEOGRAPHY statistics to patch (and ``patch_footer`` only replaces a box
    DuckDB wrote, it never adds one). A bbox covering column is per-row data,
    not footer, and stays the vertex box it was written as.

    ``pending_geo`` is the ``geo`` block the V2 M/ZM repair deferred to this
    patch (#1270): the block the output needs but does not yet carry, taken in
    place of the file's own and written whether or not the edges change it, so
    the repair and the retype cost one below-the-footer copy instead of two.
    """
    if not edges_by_col and pending_geo is None:
        return

    geo_meta = pending_geo if pending_geo is not None else _read_geo_key(output_file)
    geo_version = str((geo_meta or {}).get("version") or "")
    edges_by_col = {
        name: _edges_for_output_version(edges, geo_version) for name, edges in edges_by_col.items()
    }
    geography = _geography_candidates(output_file, edges_by_col)
    targets = {**_block_bbox_targets(geo_meta, edges_by_col, geography), **geography}
    bounds = _geodesic_bounds(output_file, targets, geo_meta)
    geography_bbox = {name: bounds[name].per_group for name in geography if name in bounds}
    file_bboxes = {name: b.file_bbox for name, b in bounds.items() if b.file_bbox is not None}
    geo_updates = _geo_block_updates(
        geo_meta,
        edges_by_col,
        file_bboxes,
        force=pending_geo is not None,
        unbounded=set(targets) - set(file_bboxes),
    )
    # A 1.x output has no GEOGRAPHY type to carry the edges, so a GeoArrow
    # column's embedded Arrow schema is stamped instead, or it says planar.
    arrow_edges = edges_by_col if geo_version.startswith("1.") else None

    try:
        retyped = patch_footer(
            output_file,
            geo_updates,
            geography=geography,
            geography_bbox=geography_bbox,
            arrow_edges=arrow_edges,
            verbose=verbose,
        )
    except FooterPatchRaced:
        # Nothing was written, and a rewrite now would describe another
        # writer's bytes (see _ensure_v2_geo_metadata).
        raise
    except FooterPatchUnsupported as exc:
        retyped = []
        warn(f"{exc}. Edge columns stay planar in the Parquet schema.")
        if geo_updates:
            _rewrite_file_with_geo_metadata(
                output_file,
                json.loads(geo_updates["geo"]),
                compression,
                compression_level,
                row_group_rows,
            )

    if geo_meta is None and not retyped and not _already_geography(output_file, edges_by_col):
        warn(
            "Input declares non-planar edges but output has no geo metadata or "
            f"GEOGRAPHY column to carry them ({', '.join(sorted(edges_by_col))}). "
            "Edge interpretation is lost."
        )
    elif verbose:
        debug(
            f"Kept non-planar edges for {', '.join(sorted(edges_by_col))}"
            + (f"; retyped GEOGRAPHY: {', '.join(retyped)}" if retyped else "")
        )


def _already_geography(output_file: str, edges_by_col: dict[str, str]) -> bool:
    """Whether every edge column is already a GEOGRAPHY, so its type carries the edges."""
    geography = {
        col["name"]
        for col in root_schema_columns(get_schema_info(output_file))
        if "GeographyType" in (col.get("logical_type") or "")
    }
    return set(edges_by_col) <= geography


def _geography_candidates(output_file: str, edges_by_col: dict[str, str]) -> dict[str, str]:
    """The columns that may be retyped GEOGRAPHY: a Parquet edge algorithm and a geographic CRS.

    Parquet's GEOGRAPHY requires a geographic CRS, so a column whose CRS is
    projected keeps its planar GEOMETRY type, as it did before #999, and says
    why; a ``geo`` block still carries what the input declared, which
    ``check spec`` warns about on its own (#601). No CRS is OGC:CRS84.
    """
    geography: dict[str, str] = {}
    candidates = {name: e for name, e in edges_by_col.items() if e in EDGE_ALGORITHMS}
    if not candidates:
        return geography
    # Only a top-level GEOMETRY can be retyped; a 1.x output's WKB has no type.
    planar = {
        col["name"]
        for col in root_schema_columns(get_schema_info(output_file))
        if "GeometryType" in (col.get("logical_type") or "")
    }
    for name, edges in candidates.items():
        if name not in planar:
            continue
        crs = extract_crs_from_parquet(output_file, geometry_column=name)
        if isinstance(crs, str):
            crs = parse_crs_string_to_projjson(crs) or crs
        if is_geographic_crs(crs):
            geography[name] = edges
        else:
            warn(
                f"Kept column '{name}' a planar GEOMETRY in the Parquet schema: "
                f"GEOGRAPHY requires a geographic CRS, and {get_crs_display_name(crs)} "
                f"is not one, so the type cannot carry edges: {edges}."
            )
    return geography


def _read_geo_key(output_file: str) -> dict | None:
    """The output's ``geo`` block, or None when it has none."""
    # Closed before the patch replaces the file (Windows requires it).
    with pq.ParquetFile(output_file) as pf:
        raw = (pf.metadata.metadata or {}).get(b"geo")
    return json.loads(raw.decode("utf-8")) if raw else None


def _block_bbox_targets(
    geo_meta: dict | None, edges_by_col: dict[str, str], geography: dict[str, str]
) -> dict[str, str]:
    """Columns not being retyped whose ``geo`` block ``bbox`` still needs geodesic bounds.

    Any column the block describes with non-planar edges and a geographic CRS
    (none means OGC:CRS84): its vertex ``bbox`` under-covers the edges as much
    as a GEOGRAPHY column's statistics would.
    """
    columns = (geo_meta or {}).get("columns") or {}
    targets = {}
    for name, edges in edges_by_col.items():
        col_meta = columns.get(name)
        if name in geography or not isinstance(col_meta, dict):
            continue
        if edges != "spherical" and edges not in ELLIPSOIDAL_ALGORITHMS:
            continue
        if is_geographic_crs(col_meta.get("crs")):
            targets[name] = edges
    return targets


def _geodesic_bounds(
    output_file: str, targets: dict[str, str], geo_meta: dict | None
) -> dict[str, ColumnBounds]:
    """Geodesic row-group and file-level bounds for ``targets`` (``{column: edges}``).

    Best-effort: a column whose bounds cannot be computed falls back to #1254's
    behavior -- its planar boxes are dropped -- rather than failing the write,
    and says so. A read error degrades; running out of memory does not.
    """
    if not targets:
        return {}
    import pyarrow as pa
    from pyproj.exceptions import CRSError, GeodError

    columns = (geo_meta or {}).get("columns") or {}
    orientations = {
        name: (columns.get(name) or {}).get("orientation")
        for name in targets
        if isinstance(columns.get(name), dict)
    }
    ellipsoids = {
        name: ellipsoid_for_crs(extract_crs_from_parquet(output_file, geometry_column=name))
        for name, edges in targets.items()
        if edges in ELLIPSOIDAL_ALGORITHMS
    }
    try:
        bounds = geography_statistics(
            output_file, targets, orientations=orientations, ellipsoids=ellipsoids
        )
    except MemoryError:
        raise
    except (OSError, ValueError, ArithmeticError, pa.ArrowException, CRSError, GeodError) as e:
        warn(
            f"Could not compute geodesic bounding boxes for {', '.join(sorted(targets))} "
            f"({e}); their bounding boxes are dropped, which only gives up row-group pruning."
        )
        return {}
    for name, column_bounds in bounds.items():
        if column_bounds.unreadable_groups:
            warn(
                f"Column '{name}': {column_bounds.unreadable_groups} row group(s) hold geometry "
                "gpio cannot bound geodesically (unreadable WKB, or a non-finite or "
                "out-of-range coordinate); their bounding boxes, and the file's, are dropped, "
                "which only gives up row-group pruning."
            )
    return bounds


def _bbox_with_geodesic_xy(existing: object, box: tuple) -> list[float]:
    """A ``geo`` block ``bbox`` with the geodesic X/Y bounds, Z kept if it had one."""
    xmin, ymin, xmax, ymax = box
    if isinstance(existing, list) and len(existing) == 6:
        return [xmin, ymin, existing[2], xmax, ymax, existing[5]]
    return [xmin, ymin, xmax, ymax]


def _geo_block_updates(
    geo_meta: dict | None,
    edges_by_col: dict[str, str],
    file_bboxes: dict[str, tuple],
    force: bool = False,
    unbounded: Collection[str] = (),
) -> dict[str, str]:
    """``{"geo": <block>}`` with edges and geodesic bboxes set, or ``{}``.

    ``force`` writes the block even unchanged -- the #1270 deferred repair's
    block is not in the file yet, so "unchanged" still has to land.
    ``unbounded`` names columns whose edges no geodesic box could be computed
    for: their planar ``bbox`` is removed rather than left to under-cover.
    """
    changed = False
    columns = (geo_meta or {}).get("columns") or {}
    for name, edges in edges_by_col.items():
        col_meta = columns.get(name)
        if isinstance(col_meta, dict) and col_meta.get("edges") != edges:
            col_meta["edges"] = edges
            changed = True
    for name, box in file_bboxes.items():
        col_meta = columns.get(name)
        if not isinstance(col_meta, dict):
            continue
        bbox = _bbox_with_geodesic_xy(col_meta.get("bbox"), box)
        if col_meta.get("bbox") != bbox:
            col_meta["bbox"] = bbox
            changed = True
    for name in unbounded:
        col_meta = columns.get(name)
        if isinstance(col_meta, dict) and "bbox" in col_meta:
            del col_meta["bbox"]
            changed = True
    if geo_meta is None or not (changed or force):
        return {}
    return {"geo": json.dumps(geo_meta)}


def _ensure_v2_geo_metadata(
    output_path: str,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    verbose: bool = False,
    primary_column: str | None = None,
    defer: bool = False,
) -> dict | None:
    """Attach GeoParquet 2.0 geo metadata if the writer omitted it (#589).

    DuckDB 1.5.4's V2 writer skips the geo KV metadata for M/ZM geometries;
    ``derive_v2_geo_metadata`` rebuilds the block the file implies and this
    writes it into the footer. The same patch corrects a block DuckDB did
    write but whose ``primary_column`` names its first geometry column rather
    than ``primary_column`` -- a secondary that comes before the primary.

    One footer key does not need the data decoded, so the key goes in with
    ``patch_footer_kv``: every byte below the footer is copied verbatim, which
    keeps the codec, the *compression level* Parquet does not record, the
    encodings, the row-group boundaries the COPY just sized and the row order by
    construction (#1177). The rewrite it replaced had to re-derive all of those
    from pyarrow's own defaults, and came back both bigger than the COPY it was
    repairing and re-chunked -- so an XYM 2.0 output had a different layout from
    an XY one for the same command. It stays as the fallback for a footer that
    cannot be read.

    This lives here rather than beside the derivation because
    ``derive_geo_from_file`` sits below ``parquet_footer`` in the import
    layering; the funnel is the only caller either way.

    With ``defer`` the derived block is returned instead of patched: an edges
    patch is about to rewrite the same footer, and folding the key into that
    one ``patch_footer`` call halves the post-write I/O (#1270). Returns the
    pending block then, ``None`` otherwise.
    """
    geo_meta = derive_v2_geo_metadata(output_path, primary_column=primary_column)
    if geo_meta is None:
        return None
    if defer:
        if verbose:
            debug("Deferring the omitted geo metadata to the edges footer patch")
        return geo_meta

    try:
        patch_footer_kv(output_path, {"geo": json.dumps(geo_meta)}, verbose=verbose)
    except FooterPatchRaced:
        # The file changed while the patch was copying it, and nothing was
        # written. The rewrite fallback below would read the bytes that have
        # arrived and stamp them with a `geo` block derived from the bytes
        # that have gone -- another writer's rows under our stats -- so the
        # refusal propagates instead (#1173's repair takes the same stance).
        raise
    except FooterPatchUnsupported as exc:
        warn(f"{exc}. Falling back to a rewrite, which re-encodes the data.")
        _rewrite_file_with_geo_metadata(
            output_path, geo_meta, compression, compression_level, row_group_rows
        )
    if verbose:
        debug(
            f"Wrote the geo metadata the writer omitted or mislabelled: {geo_meta['primary_column']!r} is primary"
        )
    return None


def _plain_copy_to(
    con,
    query: str,
    output_path: str,
    compression: str = "ZSTD",
    verbose: bool = False,
    geoparquet_version: str = "1.1",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    input_crs: dict | None = None,
    geometry_column: str | None = None,
    carry_geo_metadata: dict | None = None,
    extra_kv_metadata: dict[str, str] | None = None,
    memory_limit: str | None = None,
    defer_v2_geo_repair: bool = False,
    secondary_geo_columns: dict | None = None,
) -> dict | None:
    """
    Execute a plain DuckDB COPY TO without geo metadata manipulation.

    Used when no metadata rewrite is needed (parquet-geo-only, 2.0 passthrough).
    This is the fastest possible write path.

    DuckDB 1.5+: If input_crs is non-default, wraps the query with ST_SetCRS()
    so CRS is written into the Parquet schema natively during COPY TO.

    Args:
        con: DuckDB connection
        query: SQL query to execute
        output_path: Path to output file
        compression: Compression type
        verbose: Whether to print verbose output
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        compression_level: Compression level (codec-specific)
        row_group_rows: Target number of rows per row group
        input_crs: PROJJSON dict with CRS information (optional)
        geometry_column: Name of the geometry column (required if input_crs is set)
        carry_geo_metadata: A `geo` block to write verbatim instead of the one
            DuckDB would generate. Used to preserve a `covering` the input
            declared, which DuckDB's own generated metadata does not include.
            DuckDB does not replace a supplied `geo` key under 'V2', it appends
            its own beside it, so `geoparquet_copy_options` asks for 'NONE'
            instead (#1225). The column keeps its native GEOMETRY logical type
            and geospatial statistics either way: 'NONE' governs the KV entry
            only.
        extra_kv_metadata: Non-geo sidecar keys (fiboa, vecorel, STAC fragments)
            to carry into the output. DuckDB accepts KV_METADATA alongside
            GEOPARQUET_VERSION, so these ride along on the fast path instead of
            forcing a full metadata rewrite for an unrelated key (#709).
        memory_limit: DuckDB memory limit for the COPY (``--write-memory``).
            None means a default with headroom under the machine's memory
            ceiling -- never DuckDB's own 80%, which the allocations DuckDB
            makes outside its limit carry past a cgroup cap (#1153).
        defer_v2_geo_repair: Hand the V2 geo repair's block back to the caller
            instead of patching it here, because the caller is about to patch
            the same footer for non-planar edges and one ``patch_footer`` call
            can carry both (#1270).
        secondary_geo_columns: The ``geo.columns`` entries the secondaries'
            CRS comes from, so a 2.0 type carries each one's own CRS.

    Returns:
        The repair's pending ``geo`` block when ``defer_v2_geo_repair`` asked
        for it and the output needs one; ``None`` otherwise.
    """
    compression_map = {
        "zstd": "ZSTD",
        "gzip": "GZIP",
        "snappy": "SNAPPY",
        "lz4": "LZ4",
        "none": "UNCOMPRESSED",
        "uncompressed": "UNCOMPRESSED",
        "brotli": "BROTLI",
    }
    duckdb_compression = compression_map.get(compression.lower(), "ZSTD")

    # Map version to DuckDB GEOPARQUET_VERSION parameter
    version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])
    duckdb_version = version_config.get("duckdb_param", "V1")

    # DuckDB 1.5+: Apply CRS via ST_SetCRS so it's written natively into the
    # Parquet schema during COPY TO — no post-processing file rewrite needed.
    final_query = _wrap_query_with_crs(query, geometry_column, input_crs, geoparquet_version)
    final_query = wrap_query_with_secondary_crs(
        con, final_query, secondary_geo_columns, geometry_column, geoparquet_version
    )

    # Build options list
    options = [
        "FORMAT PARQUET",
        f"COMPRESSION {duckdb_compression}",
    ]
    # DuckDB accepts COMPRESSION_LEVEL for ZSTD only; pairing it with GZIP or
    # BROTLI is a binder error, not a silently ignored option.
    if compression_level is not None and duckdb_compression.upper() == "ZSTD":
        options.append(f"COMPRESSION_LEVEL {validate_compression_level(compression_level)}")
    if row_group_rows is not None:
        options.append(f"ROW_GROUP_SIZE {row_group_rows}")
    kv_pairs = dict(extra_kv_metadata or {})
    if carry_geo_metadata is not None:
        # Applied last, so the carried block wins over an explicit `geo` in
        # `extra_kv_metadata` -- a deliberate flip of the previous precedence.
        # Preserved keys never collide here (`extract_preserved_kv_metadata`
        # excludes `geo`); only a caller that hand-passes `geo` is affected, and
        # `carry_geo_metadata` is the block this write decided to emit.
        kv_pairs["geo"] = json.dumps(carry_geo_metadata)
    # A carried `geo` block turns the request into 'NONE', or DuckDB writes its
    # own `geo` key after ours (#1225). `duckdb_version` keeps the request, which
    # is what the V2 repair below is keyed on.
    options.extend(geoparquet_copy_options(duckdb_version, kv_pairs))

    copy_query = f"""
        COPY ({final_query})
        TO {sql_path(output_path)}
        ({", ".join(options)})
    """

    if verbose:
        debug(f"Executing plain COPY TO with {duckdb_compression} compression...")

    with scoped_write_memory_limit(con, memory_limit, verbose):
        con.execute(copy_query)

    # DuckDB 1.5.4's V2 writer omits the geo KV metadata for geometries with
    # an M dimension (XY/XYZ are written correctly). Without it the output
    # silently degrades to parquet-geo-only, so repair it from the file's own
    # native geospatial statistics (#589). When it does write the block, it
    # names the first geometry column primary, which the same repair corrects.
    pending_v2_geo = None
    if duckdb_version == "V2":
        pending_v2_geo = _ensure_v2_geo_metadata(
            output_path,
            compression=duckdb_compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            verbose=verbose,
            primary_column=geometry_column,
            defer=defer_v2_geo_repair,
        )

    if verbose:
        import pyarrow.parquet as pq

        # Closed before the caller moves or uploads this file: `--verbose` must
        # not be the difference between a fix that lands and one that dies on
        # `Could not move file: Access is denied` (#1023, #1032).
        with pq.ParquetFile(output_path) as pf:
            success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")
    return pending_v2_geo


def _custom_covering_survives(covering, original_metadata, geometry_column, output_columns) -> bool:
    """Whether a ``custom_metadata`` covering can outlive the #954 gate.

    Conservative: it survives if it brings its own ``bbox`` member, if the
    carried block already declares one for the primary, or if the output may
    carry a conventional ``bbox`` column the rewrite would declare -- including
    when the output's columns are unknown.
    """
    if not isinstance(covering, dict) or "bbox" in covering:
        return True
    if output_columns is None or SELF_EVIDENT_BBOX_COLUMN in output_columns:
        return True
    carried = sanitized_carried_geo(original_metadata)
    primary = (carried.get("columns") or {}).get(geometry_column)
    carried_covering = primary.get("covering") if isinstance(primary, dict) else None
    return isinstance(carried_covering, dict) and "bbox" in carried_covering


def _probe_output_columns(con, query: str, verbose: bool, purpose: str) -> list[str] | None:
    """The output query's column names, or ``None`` when the probe fails.

    Best-effort by design: every caller treats an unreadable schema as "unknown"
    rather than aborting the write.
    """
    import duckdb

    try:
        return _get_query_columns(con, query)
    except (duckdb.Error, RuntimeError, ValueError, AttributeError) as e:
        if verbose:
            debug(f"Could not read output schema to {purpose}: {e}")
        return None


def _merge_derived_geometry_info(caller: dict | None, derived: dict | None) -> dict | None:
    """The caller's own ``geometry_info``, extended with secondaries it did not name.

    ``convert`` builds its ``geometry_info`` from the input's ``geo`` block alone
    (``detect_all_geometry_columns``), which for a native-geo-only input names no
    secondary at all -- and a caller-supplied value used to suppress the
    derivation outright, so ``convert --geoparquet-version 1.1`` wrote the
    secondary as a native Parquet GEOMETRY inside a 1.1 file while ``extract`` of
    the same input was already correct (#1175).

    The caller still wins: every column it names, and every key it states for a
    column it shares with the derivation, is left exactly as it was. The
    derivation only *adds* -- a column, or a key the caller left blank.
    """
    if not derived:
        return caller
    if not caller:
        return derived
    merged = copy.deepcopy(caller)
    secondary = list(merged.get("secondary") or ())
    metadata = dict(merged.get("metadata") or {})
    for name in derived.get("secondary") or ():
        if name == merged.get("primary"):
            continue
        if name not in secondary:
            secondary.append(name)
        entry = dict((derived.get("metadata") or {}).get(name) or {})
        entry.update(metadata.get(name) or {})
        metadata[name] = entry
    merged["secondary"] = secondary
    merged["metadata"] = metadata
    return merged


def _resolve_geometry_info(
    con,
    query: str,
    geometry_info: dict | None,
    geometry_column: str | None,
    input_file: str | None,
    output_columns: list[str] | None,
    verbose: bool,
) -> tuple[dict | None, list[str] | None]:
    """``geometry_info`` with every OTHER geometry column of this write described.

    Which other geometry columns does this write carry? Callers that know say so
    (``convert``); the rest -- the ``add`` family, sort, extract, partition -- used
    to leave a secondary column undescribed in ``geo.columns``, which 2.0 requires
    for every geometry column in the file (#1000). Each secondary's CRS comes from
    its own logical type, never the per-file witness's.

    Two sources, in this order of preference:

    * the ``input_file`` witness, read through ``get_schema_info`` /
      ``get_geo_metadata`` -- the richer one, since a logical type carries inline
      PROJJSON and a GEOGRAPHY's edges, and the input's ``geo`` block also names
      the secondaries that are *not* native there;
    * failing that, the output query's own ``DESCRIBE``, which names every
      ``GEOMETRY`` column the write emits and the CRS each one carries. It is the
      only source a caller with no witness at all has (an Arrow stream read from
      stdin), and it is asked only of those callers: where a witness exists it
      has already answered, more completely, for the same columns (#1175). It
      cannot see a GEOGRAPHY's edges, which is why ``partition`` passes its
      input as the witness rather than relying on this (#1234).

    Gated on knowing the output's columns, so a projection cannot come out
    declaring a geometry column it dropped. Returns the resolved ``geometry_info``
    and the output columns, so a later step does not probe the query again.
    """
    if not geometry_column:
        return geometry_info, output_columns
    if output_columns is None:
        output_columns = _probe_output_columns(con, query, verbose, "derive secondary geometry")
    if output_columns is None:
        return geometry_info, output_columns
    native_types = (
        None if input_file else native_geometry_types_from_query(con, query, verbose=verbose)
    )
    derived = derive_secondary_geometry_info(
        input_file,
        geometry_column,
        output_columns=output_columns,
        verbose=verbose,
        native_types=native_types,
    )
    return _merge_derived_geometry_info(geometry_info, derived), output_columns


def _prune_metadata_to_output_columns(
    con,
    query: str,
    original_metadata: dict | None,
    geometry_column: str | None,
    verbose: bool,
) -> tuple[dict | None, list[str] | None]:
    """Drop geo metadata that references columns the output query does not emit.

    Best-effort: a schema probe that fails leaves the metadata untouched rather
    than aborting the write.

    Returns the pruned metadata and the output's column names, so a later step
    that only needs to know which columns exist does not probe the query again.
    ``None`` column names mean the probe failed, not that there are no columns.
    """
    if not original_metadata:
        return original_metadata, None

    output_columns = _probe_output_columns(con, query, verbose, "prune geo metadata")
    if output_columns is None:
        return original_metadata, None

    pruned = prune_geo_metadata_to_columns(original_metadata, output_columns)
    had_geo = "geo" in original_metadata or b"geo" in original_metadata
    if had_geo and geometry_column is None:
        warn(
            "Output has no geometry column - writing plain Parquet without geo metadata "
            f"(columns: {', '.join(output_columns)})"
        )
    return pruned, output_columns


def extract_preserved_kv_metadata(metadata: dict | None) -> dict[str, str]:
    """Return an input's non-geo file-level KV metadata as ``{str: str}``.

    Sidecar payloads (``fiboa``, ``vecorel``, STAC fragments, collection
    records) live in Parquet's file-level key/value metadata next to ``geo``.
    Every write path that rebuilds the KV block must carry them over, so this
    is the single definition of which keys survive a rewrite (#690).

    Keys and values that are not valid UTF-8 are skipped: the write paths pass
    KV metadata as text (DuckDB ``KV_METADATA`` takes SQL string literals), so a
    binary payload cannot be round-tripped and dropping it beats failing the
    write.

    Args:
        metadata: KV metadata dict from a Parquet file or Arrow schema. Keys
            and values may be ``bytes`` or ``str``; ``None`` is accepted.

    Returns:
        Decoded ``{key: value}`` for every key outside
        ``_CARRIED_SCHEMA_METADATA_KEYS``.
    """
    if not metadata:
        return {}

    preserved: dict[str, str] = {}
    for key, value in metadata.items():
        # The key is decoded inside the try as well: Parquet KV keys are
        # arbitrary bytes, and a non-UTF-8 key must skip like a non-UTF-8
        # value rather than raise out of a metadata-preservation helper.
        try:
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key
            if key_str in _CARRIED_SCHEMA_METADATA_KEYS:
                continue
            val_str = value.decode("utf-8") if isinstance(value, bytes) else value
        except UnicodeDecodeError:
            debug(f"Skipping non-UTF-8 KV metadata entry {key!r} (cannot be preserved as text)")
            continue
        preserved[key_str] = val_str
    return preserved


def read_preserved_kv_metadata(parquet_file: str, verbose: bool = False) -> dict[str, str]:
    """Read a Parquet input's preservable non-geo KV metadata.

    Returns an empty dict when the file cannot be inspected (e.g. a remote
    input without credentials): losing sidecar keys is bad, but failing an
    otherwise valid conversion because of them would be worse.
    """
    try:
        # Both the read and the decode are guarded: a malformed KV block is an
        # input-data problem, and losing sidecar keys must never be the reason
        # an otherwise valid write fails.
        preserved = extract_preserved_kv_metadata(get_kv_metadata(parquet_file))
    except Exception as e:  # noqa: BLE001 - any read failure degrades to "nothing to preserve"
        warn(f"Could not read input KV metadata from {parquet_file}: {e}")
        return {}

    if verbose and preserved:
        debug(f"Preserving input KV metadata keys: {sorted(preserved)}")
    return preserved


# Fields a carried `geo` block must still have for the 2.0 fast path to be able
# to write it verbatim. When a caller invalidated the derived stats (reproject,
# a row filter, a multi-file merge) they are stripped, and only the rewrite path
# can recompute them — so we fall back to it rather than ship a thinner block
# than DuckDB would have generated.
_REQUIRED_CARRIED_GEO_FIELDS = ("encoding", "geometry_types")

# Column keys DuckDB's own `GEOPARQUET_VERSION 'V2'` block already writes. A
# carried block is only worth substituting when it says something more than
# these; otherwise DuckDB's freshly generated block is the better one, because
# it describes the data actually written rather than the input.
_DUCKDB_GENERATED_COLUMN_FIELDS = frozenset({"encoding", "geometry_types", "bbox"})


def _carries_more_than_duckdb_generates(geo_dict: dict) -> bool:
    """Whether any column in ``geo_dict`` holds a key DuckDB would not write itself."""
    return any(
        isinstance(col_meta, dict) and not set(col_meta) <= _DUCKDB_GENERATED_COLUMN_FIELDS
        for col_meta in (geo_dict.get("columns") or {}).values()
    )


def _geo_block_to_carry_on_fast_path(
    original_metadata: dict | None,
    geometry_column: str | None,
    effective_version: str,
    con=None,
    query: str | None = None,
    verbose: bool = False,
    input_crs=None,
    input_file: str | None = None,
    output_columns: list[str] | None = None,
) -> dict | None:
    """The block the 2.0 fast path writes, dropping the must-rewrite half.

    See :func:`_fast_path_geo_decision`, which answers both halves at once. Kept
    as the name the write path and its tests ask by.
    """
    carried, _ = _fast_path_geo_decision(
        original_metadata,
        geometry_column,
        effective_version,
        con=con,
        query=query,
        verbose=verbose,
        input_crs=input_crs,
        input_file=input_file,
        output_columns=output_columns,
    )
    return carried


def _fast_path_geo_decision(
    original_metadata: dict | None,
    geometry_column: str | None,
    effective_version: str,
    con=None,
    query: str | None = None,
    verbose: bool = False,
    input_crs=None,
    input_file: str | None = None,
    output_columns: list[str] | None = None,
) -> tuple[dict | None, bool]:
    """``(block to write instead of DuckDB's, whether to take the rewrite instead)``.

    DuckDB regenerates the `geo` key on the fast path, and its generated block
    carries only `version`, `primary_column`, `encoding`, `geometry_types` and
    `bbox`. Everything else the input declared — a `covering` (#738), `epoch`,
    `orientation` (#772) — is silently dropped. Returning the carried block here
    lets `_plain_copy_to` write it verbatim, keeping the fast path's write
    configuration intact: forcing the rewrite instead clamps the write to one
    thread and caps its memory (`duckdb_kv._configure_duckdb_memory`, which needs
    `threads = 1` for the cap to hold at all), and can add a full stats rescan.
    `--compression-level` is *not* among the costs: duckdb-kv passes
    COMPRESSION_LEVEL through to its own COPY.

    The carried block goes through `apply_output_crs`, the single source of truth
    for the null-vs-default CRS rule, so the fast path cannot write a `crs: null`
    or an explicit CRS84 that the rewrite path would have stripped.

    With `con` and `query`, a conventional `bbox` column the output still carries
    is also declared as a covering, exactly as the rewrite path does. That is the
    only covering gpio invents: `covering` asserts a relationship between a bbox
    column and the geometry that cannot be verified from a column name, so
    `declare_carried_bbox_column` recognises the single universal convention and
    checks the struct's fields before declaring anything. It runs *before* the
    "says more than DuckDB would" gate below, because for a plain 2.0 input with
    an undeclared bbox column the derived covering is the whole difference
    between the two paths (`tests/test_covering_v2.py::
    test_a_conventional_bbox_column_is_declared_at_v2`). `output_columns` lets it
    skip its schema probe when the output has no bbox column to declare at all.

    Both answers are ``(None, False)`` — "let DuckDB write its own block on the
    fast path" — when the version is not 2.0 (1.x already rewrites), when the
    input resolves to more than one file (see below), or when the carried block
    says nothing DuckDB would not write itself. The one exception is an unknown
    ``input_crs`` with no carried block to state it: DuckDB's block would omit
    ``crs`` and so claim OGC:CRS84, so that write is sent to the rewrite
    (``(None, True)``, #1276).

    Otherwise the second answer is ``True`` only when the block IS worth keeping and still
    cannot be written verbatim, because a caller invalidated the derived stats and
    nothing here can recompute them. The fast path cannot merge: it replaces
    DuckDB's whole ``geo`` key with what this returns, so a block missing
    ``geometry_types`` would ship *without* the key GeoParquet requires. Taking
    the rewrite is what ``_REQUIRED_CARRIED_GEO_FIELDS`` always said the answer
    was; what actually happened was the fast path running with DuckDB's bare
    block, so ``gpio partition quadkey|kdtree`` and ``extract --where/--limit
    /--bbox`` of a 2.0 input lost the covering it declared and ``gpio check bbox``
    then called its bbox column undeclared (#1172). The thinness test is asked
    *after* the declare step for the same reason the "says more than DuckDB"
    gate is: until that step has run, there is no telling whether the block has
    anything the fast path would lose.
    """
    if effective_version != "2.0" or not geometry_column:
        return None, False
    # DuckDB's own 2.0 block omits `crs`, which reads as OGC:CRS84. An unknown
    # CRS with no carried block to say so takes the rewrite, which writes
    # `crs: null` (#1276).
    no_block = (None, is_unknown_type_crs(input_crs))
    if not original_metadata:
        return no_block

    # A glob/directory input merges several files, but `original_metadata` was
    # read from the FIRST file's footer only. Carrying its bbox/geometry_types
    # as the merged output's stats UNDER-covers the result, which makes
    # conformant readers skip data; only the rewrite path can recompute them
    # over everything written. Same test `extract` uses for the same reason.
    if input_file and is_partition_path(input_file):
        if verbose:
            debug(
                "Not carrying the input's geo block: a multi-file input's merged "
                "stats cannot come from the first file's footer"
            )
        return no_block

    # A write-path reader in the strongest sense: whatever comes back is written
    # to the output file verbatim, so the block goes through the shared shape
    # check first. `columns` as a list or a string used to abort the write with
    # `'list' object has no attribute 'get'` on the next line (#947).
    geo_dict = sanitized_carried_geo(original_metadata)
    if not geo_dict:
        return no_block
    col_meta = (geo_dict.get("columns") or {}).get(geometry_column)
    if not isinstance(col_meta, dict):
        return no_block
    carried = copy.deepcopy(geo_dict)
    carried["version"] = "2.0.0"
    # Before the gate: a block whose only extra key was a default or null
    # `crs` says nothing DuckDB would not write once that key is stripped.
    apply_output_crs(carried["columns"][geometry_column], input_crs)
    if con is not None and query is not None:
        declare_carried_bbox_column(
            con,
            query,
            carried["columns"][geometry_column],
            verbose,
            effective_version,
            output_columns=output_columns,
            geo_meta=carried,
        )
        # The declare above is primary-scoped, so a SECONDARY column's covering
        # over an illegal struct reached the output verbatim and `gpio check
        # spec` failed the file gpio had just written (#1172/#1035). Same gate
        # the Arrow builders run, over every column.
        carried = gate_illegal_bbox_covering(carried, con, query)
    # After the declare above: a carried covering still without a bbox member
    # (e.g. only a spatial-index entry) is one geopandas cannot read (#954),
    # so it is not carried onto the output either.
    carried = strip_bboxless_covering(carried, verbose)
    if not _carries_more_than_duckdb_generates(carried):
        return None, False
    if any(
        field not in carried["columns"][geometry_column] for field in _REQUIRED_CARRIED_GEO_FIELDS
    ):
        if verbose:
            debug(
                "Taking the metadata rewrite: the carried geo block says more than "
                "DuckDB's would but has had its derived stats invalidated, so the "
                "fast path cannot write it verbatim"
            )
        return None, True
    return carried, False


def _memory_limit_for_strategy(
    memory_limit: str | None,
    strategy_enum,
    write_strategy: str,
    *,
    auto_routed_strategy: bool,
    funnel_forced_rewrite: bool,
) -> str | None:
    """The memory limit a rewrite strategy may be given, or None.

    Of the rewrite strategies only duckdb-kv honours a memory limit. If *we* put
    the write here, the user did nothing wrong: warn and drop the limit rather
    than aborting a command that worked before ``--write-memory`` was plumbed
    through (#663). Two routings are ours, not theirs -- the 1.1-geoarrow reroute
    and the rewrite this funnel takes to keep a 2.0 input's own covering, which
    no flag asked for and which turned a working ``extract --limit N
    --write-strategy streaming --write-memory 2GB`` into exit 2 (#1172). A
    strategy the user explicitly asked for is a real error -- raised as a core
    exception so the CLI shows a clean message instead of a traceback.
    """
    if memory_limit is None or strategy_enum == WriteStrategy.DUCKDB_KV:
        return memory_limit
    if auto_routed_strategy:
        warn(
            "--write-memory is ignored for GeoParquet 1.1-geoarrow output: "
            "GeoArrow encoding requires the arrow-streaming write strategy, "
            "which does not support a memory limit."
        )
        return None
    if funnel_forced_rewrite:
        warn(
            f"--write-memory is ignored for this write: keeping the input's "
            f"geo metadata (its covering) needs the metadata rewrite, and the "
            f"'{write_strategy}' write strategy does not support a memory "
            f"limit. Use --write-strategy duckdb-kv to apply it."
        )
        return None
    raise InvalidParameterError(
        "--write-memory",
        f"a memory limit is only supported with the 'duckdb-kv' "
        f"write strategy, not '{write_strategy}'",
    )


def _query_geometry_is_already_native(con, query: str, geometry_column: str | None) -> bool:
    """Whether the query's geometry column is already GeoArrow-native.

    Only ``convert`` builds a ``geometry_info``, so the encoding witness the
    1.1-geoarrow guard used to read is absent on every other write path (sort,
    extract, partition, the ``add`` family). Ask the query instead: DESCRIBE
    alone, no scan. A native column describes as its nested shape
    (``STRUCT(x DOUBLE, y DOUBLE)[][]`` for a polygon); a WKB/WKT one describes
    as ``GEOMETRY``, ``GEOGRAPHY``, ``BLOB`` or ``VARCHAR`` (#1176).
    """
    if not geometry_column:
        return False
    col_type = _get_query_column_type(con, query, geometry_column)
    return bool(col_type) and not col_type.upper().startswith(
        ("GEOMETRY", "GEOGRAPHY", "BLOB", "VARCHAR")
    )


def _secondary_geo_columns(carried_geo_block: dict | None, geometry_info: dict | None):
    """The ``geo.columns`` entries the write's secondary CRS comes from."""
    return (carried_geo_block or {}).get("columns") or (geometry_info or {}).get("metadata")


def _unknown_secondary_needs_rewrite(
    carried_geo_block: dict | None,
    secondary_geo_columns: dict | None,
    geometry_column: str | None,
    effective_version: str,
) -> bool:
    """Whether an unknown secondary CRS sends a 2.0 fast-path write to the rewrite.

    DuckDB's own 2.0 block omits the ``crs`` of a ``srid:0`` column, which reads
    as OGC:CRS84. With no carried block to say ``crs: null`` for an unknown
    secondary, the rewrite writes it (#1277 review).
    """
    return (
        carried_geo_block is None
        and effective_version == "2.0"
        and has_unknown_secondary(secondary_geo_columns, geometry_column)
    )


def write_parquet_with_metadata(
    con,
    query,
    output_file,
    original_metadata=None,
    compression="ZSTD",
    compression_level=15,
    row_group_size_mb=None,
    row_group_rows=None,
    custom_metadata=None,
    verbose=False,
    show_sql=False,
    profile=None,
    geoparquet_version=None,
    input_crs=None,
    write_strategy: str = "duckdb-kv",
    memory_limit: str | None = None,
    geometry_info: dict | None = None,
    extra_kv_metadata: dict[str, str] | None = None,
    input_file: str | None = None,
    invalidate_derived_stats: bool = False,
    invalidate_derived_stats_columns: Collection[str] | None = None,
    drop_nonplanar_edges_columns: Collection[str] | None = None,
):
    """
    Write a parquet file with proper compression and metadata handling.

    Supports multiple write strategies with different memory and performance
    characteristics. The default "duckdb-kv" strategy uses DuckDB's native
    KV_METADATA for fast streaming writes.

    Supports both local and remote outputs (S3, GCS, Azure). Remote outputs
    are written to a temporary local file, then uploaded.

    Args:
        con: DuckDB connection
        query: SQL query to execute
        output_file: Path to output file (local path or remote URL)
        original_metadata: Original metadata from source file
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        custom_metadata: Optional dict with custom metadata (e.g., H3 info)
        verbose: Whether to print verbose output
        show_sql: Whether to print SQL statements before execution
        profile: AWS profile name (S3 only, optional)
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0, parquet-geo-only)
        input_crs: PROJJSON dict naming the CRS the output declares. Set by a
            write whose output CRS is not a reading of the input's: `gpio convert
            reproject`, which transforms the coordinates, and `gpio convert`,
            which names the CRS a non-Parquet source's geometry is in (`--crs`,
            for CSV/TSV input). Left None by every rewrite that keeps its input's
            coordinates: the facade then resolves it from ``input_file``, so a
            native-geo-only input's CRS — which lives only in the Parquet
            GEOMETRY logical type — reaches the output's ``geo`` block too
            (#993).
        write_strategy: Write strategy to use. Options:
            - "duckdb-kv" (default): Use DuckDB COPY TO with KV_METADATA
            - "in-memory": Load entire dataset into memory
            - "streaming": Stream Arrow RecordBatches
            - "disk-rewrite": Write with DuckDB, then rewrite with PyArrow
        memory_limit: DuckDB memory limit for the write (e.g., '2GB', '512MB').
            Honoured on the plain-COPY fast path and by the duckdb-kv strategy.
            If None, each picks a default from the system/container memory.
        geometry_info: Dict containing multi-geometry column info with keys:
            - "primary": primary geometry column name
            - "secondary": list of secondary geometry column names
            - "metadata": dict mapping column names to their metadata (crs, encoding, etc.)
        extra_kv_metadata: Additional Parquet file-level KV metadata as {key: json_string}.
            Written alongside the 'geo' key (e.g., for Vecorel collection metadata).
        input_file: Path to the input parquet file, when the write rewrites an
            existing file. The witness the facade resolves the output's version
            and CRS from, and what enables full-fidelity non-planar edges
            preservation (native GEOMETRY/GEOGRAPHY logical types, and the CRS
            stored inside them, are only visible in the file's schema); without
            it, both fall back to original_metadata's ``geo`` key, which a
            native-geo-only input does not have. Must be the file whose *rows*
            this write reads, or one lossless with respect to it: naming the
            user's file while reading a scratch rewrite that dropped the native
            type is how a wrong CRS comes to be asserted rather than omitted.
        invalidate_derived_stats: When True, strip the carried per-column
            ``bbox`` and ``geometry_types`` from ``original_metadata`` before
            building output geo metadata. Set by callers that transform geometry
            (reproject), filter rows (extract), or merge several inputs whose
            carried metadata came from only the first file: the input's stats no
            longer describe the output, so they must be recomputed from the
            written data or omitted rather than carried.
        invalidate_derived_stats_columns: Restricts that invalidation to these
            geometry columns. Set by a caller that changes coordinates in some
            geometry columns but not others — reproject transforms only the
            primary column, so a secondary column reaches the output unchanged
            and its carried stats still describe it (#890). None (the default)
            invalidates every column, which is right for a merge: its carried
            stats under-cover every column of the output. An invalidated column
            other than the primary keeps ``geometry_types`` as the spec's empty
            "not known" list rather than losing the key (#934); the duckdb-kv
            strategy then recomputes it from the rows it writes (#952), the
            others write the sentinel.
        drop_nonplanar_edges_columns: Geometry columns whose non-planar
            ``edges`` declaration must neither be carried through nor
            re-attached to the output. Set by writes that invalidate the edge
            interpretation for the columns they transform — reprojecting the
            primary geometry column to a projected CRS turns its great-circle
            edges into straight lines (#601) — where planar (the spec default)
            is the truthful description of the output. Columns not named here
            (e.g. an untransformed secondary geometry column) keep their
            declaration. The caller warns; this only stops the carry.

    Returns:
        None
    """

    configure_verbose(verbose)

    # Use geometry column from geometry_info if provided, otherwise auto-detect
    # This ensures original column names are preserved (fixes #328)
    # Resolved before the invalidation below, which has to name the one column
    # the write strategies recompute derived stats for.
    if geometry_info and geometry_info.get("primary"):
        geometry_column = geometry_info["primary"]
    else:
        geometry_column = _detect_geometry_from_query(con, query, original_metadata, verbose)

    # Callers that transform geometry, filter rows, or merge multiple inputs
    # invalidate the carried bbox/geometry_types; drop them so the write
    # strategies recompute (or omit) them instead of describing the input. A
    # caller that transforms only some geometry columns names them, so an
    # untouched secondary column keeps the stats that still describe it (#890).
    #
    # Every strategy recomputes `geometry_column`; only duckdb-kv also fills a
    # SECONDARY column's gap (#952), so a stripped secondary would otherwise be
    # left with no `geometry_types` at all -- a key GeoParquet 1.1 requires and
    # DuckDB refuses to open a file without. Naming the recomputed column leaves
    # the others the "not known" sentinel (#934), which duckdb-kv treats as a gap.
    if invalidate_derived_stats:
        original_metadata = strip_derived_stats(
            original_metadata,
            columns=invalidate_derived_stats_columns,
            recomputed_columns={geometry_column} if geometry_column else set(),
        )

    # A write that invalidates the edge interpretation for the columns it
    # transforms (reprojecting into a projected CRS, #601) must neither carry
    # those columns' declaration through nor re-attach it afterwards; planar is
    # then the truthful description. Untransformed columns keep theirs.
    if drop_nonplanar_edges_columns:
        original_metadata = strip_nonplanar_edges(
            original_metadata, columns=drop_nonplanar_edges_columns
        )

    # A column projection (e.g. ``extract --exclude-cols``) can drop the bbox
    # column a ``covering`` points at, a secondary geometry column, or the
    # geometry column itself. Carrying those references produces metadata that
    # names schema roots the output does not have.
    original_metadata, output_columns = _prune_metadata_to_output_columns(
        con, query, original_metadata, geometry_column, verbose
    )

    # The facade owns all three of these: how many rows a row group gets, which
    # version auto mode resolves to, and which CRS the output describes its
    # geometry with. Every caller of this function -- 21 of them -- used to
    # inherit whatever its writer defaulted to and whatever its own `geo` key
    # happened to say, which is how `convert` came to write 122,880-row groups
    # (#981) and how `sort`/`extract`/`partition` came to rewrite a
    # native-geo-only input as 1.1 WKB while `convert` kept it native (#600).
    #
    # The version and the CRS are resolved from the same `input_file` witness,
    # together, because they are the same question asked of the same file.
    # Answering only the version made the output native 2.0 while leaving its
    # `geo` block with no `crs` key -- EPSG:5070 in the Parquet GEOMETRY logical
    # type, OGC:CRS84 in the geo block, one file disagreeing with itself (#993).
    row_group_rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)
    geoparquet_version = resolve_output_geoparquet_version(
        geoparquet_version,
        input_file=input_file,
        original_metadata=original_metadata,
        verbose=verbose,
    )
    input_crs = resolve_input_crs(
        input_crs, input_file=input_file, geometry_column=geometry_column, verbose=verbose
    )

    effective_version = geoparquet_version or "1.1"

    # Check if we need to add/rewrite geo metadata
    rewrite_needed = needs_metadata_rewrite(effective_version, original_metadata)

    # Force rewrite if custom_metadata contains covering (e.g., bbox, H3, S2)
    # This ensures covering metadata is written even for 2.0→2.0 operations --
    # unless the covering could only be stripped again (#954): an index entry
    # with no bbox member to stand beside is dropped by every write path, so
    # forcing the single-threaded rewrite would only buy the fast path's output.
    if custom_metadata and "covering" in custom_metadata:
        if _custom_covering_survives(
            custom_metadata["covering"], original_metadata, geometry_column, output_columns
        ):
            rewrite_needed = True
            if verbose:
                debug("Forcing metadata rewrite for covering metadata")
        elif verbose:
            debug("Not forcing a rewrite: the covering has no bbox member to stand beside")

    # The 2.0 fast path's own answer, asked here rather than at the call site so
    # it can still send the write down the rewrite path. It replaces DuckDB's
    # whole `geo` key, so a block it cannot write verbatim is not a block the
    # fast path can keep half of (#1172).
    #
    # Asked only when the answer can still be used: `carried_geo_block` is read
    # in the `not rewrite_needed` branch below and nowhere else, so a write
    # another forcer has already sent down the rewrite path paid for the
    # decision's schema probes and discarded what they bought.
    carried_geo_block = None
    carried_geo_needs_rewrite = False
    if not rewrite_needed:
        carried_geo_block, carried_geo_needs_rewrite = _fast_path_geo_decision(
            original_metadata,
            geometry_column,
            effective_version,
            con=con,
            query=query,
            verbose=verbose,
            input_crs=input_crs,
            input_file=input_file,
            output_columns=output_columns,
        )
        if carried_geo_needs_rewrite:
            rewrite_needed = True

    # Which OTHER geometry columns does this write carry? Asked only where the
    # answer is used -- the rewrite, and a fast path that carries the input's own
    # block -- since it may read the input's footer again (an HTTP round trip
    # for a remote input). A fast path with no block to carry lets DuckDB
    # generate its own, and DuckDB's V2 block already describes every geometry
    # column in the file, with its real `geometry_types`.
    if rewrite_needed or carried_geo_block is not None:
        geometry_info, output_columns = _resolve_geometry_info(
            con, query, geometry_info, geometry_column, input_file, output_columns, verbose
        )
    if carried_geo_block is not None:
        # The carried block REPLACES DuckDB's generated one, which does describe
        # every geometry column -- so a block that omits a native secondary the
        # input never declared would un-describe it, and 2.0 requires an entry
        # for every geometry column (#1175). The merge adds the entry: the
        # secondary's OWN crs/edges, and the spec's "not known" `geometry_types:
        # []` where the input said nothing. `[]` rather than the real list is the
        # price of keeping the carried block at all -- only the rewrite path can
        # compute one, and the keys that make the block worth carrying (`epoch`,
        # `orientation`, a `covering`) would be lost by handing the write back to
        # DuckDB instead. Merged after the "says more than DuckDB" gate, so a
        # secondary's own `crs` cannot by itself make a block worth carrying.
        merge_secondary_geometry_metadata(carried_geo_block, geometry_info)
    secondary_geo_columns = _secondary_geo_columns(carried_geo_block, geometry_info)
    rewrite_needed = rewrite_needed or _unknown_secondary_needs_rewrite(
        carried_geo_block, secondary_geo_columns, geometry_column, effective_version
    )

    # Preserve non-geo KV metadata from input (e.g., vecorel, fiboa).
    # Build a merged local dict rather than mutating the caller-supplied
    # extra_kv_metadata: partition loops reuse one dict across writes, and
    # writing into it in place leaked prior files' keys into later ones.
    preserved_keys = extract_preserved_kv_metadata(original_metadata)
    if preserved_keys:
        # Caller-supplied entries win over preserved keys of the same name.
        extra_kv_metadata = {**preserved_keys, **(extra_kv_metadata or {})}

    # Sidecar keys no longer force a rewrite: DuckDB accepts KV_METADATA in the
    # same COPY as GEOPARQUET_VERSION, so `_plain_copy_to` writes them itself
    # (#709). A 2.0 -> 2.0 copy that happens to carry a fiboa or vecorel payload
    # used to pay for a full metadata rewrite -- an extra scan of the geometry
    # column to recompute bbox and geometry types -- for a key that has nothing
    # to do with the geo block.
    #
    # The fast path is no longer thinner than the rewrite: DuckDB regenerates
    # the `geo` key, but `_geo_block_to_carry_on_fast_path` substitutes the
    # input's own block -- `epoch`, `orientation`, a declared `covering`, plus
    # one auto-declared for a conventional bbox column the output carries -- so
    # for the same input, and absent `custom_metadata`, both paths write the
    # same block (#772). `custom_metadata` is the exception: only the rewrite
    # path merges it, and a `covering` in it already forces that path above.
    if extra_kv_metadata and verbose:
        debug(f"Carrying extra KV metadata: {list(extra_kv_metadata.keys())}")

    # Which columns' non-planar edges this write must restore afterwards (#588).
    # Asked before the write so the fast path can fold the V2 M/ZM geo repair
    # into the edges footer patch when both are coming (#1270); the answer only
    # reads the input, which the write does not change.
    edges_to_restore = _edges_to_restore(
        input_file, original_metadata, exclude_columns=drop_nonplanar_edges_columns
    )

    if show_sql:
        info("\n-- Query:")
        progress(query)

    # No AWS_PROFILE env mutation here: the write target inside this block is a
    # local (temp) file, and the upload at the end is credentialed by passing
    # profile= straight through to upload().
    with remote_write_context(output_file, is_directory=False, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        if not rewrite_needed:
            # Fast path: plain DuckDB COPY TO without geo metadata manipulation
            if verbose:
                debug(f"Writing GeoParquet version: {effective_version}")
                debug(f"No metadata rewrite needed for {effective_version} - using plain COPY TO")

            # Nothing but a bare DuckDB COPY writes this file, so a sub-vector
            # request lands at 2,048 whatever was asked for. Say so here, where
            # that is certain, rather than let the footer be the first to tell
            # the user (#986).
            note_duckdb_copy_rounding(row_group_rows)
            pending_v2_geo = _plain_copy_to(
                con=con,
                query=query,
                output_path=actual_output,
                compression=compression,
                compression_level=compression_level,
                row_group_rows=row_group_rows,
                verbose=verbose,
                geoparquet_version=effective_version,
                input_crs=input_crs,
                geometry_column=geometry_column,
                carry_geo_metadata=carried_geo_block,
                extra_kv_metadata=extra_kv_metadata,
                memory_limit=memory_limit,
                # An edges patch is coming: one patch_footer call carries the
                # M/ZM geo repair and the retype together (#1270).
                defer_v2_geo_repair=bool(edges_to_restore),
                secondary_geo_columns=secondary_geo_columns,
            )
        else:
            pending_v2_geo = None
            # Metadata rewrite needed - use strategy pattern

            # 1.1-geoarrow produces native GeoArrow encoding from WKB/text inputs,
            # which requires the arrow-streaming strategy (DuckDB COPY TO cannot emit
            # nested GeoArrow types). Already-native inputs keep their preservation
            # path (duckdb-kv passes the native column through unchanged).
            auto_routed_strategy = False
            if geoparquet_version == "1.1-geoarrow":
                primary = (geometry_info or {}).get("primary")
                input_encoding = (
                    (geometry_info or {}).get("metadata", {}).get(primary, {}).get("encoding")
                )
                # `geometry_info` is convert's witness; on every other path it
                # is None, so fall back to asking the query what the geometry
                # column's type actually is (#1176).
                already_native = (
                    bool(input_encoding and input_encoding.lower() not in ("wkb", "wkt"))
                    if input_encoding
                    else _query_geometry_is_already_native(con, query, geometry_column)
                )
                if not already_native and write_strategy != "streaming":
                    if verbose:
                        debug("Routing 1.1-geoarrow WKB input through arrow-streaming")
                    write_strategy = "streaming"
                    auto_routed_strategy = True

            strategy_enum = WriteStrategy(write_strategy)
            strategy = WriteStrategyFactory.get_strategy(strategy_enum)

            memory_limit = _memory_limit_for_strategy(
                memory_limit,
                strategy_enum,
                write_strategy,
                auto_routed_strategy=auto_routed_strategy,
                funnel_forced_rewrite=carried_geo_needs_rewrite,
            )

            if verbose:
                debug(f"Writing GeoParquet version: {effective_version}")
                debug(f"Using write strategy: {strategy.name}")

            # Build kwargs - only pass memory_limit for duckdb-kv
            write_kwargs = {
                "con": con,
                "query": query,
                "output_path": actual_output,
                # None (no geometry in the output) is meaningful: every strategy
                # falls back to a plain-Parquet write rather than advertising a
                # geometry column that is not there.
                "geometry_column": geometry_column,
                "original_metadata": original_metadata,
                "geoparquet_version": effective_version,
                "compression": compression,
                "compression_level": compression_level,
                "row_group_size_mb": row_group_size_mb,
                "row_group_rows": row_group_rows,
                "input_crs": input_crs,
                "verbose": verbose,
                "custom_metadata": custom_metadata,
                "geometry_info": geometry_info,
                "extra_kv_metadata": extra_kv_metadata,
            }
            if strategy_enum == WriteStrategy.DUCKDB_KV:
                write_kwargs["memory_limit"] = memory_limit
            # duckdb-kv alone lets DuckDB's COPY have the last word on row-group
            # size, so it alone rounds a sub-vector request up to 2,048 (#986).
            # Measured with --row-group-size 249 on a 10,000-row input:
            # duckdb-kv writes 2,048-row groups; in-memory, streaming and
            # disk-rewrite all write 249-row ones -- disk-rewrite because its
            # pyarrow pass regroups the COPY output at the requested size.
            if strategy_enum == WriteStrategy.DUCKDB_KV:
                note_duckdb_copy_rounding(row_group_rows)

            strategy.write_from_query(**write_kwargs)

        # Non-planar edges must survive every rewrite path (#588): DuckDB
        # regenerates geo metadata without `edges`, silently demoting geography
        # data to planar. Runs on the local temp file, so remote outputs are
        # patched before upload. Columns whose declaration this write
        # invalidated (#601) were excluded at collection rather than
        # re-attached. The fast path's deferred V2 geo repair rides along, so
        # both edits cost one footer patch (#1270).
        apply_nonplanar_edges(
            edges_to_restore,
            actual_output,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            verbose=verbose,
            pending_geo=pending_v2_geo,
        )

        # Auto-fix vecorel schema compliance when collection metadata is present
        if not is_remote:
            _auto_fix_vecorel_if_needed(actual_output, extra_kv_metadata, original_metadata)

        if is_remote:
            upload_if_remote(
                actual_output,
                output_file,
                profile=profile,
                is_directory=False,
                verbose=verbose,
            )


def declared_nonplanar_edges(
    input_file: str | None, original_metadata: dict | None
) -> dict[str, str]:
    """Map geometry column -> non-planar edges an input declares, from either carrier.

    Prefers the input file, which sees native GEOGRAPHY logical types as well as
    the ``geo`` block; falls back to the input's KV metadata dict when the file
    says nothing or there is no re-readable file (an Arrow stream read from
    stdin passes ``input_file=None``). One answer serves both the planar-repair
    gate (#1287) and the edges a write restores (#588).
    """
    edges_by_col = collect_nonplanar_edges(input_file) if input_file else {}
    return edges_by_col or _collect_nonplanar_edges_from_metadata(original_metadata)


def _edges_to_restore(
    input_file: str | None,
    original_metadata: dict | None,
    exclude_columns: Collection[str] | None = None,
) -> dict[str, str]:
    """The non-planar edges the writer will drop and the write must restore.

    The input's declaration (:func:`declared_nonplanar_edges`), less
    ``exclude_columns``: columns whose declaration the write itself invalidated
    (#601). The input still declares them, but they must not come back.

    Collection only; :func:`apply_nonplanar_edges` writes the answer back
    after the write, on any rewrite path.
    """
    edges_by_col = declared_nonplanar_edges(input_file, original_metadata)
    if exclude_columns:
        edges_by_col = {k: v for k, v in edges_by_col.items() if k not in exclude_columns}
    return edges_by_col


def table_nonplanar_edges(table, geometry_column: str, edges: str | None) -> dict[str, str]:
    """Map geometry column -> non-planar edges an in-memory table declares.

    The one reader of a table's edges (the repair gate's
    ``geometry_repair.nonplanar_edges_of_arrow_table`` is a view of it). The
    caller's ``edges`` wins for the primary, then the carried ``geo`` block,
    then a GeoArrow field's own edge type. A block that states ``edges`` wins
    over the type when they disagree, ``"planar"`` included. A block without
    ``edges`` says nothing, so the type decides. Only a GeoParquet edge
    algorithm (``GEOGRAPHY_EDGE_ALGORITHMS``) counts as a declaration, so a
    typo falls through to the type (#1288). An explicit ``"planar"`` from the
    caller is a choice too, so it drops the primary. The table is the only
    witness the Arrow funnel has, since it never sees the input file.
    """
    edges_by_col = nonplanar_edges_from_schema(table.schema)
    for name, declared in _block_edges(table.schema.metadata).items():
        if declared == "planar":
            edges_by_col.pop(name, None)
        elif declared in GEOGRAPHY_EDGE_ALGORITHMS:
            edges_by_col[name] = declared
    if edges == "planar":
        edges_by_col.pop(geometry_column, None)
    elif edges:
        edges_by_col[geometry_column] = edges
    return {name: e for name, e in edges_by_col.items() if name in table.column_names}


def _auto_fix_vecorel_if_needed(
    output_path: str,
    extra_kv_metadata: dict | None,
    original_metadata: dict | None,
) -> None:
    """Fix vecorel schema compliance if the output has collection metadata."""
    has_collection = False
    if extra_kv_metadata and "collection" in extra_kv_metadata:
        has_collection = True
    elif original_metadata:
        if "collection" in original_metadata or b"collection" in original_metadata:
            has_collection = True

    if has_collection:
        import pyarrow.parquet as pq

        from geoparquet_io.core.constants import VECOREL_NON_NULLABLE, _fix_vecorel_schema

        pf = pq.ParquetFile(output_path)
        columns = set(pf.schema_arrow.names)
        # Close before _fix_vecorel_schema rewrites the file in place; an open
        # handle here would make its os.replace() fail on Windows.
        pf.close()
        non_nullable = [c for c in VECOREL_NON_NULLABLE if c in columns]
        if non_nullable:
            _fix_vecorel_schema(output_path, non_nullable)


def write_geoparquet_table(
    table,
    output_file: str,
    geometry_column: str | None = None,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    profile: str | None = None,
    edges: str | None = None,
    geo_bbox: list[float] | None = None,
) -> None:
    """
    Write a PyArrow Table to a GeoParquet file with proper metadata.

    This is the table-centric version for writing GeoParquet files.
    It applies proper GeoParquet metadata and handles compression settings.

    Args:
        table: PyArrow Table to write
        output_file: Path to output file (local path or remote URL)
        geometry_column: Name of geometry column (auto-detected if None)
        compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
        compression_level: Compression level (varies by format)
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        geoparquet_version: GeoParquet version to write (1.0, 1.1, 2.0)
        verbose: Whether to print verbose output
        profile: AWS profile name (S3 only, optional)
        edges: Edge interpretation, "spherical" or "planar" (default None = planar).
               Use "spherical" for data from BigQuery or other S2-based sources.
        geo_bbox: Pre-computed [xmin, ymin, xmax, ymax] for the geometry column.
               Pass the RFC 7946 5.2 wrap form (xmin > xmax) for data that
               crosses the antimeridian; None computes a plain extent.
    """
    # A write path: the column name is quoted into the output's metadata and the
    # CRS is written to the output file, so the carried block goes through the
    # shared shape check rather than being indexed raw. `columns: null` used to
    # raise `argument of type 'NoneType' is not iterable` here, and a list-,
    # string- or non-object-entry `columns` a `TypeError` one line later (#947).
    geo_meta = sanitized_carried_geo(table.schema.metadata)

    # The facade's row-group decision, so an Arrow-side write (arcgis, carto,
    # wfs, bigquery, aggregate, overview) lands on the same number a DuckDB-side
    # one does. Without it these fell through to ParquetWriteSettings' old
    # 100,000 while the COPY paths took DuckDB's 122,880.
    row_group_rows = resolve_row_group_rows(row_group_rows, row_group_size_mb)

    if geometry_column is None:
        geometry_column = carried_geometry_column(geo_meta, table.column_names) or "geometry"

    # Check if geometry column exists
    has_geometry = geometry_column in table.column_names

    # Extract original metadata for preservation
    original_metadata = table.schema.metadata

    # Extract CRS from original metadata if available
    col_meta = (geo_meta.get("columns") or {}).get(geometry_column)
    input_crs = col_meta.get("crs") if isinstance(col_meta, dict) else None

    # Validate and normalize compression settings
    validated_compression, validated_level, _ = validate_compression_settings(
        compression or "ZSTD", compression_level, verbose
    )
    # Handle UNCOMPRESSED - pass None for compression when uncompressed
    if validated_compression == "UNCOMPRESSED":
        validated_compression = None

    # Normalize large_string/large_binary back to string/binary for Parquet compatibility
    table = _normalize_arrow_large_types(table)

    # Read before the metadata pass rebuilds the column: a 1.x write strips the
    # GeoArrow edge type, and a 2.0 one regenerates a block without `edges`.
    edges_by_col = table_nonplanar_edges(table, geometry_column, edges) if has_geometry else {}
    if has_geometry and edges == "planar":
        # An explicit planar choice also clears a native GEOGRAPHY type, which
        # a 2.0 write would otherwise keep as spherical.
        table = with_planar_edge_types(table, [geometry_column])

    # No AWS_PROFILE env mutation here: the write target inside this block is a
    # local (temp) file, and the upload at the end is credentialed by passing
    # profile= straight through to upload().
    with remote_write_context(output_file, is_directory=False, verbose=verbose) as (
        actual_output,
        is_remote,
    ):
        # Apply GeoParquet metadata if the geometry column exists, or when
        # parquet-geo-only was explicitly requested: that request has to strip a
        # carried geo key whether or not the column survived a projection (#701).
        if has_geometry or geoparquet_version == "parquet-geo-only":
            table = _apply_geoparquet_metadata(
                table,
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
                original_metadata=original_metadata,
                input_crs=input_crs,
                custom_metadata=None,
                verbose=verbose,
                edges=edges,
                geo_bbox=geo_bbox,
            )

        # Write to disk with proper settings
        _write_table_with_settings(
            table,
            actual_output,
            compression=validated_compression or "UNCOMPRESSED",
            compression_level=validated_level,
            row_group_rows=row_group_rows,
            row_group_size_mb=row_group_size_mb,
            geoparquet_version=geoparquet_version,
            geometry_column=geometry_column,
            verbose=verbose,
        )

        # The DuckDB-query funnel restores edges in `_preserve_edges_after_write`.
        # This is the same step for a table: the block's `edges` and, at 2.0,
        # the GEOGRAPHY logical type, so the two never disagree (#1269).
        apply_nonplanar_edges(
            edges_by_col,
            actual_output,
            compression=validated_compression or "UNCOMPRESSED",
            compression_level=validated_level,
            row_group_rows=row_group_rows,
            verbose=verbose,
        )

        # Upload to remote if needed
        if is_remote:
            upload_if_remote(
                actual_output,
                output_file,
                profile=profile,
                is_directory=False,
                verbose=verbose,
            )
