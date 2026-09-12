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
dependency runs one way and only one way: ``parquet_write`` imports
``parquet_writer``, never the reverse.

Split out of ``common.py`` unchanged; every name is still importable from there.
"""

import copy
import json
from collections.abc import Collection

import duckdb
import pyarrow.parquet as pq

from geoparquet_io.core.arrow_geo_metadata import (
    _CARRIED_SCHEMA_METADATA_KEYS,
    _apply_geoparquet_metadata,
    _normalize_arrow_large_types,
    _write_table_with_settings,
)
from geoparquet_io.core.compression import validate_compression_settings
from geoparquet_io.core.crs_utils import _wrap_query_with_crs, apply_output_crs
from geoparquet_io.core.duckdb_metadata import (
    get_geo_metadata,
    get_kv_metadata,
    get_schema_info,
    parse_geometry_logical_type,
)
from geoparquet_io.core.duckdb_utils import (
    _get_query_columns,
    build_kv_metadata_clause,
    sql_path,
    validate_compression_level,
)
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import is_partition_path
from geoparquet_io.core.geo_metadata import (
    GEOPARQUET_VERSIONS,
    carried_geometry_column,
    declare_carried_bbox_column,
    prune_geo_metadata_to_columns,
    sanitized_carried_geo,
    strip_derived_stats,
    strip_nonplanar_edges,
)
from geoparquet_io.core.geo_metadata_repair import (
    _ensure_v2_geo_metadata,
    _rewrite_file_with_geo_metadata,
)
from geoparquet_io.core.geometry_detection import _detect_geometry_from_query
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    info,
    progress,
    success,
    warn,
)
from geoparquet_io.core.parquet_writer import (
    note_duckdb_copy_rounding,
    resolve_input_crs,
    resolve_output_geoparquet_version,
    resolve_row_group_rows,
)
from geoparquet_io.core.remote import remote_write_context, upload_if_remote


def collect_nonplanar_edges(input_file: str) -> dict[str, str]:
    """Map geometry column -> non-planar edges value declared by the input.

    Sources: the native GEOGRAPHY logical type's algorithm, and any non-planar
    ``edges`` in the input's geo column metadata.
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
            if edges and edges != "planar":
                edges_by_col.setdefault(name, edges)
    except Exception as e:
        debug(f"Could not collect edges metadata from {input_file}: {e}")
        return {}
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


def _collect_nonplanar_edges_from_metadata(original_metadata: dict | None) -> dict[str, str]:
    """Map geometry column -> non-planar edges declared in an input KV metadata dict.

    Fallback for write paths that carry the input's KV metadata but not its
    path (the native GEOGRAPHY logical-type half needs the file itself).
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
        edges_by_col: dict[str, str] = {}
        for name, col_meta in (geo_meta.get("columns") or {}).items():
            edges = col_meta.get("edges") if isinstance(col_meta, dict) else None
            if edges and edges != "planar":
                edges_by_col[name] = edges
        return edges_by_col
    except Exception:
        return {}


def _apply_nonplanar_edges(
    edges_by_col: dict[str, str],
    output_file: str,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    verbose: bool = False,
) -> None:
    """Re-attach non-planar edges declarations to an output's geo metadata (#588).

    DuckDB has no GEOGRAPHY type, so rewrites demote native GEOGRAPHY columns
    to GEOMETRY and drop ``edges`` from regenerated metadata. Losing the edge
    interpretation silently corrupts semantics (great-circle edges read as
    planar), so re-attach it to the output's geo metadata.
    """
    if not edges_by_col:
        return

    pf = pq.ParquetFile(output_file)
    try:
        kv = pf.metadata.metadata or {}
        if b"geo" not in kv:
            warn(
                "Input declares non-planar edges but output has no geo metadata "
                f"to carry them ({', '.join(sorted(edges_by_col))}). Edge "
                "interpretation is lost; avoid parquet-geo-only for geography data."
            )
            return
        geo_meta = json.loads(kv[b"geo"].decode("utf-8"))
    finally:
        # Release the read handle before rewriting (Windows requires it).
        pf.close()

    geo_version = str(geo_meta.get("version") or "")
    changed = False
    for name, edges in edges_by_col.items():
        edges = _edges_for_output_version(edges, geo_version)
        col_meta = geo_meta.get("columns", {}).get(name)
        if isinstance(col_meta, dict) and col_meta.get("edges") != edges:
            col_meta["edges"] = edges
            changed = True

    if not changed:
        return

    _rewrite_file_with_geo_metadata(
        output_file, geo_meta, compression, compression_level, row_group_rows
    )
    # Neutral wording: the input may have declared edges via metadata only,
    # without ever having a native GEOGRAPHY logical type.
    warn(
        "Preserved non-planar edges declaration in geo metadata for: "
        f"{', '.join(sorted(edges_by_col))}. Note: DuckDB has no geography "
        "type, so any native GEOGRAPHY input is rewritten as GEOMETRY (the "
        "edges metadata carries the interpretation)."
    )


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
) -> None:
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
            Supplying it alongside GEOPARQUET_VERSION 'V2' keeps the native
            GEOMETRY logical type and its geospatial statistics — only the KV
            entry is replaced.
        extra_kv_metadata: Non-geo sidecar keys (fiboa, vecorel, STAC fragments)
            to carry into the output. DuckDB accepts KV_METADATA alongside
            GEOPARQUET_VERSION, so these ride along on the fast path instead of
            forcing a full metadata rewrite for an unrelated key (#709).
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
    final_query = _wrap_query_with_crs(query, geometry_column, input_crs)

    # Build options list
    options = [
        "FORMAT PARQUET",
        f"COMPRESSION {duckdb_compression}",
        f"GEOPARQUET_VERSION '{duckdb_version}'",
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
    kv_clause = build_kv_metadata_clause(kv_pairs)
    if kv_clause:
        options.append(kv_clause)

    copy_query = f"""
        COPY ({final_query})
        TO {sql_path(output_path)}
        ({", ".join(options)})
    """

    if verbose:
        debug(f"Executing plain COPY TO with {duckdb_compression} compression...")

    con.execute(copy_query)

    # DuckDB 1.5.4's V2 writer omits the geo KV metadata for geometries with
    # an M dimension (XY/XYZ are written correctly). Without it the output
    # silently degrades to parquet-geo-only, so repair it from the file's own
    # native geospatial statistics (#589).
    if duckdb_version == "V2":
        _ensure_v2_geo_metadata(
            output_path,
            compression=duckdb_compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            verbose=verbose,
            primary_column=geometry_column,
        )

    if verbose:
        import pyarrow.parquet as pq

        # Closed before the caller moves or uploads this file: `--verbose` must
        # not be the difference between a fix that lands and one that dies on
        # `Could not move file: Access is denied` (#1023, #1032).
        with pq.ParquetFile(output_path) as pf:
            success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")


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

    try:
        output_columns = _get_query_columns(con, query)
    except (duckdb.Error, RuntimeError, ValueError, AttributeError) as e:
        if verbose:
            debug(f"Could not read output schema to prune geo metadata: {e}")
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
    """The `geo` block the 2.0 fast path must write instead of DuckDB's generated one.

    DuckDB regenerates the `geo` key on the fast path, and its generated block
    carries only `version`, `primary_column`, `encoding`, `geometry_types` and
    `bbox`. Everything else the input declared — a `covering` (#738), `epoch`,
    `orientation` (#772) — is silently dropped. Returning the carried block here
    lets `_plain_copy_to` write it verbatim, keeping the fast path's write
    configuration intact: forcing the rewrite instead would clamp threads and
    memory, drop `--compression-level`, and can add a full stats rescan.

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

    Returns None when the version is not 2.0 (1.x already rewrites), when the
    input resolves to more than one file (see below), when the carried block is
    too thin to stand in for DuckDB's (a caller invalidated the derived stats and
    only the rewrite path can recompute them), or when it says nothing DuckDB
    would not write itself — the caller then keeps its existing behaviour.
    """
    if effective_version != "2.0" or not geometry_column or not original_metadata:
        return None

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
        return None

    # A write-path reader in the strongest sense: whatever comes back is written
    # to the output file verbatim, so the block goes through the shared shape
    # check first. `columns` as a list or a string used to abort the write with
    # `'list' object has no attribute 'get'` on the next line (#947).
    geo_dict = sanitized_carried_geo(original_metadata)
    if not geo_dict:
        return None
    col_meta = (geo_dict.get("columns") or {}).get(geometry_column)
    if not isinstance(col_meta, dict):
        return None
    if any(field not in col_meta for field in _REQUIRED_CARRIED_GEO_FIELDS):
        return None
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
        )
    if not _carries_more_than_duckdb_generates(carried):
        return None
    return carried


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
        memory_limit: DuckDB memory limit for streaming writes (e.g., '2GB', '512MB').
            If None, auto-detects based on available system/container memory.
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
            "not known" list rather than losing the key, since nothing here
            recomputes it (#934).
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
    from geoparquet_io.core.write_strategies import (
        WriteStrategy,
        WriteStrategyFactory,
        needs_metadata_rewrite,
    )

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
    # Every strategy recomputes `geometry_column` and only that, so a stripped
    # SECONDARY column would be left with no `geometry_types` at all -- a key
    # GeoParquet 1.1 requires and DuckDB refuses to open a file without. Naming
    # the recomputed column leaves the others the "not known" sentinel (#934).
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
    # This ensures covering metadata is written even for 2.0→2.0 operations
    if custom_metadata and "covering" in custom_metadata:
        rewrite_needed = True
        if verbose:
            debug("Forcing metadata rewrite for covering metadata")

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
            _plain_copy_to(
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
                carry_geo_metadata=_geo_block_to_carry_on_fast_path(
                    original_metadata,
                    geometry_column,
                    effective_version,
                    con=con,
                    query=query,
                    verbose=verbose,
                    input_crs=input_crs,
                    input_file=input_file,
                    output_columns=output_columns,
                ),
                extra_kv_metadata=extra_kv_metadata,
            )
        else:
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
                already_native = bool(
                    input_encoding and input_encoding.lower() not in ("wkb", "wkt")
                )
                if not already_native and write_strategy != "streaming":
                    if verbose:
                        debug("Routing 1.1-geoarrow WKB input through arrow-streaming")
                    write_strategy = "streaming"
                    auto_routed_strategy = True

            strategy_enum = WriteStrategy(write_strategy)
            strategy = WriteStrategyFactory.get_strategy(strategy_enum)

            # Only duckdb-kv can honour a memory limit. If *we* rerouted the
            # strategy (1.1-geoarrow above), the user did nothing wrong: warn and
            # drop the limit rather than aborting a command that worked before
            # --write-memory was plumbed through (#663). A strategy the user
            # explicitly asked for is a real error — raised as a core exception so
            # the CLI shows a clean message instead of a traceback.
            if memory_limit is not None and strategy_enum != WriteStrategy.DUCKDB_KV:
                if auto_routed_strategy:
                    warn(
                        "--write-memory is ignored for GeoParquet 1.1-geoarrow output: "
                        "GeoArrow encoding requires the arrow-streaming write strategy, "
                        "which does not support a memory limit."
                    )
                    memory_limit = None
                else:
                    raise InvalidParameterError(
                        "--write-memory",
                        f"a memory limit is only supported with the 'duckdb-kv' "
                        f"write strategy, not '{write_strategy}'",
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
        # invalidated (#601) are excluded rather than re-attached.
        _preserve_edges_after_write(
            input_file,
            original_metadata,
            actual_output,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            verbose=verbose,
            exclude_columns=drop_nonplanar_edges_columns,
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


def _preserve_edges_after_write(
    input_file: str | None,
    original_metadata: dict | None,
    output_path: str,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_rows: int | None = None,
    verbose: bool = False,
    exclude_columns: Collection[str] | None = None,
) -> None:
    """Restore non-planar edges dropped by the writer, on any rewrite path.

    Prefers the input file (sees native GEOGRAPHY logical types as well as geo
    metadata); falls back to the input's KV metadata dict when only that is
    available (e.g. partition staging rewrites). ``exclude_columns`` names
    columns whose declaration the write itself invalidated (#601): the input
    still declares them, but they must not come back.
    """
    edges_by_col = collect_nonplanar_edges(input_file) if input_file else {}
    if not edges_by_col:
        edges_by_col = _collect_nonplanar_edges_from_metadata(original_metadata)
    if exclude_columns:
        edges_by_col = {k: v for k, v in edges_by_col.items() if k not in exclude_columns}
    _apply_nonplanar_edges(
        edges_by_col,
        output_path,
        compression=compression,
        compression_level=compression_level,
        row_group_rows=row_group_rows,
        verbose=verbose,
    )


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

        # Upload to remote if needed
        if is_remote:
            upload_if_remote(
                actual_output,
                output_file,
                profile=profile,
                is_directory=False,
                verbose=verbose,
            )
