"""
DuckDB-based Parquet metadata extraction.

This module provides functions to read Parquet metadata using DuckDB's
built-in parquet functions instead of fsspec/PyArrow. This simplifies
remote file access (S3, HTTP, etc.) by leveraging DuckDB's httpfs extension.

Performance Optimization (Issue #232):
For LOCAL files, we use PyArrow for metadata reads which is ~300x faster
than DuckDB (0.1ms vs 44ms). DuckDB is still used for remote files (S3, HTTP)
where PyArrow would require fsspec configuration.
"""

import json
import re
from collections.abc import Callable
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.column_selection import named_entries
from geoparquet_io.core.crs_utils import (
    _projjson_from_authority,
    column_is_geographic,
    geoarrow_crs_to_projjson,
)
from geoparquet_io.core.duckdb_utils import (
    describe_query,
    fetch_one,
    sql_literal,
    sql_path,
)
from geoparquet_io.core.edges import logical_type_algorithm
from geoparquet_io.core.exceptions import (
    FailureKind,
    GeoParquetError,
    InvalidParameterError,
    classify_failure,
    not_parquet_file_error,
)
from geoparquet_io.core.file_utils import (
    get_first_parquet_file,
    is_partition_path,
    resolve_file_url,
)
from geoparquet_io.core.geo_metadata import row_group_geometry_stats
from geoparquet_io.core.geoarrow_compat import srid_crs_as_text
from geoparquet_io.core.parquet_schema import root_schema_columns
from geoparquet_io.core.remote import is_remote_url, is_s3_url, needs_httpfs

# =============================================================================
# PyArrow Fast Path for Local Files (Issue #232 Performance Fix)
# =============================================================================


def _is_local_file(path: str) -> bool:
    """Check if path is a local file (not remote URL)."""

    return not is_remote_url(path)


def _resolve_local_path(parquet_file: str) -> str:
    """Resolve local path, handling partitions and globs.

    For partitioned datasets, returns the first parquet file.
    """

    if is_partition_path(parquet_file):
        first_file = get_first_parquet_file(parquet_file)
        if first_file:
            return first_file
    return parquet_file


def _pyarrow_get_kv_metadata(parquet_file: str) -> dict[bytes, bytes]:
    """Get key-value metadata using PyArrow (fast path for local files).

    The one footer read for both the KV and the ``geo`` readers.
    """
    try:
        # Closed on exit, not left to the collector: an in-place `check --fix`
        # renames over this file next, and Windows refuses while a reader holds
        # it (#1032).
        with pq.ParquetFile(parquet_file) as pf:
            metadata = pf.metadata.metadata
        return dict(metadata) if metadata else {}
    except Exception as e:
        error_msg = str(e)
        if classify_failure(e).kind is FailureKind.NOT_PARQUET:
            raise not_parquet_file_error(parquet_file) from e
        raise GeoParquetError(f"Cannot read file: {parquet_file}\n{error_msg}") from e


def _pyarrow_get_file_metadata(parquet_file: str) -> dict:
    """Get file-level metadata using PyArrow (fast path for local files)."""
    try:
        metadata = pq.read_metadata(parquet_file)
        return {
            "num_rows": metadata.num_rows,
            "num_row_groups": metadata.num_row_groups,
            "created_by": metadata.created_by,
            "format_version": metadata.format_version,
        }
    except Exception as e:
        raise GeoParquetError(f"Cannot read file: {parquet_file}\n{str(e)}") from e


def _pyarrow_get_schema_info(parquet_file: str) -> list[dict] | None:
    """Get schema info using PyArrow (fast path for local files).

    Returns list of dicts matching DuckDB's parquet_schema() format,
    or None if PyArrow cannot handle the file (e.g., unsupported CRS format).
    """
    try:
        with pq.ParquetFile(parquet_file) as pf:
            schema = pf.schema_arrow
            parquet_schema = pf.schema  # Physical Parquet schema
        result = []

        # Build a map from TOP-LEVEL column name to Parquet physical schema for
        # logical type lookup. `parquet_schema.column(i)` walks *leaves*, whose
        # `name` is the leaf's own, so a `GEOMETRY` leaf nested in a struct
        # (`meta.label`) was keyed as "label" and overwrote the real top-level
        # column of that name -- which is how a VARCHAR column came to be
        # reported with a Geometry logical type, and read back as a secondary
        # geometry column by anything deriving geometry columns from this listing
        # (#1175). A leaf whose `path` is its `name` is top-level by definition.
        parquet_col_map = {}
        for i in range(len(parquet_schema)):
            col = parquet_schema.column(i)
            if col.path == col.name:
                parquet_col_map[col.name] = col

        # Get schema info with proper type mapping
        for field in schema:
            # Prefer the Parquet physical schema's native GEOMETRY/GEOGRAPHY
            # logical type: it is stable, while the Arrow-level view of the
            # same column flips to a geoarrow extension type (dropping the
            # Geometry/Geography distinction) once geoarrow.pyarrow has been
            # imported anywhere in the process.
            logical_type = None
            if field.name in parquet_col_map:
                parquet_col = parquet_col_map[field.name]
                if parquet_col.logical_type is not None:
                    logical_type_str = str(parquet_col.logical_type)
                    # PyArrow returns "Geometry(crs=srid:5070)" format
                    # DuckDB returns "GeometryType(crs=srid:5070)" format
                    if logical_type_str.startswith("Geometry("):
                        # Convert to DuckDB format
                        logical_type = "GeometryType" + logical_type_str[8:]
                    elif logical_type_str.startswith("Geography("):
                        logical_type = "GeographyType" + logical_type_str[9:]

            # Fall back to the Arrow schema (GeoArrow extension types, e.g.
            # 1.1-geoarrow files with no native Parquet logical type)
            if logical_type is None:
                logical_type = _get_pyarrow_logical_type(field)

            # Report the storage type for extension fields: whether pyarrow
            # sees geoarrow.wkb here depends on process-global extension
            # registration (importing geoarrow.pyarrow anywhere flips it), and
            # schema info must not vary with import order.
            field_type = field.type
            # BaseExtensionType also covers C++-defined extensions (e.g.
            # pyarrow's built-in ones), which pa.ExtensionType would miss.
            if isinstance(field_type, pa.BaseExtensionType):
                field_type = field_type.storage_type

            # Only field-bearing types are iterable, so a list/map column
            # renders as a single row with no children while a struct renders
            # its fields. ``num_children`` therefore has to count the rows this
            # listing actually goes on to emit, not the type's fields --
            # consumers read the listing as a depth-first tree and skip a
            # subtree by that count, so a column claiming children it never
            # emits swallows the next root column (issue #931). Nested struct
            # children are emitted with ``num_children: 0`` for the same
            # reason: their own fields are not rendered either.
            children = list(field.type) if hasattr(field.type, "__iter__") else []

            col_info = {
                "name": field.name,
                "type": str(field_type),
                "type_length": None,
                "repetition_type": "OPTIONAL" if field.nullable else "REQUIRED",
                "num_children": len(children),
                "converted_type": None,
                "scale": None,
                "precision": None,
                "field_id": field.metadata.get(b"PARQUET:field_id", None)
                if field.metadata
                else None,
                "logical_type": logical_type,
            }

            result.append(col_info)

            for child_field in children:
                child_info = {
                    "name": child_field.name,
                    "type": str(child_field.type),
                    "type_length": None,
                    "repetition_type": "OPTIONAL" if child_field.nullable else "REQUIRED",
                    "num_children": 0,
                    "converted_type": None,
                    "scale": None,
                    "precision": None,
                    "field_id": None,
                    "logical_type": None,
                }
                result.append(child_info)

        return result
    except ValueError as e:
        # geoarrow-pyarrow may fail on unsupported CRS formats (e.g., srid:5070)
        # Return None to fall back to DuckDB
        if "Can't create geoarrow.types.Crs" in str(e):
            return None
        raise GeoParquetError(f"Cannot read schema: {parquet_file}\n{str(e)}") from e
    except Exception as e:
        raise GeoParquetError(f"Cannot read schema: {parquet_file}\n{str(e)}") from e


def _format_geometry_type_crs(crs) -> str:
    """Render a CRS into DuckDB's ``GeometryType(crs=...)`` logical-type string.

    A string stays verbatim (a reference such as ``projjson:key_name``,
    ``srid:5070`` or a bare ``EPSG:32633``); PROJJSON is inlined as JSON.
    """
    if isinstance(crs, str):
        return f"GeometryType(crs={crs})"
    return f"GeometryType(crs={json.dumps(crs)})"


def _get_pyarrow_logical_type(field) -> str | None:
    """Extract logical type string from PyArrow field, including geo types.

    Returns DuckDB-compatible logical type string like:
    - GeometryType(crs=projjson:key_name)
    - GeometryType(crs=srid:5070)
    - GeometryType(crs=EPSG:32633)  (bare authority code)
    - GeometryType(crs={...})  (inline PROJJSON)
    - GeometryType()  (no CRS)

    Handles two scenarios:
    1. geoarrow-pyarrow imported: CRS is in field.type.crs attribute
    2. geoarrow-pyarrow not imported: CRS is in field.metadata['ARROW:extension:metadata']
    """
    geoarrow_extensions = (
        "geoarrow.wkb",
        "ogc.wkb",
        "geoarrow.point",
        "geoarrow.linestring",
        "geoarrow.polygon",
        "geoarrow.multipoint",
        "geoarrow.multilinestring",
        "geoarrow.multipolygon",
        "geoarrow.geometry",
    )

    # Check for extension type registered with PyArrow
    if hasattr(field.type, "extension_name"):
        ext_name = field.type.extension_name
        if ext_name in geoarrow_extensions:
            # Case 1: geoarrow-pyarrow is imported - CRS is in field.type.crs
            # This takes priority because geoarrow-pyarrow consumes the metadata.
            # `geoarrow_crs_to_projjson` is the one reader for that attribute
            # (issue #863): it tries to_json_dict() then to_json(), passes plain
            # dicts/strings/bytes through, and warns instead of raising or
            # silently dropping a CRS it cannot read.
            resolved = geoarrow_crs_to_projjson(getattr(field.type, "crs", None))
            if resolved is not None:
                return _format_geometry_type_crs(resolved)

            # Case 2: Try extension_metadata (older geoarrow or direct Arrow)
            if hasattr(field.type, "extension_metadata") and field.type.extension_metadata:
                try:
                    ext_meta = json.loads(field.type.extension_metadata)
                    crs = srid_crs_as_text(ext_meta).get("crs")
                    if crs:
                        return _format_geometry_type_crs(crs)
                except (json.JSONDecodeError, TypeError):
                    pass
            return "GeometryType()"

    # Check field metadata for GeoArrow extension types (Arrow IPC schema stores these here)
    if field.metadata:
        ext_name = field.metadata.get(b"ARROW:extension:name")
        if ext_name:
            ext_name_str = ext_name.decode("utf-8") if isinstance(ext_name, bytes) else ext_name
            if ext_name_str in (
                "geoarrow.wkb",
                "ogc.wkb",
                "geoarrow.point",
                "geoarrow.linestring",
                "geoarrow.polygon",
                "geoarrow.multipoint",
                "geoarrow.multilinestring",
                "geoarrow.multipolygon",
                "geoarrow.geometry",
            ):
                # Get extension metadata
                ext_meta_raw = field.metadata.get(b"ARROW:extension:metadata")
                if ext_meta_raw:
                    try:
                        ext_meta_str = (
                            ext_meta_raw.decode("utf-8")
                            if isinstance(ext_meta_raw, bytes)
                            else ext_meta_raw
                        )
                        ext_meta = json.loads(ext_meta_str)
                        crs = ext_meta.get("crs")
                        if crs:
                            return _format_geometry_type_crs(crs)
                    except (json.JSONDecodeError, TypeError):
                        pass
                return "GeometryType()"

    # Check for nested struct types
    type_str = str(field.type)
    if "struct" in type_str.lower():
        return None

    return None


# =============================================================================
# DuckDB Connection Management
# =============================================================================


def _get_connection_for_file(
    parquet_file: str, existing_con=None, load_spatial: bool | None = None
):
    """Get or create DuckDB connection appropriate for file location.

    Args:
        parquet_file: Path or URL to the parquet file
        existing_con: Existing connection to reuse
        load_spatial: Whether to load spatial extension. Set to False for
                     metadata-only operations to save ~100ms. The default
                     (None) is the factory's auto mode, resolved by
                     :func:`get_duckdb_connection` itself: spatial only when
                     DuckDB's core lacks the GEOMETRY type (#1353); every
                     caller that leaves it unset runs nothing beyond
                     ``parquet_metadata()``/``parquet_schema()``/``DESCRIBE``,
                     which DuckDB >= 2.0 covers without spatial.

    Returns:
        Tuple of (connection, should_close)
    """
    from geoparquet_io.core.duckdb_utils import (
        get_duckdb_connection,
        get_duckdb_connection_for_s3,
    )

    if existing_con:
        return existing_con, False  # False = don't close

    if is_s3_url(parquet_file):
        return get_duckdb_connection_for_s3(parquet_file, load_spatial=load_spatial), True
    else:
        return get_duckdb_connection(
            load_spatial=load_spatial, load_httpfs=needs_httpfs(parquet_file)
        ), True


def _metadata_path(parquet_file: str) -> str:
    """Resolve the RAW path DuckDB should read for a metadata query.

    For partitioned datasets (directories or glob patterns), returns
    path to the first file for metadata operations that require a single file.

    The result is **RAW**, not escaped: every caller hands it to
    :func:`~geoparquet_io.core.duckdb_utils.sql_path`, which quotes and escapes
    it in one step at the SQL boundary. Escaping here as well would double the
    doubling (#802).
    """

    # For partitions, use first file for metadata operations
    if is_partition_path(parquet_file):
        first_file = get_first_parquet_file(parquet_file)
        if first_file:
            parquet_file = first_file

    return resolve_file_url(parquet_file, verbose=False)


def get_kv_metadata(parquet_file: str, con=None) -> dict[bytes, bytes]:
    """
    Extract key-value metadata using parquet_kv_metadata().

    Returns dict like {b'geo': b'{"version": "1.1.0", ...}'}

    For local files, uses PyArrow for ~300x faster reads.
    For remote files (S3, HTTP), uses DuckDB with httpfs.

    Raises:
        GeoParquetError: If file is not a valid Parquet file or cannot be read.
    """
    # Fast path: use PyArrow for local files (no existing connection required)
    resolved_path = _resolve_local_path(parquet_file)
    if _is_local_file(resolved_path) and con is None:
        return _pyarrow_get_kv_metadata(resolved_path)

    # Slow path: use DuckDB for remote files or when connection is provided
    # Convert to dict with bytes keys for backward compatibility
    kv_dict = {}
    for key_bytes, val_bytes in _read_kv_entries(parquet_file, con):
        if key_bytes:
            kv_dict[key_bytes] = val_bytes
    return kv_dict


def count_kv_key(parquet_file: str, key: str, con=None) -> int:
    """How many file-level key/value entries the footer carries under ``key``.

    The Parquet footer is a list, not a map, so a key can repeat. Every dict
    this module returns collapses the repeats to one value, which is why a
    footer with two ``geo`` entries looked fine to ``gpio check spec`` (#1229).
    Read through DuckDB, local files included, since pyarrow's
    ``FileMetaData.metadata`` is a dict too. DuckDB refuses a footer whose
    ``geo`` value it cannot parse; ``parquet_footer.read_footer_kv`` reads a
    local footer without that limit.

    Raises:
        GeoParquetError: If file is not a valid Parquet file or cannot be read.
    """
    wanted = key.encode("utf-8")
    return sum(1 for key_bytes, _ in _read_kv_entries(parquet_file, con) if key_bytes == wanted)


def _read_kv_entries(parquet_file: str, con=None) -> list[tuple[bytes | None, bytes | None]]:
    """Every footer key/value entry, in order and with repeats, read by DuckDB."""
    import duckdb

    metadata_path = _metadata_path(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con, load_spatial=False)

    try:
        # Get raw bytes to avoid DuckDB's VARCHAR escaping
        result = connection.execute(f"""
            SELECT key, value
            FROM parquet_kv_metadata({sql_path(metadata_path)})
        """).fetchall()

        # Handle both bytes and string types
        return [
            (
                k if isinstance(k, bytes) else k.encode("utf-8") if k else None,
                v if isinstance(v, bytes) else v.encode("utf-8") if v else None,
            )
            for k, v in result
        ]
    except duckdb.InvalidInputException as e:
        # File is not a valid Parquet file (e.g., wrong format, corrupt)
        error_msg = str(e)
        if classify_failure(e).kind is FailureKind.NOT_PARQUET:
            raise not_parquet_file_error(parquet_file) from e
        raise GeoParquetError(f"Invalid Parquet file: {parquet_file}\n{error_msg}") from e
    except duckdb.IOException as e:
        # File not found, permission denied, or other I/O error
        raise GeoParquetError(f"Cannot read file: {parquet_file}\n{str(e)}") from e
    finally:
        if should_close:
            connection.close()


def get_file_metadata(parquet_file: str, con=None) -> dict:
    """
    Get file-level metadata (num_rows, num_row_groups).

    Uses parquet_file_metadata() for fast access.

    For local files, uses PyArrow for ~300x faster reads.
    For remote files (S3, HTTP), uses DuckDB with httpfs.
    """
    # Fast path: use PyArrow for local files
    resolved_path = _resolve_local_path(parquet_file)
    if _is_local_file(resolved_path) and con is None:
        return _pyarrow_get_file_metadata(resolved_path)

    # Slow path: use DuckDB for remote files or when connection is provided
    metadata_path = _metadata_path(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con, load_spatial=False)

    try:
        result = fetch_one(
            connection.execute(f"""
            SELECT * FROM parquet_file_metadata({sql_path(metadata_path)})
        """)
        )

        columns = [desc[0] for desc in connection.description]
        return dict(zip(columns, result, strict=True)) if result else {}
    finally:
        if should_close:
            connection.close()


def get_schema_info(parquet_file: str, con=None) -> list[dict]:
    """
    Get schema column info using parquet_schema().

    Returns list of dicts with 'name', 'type', 'logical_type', etc.

    For local files, uses PyArrow for ~300x faster reads.
    For remote files (S3, HTTP), uses DuckDB with httpfs.
    """
    # Fast path: use PyArrow for local files
    resolved_path = _resolve_local_path(parquet_file)
    if _is_local_file(resolved_path) and con is None:
        result = _pyarrow_get_schema_info(resolved_path)
        if result is not None:
            return result
        # Fall through to DuckDB if PyArrow couldn't handle the file

    # Slow path: use DuckDB for remote files or when connection is provided
    metadata_path = _metadata_path(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con, load_spatial=False)

    try:
        result = connection.execute(f"""
            SELECT * FROM parquet_schema({sql_path(metadata_path)})
        """).fetchall()

        columns = [desc[0] for desc in connection.description]
        return [dict(zip(columns, row, strict=True)) for row in result]
    finally:
        if should_close:
            connection.close()


def get_column_names(parquet_file: str, con=None) -> list[str]:
    """Get list of column names from schema."""
    schema = get_schema_info(parquet_file, con)
    # Filter out the schema root element and nested struct fields: the listing is
    # depth first, so a struct's children have to be skipped as whole subtrees.
    return [col["name"] for col in root_schema_columns(schema)]


def get_usable_columns(parquet_file: str, con=None) -> list[dict]:
    """
    Get user-facing columns as DuckDB exposes them.

    Describes ``SELECT * FROM read_parquet()``, which returns the actual
    columns users can query. This handles quirky Parquet files that wrap
    all data in a struct (e.g., a 'schema' struct containing all fields).

    Returns list of dicts with 'name' and 'type' keys.
    """
    metadata_path = _metadata_path(parquet_file)
    connection, should_close = _get_connection_for_file(parquet_file, con)

    try:
        described = describe_query(
            connection, f"SELECT * FROM read_parquet({sql_path(metadata_path)})"
        )
        return [{"name": name, "type": type_} for name, type_ in described]
    finally:
        if should_close:
            connection.close()


def get_usable_column_names(
    parquet_file: str, con=None, *, read_expr: str | None = None
) -> list[str]:
    """The column names a query over the input can name (#963).

    The one answer to "can the SQL I am about to write name this column?". The
    physical schema (:func:`get_column_names`) cannot say yes to a hive
    partition column, which ``read_parquet`` synthesizes from the directory
    names and no file carries.

    By default this is :func:`get_usable_columns`: the first file's
    ``SELECT * FROM read_parquet()``, which exposes a hive column because DuckDB
    detects ``key=value`` directories from that file's path. A caller that reads
    more than that -- the union of every file under ``--allow-schema-diff``, an
    explicit ``hive_partitioning`` -- passes the read expression it will use as
    ``read_expr``, and the names are those of ``SELECT * FROM <read_expr>``.
    """
    if read_expr is None:
        return [col["name"] for col in get_usable_columns(parquet_file, con)]
    connection, should_close = _get_connection_for_file(parquet_file, con)
    try:
        return [name for name, _ in describe_query(connection, f"SELECT * FROM {read_expr}")]
    finally:
        if should_close:
            connection.close()


def get_row_group_metadata(parquet_file: str, con=None) -> list[dict]:
    """
    Get per-row-group statistics using parquet_metadata().

    Returns list of dicts with row_group_id, path_in_schema, stats_min, stats_max, etc.
    """
    return _parquet_metadata_rows(parquet_file, con, "SELECT * FROM {source}", load_spatial=None)


def _parquet_metadata_rows(
    parquet_file: str, con, select: str | Callable[[str], str], *, load_spatial: bool | None
) -> list[dict]:
    """Rows of one query over the file's ``parquet_metadata()``, as dicts.

    The one reader behind the per-row-group footer views here
    (:func:`get_row_group_metadata`, :func:`get_compression_stats`,
    :func:`get_row_group_sizes`): ``select`` names the table function as
    ``{source}``; ``load_spatial`` is what differed between them (the raw view
    keeps the connection factory's auto mode, the aggregates skip spatial). The connection is the caller's ``con`` or one
    opened for the file and closed here.
    """
    metadata_path = _metadata_path(parquet_file)
    connection, should_close = _get_connection_for_file(
        parquet_file, con, load_spatial=load_spatial
    )
    try:
        source = f"parquet_metadata({sql_path(metadata_path)})"
        # A constant query names the table function as ``{source}``; a query that
        # interpolates a column name is a callable taking it, so no brace in the
        # name can reach ``str.format``.
        query = select(source) if callable(select) else select.format(source=source)
        # The result's own description: a wrapped connection (validate's
        # reopening one) answers ``execute`` but carries no ``description``.
        result = connection.execute(query)
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
    finally:
        if should_close:
            connection.close()


def get_compression_stats(parquet_file: str, con=None) -> list[dict]:
    """Get per-column compression statistics from Parquet metadata.

    Queries parquet_metadata() and aggregates compressed/uncompressed sizes
    per column across all row groups.

    Args:
        parquet_file: Path or URL to the parquet file
        con: Optional existing DuckDB connection

    Returns:
        List of dicts ordered by compressed_bytes descending, each with:
        - column_name: Column path in schema
        - compression: Compression codec name
        - compressed_bytes: Total compressed size in bytes
        - uncompressed_bytes: Total uncompressed size in bytes
        - ratio: Compression ratio (uncompressed / compressed), rounded to 2 decimals
    """
    return _parquet_metadata_rows(
        parquet_file,
        con,
        """
            SELECT
                path_in_schema AS column_name,
                ANY_VALUE(compression) AS compression,
                SUM(total_compressed_size)::BIGINT AS compressed_bytes,
                SUM(total_uncompressed_size)::BIGINT AS uncompressed_bytes,
                ROUND(
                    SUM(total_uncompressed_size)::FLOAT
                    / NULLIF(SUM(total_compressed_size), 0),
                    2
                ) AS ratio
            FROM {source}
            GROUP BY path_in_schema
            ORDER BY compressed_bytes DESC
        """,
        load_spatial=False,
    )


def get_row_count(parquet_file: str, con=None) -> int:
    """Get total row count from file metadata."""
    metadata = get_file_metadata(parquet_file, con)
    return metadata.get("num_rows", 0)


def get_num_row_groups(parquet_file: str, con=None) -> int:
    """Get number of row groups from file metadata."""
    metadata = get_file_metadata(parquet_file, con)
    return metadata.get("num_row_groups", 0)


#: The prefixed CRS forms, which have the same ``a:b`` shape as a bare
#: ``<authority>:<code>`` and must therefore be matched *before* it. ``srid:`` is
#: an SRID (``srid:0`` meaning "unknown"), not an authority named "srid", and
#: ``projjson:`` names a file-metadata key, not a CRS code.
_PREFIXED_CRS_FORMS = ("projjson:", "srid:")

#: A bare ``<authority>:<code>`` CRS, e.g. ``EPSG:32633`` or ``OGC:CRS84``.
#:
#: The Parquet geospatial spec permits it alongside ``srid:``, ``projjson:`` and
#: inline PROJJSON, and geoarrow-based writers emit it whenever their CRS is a
#: plain authority code. Deliberately narrow: an authority is an identifier and
#: a code is alphanumeric, so neither ``<null>`` nor an empty ``crs=`` (what
#: DuckDB emits for a type that declares no CRS) can match.
_AUTHORITY_CODE_CRS = re.compile(r"^[A-Za-z][A-Za-z0-9_.\-]*:[A-Za-z0-9_.\-]+$")


def _authority_code_crs(crs_value: Any) -> tuple[str, str] | None:
    """The ``(authority, code)`` a bare ``<authority>:<code>`` CRS names, else None.

    Returns None for the prefixed forms (``srid:``, ``projjson:``), which share
    the shape but not the meaning, and for every value that is not that form.
    """
    if not isinstance(crs_value, str):
        return None
    token = crs_value.strip()
    if token.lower().startswith(_PREFIXED_CRS_FORMS):
        return None
    if not _AUTHORITY_CODE_CRS.match(token):
        return None
    authority, code = token.split(":", 1)
    return authority, code


def resolve_authority_code_crs(crs_value: Any) -> Any:
    """Resolve a bare ``<authority>:<code>`` CRS to PROJJSON, via pyproj.

    Anything else -- another CRS form, an unknown authority, a code the PROJ
    database does not carry -- is returned unchanged, so callers can keep
    treating the value as opaque.
    """
    pair = _authority_code_crs(crs_value)
    if pair is None:
        return crs_value

    projjson = _projjson_from_authority(*pair)
    return crs_value if projjson is None else json.loads(projjson)


#: The DuckDB spellings of "this geo logical type declares no CRS at all".
#:
#: DuckDB renders a CRS-less type as ``crs=`` and a null one as ``crs=<null>``.
#: These two are the *only* values that may become None here: the Parquet spec
#: defines "no CRS" as OGC:CRS84, so anything else mapped to None would become a
#: CRS84 claim the file never made (#866).
_NO_CRS_TOKENS = frozenset({"", "<null>"})

#: DuckDB's own parameter after ``crs=`` on a ``GEOGRAPHY`` logical type.
#:
#: The Parquet ``crs`` field is free-form and may itself contain commas (WKT
#: does), so the value ends here rather than at the first comma.
_ALGORITHM_PARAM = ", algorithm="


def _parse_crs_property(params: str) -> Any:
    """The ``crs=`` property of a geo logical type's parameters, or None if it has none.

    Returns the property as the value it means:

    - ``crs=`` / ``crs=<null>`` -- the type declares no CRS: None
    - ``crs={...}`` -- inline PROJJSON: the parsed dict
    - anything else -- the literal string, for :func:`resolve_crs_reference` to
      resolve. That covers the spec's named forms (``srid:XXXX``,
      ``projjson:key``, a bare authority code like ``EPSG:32633``) *and* the
      free-form values the Parquet spec also permits (``4326``, ``WGS 84``,
      raw WKT). An unresolvable value stays a string all the way through, so
      the CRS checks compare it as-is and fail closed.

    Returning None for "declares no CRS" is what keeps the ``crs`` key out of
    :func:`parse_geometry_logical_type`'s result; callers read that absence as
    the Parquet spec's OGC:CRS84 default, so no *unrecognized* value may reach
    it -- that would turn a CRS the file names into a CRS84 claim it never made
    (#814, #866).
    """
    crs_start = params.find("crs=")
    if crs_start == -1:
        return None
    crs_value = params[crs_start + 4 :]

    if crs_value.startswith("{"):
        # Find the matching closing brace for inline PROJJSON.
        brace_count = 0
        end_pos = 0
        for i, char in enumerate(crs_value):
            if char == "{":
                brace_count += 1
            elif char == "}":
                brace_count -= 1
                if brace_count == 0:
                    end_pos = i + 1
                    break
        if end_pos == 0:
            # Unbalanced braces: malformed, but still a CRS the type *declares*.
            # Carry the raw value through as a string so it fails closed
            # downstream, exactly like unparsable-JSON below -- mapping it to
            # None would read as the OGC:CRS84 default (#866).
            return crs_value
        crs_json_str = crs_value[:end_pos]
        try:
            return json.loads(crs_json_str)
        except json.JSONDecodeError:
            return crs_json_str

    # Every other form is one free-form value, ending at DuckDB's own trailing
    # algorithm parameter when there is one.
    token = crs_value.split(_ALGORITHM_PARAM, 1)[0].strip()
    return None if token.lower() in _NO_CRS_TOKENS else token


def parse_geometry_logical_type(logical_type: str) -> dict | None:
    """
    Parse a Geometry/Geography logical type string from a Parquet schema.

    Handles strings like:
    - GeometryType(crs={"$schema": "...", "id": {"authority": "EPSG", "code": 4326}})
    - GeographyType(algorithm=spherical)
    - GeometryType(crs=<null>)
    - GeometryType(crs=EPSG:32633)

    The two readers spell the same type differently -- DuckDB's
    ``parquet_schema()`` renders ``GeometryType(...)``, PyArrow's
    ``ParquetLogicalType`` renders ``Geometry(...)`` -- so the ``Type`` suffix is
    optional. Rejecting PyArrow's spelling made the CRS invisible to the callers
    that read the schema through PyArrow (#785).

    Returns dict with keys: geo_type, geometry_type, coordinate_dimension, crs, algorithm

    The ``crs`` key tells three states apart, and callers must keep them apart:
    *absent* means the type declares no CRS (the Parquet spec's OGC:CRS84
    default), a dict is resolved inline PROJJSON, and a **string** is a CRS the
    type does name but this function does not resolve -- a reference form for
    :func:`resolve_crs_reference`, or a free-form value (``4326``, ``WGS 84``,
    WKT) that nothing can resolve. A string is never the default: reading one as
    CRS84 invents a claim the file never made (#866).
    """
    if not logical_type:
        return None

    match = re.match(r"(Geometry|Geography)(?:Type)?\((.*)\)$", logical_type, re.DOTALL)
    if not match:
        return None

    geo_type = match.group(1)
    params = match.group(2)

    result: dict[str, Any] = {"geo_type": geo_type}

    # Parse CRS if present. A type that declares no CRS leaves no "crs" key at
    # all, which callers read as the Parquet spec's OGC:CRS84 default.
    crs = _parse_crs_property(params)
    if crs is not None:
        result["crs"] = crs

    # Parse algorithm for Geography (the owner's reader, #1272).
    algorithm = logical_type_algorithm(params)
    if algorithm:
        result["algorithm"] = algorithm

    # Parse geometry type and coordinate dimension from positional params
    # Split by comma but respect braces
    parts = []
    current_part: list[str] = []
    brace_depth = 0
    for char in params:
        if char == "{":
            brace_depth += 1
            current_part.append(char)
        elif char == "}":
            brace_depth -= 1
            current_part.append(char)
        elif char == "," and brace_depth == 0:
            parts.append("".join(current_part).strip())
            current_part = []
        else:
            current_part.append(char)
    if current_part:
        parts.append("".join(current_part).strip())

    # First positional param is geometry type
    valid_geom_types = [
        "Point",
        "LineString",
        "Polygon",
        "MultiPoint",
        "MultiLineString",
        "MultiPolygon",
        "GeometryCollection",
    ]
    if len(parts) >= 1:
        geom_type = parts[0].strip()
        if geom_type in valid_geom_types:
            result["geometry_type"] = geom_type

    # Second positional param is coordinate dimension
    valid_coord_dims = ["XY", "XYZ", "XYM", "XYZM"]
    if len(parts) >= 2:
        coord_dim = parts[1].strip()
        if coord_dim in valid_coord_dims:
            result["coordinate_dimension"] = coord_dim

    return result


def native_geo_type(logical_type: object) -> str | None:
    """``"GEOMETRY"``, ``"GEOGRAPHY"`` or None for a column's Parquet logical type.

    The one answer to "is this column natively GEOMETRY or GEOGRAPHY?" (#1367).
    Takes the logical type as either reader renders it: DuckDB's
    ``parquet_schema()`` (``GeometryType(...)``/``GeographyType(...)``, the
    ``logical_type`` of a :func:`get_schema_info` row) or PyArrow's
    ``ColumnSchema.logical_type`` (``Geometry(...)``/``Geography(...)``).
    """
    text = str(logical_type or "")
    if text.startswith("Geometry"):
        return "GEOMETRY"
    if text.startswith("Geography"):
        return "GEOGRAPHY"
    return None


def is_native_geo_column(schema_info: list[dict] | None, column: str) -> str | None:
    """:func:`native_geo_type` of ``column`` in :func:`get_schema_info` rows; None if absent."""
    rows = schema_info or []
    for index in named_entries(rows, column):
        return native_geo_type(rows[index].get("logical_type"))
    return None


def is_geometry_column(logical_type: str) -> bool:
    """Whether ``logical_type`` is a native GEOMETRY or GEOGRAPHY (see :func:`native_geo_type`)."""
    return native_geo_type(logical_type) is not None


def resolve_crs_reference(parquet_file: str, crs_value: Any) -> Any:
    """
    Resolve a CRS value, looking up references from file metadata if needed.

    The Parquet geo spec allows CRS to be specified as:
    - Inline PROJJSON (dict)
    - Reference to metadata field: "projjson:key_name"
    - SRID reference: "srid:XXXX" (interpreted as EPSG:XXXX)
    - Bare authority code: "EPSG:32633"

    Args:
        parquet_file: Path to the parquet file
        crs_value: The CRS value from parse_geometry_logical_type, either:
            - A dict (already resolved PROJJSON)
            - A string like "projjson:key_name" (reference to metadata field)
            - A string like "srid:5070" (interpreted as EPSG:5070)
            - A string like "EPSG:32633" (a bare authority code)
            - None

    Returns:
        Resolved CRS as dict (PROJJSON) or None if not found/applicable
    """
    if crs_value is None:
        return None

    # If already a dict (inline PROJJSON), return as-is
    if isinstance(crs_value, dict):
        return crs_value

    # Handle projjson:key_name reference to file metadata. The prefixes are
    # matched case-insensitively, like the parser's guard that carried them here.
    if isinstance(crs_value, str) and crs_value.lower().startswith("projjson:"):
        key_name = crs_value[9:]  # Skip "projjson:"
        try:
            import pyarrow.parquet as pq

            pf = pq.ParquetFile(parquet_file)
            try:
                file_metadata = pf.metadata.metadata
                if file_metadata:
                    # Keys are bytes in PyArrow metadata
                    key_bytes = key_name.encode("utf-8")
                    if key_bytes in file_metadata:
                        projjson_str = file_metadata[key_bytes].decode("utf-8")
                        return json.loads(projjson_str)
            finally:
                # Callers resolve a CRS on the way to rewriting the file in
                # place; a lingering read handle makes os.replace fail on
                # Windows.
                pf.close()
        except Exception:
            pass  # Fall through to return the reference string
        return crs_value  # Return the reference string if resolution failed

    # Handle srid:XXXX format - convert to PROJJSON using pyproj
    if isinstance(crs_value, str) and crs_value.lower().startswith("srid:"):
        srid = crs_value[5:]  # Skip "srid:"
        if srid.strip() == "0":
            # `srid:0` is the type's spelling of an unknown CRS, not EPSG:0.
            return None
        try:
            from pyproj import CRS

            crs = CRS.from_authority("EPSG", int(srid))
            return crs.to_json_dict()
        except Exception:
            pass  # Fall through to return the original value
        return crs_value  # Return the srid string if resolution failed

    # Handle a bare <authority>:<code>, e.g. "EPSG:32633". Checked last: the
    # prefixed forms above share its shape and mean something else.
    return resolve_authority_code_crs(crs_value)


def detect_geometry_columns(parquet_file: str, con=None) -> dict[str, str]:
    """
    Detect GEOMETRY/GEOGRAPHY logical types from schema.

    Returns dict mapping column name to geo type ('Geometry' or 'Geography').
    """
    schema = get_schema_info(parquet_file, con)
    geo_columns = {}

    for col in schema:
        logical_type = col.get("logical_type") or ""
        name = col.get("name", "")

        if not name:
            continue

        kind = native_geo_type(logical_type)
        if kind is not None:
            geo_columns[name] = kind.title()

    return geo_columns


# Arrow storage types that hold coordinates rather than serialized bytes.
_NESTED_TYPE_PREFIXES = ("struct", "list", "large_list", "fixed_size_list")


def detect_native_geometry_columns(parquet_file: str, con=None) -> dict[str, str]:
    """The subset of ``detect_geometry_columns`` stored as Parquet GEOMETRY/GEOGRAPHY.

    The local schema reader also reports a GeoArrow extension column
    (``geoarrow.point`` and its kin) as ``GeometryType()``, which is right for
    "which columns are geometry" but wrong for "does this file use the native
    Parquet geo types". Those types annotate a BYTE_ARRAY, so a struct or list
    column carrying a GeoArrow extension name is never one, and a file whose
    only geometry is such a column is not ``parquet_geo_only`` (#624).
    """
    geo_columns = detect_geometry_columns(parquet_file, con)
    if not geo_columns:
        return geo_columns
    nested = {
        col.get("name")
        for col in get_schema_info(parquet_file, con)
        if col.get("num_children")
        or str(col.get("type") or "").lower().startswith(_NESTED_TYPE_PREFIXES)
    }
    return {name: kind for name, kind in geo_columns.items() if name not in nested}


def _bbox_stats_aggregates(bbox_column: str) -> str:
    """The four ``MIN/MAX(stats) FILTER`` columns over a bbox struct's chunk statistics."""

    # path_in_schema is compared against a string LITERAL, not an identifier.
    def field(axis: str) -> str:
        return sql_literal(f"{bbox_column}, {axis}")

    return f"""
                MIN(TRY_CAST(stats_min AS DOUBLE))
                    FILTER (WHERE path_in_schema = {field("xmin")}) as xmin,
                MIN(TRY_CAST(stats_min AS DOUBLE))
                    FILTER (WHERE path_in_schema = {field("ymin")}) as ymin,
                MAX(TRY_CAST(stats_max AS DOUBLE))
                    FILTER (WHERE path_in_schema = {field("xmax")}) as xmax,
                MAX(TRY_CAST(stats_max AS DOUBLE))
                    FILTER (WHERE path_in_schema = {field("ymax")}) as ymax"""


def get_per_row_group_bbox_stats(
    parquet_file: str, bbox_column: str = "bbox", con=None
) -> list[dict]:
    """
    Get bbox statistics per row group.

    Returns list of dicts with row_group_id, xmin, ymin, xmax, ymax.
    """
    # parquet_metadata() only: no spatial extension, which is most of a connection's cost.
    rows = _parquet_metadata_rows(
        parquet_file,
        con,
        lambda source: (
            f"""
            SELECT row_group_id, {_bbox_stats_aggregates(bbox_column)}
            FROM {source}
            GROUP BY row_group_id
            ORDER BY row_group_id
        """
        ),
        load_spatial=False,
    )
    keys = ("xmin", "ymin", "xmax", "ymax")
    return [
        {"row_group_id": row["row_group_id"], **{k: row[k] for k in keys}}
        for row in rows
        if all(row[k] is not None for k in keys)
    ]


def get_compression_info(parquet_file: str, column_name: str | None = None, con=None) -> dict:
    """
    Get compression information for columns.

    Returns dict mapping column path to compression algorithm.
    """
    where = f" WHERE path_in_schema = {sql_literal(column_name)}" if column_name else ""

    def query(source: str) -> str:
        return f"""
            SELECT DISTINCT path_in_schema, compression
            FROM {source}
        {where}"""

    # parquet_metadata() only: no spatial extension, which is most of a connection's cost.
    rows = _parquet_metadata_rows(parquet_file, con, query, load_spatial=False)
    return {row["path_in_schema"]: row["compression"] for row in rows}


def get_row_group_stats_summary(parquet_file: str, con=None) -> dict:
    """
    Get summary statistics about row groups.

    Returns dict with num_groups, total_rows, avg_rows_per_group, total_size, avg_group_size.
    """
    # parquet_metadata() only: no spatial extension, which is most of a connection's cost.
    connection, should_close = _get_connection_for_file(parquet_file, con, load_spatial=False)
    try:
        # Get file-level info
        file_meta = get_file_metadata(parquet_file, connection)
        num_groups = file_meta.get("num_row_groups", 0)
        total_rows = file_meta.get("num_rows", 0)

        # Get total size from row groups
        rows = _parquet_metadata_rows(
            parquet_file,
            connection,
            "SELECT SUM(total_compressed_size) AS total_size FROM {source}",
            load_spatial=False,
        )
    finally:
        if should_close:
            connection.close()

    total_size = rows[0]["total_size"] if rows and rows[0]["total_size"] else 0
    return {
        "num_groups": num_groups,
        "total_rows": total_rows,
        "avg_rows_per_group": total_rows / num_groups if num_groups > 0 else 0,
        "total_size": total_size,
        "avg_group_size": total_size / num_groups if num_groups > 0 else 0,
    }


def get_row_group_sizes(parquet_file: str, con=None) -> list[dict]:
    """Per-row-group row counts and compressed byte sizes.

    The physical cost of touching each row group: what a reader downloads
    (every column chunk's compressed bytes) and how many rows it gets back.
    The bbox preview (:mod:`geoparquet_io.core.preview`) prices its selected
    groups with this.

    Returns:
        One dict per row group, ordered by row_group_id, with ``row_group_id``,
        ``num_rows`` and ``compressed_bytes`` (summed over all column chunks).
    """
    # parquet_metadata() only: no spatial extension, which is most of a connection's cost.
    rows = _parquet_metadata_rows(
        parquet_file,
        con,
        """
            SELECT
                row_group_id,
                MIN(row_group_num_rows) AS num_rows,
                SUM(total_compressed_size)::BIGINT AS compressed_bytes
            FROM {source}
            GROUP BY row_group_id
            ORDER BY row_group_id
        """,
        load_spatial=False,
    )
    return [
        {
            "row_group_id": row["row_group_id"],
            "num_rows": int(row["num_rows"]),
            "compressed_bytes": int(row["compressed_bytes"]),
        }
        for row in rows
    ]


def get_bloom_filter_info(parquet_file: str, con=None) -> list[dict]:
    """
    Get bloom filter information for all columns in a parquet file.

    Queries parquet_metadata() for bloom_filter_offset and bloom_filter_length
    to determine which columns have bloom filters and their coverage.

    Returns list of dicts sorted by bloom filter presence (filtered columns first),
    each containing:
        - column_name: Name of the column
        - row_groups: Total number of row groups
        - row_groups_with_bloom_filter: Number of row groups with a bloom filter
        - bloom_filter_coverage_pct: Percentage of row groups with bloom filters
        - total_bloom_filter_bytes: Total size of bloom filter data in bytes
    """
    return _parquet_metadata_rows(
        parquet_file,
        con,
        """
            SELECT
                path_in_schema AS column_name,
                COUNT(*) AS row_groups,
                COUNT(bloom_filter_offset) AS row_groups_with_bloom_filter,
                ROUND(
                    100.0 * COUNT(bloom_filter_offset) / COUNT(*), 1
                ) AS bloom_filter_coverage_pct,
                COALESCE(SUM(bloom_filter_length), 0) AS total_bloom_filter_bytes
            FROM {source}
            GROUP BY path_in_schema
            ORDER BY row_groups_with_bloom_filter DESC, path_in_schema
        """,
        load_spatial=False,
    )


def get_row_group_bbox_stats(
    parquet_file: str,
    *,
    bbox_column: Callable[[], str | None],
    native: Callable[[], list[dict]] | None = None,
) -> tuple[list[dict], Literal["native", "bbox_struct"] | None]:
    """Per-row-group bboxes and where they came from, or ``([], None)`` (#1367 concept 17).

    The one reader of a file's per-row-group spatial statistics: native Parquet
    geo statistics first, then a bbox covering column's chunk statistics.
    ``bbox_column`` resolves the covering column, called only when the native
    rung is empty (the resolution lives above this module, in
    ``bbox_structure``). ``native`` replaces the native rung for a caller that
    reads one named column; by default it is the file's primary geometry
    (:func:`get_per_row_group_native_geo_stats`).
    """
    stats = native() if native is not None else get_per_row_group_native_geo_stats(parquet_file)
    if stats:
        return stats, "native"
    column = bbox_column()
    if column:
        stats = get_per_row_group_bbox_stats(parquet_file, column)
        if stats:
            return stats, "bbox_struct"
    return [], None


def get_native_geo_stats_by_row_group(
    parquet_file: str, geometry_column: str, con=None
) -> list[dict] | None:
    """Native Parquet geospatial statistics for one column, per column chunk.

    Args:
        parquet_file: Path or URL to the parquet file
        geometry_column: Name of the geometry column
        con: Optional existing DuckDB connection to reuse

    Returns:
        None if the file carries no column of that name, otherwise one dict per
        column chunk carrying statistics, ordered by row group: row_group_id,
        xmin/ymin/xmax/ymax, zmin/zmax (None when absent) and geometry_types
        (raw type codes, named by ``geo_metadata.geometry_type_name``).

    Raises:
        Whatever DuckDB raises when the file itself cannot be read. ``None``
        means "no such column" and nothing else, so a caller can report an
        unreachable or corrupt file as the failure it is instead of diagnosing
        it as a missing geometry column.
    """
    # path_in_schema is compared against a string LITERAL, not an
    # identifier, so the guard is sql_literal, not quote_identifier.
    rows = _parquet_metadata_rows(
        parquet_file,
        con,
        lambda source: (
            f"""
            SELECT row_group_id, geo_bbox, geo_types
            FROM {source}
            WHERE path_in_schema = {sql_literal(geometry_column)}
            ORDER BY row_group_id
        """
        ),
        load_spatial=None,
    )
    if not rows:
        return None

    chunks = []
    for row_group_id, geo_bbox, geo_types in (tuple(r.values()) for r in rows):
        if not geo_bbox and not geo_types:
            continue
        bbox = geo_bbox or {}
        chunks.append(
            {
                "row_group_id": row_group_id,
                "xmin": bbox.get("xmin"),
                "ymin": bbox.get("ymin"),
                "xmax": bbox.get("xmax"),
                "ymax": bbox.get("ymax"),
                "zmin": bbox.get("zmin"),
                "zmax": bbox.get("zmax"),
                "geometry_types": list(geo_types or []),
            }
        )
    return chunks


def get_native_geo_statistics(parquet_file: str, geometry_column: str, con=None) -> dict | None:
    """
    Get native Parquet GeospatialStatistics for a geometry column.

    This retrieves the geo_bbox and geo_types from the Parquet column metadata,
    which is part of the native Parquet geospatial support (not GeoParquet metadata).

    Reports the first column chunk that carries statistics; for the whole file's
    bounds use :func:`get_aggregated_native_geo_stats`.

    Args:
        parquet_file: Path to the parquet file
        geometry_column: Name of the geometry column
        con: Optional existing DuckDB connection

    Returns:
        dict with 'bbox' (list of 4 floats) and 'geometry_types' (list of strings),
        or None if not available
    """
    try:
        chunks = get_native_geo_stats_by_row_group(parquet_file, geometry_column, con)
    except Exception:
        return None
    if not chunks:
        return None

    # Both halves must describe the same column chunk: reporting one row group's
    # bbox beside another's geometry types would be a dict that matches no row
    # group in the file.
    first = next((c for c in chunks if c["xmin"] is not None), chunks[0])
    # One chunk folds to itself on either reading of the CRS.
    return aggregate_native_geo_stats([first], geographic=True) or None


def get_aggregated_native_geo_stats(
    parquet_file: str, geometry_column: str, con=None, *, geographic: bool | None = None
) -> dict:
    """
    Get aggregated native Parquet GeospatialStatistics across all row groups.

    Returns overall bbox and combined geometry types for the entire file.

    Args:
        parquet_file: Path to the parquet file
        geometry_column: Name of the geometry column
        con: Optional existing DuckDB connection
        geographic: Whether the column's CRS is geographic, when the caller has
            already read it; None reads it (``crs_utils.column_is_geographic``)

    Returns:
        dict with 'bbox' (list of 4+ floats) and 'geometry_types' (list of strings)
    """
    try:
        chunks = get_native_geo_stats_by_row_group(parquet_file, geometry_column, con)
    except Exception:
        return {}
    if not chunks:
        return {}
    if geographic is None:
        geographic = column_is_geographic(parquet_file, geometry_column)
    return aggregate_native_geo_stats(chunks, geographic=geographic)


def aggregate_native_geo_stats(chunks: list[dict], *, geographic: bool) -> dict:
    """Per-column-chunk statistics as one ``{'bbox', 'geometry_types'}`` dict.

    A reader over ``geo_metadata.row_group_geometry_stats``, the one fold: X
    merged on the circle when ``geographic`` (a chunk may wrap the
    antimeridian), plainly for a projected CRS, Z over the chunks
    that carry it, the bbox in the spec's interleaved layout, and spec type
    names only (code 0 and unknown values dropped). A key is absent when
    nothing supplies it.

    Args:
        chunks: Output of :func:`get_native_geo_stats_by_row_group`
    """
    folded = row_group_geometry_stats(chunks, geographic=geographic)
    stats: dict = {}
    if folded.bbox is not None:
        stats["bbox"] = folded.bbox
    if folded.geometry_types:
        stats["geometry_types"] = folded.geometry_types
    return stats


def _chunk_bboxes(chunks: list[dict]) -> list[dict]:
    """The per-row-group bbox rows of the chunks that carry one."""
    return [
        {key: chunk[key] for key in ("row_group_id", "xmin", "ymin", "xmax", "ymax")}
        for chunk in chunks
        if chunk["xmin"] is not None
    ]


def get_per_row_group_native_geo_stats(
    parquet_file: str, geometry_column: str | None = None, con=None, *, strict: bool = False
) -> list[dict]:
    """
    Get per-row-group native Parquet geo_bbox statistics.

    Works with GeoParquet 2.0 / parquet-geo-only files that store geo_bbox
    in native Parquet column statistics (no separate bbox column needed).

    Args:
        parquet_file: Path to the parquet file
        geometry_column: Name of the geometry column (auto-detected if None)
        con: Optional existing DuckDB connection
        strict: For a caller that named ``geometry_column`` itself (the preview's
            explicit column): a column the file does not have raises
            ``InvalidParameterError`` and a file that cannot be read raises,
            instead of both reading as "no statistics". Ignored when the column
            is auto-detected.

    Returns:
        List of dicts with row_group_id, xmin, ymin, xmax, ymax (empty if no stats)
    """
    if not geometry_column:
        from geoparquet_io.core.geometry_detection import primary_geometry_column

        try:
            geometry_column = primary_geometry_column(parquet_file, con=con)
        except Exception:
            return []
        if geometry_column is None:
            return []
    elif strict:
        chunks = get_native_geo_stats_by_row_group(parquet_file, geometry_column, con)
        if chunks is None:
            raise InvalidParameterError(
                "geometry_column", f"the file has no column named {geometry_column!r}"
            )
        return _chunk_bboxes(chunks)

    try:
        chunks = get_native_geo_stats_by_row_group(parquet_file, geometry_column, con)
    except Exception:
        return []
    return _chunk_bboxes(chunks or [])
