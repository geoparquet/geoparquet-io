"""
Base classes and types for write strategy implementations.

This module provides the Strategy Pattern foundation for GeoParquet write operations.
Each strategy encapsulates a different approach to writing GeoParquet files with
varying memory and performance characteristics.
"""

from __future__ import annotations

import os
import re
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

from geoparquet_io.core.crs_utils import apply_output_crs
from geoparquet_io.core.geo_metadata import (
    GEOPARQUET_VERSIONS,
    decode_carried_geo,
    sanitize_geo_metadata,
    strip_unsupported_covering,
)

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa


def resolve_geometry_columns(
    geometry_column: str,
    geometry_info: dict | None = None,
    geo_meta: dict | None = None,
) -> set[str]:
    """Every column the write must treat as geometry, not just the primary.

    A file may declare more than one geometry column: `geo["columns"]` carries a
    `primary_column` plus secondaries, and gpio threads those through
    `geometry_info["secondary"]`. Validation applies the same per-version
    requirements to *every* column in `geo["columns"]`, so every strategy has to
    make the same per-version carrier decision for all of them (#706).

    The secondary names must be taken from `geometry_info` and not recovered from
    `geo_meta` alone: `parquet-geo-only` writes no geo metadata at all, so by
    schema-build time there is nothing left to name them by.
    """
    columns = {geometry_column}
    if geometry_info:
        columns.update(geometry_info.get("secondary") or ())
        columns.update(geometry_info.get("metadata") or {})
    if geo_meta:
        columns.update(geo_meta.get("columns") or {})
    columns.discard(None)
    return columns


def native_geometry_crs(
    geoparquet_version: str,
    geo_meta: dict | None,
    geometry_column: str,
    geometry_info: dict | None = None,
) -> dict[str, dict | None]:
    """Geometry columns needing a native Parquet GEOMETRY type, each with its CRS.

    Empty below 2.0, where geometry is plain BYTE_ARRAY WKB and the CRS lives in
    the ``geo`` block alone.

    EVERY declared geometry column, not just the primary: 2.0 validation applies
    the same requirement to each column in ``geo["columns"]``, and under
    parquet-geo-only -- which writes no ``geo`` block at all -- the logical type
    is a column's only geometry identity (#706).

    Each CRS is read back out of the metadata just built for the file, so the
    type and the block cannot disagree (``v2_crs_consistency``). A column the
    block gives no ``crs`` is the spec default, and carries none in its type.

    ``geo_meta`` is therefore the *output's* block, after ``apply_output_crs``
    has resolved a requested ``input_crs`` against what the source declared --
    never the source's own. Callers that write no block (parquet-geo-only) still
    build one and key the types off it, because the alternative is what #848 was:
    the primary column typed from an ``input_crs`` that is ``None`` on every
    write except a reprojection, and so left bare beside a block declaring
    EPSG:3857.

    One definition, shared by every strategy: the streaming writer's output
    schema, the disk-rewrite writer's, and the in-memory path in
    ``common._apply_geoparquet_metadata``.
    """
    if geoparquet_version not in ("2.0", "parquet-geo-only"):
        return {}

    column_metadata = (geo_meta or {}).get("columns") or {}
    return {
        column: (column_metadata.get(column) or {}).get("crs")
        for column in resolve_geometry_columns(geometry_column, geometry_info, geo_meta)
    }


def merge_secondary_geometry_metadata(geo_meta: dict, geometry_info: dict | None) -> None:
    """Give every secondary geometry column an entry in ``geo["columns"]``.

    Each secondary keeps the metadata the input declared for it -- crucially its
    own ``crs``, which is what the native Parquet GEOMETRY type is then built
    from -- and gains the ``encoding`` the spec requires when the input named
    none. Existing keys win, so a caller that has already resolved something for
    the column is not overwritten.

    Mutates ``geo_meta`` in place. Shared by the three write paths that build a
    block from ``geometry_info``: this module's ``build_geo_metadata``, the
    streaming strategy's, and ``common._apply_geoparquet_metadata``.
    """
    if not geometry_info:
        return

    column_metadata = geometry_info.get("metadata") or {}
    for sec_col in geometry_info.get("secondary") or ():
        sec_meta = geo_meta.setdefault("columns", {}).setdefault(sec_col, {})
        for key, value in column_metadata.get(sec_col, {}).items():
            if key not in sec_meta:
                sec_meta[key] = value
        if "encoding" not in sec_meta:
            sec_meta["encoding"] = "WKB"


def build_geo_metadata(
    geometry_column: str,
    geoparquet_version: str,
    original_metadata: dict | None = None,
    input_crs: dict | None = None,
    custom_metadata: dict | None = None,
    bbox: list[float] | None = None,
    geometry_types: list[str] | None = None,
    geometry_info: dict | None = None,
) -> dict:
    """
    Build GeoParquet metadata - single source of truth for all strategies.

    This helper consolidates the metadata building logic that was previously
    duplicated across strategies.

    Args:
        geometry_column: Name of the geometry column (primary)
        geoparquet_version: Target GeoParquet version (1.0, 1.1, 2.0)
        original_metadata: Original file metadata to parse for existing geo metadata
        input_crs: PROJJSON dict with CRS to apply to primary column
        custom_metadata: Custom metadata (e.g., H3 covering info)
        bbox: Bounding box [xmin, ymin, xmax, ymax] for primary column
        geometry_types: List of geometry types for primary column
        geometry_info: Multi-geometry column info dict with keys:
            - "primary": primary geometry column name
            - "secondary": list of secondary geometry column names
            - "metadata": dict mapping column names to their metadata

    Returns:
        dict: Complete geo metadata structure ready for embedding in Parquet
    """

    version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])
    metadata_version = version_config.get("metadata_version", "1.1.0")

    # Parse existing geo metadata if provided
    geo_meta = _parse_existing_geo_metadata(original_metadata)

    # Initialize or update structure
    geo_meta = _initialize_geo_metadata(geo_meta, geometry_column, metadata_version)

    col_meta = geo_meta["columns"][geometry_column]

    # Encoding (required by GeoParquet spec)
    if "encoding" not in col_meta:
        # For 1.1-geoarrow, preserve the input encoding (native type) from geometry_info.
        # For all other v1.x versions, geometry is converted to WKB blob before writing.
        input_encoding = (
            geometry_info.get("metadata", {}).get(geometry_column, {}).get("encoding")
            if geometry_info and geoparquet_version == "1.1-geoarrow"
            else None
        )
        col_meta["encoding"] = input_encoding or "WKB"

    # Geometry types
    if geometry_types is not None:
        col_meta["geometry_types"] = geometry_types

    # Bounding box
    if bbox is not None:
        col_meta["bbox"] = bbox

    # CRS: write non-default explicitly; omit (and strip any stale/default/null
    # value carried from the source) when the output is the spec default.
    apply_output_crs(col_meta, input_crs)

    # Merge custom metadata into geometry column.
    #
    # `covering` is merged one entry deep rather than replaced: it holds one
    # entry per covering kind (bbox, h3, s2, a5, quadkey), and callers supply
    # only the one they just produced. Replacing the dict made `gpio sort
    # quadkey` destroy the bbox covering its input declared while adding its own
    # (#738). Individual entries still win, so a caller can correct one.
    if custom_metadata:
        for key, value in custom_metadata.items():
            if key == "covering" and isinstance(value, dict):
                existing = col_meta.get("covering")
                col_meta["covering"] = (
                    {**existing, **value} if isinstance(existing, dict) else dict(value)
                )
            else:
                col_meta[key] = value

    # Handle secondary geometry columns from geometry_info
    merge_secondary_geometry_metadata(geo_meta, geometry_info)

    # 'covering' is 1.1-only: drop whatever the source file or custom_metadata carried
    return strip_unsupported_covering(geo_meta, geoparquet_version)


def _parse_existing_geo_metadata(original_metadata: dict | None) -> dict | None:
    """Parse existing geo metadata from file metadata.

    A write-path reader: the result is indexed into while the output block is
    built, so it goes through ``sanitize_geo_metadata`` -- a carried block whose
    ``columns`` is not an object per column would otherwise abort the write with
    a bare ``TypeError`` (#771).
    """

    return sanitize_geo_metadata(_decode_geo_key(original_metadata))


def _decode_geo_key(original_metadata: dict | None):
    """Decode the raw ``geo`` key, accepting bytes or str keys and values.

    Bytes that are not UTF-8 and JSON that does not parse are treated like a
    malformed block -- dropped with a warning, so fresh metadata gets built --
    via :func:`geo_metadata.decode_carried_geo` (#771 follow-up).
    """

    if not isinstance(original_metadata, dict) or not original_metadata:
        return None

    if "geo" in original_metadata:
        return decode_carried_geo(original_metadata["geo"])
    if b"geo" in original_metadata:
        return decode_carried_geo(original_metadata[b"geo"])

    return None


def _initialize_geo_metadata(geo_meta: dict | None, geometry_column: str, version: str) -> dict:
    """Initialize geo metadata structure with column entry."""
    if geo_meta is None:
        return {
            "version": version,
            "primary_column": geometry_column,
            "columns": {geometry_column: {}},
        }

    # Work with a copy to avoid mutation
    geo_meta = dict(geo_meta)
    # Always use the target version, not the original
    geo_meta["version"] = version
    if "primary_column" not in geo_meta:
        geo_meta["primary_column"] = geometry_column
    if "columns" not in geo_meta:
        geo_meta["columns"] = {}
    if geometry_column not in geo_meta["columns"]:
        geo_meta["columns"][geometry_column] = {}

    return geo_meta


class WriteStrategy(str, Enum):
    """Available write strategies for GeoParquet metadata writes."""

    ARROW_MEMORY = "in-memory"
    ARROW_STREAMING = "streaming"
    DUCKDB_KV = "duckdb-kv"
    DISK_REWRITE = "disk-rewrite"


@contextmanager
def atomic_write(output_path: str, suffix: str = ".parquet"):
    """
    Context manager for atomic file writes with cleanup.

    Writes to a temp file in the same directory, then renames atomically on success.
    Ensures partial files are cleaned up on failure.

    Args:
        output_path: Final destination path for the file
        suffix: File suffix for temp file (default: .parquet)

    Yields:
        str: Path to temp file to write to

    Raises:
        Any exception from the write operation (after cleanup)
    """
    dir_path = os.path.dirname(output_path) or "."
    fd, temp_path = tempfile.mkstemp(suffix=suffix, dir=dir_path)
    os.close(fd)

    try:
        yield temp_path
        os.replace(temp_path, output_path)
    except Exception:
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except OSError:
                pass
        raise


class BaseWriteStrategy(ABC):
    """
    Base class for write strategy implementations.

    Each strategy must implement write_from_query() and write_from_table()
    to handle both DuckDB query results and Arrow tables as input.
    """

    name: str
    description: str
    supports_streaming: bool
    supports_remote: bool

    @abstractmethod
    def write_from_query(
        self,
        con: duckdb.DuckDBPyConnection,
        query: str,
        output_path: str,
        geometry_column: str,
        original_metadata: dict | None,
        geoparquet_version: str,
        compression: str,
        compression_level: int,
        row_group_size_mb: int | None,
        row_group_rows: int | None,
        input_crs: dict | None,
        verbose: bool,
        custom_metadata: dict | None = None,
        geometry_info: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """
        Write query results to GeoParquet file.

        Args:
            con: DuckDB connection with spatial extension loaded
            query: SQL SELECT query to execute
            output_path: Path to output file
            geometry_column: Name of geometry column
            original_metadata: Metadata dict from input file
            geoparquet_version: Target GeoParquet version
            compression: Compression codec
            compression_level: Compression level
            row_group_size_mb: Target row group size in MB
            row_group_rows: Exact number of rows per row group
            input_crs: CRS dict from input file
            verbose: Enable verbose logging
            custom_metadata: Optional dict with custom metadata (e.g., H3 covering info)
            geometry_info: Multi-geometry column info from input file
            extra_kv_metadata: Additional Parquet KV metadata (e.g., Vecorel collection info)
        """
        ...

    @abstractmethod
    def write_from_table(
        self,
        table: pa.Table,
        output_path: str,
        geometry_column: str,
        geoparquet_version: str,
        compression: str,
        compression_level: int,
        row_group_size_mb: int | None,
        row_group_rows: int | None,
        verbose: bool,
        input_crs: dict | None = None,
        custom_metadata: dict | None = None,
        extra_kv_metadata: dict[str, str] | None = None,
    ) -> None:
        """
        Write Arrow table to GeoParquet file.

        Args:
            table: Arrow table to write
            output_path: Path to output file
            geometry_column: Name of geometry column
            geoparquet_version: Target GeoParquet version
            compression: Compression codec
            compression_level: Compression level
            row_group_size_mb: Target row group size in MB
            row_group_rows: Exact number of rows per row group
            verbose: Enable verbose logging
            input_crs: CRS dict to apply to geometry column
            custom_metadata: Optional dict with custom metadata (e.g., H3 covering info)
            extra_kv_metadata: Additional Parquet file-level KV metadata as
                {key: json_string}, written alongside 'geo'. Callers pass the
                input's preserved non-geo keys here so sidecar payloads
                (fiboa, vecorel, STAC) survive on every strategy (#690).
        """
        ...

    def _validate_output_path(self, output_path: str) -> None:
        """
        Validate output path for security concerns.

        Prevents path traversal attacks, symlink attacks, and SQL injection vectors.

        Args:
            output_path: Path to validate

        Raises:
            ValueError: If path contains security concerns
        """
        # Check for dangerous characters that could break SQL or cause issues
        if re.search(r"[;\x00]", output_path):
            raise ValueError(f"Invalid characters in output path: {output_path}")

        # Check for directory traversal in the original path (before resolution)
        normalized_input = os.path.normpath(output_path)
        if ".." in normalized_input.split(os.sep):
            raise ValueError(f"Invalid output path (directory traversal detected): {output_path}")

        # Resolve symlinks and normalize for robust path handling
        try:
            Path(output_path).resolve()
        except (OSError, ValueError) as e:
            raise ValueError(f"Invalid output path: {output_path}") from e


def needs_metadata_rewrite(
    geoparquet_version: str,
    original_metadata: dict | None,
    operation: str = "default",
) -> bool:
    """
    Determine if metadata rewrite is needed for this operation.

    Some operations can skip metadata rewriting when the output format
    already has sufficient metadata or doesn't require geo metadata.

    Args:
        geoparquet_version: Target GeoParquet version
        original_metadata: Metadata from input file
        operation: Type of operation (columns_only, sort, default)

    Returns:
        True if metadata rewrite is needed
    """

    version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])

    if geoparquet_version == "parquet-geo-only":
        # For parquet-geo-only, we need to strip any existing geo metadata
        # Check if input has geo metadata that needs to be stripped
        if original_metadata:
            has_geo = "geo" in original_metadata or b"geo" in original_metadata
            if has_geo:
                return True  # Need rewrite to strip the metadata
        return False

    if geoparquet_version == "2.0":
        if operation in ("columns_only", "sort"):
            return False
        # For 2.0 output, check if input has different version that needs updating
        if original_metadata:
            geo_data = original_metadata.get("geo") or original_metadata.get(b"geo")
            if geo_data:
                # Undecodable or non-object carried metadata cannot vouch for a
                # 2.x version, so it needs the rewrite (which builds fresh
                # metadata); a raw json.loads here aborted the write instead.
                geo_meta = decode_carried_geo(geo_data)
                if not isinstance(geo_meta, dict):
                    return True
                input_version = geo_meta.get("version", "")
                # Need rewrite if input version is not 2.x
                if not isinstance(input_version, str) or not input_version.startswith("2."):
                    return True
        return False

    return version_config.get("rewrite_metadata", True)
