"""
Base classes and types for write strategy implementations.

This module provides the Strategy Pattern foundation for GeoParquet write operations.
Each strategy encapsulates a different approach to writing GeoParquet files with
varying memory and performance characteristics.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa


def build_geo_metadata(
    geometry_column: str,
    geoparquet_version: str,
    original_metadata: dict | None = None,
    input_crs: dict | None = None,
    custom_metadata: dict | None = None,
    bbox: list[float] | None = None,
    geometry_types: list[str] | None = None,
) -> dict:
    """
    Build GeoParquet metadata - single source of truth for all strategies.

    This helper consolidates the metadata building logic that was previously
    duplicated across strategies.

    Args:
        geometry_column: Name of the geometry column
        geoparquet_version: Target GeoParquet version (1.0, 1.1, 2.0)
        original_metadata: Original file metadata to parse for existing geo metadata
        input_crs: PROJJSON dict with CRS to apply
        custom_metadata: Custom metadata (e.g., H3 covering info)
        bbox: Bounding box [xmin, ymin, xmax, ymax]
        geometry_types: List of geometry types (e.g., ["Point", "MultiPoint"])

    Returns:
        dict: Complete geo metadata structure ready for embedding in Parquet
    """
    from geoparquet_io.core.common import GEOPARQUET_VERSIONS, is_default_crs

    version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])
    metadata_version = version_config.get("metadata_version", "1.1.0")

    # Parse existing geo metadata if provided
    geo_meta = _parse_existing_geo_metadata(original_metadata)

    # Initialize or update structure
    geo_meta = _initialize_geo_metadata(geo_meta, geometry_column, metadata_version)

    col_meta = geo_meta["columns"][geometry_column]

    # Encoding (required by GeoParquet spec)
    if "encoding" not in col_meta:
        col_meta["encoding"] = "WKB"

    # Geometry types
    if geometry_types is not None:
        col_meta["geometry_types"] = geometry_types

    # Bounding box
    if bbox is not None:
        col_meta["bbox"] = bbox

    # CRS (only if non-default)
    if input_crs and not is_default_crs(input_crs):
        col_meta["crs"] = input_crs

    # Merge custom metadata into geometry column
    if custom_metadata:
        for key, value in custom_metadata.items():
            col_meta[key] = value

    return geo_meta


def _parse_existing_geo_metadata(original_metadata: dict | None) -> dict | None:
    """Parse existing geo metadata from file metadata."""
    if not original_metadata:
        return None

    if isinstance(original_metadata, dict):
        if "geo" in original_metadata:
            geo_data = original_metadata["geo"]
            if isinstance(geo_data, str):
                return json.loads(geo_data)
            return geo_data
        if b"geo" in original_metadata:
            geo_data = original_metadata[b"geo"]
            if isinstance(geo_data, bytes):
                return json.loads(geo_data.decode("utf-8"))
            if isinstance(geo_data, str):
                return json.loads(geo_data)
            return geo_data

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
    from geoparquet_io.core.common import GEOPARQUET_VERSIONS

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
            import json

            geo_data = original_metadata.get("geo") or original_metadata.get(b"geo")
            if geo_data:
                if isinstance(geo_data, bytes):
                    geo_data = geo_data.decode("utf-8")
                if isinstance(geo_data, str):
                    geo_meta = json.loads(geo_data)
                else:
                    geo_meta = geo_data
                input_version = geo_meta.get("version", "")
                # Need rewrite if input version is not 2.x
                if not input_version.startswith("2."):
                    return True
        return False

    return version_config.get("rewrite_metadata", True)
