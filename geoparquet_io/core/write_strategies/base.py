"""
Base classes and types for write strategy implementations.

This module provides the Strategy Pattern foundation for GeoParquet write operations.
Each strategy encapsulates a different approach to writing GeoParquet files with
varying memory and performance characteristics.
"""

from __future__ import annotations

import os
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa


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
        """
        ...

    def _validate_output_path(self, output_path: str) -> None:
        """
        Validate output path for security concerns.

        Prevents path traversal attacks by checking for ".." components.

        Args:
            output_path: Path to validate

        Raises:
            ValueError: If path contains directory traversal attempts
        """
        normalized = os.path.normpath(output_path)
        if ".." in normalized.split(os.sep):
            raise ValueError(f"Invalid output path (directory traversal detected): {output_path}")


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
