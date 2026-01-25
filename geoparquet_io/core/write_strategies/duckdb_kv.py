"""
DuckDB KV_METADATA write strategy.

This strategy uses DuckDB's native COPY TO with the KV_METADATA option to write
geo metadata directly during the streaming write. Single atomic operation with
no post-processing required.

Best for: Very large files, minimal memory usage
Memory: O(1) - nearly constant
Speed: Fast writes, no post-processing needed
Reliability: Atomic write - either succeeds completely or fails
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.write_strategies.base import BaseWriteStrategy, build_geo_metadata

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

# Valid compression values whitelist (prevents injection via compression param)
VALID_COMPRESSIONS = frozenset({"ZSTD", "SNAPPY", "GZIP", "LZ4", "UNCOMPRESSED", "BROTLI"})


def _get_available_memory() -> int | None:
    """
    Get available memory in bytes, accounting for container limits.

    Checks cgroup v2 and v1 limits first (Docker, Kubernetes, etc.),
    then falls back to psutil for bare-metal systems.

    Returns:
        Available memory in bytes, or None if detection fails
    """
    # Check cgroup v2 memory limit (Docker, Kubernetes)
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            limit = f.read().strip()
            if limit != "max":
                cgroup_limit = int(limit)
                # Try to get current usage to calculate available
                try:
                    with open("/sys/fs/cgroup/memory.current") as f2:
                        current = int(f2.read().strip())
                        return cgroup_limit - current
                except (FileNotFoundError, ValueError):
                    # Return 80% of limit if we can't get current usage
                    return int(cgroup_limit * 0.8)
    except (FileNotFoundError, ValueError):
        pass

    # Check cgroup v1 memory limit
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            limit = int(f.read().strip())
            # Values near 2^63 indicate no limit
            if limit < 2**60:
                try:
                    with open("/sys/fs/cgroup/memory/memory.usage_in_bytes") as f2:
                        usage = int(f2.read().strip())
                        return limit - usage
                except (FileNotFoundError, ValueError):
                    return int(limit * 0.8)
    except (FileNotFoundError, ValueError):
        pass

    # Fall back to psutil for non-containerized environments
    try:
        import psutil

        return psutil.virtual_memory().available
    except ImportError:
        return None


def get_default_memory_limit() -> str:
    """
    Get default memory limit for DuckDB streaming (50% of available RAM).

    Container-aware: detects Docker/Kubernetes memory limits via cgroups
    before falling back to psutil for bare-metal systems.

    Returns:
        Memory limit string for DuckDB (e.g., '2GB', '512MB')
    """
    available = _get_available_memory()

    if available is None:
        return "2GB"  # Conservative fallback

    # Use 50% of available memory
    limit_bytes = int(available * 0.5)
    limit_gb = limit_bytes / (1024**3)

    if limit_gb >= 1:
        return f"{limit_gb:.1f}GB"

    limit_mb = limit_bytes / (1024**2)
    return f"{max(128, int(limit_mb))}MB"  # Minimum 128MB


class DuckDBKVStrategy(BaseWriteStrategy):
    """
    Use DuckDB COPY TO with native KV_METADATA for geo metadata.

    This strategy streams data directly through DuckDB's COPY TO command
    with the KV_METADATA option, which embeds geo metadata directly in
    the Parquet footer during the write. No post-processing is needed.
    """

    name = "duckdb-kv"
    description = "DuckDB streaming write with native metadata support"
    supports_streaming = True
    supports_remote = True

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
        memory_limit: str | None = None,
    ) -> None:
        """Write query results to GeoParquet using DuckDB COPY TO with KV_METADATA."""
        from geoparquet_io.core.common import (
            _wrap_query_with_wkb_conversion,
            compute_bbox_via_sql,
            compute_geometry_types_via_sql,
            is_remote_url,
            upload_if_remote,
        )

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        compression_upper = compression.upper()
        if compression_upper not in VALID_COMPRESSIONS:
            raise ValueError(
                f"Invalid compression: {compression}. Valid: {', '.join(VALID_COMPRESSIONS)}"
            )

        # Single-threaded execution required for memory control (DuckDB issue #8270)
        con.execute("SET threads = 1")
        effective_limit = memory_limit or get_default_memory_limit()
        con.execute(f"SET memory_limit = '{effective_limit}'")
        if verbose:
            debug(f"DuckDB memory limit: {effective_limit}")

        is_remote = is_remote_url(output_path)
        if is_remote:
            # Create temp file and close it immediately so DuckDB can write to it (Windows)
            fd, local_path = tempfile.mkstemp(suffix=".parquet")
            os.close(fd)
        else:
            local_path = output_path

        try:
            # For parquet-geo-only, use DuckDB's native option to skip geo metadata
            if geoparquet_version == "parquet-geo-only":
                if verbose:
                    debug("Writing parquet-geo-only (no geo metadata)...")

                final_query = _wrap_query_with_wkb_conversion(query, geometry_column, con)
                escaped_path = local_path.replace("'", "''")

                # Build COPY TO options for parquet-geo-only
                geo_only_options = [
                    "FORMAT PARQUET",
                    f"COMPRESSION {compression_upper}",
                    "GEOPARQUET_VERSION 'NONE'",
                ]
                if row_group_rows:
                    geo_only_options.append(f"ROW_GROUP_SIZE {row_group_rows}")

                copy_query = f"""
                    COPY ({final_query})
                    TO '{escaped_path}'
                    ({", ".join(geo_only_options)})
                """

                con.execute(copy_query)

                if verbose:
                    pf = pq.ParquetFile(local_path)
                    success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")

                if is_remote:
                    upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

                return

            geo_meta = build_geo_metadata(
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
                original_metadata=original_metadata,
                input_crs=input_crs,
                custom_metadata=custom_metadata,
            )

            col_meta = geo_meta["columns"][geometry_column]

            if "bbox" not in col_meta:
                if verbose:
                    debug("Computing bbox via SQL...")
                bbox = compute_bbox_via_sql(con, query, geometry_column)
                if bbox:
                    col_meta["bbox"] = bbox

            if "geometry_types" not in col_meta:
                if verbose:
                    debug("Computing geometry types via SQL...")
                types = compute_geometry_types_via_sql(con, query, geometry_column)
                col_meta["geometry_types"] = types

            schema_result = con.execute(f"SELECT * FROM ({query}) LIMIT 0").arrow()
            # Detect bbox column by common naming conventions
            bbox_col_name = None
            for name in schema_result.schema.names:
                if name in ["bbox", "bounds", "extent"] or name.endswith("_bbox"):
                    bbox_col_name = name
                    break
            if bbox_col_name:
                col_meta["covering"] = {
                    "bbox": {
                        "xmin": [bbox_col_name, "xmin"],
                        "ymin": [bbox_col_name, "ymin"],
                        "xmax": [bbox_col_name, "xmax"],
                        "ymax": [bbox_col_name, "ymax"],
                    }
                }
                if verbose:
                    debug(f"Added bbox covering metadata for column '{bbox_col_name}'")

            final_query = _wrap_query_with_wkb_conversion(query, geometry_column, con)

            escaped_path = local_path.replace("'", "''")

            geo_meta_json = json.dumps(geo_meta)
            geo_meta_escaped = geo_meta_json.replace("'", "''")

            # Build COPY TO options
            copy_options = [
                "FORMAT PARQUET",
                f"COMPRESSION {compression_upper}",
                f"KV_METADATA {{geo: '{geo_meta_escaped}'}}",
            ]
            if row_group_rows:
                copy_options.append(f"ROW_GROUP_SIZE {row_group_rows}")

            copy_query = f"""
                COPY ({final_query})
                TO '{escaped_path}'
                ({", ".join(copy_options)})
            """

            if verbose:
                debug(f"Writing via DuckDB COPY TO with {compression_upper} compression...")

            con.execute(copy_query)

            if verbose:
                pf = pq.ParquetFile(local_path)
                success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")

            if is_remote:
                upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

        finally:
            if is_remote and Path(local_path).exists():
                Path(local_path).unlink()

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
        """Write Arrow table to GeoParquet using DuckDB COPY TO with KV_METADATA."""
        import duckdb

        from geoparquet_io.core.common import _detect_version_from_table

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        # Auto-detect version from table schema metadata if not specified
        effective_version = geoparquet_version
        if effective_version is None:
            effective_version = _detect_version_from_table(table, verbose)

        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial")
            con.register("input_table", table)

            # Convert WKB bytes to GEOMETRY for proper spatial processing
            escaped_geom = geometry_column.replace('"', '""')
            query = f"""
                SELECT * REPLACE (ST_GeomFromWKB("{escaped_geom}") AS "{escaped_geom}")
                FROM input_table
            """

            self.write_from_query(
                con=con,
                query=query,
                output_path=output_path,
                geometry_column=geometry_column,
                original_metadata=None,
                geoparquet_version=effective_version,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                input_crs=input_crs,
                verbose=verbose,
                custom_metadata=custom_metadata,
            )
        finally:
            con.close()
