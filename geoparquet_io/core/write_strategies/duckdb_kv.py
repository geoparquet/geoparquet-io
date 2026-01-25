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
import re
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.write_strategies.base import BaseWriteStrategy

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

# Valid compression values whitelist (prevents injection via compression param)
VALID_COMPRESSIONS = frozenset({"ZSTD", "SNAPPY", "GZIP", "LZ4", "UNCOMPRESSED"})


def get_default_memory_limit() -> str:
    """Get default memory limit for DuckDB streaming (50% of system RAM)."""
    try:
        import psutil

        available = psutil.virtual_memory().available
        limit_bytes = int(available * 0.5)
        limit_gb = limit_bytes / (1024**3)
        if limit_gb >= 1:
            return f"{limit_gb:.1f}GB"
        limit_mb = limit_bytes / (1024**2)
        return f"{int(limit_mb)}MB"
    except ImportError:
        return "2GB"


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

        con.execute("SET threads = 1")
        effective_limit = get_default_memory_limit()
        con.execute(f"SET memory_limit = '{effective_limit}'")
        if verbose:
            debug(f"DuckDB memory limit: {effective_limit}")

        is_remote = is_remote_url(output_path)
        if is_remote:
            local_path = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet").name
        else:
            local_path = output_path

        try:
            # For parquet-geo-only, use DuckDB's native option to skip geo metadata
            if geoparquet_version == "parquet-geo-only":
                if verbose:
                    debug("Writing parquet-geo-only (no geo metadata)...")

                final_query = _wrap_query_with_wkb_conversion(query, geometry_column, con)
                escaped_path = local_path.replace("'", "''")

                copy_query = f"""
                    COPY ({final_query})
                    TO '{escaped_path}'
                    (FORMAT PARQUET, COMPRESSION {compression_upper}, GEOPARQUET_VERSION 'NONE')
                """

                con.execute(copy_query)

                if verbose:
                    pf = pq.ParquetFile(local_path)
                    success(f"Wrote {pf.metadata.num_rows:,} rows to {output_path}")

                if is_remote:
                    upload_if_remote(local_path, output_path, is_directory=False, verbose=verbose)

                return

            geo_meta = self._prepare_geo_metadata(
                original_metadata=original_metadata,
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
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
            if "bbox" in schema_result.schema.names:
                col_meta["covering"] = {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    }
                }
                if verbose:
                    debug("Added bbox covering metadata")

            final_query = _wrap_query_with_wkb_conversion(query, geometry_column, con)

            escaped_path = local_path.replace("'", "''")

            geo_meta_json = json.dumps(geo_meta)
            geo_meta_escaped = geo_meta_json.replace("'", "''")

            copy_query = f"""
                COPY ({final_query})
                TO '{escaped_path}'
                (FORMAT PARQUET, COMPRESSION {compression_upper}, KV_METADATA {{geo: '{geo_meta_escaped}'}})
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
    ) -> None:
        """Write Arrow table to GeoParquet using DuckDB COPY TO with KV_METADATA."""
        import duckdb

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")

        con.register("input_table", table)

        query = "SELECT * FROM input_table"

        self.write_from_query(
            con=con,
            query=query,
            output_path=output_path,
            geometry_column=geometry_column,
            original_metadata=None,
            geoparquet_version=geoparquet_version,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            input_crs=None,
            verbose=verbose,
        )

        con.close()

    def _validate_output_path(self, output_path: str) -> None:
        """Validate output path for security concerns."""
        super()._validate_output_path(output_path)

        if re.search(r"[;\x00]", output_path):
            raise ValueError(f"Invalid characters in output path: {output_path}")

    def _prepare_geo_metadata(
        self,
        original_metadata: dict | None,
        geometry_column: str,
        geoparquet_version: str,
        input_crs: dict | None,
        custom_metadata: dict | None = None,
    ) -> dict:
        """Prepare GeoParquet metadata for streaming write."""
        from geoparquet_io.core.common import GEOPARQUET_VERSIONS, is_default_crs

        version_config = GEOPARQUET_VERSIONS.get(geoparquet_version, GEOPARQUET_VERSIONS["1.1"])
        metadata_version = version_config.get("metadata_version", "1.1.0")

        geo_meta = self._parse_existing_geo_metadata(original_metadata)
        geo_meta = self._initialize_geo_metadata(
            geo_meta, geometry_column, version=metadata_version
        )

        if "encoding" not in geo_meta["columns"][geometry_column]:
            geo_meta["columns"][geometry_column]["encoding"] = "WKB"

        if input_crs and not is_default_crs(input_crs):
            geo_meta["columns"][geometry_column]["crs"] = input_crs

        # Merge custom metadata (e.g., H3 covering info) into geometry column
        if custom_metadata:
            for key, value in custom_metadata.items():
                geo_meta["columns"][geometry_column][key] = value

        return geo_meta

    def _parse_existing_geo_metadata(self, original_metadata: dict | None) -> dict | None:
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

    def _initialize_geo_metadata(
        self, geo_meta: dict | None, geometry_column: str, version: str
    ) -> dict:
        """Initialize geo metadata structure with column entry."""
        if geo_meta is None:
            geo_meta = {
                "version": version,
                "primary_column": geometry_column,
                "columns": {geometry_column: {}},
            }
        else:
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
