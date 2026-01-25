"""
Arrow in-memory write strategy.

This strategy loads the entire dataset into memory as an Arrow table,
applies GeoParquet metadata, and writes once to disk.

Best for: Small to medium files that fit in memory
Memory: O(n) - proportional to dataset size
Speed: Fast for files that fit in RAM
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.write_strategies.base import BaseWriteStrategy

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa


class ArrowMemoryStrategy(BaseWriteStrategy):
    """
    Write by loading full Arrow table in memory, then write once.

    This is the default strategy, optimized for small to medium files.
    It provides the fastest writes when data fits in available memory.
    """

    name = "in-memory"
    description = "Load entire dataset into memory, apply metadata, write once"
    supports_streaming = False
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
        """Write query results to GeoParquet using in-memory Arrow approach."""
        from geoparquet_io.core.common import (
            _apply_geoparquet_metadata,
            _get_query_columns,
            _normalize_arrow_large_types,
            _wrap_query_with_wkb_conversion,
            _write_table_with_settings,
            validate_compression_settings,
        )

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        query_columns = _get_query_columns(con, query)
        has_geometry = geometry_column in query_columns

        compression, compression_level, compression_desc = validate_compression_settings(
            compression, compression_level, verbose
        )

        if verbose:
            debug(f"Writing output with {compression_desc} compression (in-memory strategy)...")
            debug(f"Using GeoParquet version: {geoparquet_version}")

        if has_geometry:
            final_query = _wrap_query_with_wkb_conversion(query, geometry_column, con)
        else:
            final_query = query
            if verbose:
                debug(
                    f"Geometry column '{geometry_column}' not in query - writing as regular Parquet"
                )

        if verbose:
            debug("Fetching query results as Arrow table...")

        result = con.execute(final_query)
        table = result.fetch_arrow_table()

        table = _normalize_arrow_large_types(table)

        if verbose:
            debug(f"Fetched {table.num_rows:,} rows, {len(table.column_names)} columns")

        if has_geometry:
            table = _apply_geoparquet_metadata(
                table,
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
                original_metadata=original_metadata,
                input_crs=input_crs,
                custom_metadata=custom_metadata,
                verbose=verbose,
            )

        _write_table_with_settings(
            table,
            output_path,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            row_group_size_mb=row_group_size_mb,
            geoparquet_version=geoparquet_version,
            geometry_column=geometry_column,
            verbose=verbose,
        )

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
        """Write Arrow table to GeoParquet file."""
        from geoparquet_io.core.common import (
            _apply_geoparquet_metadata,
            _normalize_arrow_large_types,
            _write_table_with_settings,
            validate_compression_settings,
        )

        configure_verbose(verbose)
        self._validate_output_path(output_path)

        compression, compression_level, compression_desc = validate_compression_settings(
            compression, compression_level, verbose
        )

        if verbose:
            debug(f"Writing {table.num_rows:,} rows with {compression_desc} compression...")

        table = _normalize_arrow_large_types(table)

        has_geometry = geometry_column in table.column_names
        if has_geometry:
            table = _apply_geoparquet_metadata(
                table,
                geometry_column=geometry_column,
                geoparquet_version=geoparquet_version,
                original_metadata=None,
                input_crs=None,
                custom_metadata=None,
                verbose=verbose,
            )

        _write_table_with_settings(
            table,
            output_path,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_rows,
            row_group_size_mb=row_group_size_mb,
            geoparquet_version=geoparquet_version,
            geometry_column=geometry_column,
            verbose=verbose,
        )

        if verbose:
            success(f"Wrote {table.num_rows:,} rows to {output_path}")
