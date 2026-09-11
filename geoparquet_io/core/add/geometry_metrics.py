#!/usr/bin/env python3

"""
Add geometry metrics (area and perimeter) to GeoParquet files.

Calculates geodesic area (m²) and perimeter (m) using DuckDB's spheroid
functions, following the Vecorel geometry-metrics extension specification.
"""

from __future__ import annotations

import json

from geoparquet_io.core.common import (
    add_computed_column,
    get_parquet_metadata,
)
from geoparquet_io.core.constants import VECOREL_METRICS_SCHEMA, build_collection_metadata
from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.file_utils import handle_output_overwrite
from geoparquet_io.core.geometry_detection import find_primary_geometry_column
from geoparquet_io.core.logging_config import success
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.stream_io import execute_transform
from geoparquet_io.core.streaming import is_stdin, should_stream_output

AREA_COLUMN = "metrics:area"
PERIMETER_COLUMN = "metrics:perimeter"


def _build_vecorel_metadata(
    existing_metadata: dict | None = None,
) -> dict[str, str]:
    """Build Vecorel collection metadata with geometry-metrics schema URL."""
    return build_collection_metadata([VECOREL_METRICS_SCHEMA], existing_metadata)


def add_geometry_metrics(
    input_parquet: str,
    output_parquet: str | None = None,
    vecorel: bool = True,
    dry_run: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    show_sql: bool = False,
    memory_limit: str | None = None,
) -> None:
    """
    Add geometry metrics (area and perimeter) to a GeoParquet file.

    Calculates geodesic area (m²) and perimeter (m) for each geometry
    using spheroid-based calculations (assumes WGS84/EPSG:4326 input).

    Uses add_computed_column for each metric — two passes over the data.

    Args:
        input_parquet: Path to the input parquet file
        output_parquet: Path to output file
        vecorel: Add Vecorel schema metadata (default True)
        dry_run: Print SQL without executing
        verbose: Print verbose output
        compression: Compression type
        compression_level: Compression level
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name
        geoparquet_version: GeoParquet version to write
        overwrite: Overwrite existing output file
        show_sql: Print SQL statements
        memory_limit: DuckDB memory limit for the write (e.g. '2GB')
    """
    is_streaming = is_stdin(input_parquet) or should_stream_output(output_parquet)

    if is_streaming and not dry_run:
        _add_metrics_streaming(
            input_parquet,
            output_parquet,
            vecorel,
            verbose,
            compression,
            compression_level,
            row_group_size_mb,
            row_group_rows,
            profile,
            geoparquet_version,
            memory_limit,
        )
        return

    _add_metrics_file_based(
        input_parquet,
        output_parquet,
        vecorel,
        dry_run,
        verbose,
        compression,
        compression_level,
        row_group_size_mb,
        row_group_rows,
        profile,
        geoparquet_version,
        overwrite,
        show_sql,
        memory_limit,
    )


def _add_metrics_streaming(
    input_path,
    output_path,
    vecorel,
    verbose,
    compression,
    compression_level,
    row_group_size_mb,
    row_group_rows,
    profile,
    geoparquet_version,
    memory_limit,
) -> None:
    """Handle streaming input/output for geometry metrics."""
    from geoparquet_io.core.geometry_detection import STANDARD_GEOMETRY_NAMES

    if should_stream_output(output_path):
        verbose = False

    def make_query(source: str, con) -> str:
        sample = con.execute(f"SELECT * FROM {source} LIMIT 0").description
        col_names = [col[0] for col in sample]

        geom_col = None
        for name in STANDARD_GEOMETRY_NAMES:
            if name in col_names:
                geom_col = name
                break
        if not geom_col:
            geom_col = "geometry"

        return (
            f"SELECT *, "
            f"CAST(ST_Area_Spheroid({quote_identifier(geom_col)}) AS FLOAT) AS {quote_identifier(AREA_COLUMN)}, "
            f"CAST(ST_Perimeter_Spheroid({quote_identifier(geom_col)}) AS FLOAT) AS {quote_identifier(PERIMETER_COLUMN)} "
            f"FROM {source}"
        )

    extra_kv = _build_vecorel_metadata() if vecorel else None

    execute_transform(
        input_path,
        output_path,
        make_query,
        verbose=verbose,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        profile=profile,
        geoparquet_version=geoparquet_version,
        extra_kv_metadata=extra_kv,
        memory_limit=memory_limit,
    )

    if not should_stream_output(output_path):
        success(f"Added geometry metrics to: {output_path}")


def _add_metrics_file_based(
    input_parquet,
    output_parquet,
    vecorel,
    dry_run,
    verbose,
    compression,
    compression_level,
    row_group_size_mb,
    row_group_rows,
    profile,
    geoparquet_version,
    overwrite,
    show_sql,
    memory_limit,
) -> None:
    """Handle file-based geometry metrics addition — two passes via add_computed_column."""
    handle_output_overwrite(output_parquet, overwrite, input_parquet)
    require_single_file(input_parquet, "add geometry-metrics")

    geom_col = find_primary_geometry_column(input_parquet, verbose)

    area_expr = f"CAST(ST_Area_Spheroid({quote_identifier(geom_col)}) AS FLOAT)"
    perimeter_expr = f"CAST(ST_Perimeter_Spheroid({quote_identifier(geom_col)}) AS FLOAT)"

    if dry_run:
        from geoparquet_io.core.logging_config import info

        info("Dry run — SQL expressions that would be computed:")
        info(f"  {AREA_COLUMN}: {area_expr}")
        info(f"  {PERIMETER_COLUMN}: {perimeter_expr}")
        return

    import os
    import tempfile

    # Pass 1: add area column to a temp file
    fd, temp_file = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    os.unlink(temp_file)

    try:
        add_computed_column(
            input_parquet,
            temp_file,
            column_name=AREA_COLUMN,
            sql_expression=area_expr,
            verbose=verbose,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            profile=profile,
            geoparquet_version=geoparquet_version,
            memory_limit=memory_limit,
        )

        # Pass 2: add perimeter column to final output
        add_computed_column(
            temp_file,
            output_parquet,
            column_name=PERIMETER_COLUMN,
            sql_expression=perimeter_expr,
            verbose=verbose,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            profile=profile,
            geoparquet_version=geoparquet_version,
            memory_limit=memory_limit,
        )
    finally:
        if os.path.exists(temp_file):
            os.unlink(temp_file)

    # Add Vecorel metadata and ensure required columns
    if vecorel:
        from geoparquet_io.core.constants import ensure_vecorel_columns

        _add_vecorel_metadata_to_file(output_parquet, verbose, memory_limit)
        ensure_vecorel_columns(output_parquet, verbose)

    success(f"Added {AREA_COLUMN} and {PERIMETER_COLUMN} to: {output_parquet}")


def _add_vecorel_metadata_to_file(
    parquet_file: str, verbose: bool, memory_limit: str | None
) -> None:
    """Add Vecorel schema metadata to an existing file via rewrite."""
    from geoparquet_io.core.common import write_parquet_with_metadata
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
    from geoparquet_io.core.file_utils import resolve_file_url
    from geoparquet_io.core.remote import needs_httpfs

    metadata, _ = get_parquet_metadata(parquet_file, verbose=False)
    extra_kv = _build_vecorel_metadata(metadata)

    # Check if vecorel metadata already present (from preservation)
    if metadata:
        existing = metadata.get("collection") or metadata.get(b"collection")
        if existing:
            try:
                parsed = json.loads(existing if isinstance(existing, str) else existing.decode())
                if VECOREL_METRICS_SCHEMA in parsed.get("schemas", {}).get("default", []):
                    return
            except (json.JSONDecodeError, AttributeError):
                pass

    import os
    import tempfile

    input_path = resolve_file_url(parquet_file, verbose=False)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(parquet_file))

    fd, temp_out = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    os.unlink(temp_out)

    try:
        try:
            write_parquet_with_metadata(
                con,
                f"SELECT * FROM {sql_path(input_path)}",
                temp_out,
                original_metadata=metadata,
                extra_kv_metadata=extra_kv,
                verbose=verbose,
                # A whole-file rewrite to attach one sidecar key must not also
                # change the geometry encoding. Without this witness auto mode
                # read the carried `geo` key, which does not say whether the
                # column is native, and the rewrite re-encoded it as WKB (#993).
                input_file=parquet_file,
                memory_limit=memory_limit,
            )
        finally:
            # Release DuckDB's read handle on the source before replacing it;
            # an open connection makes os.replace() fail on Windows.
            con.close()
        os.replace(temp_out, parquet_file)
    finally:
        if os.path.exists(temp_out):
            os.unlink(temp_out)
