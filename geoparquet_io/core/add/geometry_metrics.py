#!/usr/bin/env python3

"""
Add geometry metrics (area and perimeter) to GeoParquet files.

Calculates geodesic area (m²) and perimeter (m) using DuckDB's spheroid
functions, following the Vecorel geometry-metrics extension specification.

The spheroid functions read lon/lat degrees. Input in any other CRS is
transformed to OGC:CRS84 first: handed projected metres they return NaN, which
the command used to write without a word (#1193).
"""

from __future__ import annotations

import functools
import json

from geoparquet_io.core.common import (
    add_computed_column,
    get_parquet_metadata,
)
from geoparquet_io.core.constants import VECOREL_METRICS_SCHEMA, build_collection_metadata
from geoparquet_io.core.crs_utils import (
    crs_transform_sql_expr,
    extract_crs_from_parquet,
    transform_geom_sql,
)
from geoparquet_io.core.duckdb_utils import query_schema, quote_identifier
from geoparquet_io.core.file_utils import handle_output_overwrite
from geoparquet_io.core.geometry_detection import (
    STANDARD_GEOMETRY_NAMES,
    find_primary_geometry_column,
)
from geoparquet_io.core.logging_config import success
from geoparquet_io.core.parquet_footer import patch_or_rewrite
from geoparquet_io.core.parquet_writer import WriteOptions
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.stream_io import execute_transform
from geoparquet_io.core.streaming import is_stdin, should_stream_output
from geoparquet_io.core.write_funnels import rewrite_footer_kv

AREA_COLUMN = "metrics:area"
PERIMETER_COLUMN = "metrics:perimeter"


def _metric_exprs(geom_sql: str) -> tuple[str, str]:
    """``(area, perimeter)`` SQL over a lon/lat geometry expression."""
    return (
        f"CAST(ST_Area_Spheroid({geom_sql}) AS FLOAT)",
        f"CAST(ST_Perimeter_Spheroid({geom_sql}) AS FLOAT)",
    )


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
    compression: str | None = None,
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
    using spheroid-based calculations. The primary geometry's CRS is read from
    the input and a non-default one is transformed to OGC:CRS84 first; an
    explicit ``crs: null`` (unknown) warns and is measured as lon/lat.

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

    write = WriteOptions.pick(locals())
    if is_streaming and not dry_run:
        _add_metrics_streaming(
            input_parquet,
            output_parquet,
            vecorel,
            verbose,
            write,
        )
        return

    _add_metrics_file_based(
        input_parquet,
        output_parquet,
        vecorel,
        dry_run,
        verbose,
        write,
        overwrite,
        show_sql,
    )


def _add_metrics_streaming(
    input_path,
    output_path,
    vecorel,
    verbose,
    write: WriteOptions,
) -> None:
    """Handle streaming input/output for geometry metrics."""

    if should_stream_output(output_path):
        verbose = False

    def make_query(source: str, con, source_crs: str | None, edges, metadata) -> str:
        col_names = query_schema(con, f"SELECT * FROM {source}").names

        geom_col = None
        for name in STANDARD_GEOMETRY_NAMES:
            if name in col_names:
                geom_col = name
                break
        if not geom_col:
            geom_col = "geometry"

        # `source_crs` is the input's CRS as an AUTH:CODE string, read by
        # execute_transform from the file or the stream's geo metadata.
        area, perimeter = _metric_exprs(transform_geom_sql(quote_identifier(geom_col), source_crs))
        return (
            f"SELECT *, {area} AS {quote_identifier(AREA_COLUMN)}, "
            f"{perimeter} AS {quote_identifier(PERIMETER_COLUMN)} "
            f"FROM {source}"
        )

    extra_kv = _build_vecorel_metadata() if vecorel else None

    execute_transform(
        input_path,
        output_path,
        make_query,
        verbose=verbose,
        input_aware=True,
        extra_kv_metadata=extra_kv,
        **write.funnel_kwargs(),
    )

    if not should_stream_output(output_path):
        success(f"Added geometry metrics to: {output_path}")


def _add_metrics_file_based(
    input_parquet,
    output_parquet,
    vecorel,
    dry_run,
    verbose,
    write: WriteOptions,
    overwrite,
    show_sql,
) -> None:
    """Handle file-based geometry metrics addition — two passes via add_computed_column."""
    handle_output_overwrite(output_parquet, overwrite, input_parquet)
    require_single_file(input_parquet, "add geometry-metrics")

    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # RAW path: extract_crs_from_parquet escapes its own argument (#718).
    source_crs = extract_crs_from_parquet(input_parquet, verbose, geometry_column=geom_col)
    area_expr, perimeter_expr = _metric_exprs(
        crs_transform_sql_expr(quote_identifier(geom_col), source_crs)
    )

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
            **write.funnel_kwargs(),
        )

        # Pass 2: add perimeter column to final output
        add_computed_column(
            temp_file,
            output_parquet,
            column_name=PERIMETER_COLUMN,
            sql_expression=perimeter_expr,
            verbose=verbose,
            **write.funnel_kwargs(),
        )
    finally:
        if os.path.exists(temp_file):
            os.unlink(temp_file)

    # Add Vecorel metadata and ensure required columns. These rewrite the file
    # in place, so they must keep writing at the caller's resolved settings —
    # left to their own defaults they undid the level of everything above
    # (DuckDB's 3 for the id column, pyarrow's 1 for the schema fix, before
    # both resolved through `compression.resolve_codec`).
    if vecorel:
        from geoparquet_io.core.constants import ensure_vecorel_columns

        _add_vecorel_metadata_to_file(
            output_parquet, verbose, write.compression, write.compression_level
        )
        ensure_vecorel_columns(
            output_parquet,
            verbose,
            compression=write.compression,
            compression_level=write.compression_level,
        )

    success(f"Added {AREA_COLUMN} and {PERIMETER_COLUMN} to: {output_parquet}")


def _add_vecorel_metadata_to_file(
    parquet_file: str,
    verbose: bool,
    compression: str | None = None,
    compression_level: int | None = None,
) -> None:
    """Add the Vecorel ``collection`` key to the file this command just wrote.

    One footer key: a footer patch, every page kept as the command wrote it.
    A footer that cannot be patched falls back to the funnel's rewrite at the
    command's own codec (``patch_or_rewrite``); the caller's codec always goes
    through, so a GZIP output is not re-encoded as ZSTD on that fallback.
    """
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

    patch_or_rewrite(
        parquet_file,
        extra_kv,
        rewrite=functools.partial(
            rewrite_footer_kv, compression=compression, compression_level=compression_level
        ),
        verbose=verbose,
    )
