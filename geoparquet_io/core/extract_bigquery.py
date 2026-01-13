#!/usr/bin/env python3
"""
BigQuery extraction to GeoParquet.

Uses DuckDB BigQuery extension to read from BigQuery tables,
converting GEOGRAPHY columns to GeoParquet geometry with spherical edges.
"""

from __future__ import annotations

import os

import duckdb
import pyarrow as pa

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    progress,
    success,
    warn,
)


def get_bigquery_connection(
    project: str | None = None,
    credentials_file: str | None = None,
    geography_as_geometry: bool = True,
) -> duckdb.DuckDBPyConnection:
    """
    Create DuckDB connection with BigQuery extension loaded.

    CRITICAL: Spatial extension must be loaded BEFORE setting
    bq_geography_as_geometry=true for proper GEOGRAPHY conversion.

    Args:
        project: Default GCP project ID (optional, uses gcloud default if not set)
        credentials_file: Path to service account JSON file (optional)
        geography_as_geometry: Convert GEOGRAPHY to GEOMETRY (default: True)

    Returns:
        Configured DuckDB connection with BigQuery extension
    """
    con = duckdb.connect()

    # CRITICAL ORDER: Load spatial FIRST, then BigQuery
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    con.execute("INSTALL bigquery FROM community;")
    con.execute("LOAD bigquery;")

    # Configure authentication via environment variable if credentials file provided
    if credentials_file:
        # Expand user paths like ~/
        credentials_file = os.path.expanduser(credentials_file)
        if not os.path.exists(credentials_file):
            raise FileNotFoundError(f"Credentials file not found: {credentials_file}")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_file

    # Set geography conversion AFTER spatial is loaded
    if geography_as_geometry:
        con.execute("SET bq_geography_as_geometry=true;")

    # Set project if provided
    if project:
        con.execute(f"SET bq_project_id='{project}';")

    return con


def _detect_geometry_column(table: pa.Table) -> str | None:
    """
    Detect geometry column from table schema.

    Args:
        table: PyArrow Table to check

    Returns:
        Name of detected geometry column, or None
    """
    # Look for known geometry column names (case insensitive)
    common_names = ["geometry", "geom", "the_geom", "shape", "geo", "geography"]
    lower_names = {name.lower(): name for name in table.column_names}

    for name in common_names:
        if name in lower_names:
            return lower_names[name]

    # Fallback: look for GEOMETRY type columns by checking for binary/blob types
    # that might contain WKB data
    for field in table.schema:
        field_name_lower = field.name.lower()
        if "geom" in field_name_lower or "geo" in field_name_lower:
            return field.name

    return None


def extract_bigquery_table(
    table: pa.Table,
    where: str | None = None,
    limit: int | None = None,
    columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
) -> pa.Table:
    """
    Extract rows and columns from a PyArrow Table (Python API).

    This is the table-centric version for use after reading from BigQuery.

    Args:
        table: Input PyArrow Table (already loaded from BigQuery)
        where: SQL WHERE clause for filtering (not applicable to in-memory tables)
        limit: Maximum rows to return
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude

    Returns:
        Filtered PyArrow Table
    """
    # For in-memory tables, apply column selection and limit
    # Note: where is not applicable to in-memory tables, must be applied at query time

    result = table

    # Apply column selection
    if columns:
        # Ensure geometry column is included
        geom_col = _detect_geometry_column(result)
        if geom_col and geom_col not in columns:
            columns = list(columns) + [geom_col]
        available = [c for c in columns if c in result.column_names]
        result = result.select(available)

    # Apply column exclusion
    if exclude_columns:
        keep_cols = [c for c in result.column_names if c not in exclude_columns]
        result = result.select(keep_cols)

    # Apply limit
    if limit and result.num_rows > limit:
        result = result.slice(0, limit)

    return result


def _detect_geometry_column_from_schema(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    geography_column: str | None = None,
) -> str | None:
    """
    Detect geometry column from BigQuery table schema.

    Args:
        con: DuckDB connection with BigQuery extension
        table_id: Fully qualified BigQuery table ID
        geography_column: Explicit column name (if provided, validates it exists)

    Returns:
        Name of detected geometry column, or None
    """
    # Query schema to find GEOMETRY columns
    schema_query = f"DESCRIBE SELECT * FROM bigquery_scan('{table_id}') LIMIT 0"
    schema_result = con.execute(schema_query).fetchall()

    geometry_cols = []
    all_cols = []
    for row in schema_result:
        col_name = row[0]
        col_type = str(row[1]).upper()
        all_cols.append(col_name)
        if "GEOMETRY" in col_type:
            geometry_cols.append(col_name)

    # If explicit column provided, validate it
    if geography_column:
        if geography_column in all_cols:
            return geography_column
        # Try case-insensitive match
        lower_map = {c.lower(): c for c in all_cols}
        if geography_column.lower() in lower_map:
            return lower_map[geography_column.lower()]
        return None

    # Return first geometry column found
    if geometry_cols:
        return geometry_cols[0]

    # Fallback: look for common geometry column names
    common_names = ["geometry", "geom", "the_geom", "shape", "geo", "geography"]
    lower_map = {c.lower(): c for c in all_cols}
    for name in common_names:
        if name in lower_map:
            return lower_map[name]

    return None


def _build_select_with_wkb(
    columns: list[str] | None,
    geometry_column: str | None,
    con: duckdb.DuckDBPyConnection,
    table_id: str,
) -> tuple[str, list[str]]:
    """
    Build SELECT clause with ST_AsWKB for geometry columns.

    DuckDB's GEOMETRY type uses an internal binary format when exported to Arrow,
    not standard WKB. We must use ST_AsWKB() to convert to proper WKB for GeoParquet.

    Args:
        columns: List of columns to select (None = all)
        geometry_column: Name of geometry column (already detected)
        con: DuckDB connection
        table_id: BigQuery table ID

    Returns:
        Tuple of (SELECT clause string, list of actual column names)
    """
    # Get all column names if selecting all
    if columns is None:
        schema_query = f"DESCRIBE SELECT * FROM bigquery_scan('{table_id}') LIMIT 0"
        schema_result = con.execute(schema_query).fetchall()
        columns = [row[0] for row in schema_result]

    # Build SELECT with ST_AsWKB for geometry column
    select_parts = []
    for col in columns:
        if geometry_column and col.lower() == geometry_column.lower():
            # Use ST_AsWKB to convert DuckDB GEOMETRY to proper WKB
            select_parts.append(f'ST_AsWKB("{col}") AS "{col}"')
        else:
            select_parts.append(f'"{col}"')

    return ", ".join(select_parts), columns


def extract_bigquery(
    table_id: str,
    output_parquet: str | None = None,
    *,
    project: str | None = None,
    credentials_file: str | None = None,
    where: str | None = None,
    limit: int | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    geography_column: str | None = None,
    dry_run: bool = False,
    show_sql: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
) -> pa.Table | None:
    """
    Extract data from BigQuery table to GeoParquet.

    Uses DuckDB's BigQuery extension with the Storage Read API for
    efficient Arrow-based scanning with filter pushdown.

    BigQuery GEOGRAPHY columns are converted to GeoParquet geometry with
    spherical edges (edges: "spherical" in metadata).

    Args:
        table_id: Fully qualified BigQuery table ID (project.dataset.table)
        output_parquet: Output GeoParquet file path (None = return table only)
        project: GCP project ID (overrides table_id project if set)
        credentials_file: Path to service account JSON file
        where: SQL WHERE clause for filtering (BigQuery SQL syntax)
        limit: Maximum rows to extract
        include_cols: Comma-separated columns to include
        exclude_cols: Comma-separated columns to exclude
        geography_column: Name of GEOGRAPHY column (auto-detected if not set)
        dry_run: Show SQL without executing
        show_sql: Print SQL being executed
        verbose: Enable verbose output
        compression: Output compression type
        compression_level: Compression level
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact rows per row group
        geoparquet_version: GeoParquet version to write

    Returns:
        PyArrow Table if output_parquet is None, otherwise None

    Raises:
        click.ClickException: If BigQuery query fails
    """
    configure_verbose(verbose)

    # Parse column lists
    include_list = [c.strip() for c in include_cols.split(",")] if include_cols else None
    exclude_list = [c.strip() for c in exclude_cols.split(",")] if exclude_cols else None

    # Handle dry_run without connecting to BigQuery
    # (can't detect geometry column without connecting, so show simplified query)
    if dry_run:
        if include_list:
            select_cols = ", ".join(f'"{c}"' for c in include_list)
        else:
            select_cols = "*"
        query = f"SELECT {select_cols} FROM bigquery_scan('{table_id}')"
        if where:
            query += f" WHERE ({where})"
        if limit:
            query += f" LIMIT {limit}"
        progress(f"SQL: {query}")
        progress("(Actual query will use ST_AsWKB for geometry columns)")
        return None

    # Connect to BigQuery to detect geometry column
    debug("Connecting to BigQuery...")
    con = get_bigquery_connection(
        project=project,
        credentials_file=credentials_file,
        geography_as_geometry=True,
    )

    try:
        # Detect geometry column from schema
        geom_col = _detect_geometry_column_from_schema(con, table_id, geography_column)
        if geom_col:
            debug(f"Detected geometry column: {geom_col}")
        else:
            warn("No geometry column detected - output may not be valid GeoParquet")

        # Build SELECT clause with ST_AsWKB for proper WKB output
        # (DuckDB GEOMETRY uses internal binary format, not standard WKB)
        select_cols, all_columns = _build_select_with_wkb(include_list, geom_col, con, table_id)

        # Build query using bigquery_scan for filter pushdown
        query = f"SELECT {select_cols} FROM bigquery_scan('{table_id}')"

        conditions = []
        if where:
            conditions.append(f"({where})")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        if limit:
            query += f" LIMIT {limit}"

        if show_sql:
            progress(f"SQL: {query}")

        # Execute query
        debug(f"Executing BigQuery query: {query}")
        progress("Querying BigQuery...")
        result = con.execute(query).fetch_arrow_table()
        row_count = result.num_rows
        progress(f"Retrieved {row_count:,} rows from BigQuery")

        # Handle column exclusion after fetch (can't push down to BQ for *)
        if exclude_list:
            keep_cols = [c for c in result.column_names if c not in exclude_list]
            result = result.select(keep_cols)

        # Write output if path provided
        if output_parquet:
            write_geoparquet_table(
                result,
                output_parquet,
                geometry_column=geom_col,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
                edges="spherical",  # BigQuery GEOGRAPHY uses spherical edges
            )
            success(f"Extracted {row_count:,} rows to {output_parquet}")
            return None
        else:
            return result

    finally:
        con.close()
