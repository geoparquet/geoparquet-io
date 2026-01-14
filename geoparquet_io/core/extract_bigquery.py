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

# Regex patterns for GCP resource validation
# Project IDs: 6-30 chars, lowercase letters, digits, hyphens, must start with letter
_PROJECT_ID_PATTERN = r"^[a-z][a-z0-9\-]{5,29}$"
# Table IDs: project.dataset.table format, each part is alphanumeric with underscores
_TABLE_ID_PATTERN = r"^[a-zA-Z0-9_\-]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$"


def _validate_project_id(project: str) -> str:
    """Validate GCP project ID to prevent SQL injection.

    Args:
        project: Project ID to validate

    Returns:
        The validated project ID

    Raises:
        ValueError: If project ID doesn't match GCP naming rules
    """
    import re

    if not re.match(_PROJECT_ID_PATTERN, project):
        raise ValueError(
            f"Invalid GCP project ID: '{project}'. "
            "Project IDs must be 6-30 characters, start with a lowercase letter, "
            "and contain only lowercase letters, digits, and hyphens."
        )
    return project


def _validate_table_id(table_id: str) -> str:
    """Validate BigQuery table ID to prevent SQL injection.

    Args:
        table_id: Fully qualified table ID (project.dataset.table)

    Returns:
        The validated table ID

    Raises:
        ValueError: If table ID doesn't match expected format
    """
    import re

    if not re.match(_TABLE_ID_PATTERN, table_id):
        raise ValueError(
            f"Invalid BigQuery table ID: '{table_id}'. "
            "Expected format: project.dataset.table with alphanumeric characters, "
            "underscores, and hyphens only."
        )
    return table_id


class BigQueryConnection:
    """Context manager for DuckDB connection with BigQuery extension.

    Handles proper cleanup of environment variables and connection resources.
    """

    def __init__(
        self,
        project: str | None = None,
        credentials_file: str | None = None,
        geography_as_geometry: bool = True,
    ):
        self.project = project
        self.credentials_file = credentials_file
        self.geography_as_geometry = geography_as_geometry
        self._original_creds: str | None = None
        self._creds_was_set: bool = False
        self._con: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        # Save original credentials state
        self._original_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        self._creds_was_set = "GOOGLE_APPLICATION_CREDENTIALS" in os.environ

        self._con = duckdb.connect()

        # CRITICAL ORDER: Load spatial FIRST, then BigQuery
        self._con.execute("INSTALL spatial;")
        self._con.execute("LOAD spatial;")

        self._con.execute("INSTALL bigquery FROM community;")
        self._con.execute("LOAD bigquery;")

        # Configure authentication via environment variable if credentials file provided
        if self.credentials_file:
            # Expand user paths like ~/
            expanded_path = os.path.expanduser(self.credentials_file)
            if not os.path.exists(expanded_path):
                raise FileNotFoundError(f"Credentials file not found: {expanded_path}")
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = expanded_path

        # Set geography conversion AFTER spatial is loaded
        if self.geography_as_geometry:
            self._con.execute("SET bq_geography_as_geometry=true;")

        # Set project if provided (validated)
        if self.project:
            validated_project = _validate_project_id(self.project)
            self._con.execute(f"SET bq_project_id='{validated_project}';")

        return self._con

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        # Close connection
        if self._con:
            self._con.close()

        # Restore original credentials state
        if self._creds_was_set:
            if self._original_creds is not None:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._original_creds
        else:
            # Original was not set, remove if we set it
            os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)

        return False  # Don't suppress exceptions


def get_bigquery_connection(
    project: str | None = None,
    credentials_file: str | None = None,
    geography_as_geometry: bool = True,
) -> duckdb.DuckDBPyConnection:
    """
    Create DuckDB connection with BigQuery extension loaded.

    CRITICAL: Spatial extension must be loaded BEFORE setting
    bq_geography_as_geometry=true for proper GEOGRAPHY conversion.

    NOTE: This function mutates GOOGLE_APPLICATION_CREDENTIALS environment variable.
    For proper cleanup, use BigQueryConnection context manager instead.

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

    # Set project if provided (validated)
    if project:
        validated_project = _validate_project_id(project)
        con.execute(f"SET bq_project_id='{validated_project}';")

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
    limit: int | None = None,
    columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
) -> pa.Table:
    """
    Apply column selection and row limits to an in-memory PyArrow Table.

    This function processes tables that have already been loaded from BigQuery.
    For filtering with WHERE clauses or bbox, use extract_bigquery() which
    pushes filters to BigQuery for better performance.

    Args:
        table: Input PyArrow Table (already loaded from BigQuery)
        limit: Maximum rows to return
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude

    Returns:
        Filtered PyArrow Table
    """
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
    bbox: tuple[float, float, float, float] | str | None = None,
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
        bbox: Bounding box filter as (xmin, ymin, xmax, ymax) or "xmin,ymin,xmax,ymax"
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
        ValueError: If table_id or project is invalid
        click.ClickException: If BigQuery query fails
    """
    configure_verbose(verbose)

    # Validate table_id to prevent SQL injection
    validated_table_id = _validate_table_id(table_id)

    # Parse bbox if provided as string
    bbox_tuple: tuple[float, float, float, float] | None = None
    if bbox:
        if isinstance(bbox, str):
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError(f"Invalid bbox format: '{bbox}'. Expected 'xmin,ymin,xmax,ymax'")
            bbox_tuple = (parts[0], parts[1], parts[2], parts[3])
        else:
            bbox_tuple = bbox

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
        query = f"SELECT {select_cols} FROM bigquery_scan('{validated_table_id}')"
        dry_conditions = []
        if where:
            dry_conditions.append(f"({where})")
        if bbox_tuple:
            xmin, ymin, xmax, ymax = bbox_tuple
            dry_conditions.append(
                f"ST_Intersects(<geometry>, ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
            )
        if dry_conditions:
            query += " WHERE " + " AND ".join(dry_conditions)
        if limit:
            query += f" LIMIT {limit}"
        progress(f"SQL: {query}")
        progress("(Actual query will use ST_AsWKB for geometry columns)")
        return None

    # Use context manager for proper cleanup of connection and environment variables
    debug("Connecting to BigQuery...")
    with BigQueryConnection(
        project=project,
        credentials_file=credentials_file,
        geography_as_geometry=True,
    ) as con:
        # Detect geometry column from schema
        geom_col = _detect_geometry_column_from_schema(con, validated_table_id, geography_column)
        if geom_col:
            debug(f"Detected geometry column: {geom_col}")
        else:
            warn("No geometry column detected - output may not be valid GeoParquet")

        # Build SELECT clause with ST_AsWKB for proper WKB output
        # (DuckDB GEOMETRY uses internal binary format, not standard WKB)
        select_cols, all_columns = _build_select_with_wkb(
            include_list, geom_col, con, validated_table_id
        )

        # Build query using bigquery_scan for filter pushdown
        query = f"SELECT {select_cols} FROM bigquery_scan('{validated_table_id}')"

        conditions = []
        if where:
            conditions.append(f"({where})")

        # Add bbox filter if provided (uses ST_Intersects with envelope)
        if bbox_tuple and geom_col:
            xmin, ymin, xmax, ymax = bbox_tuple
            bbox_condition = (
                f'ST_Intersects("{geom_col}", ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))'
            )
            conditions.append(bbox_condition)
        elif bbox_tuple and not geom_col:
            warn("bbox filter specified but no geometry column detected - ignoring")

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
