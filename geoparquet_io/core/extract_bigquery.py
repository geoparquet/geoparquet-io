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
from geoparquet_io.core.extract import parse_bbox
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


def _get_table_row_count(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
) -> int | None:
    """
    Get approximate row count from BigQuery table metadata.

    Uses __TABLES__ metadata which is fast and doesn't scan the table.
    Returns None if metadata lookup fails.

    Args:
        con: DuckDB connection with BigQuery extension loaded
        table_id: Fully qualified BigQuery table ID (project.dataset.table)

    Returns:
        Row count or None if lookup fails
    """
    try:
        # Parse table_id to get project.dataset.table
        parts = table_id.split(".")
        if len(parts) == 3:
            project, dataset, table = parts
        elif len(parts) == 2:
            # Use default project from connection
            dataset, table = parts
            project = None
        else:
            return None

        # Build metadata query using __TABLES__
        if project:
            metadata_table = f"`{project}.{dataset}.__TABLES__`"
            query_project = project
        else:
            metadata_table = f"`{dataset}.__TABLES__`"
            query_project = ""

        query = f"""
        SELECT * FROM bigquery_query(
            '{query_project}',
            'SELECT row_count FROM {metadata_table} WHERE table_id = "{table}"'
        )
        """
        result = con.execute(query).fetchone()
        return result[0] if result else None
    except Exception:
        return None


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
        limit: Maximum rows to return (0 returns empty table)
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude

    Returns:
        Filtered PyArrow Table

    Raises:
        ValueError: If limit is negative
    """
    # Validate limit
    if limit is not None and limit < 0:
        raise ValueError(f"limit must be non-negative, got {limit}")

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

    # Apply limit (limit=0 returns empty table)
    if limit is not None and result.num_rows > limit:
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
        Name of detected geometry column, or None if no geometry column found
        and no explicit geography_column was requested

    Raises:
        ValueError: If geography_column is provided but not found in the table
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
        # Column not found - raise error with helpful message
        geom_hint = ""
        if geometry_cols:
            geom_hint = f" Detected geometry columns: {geometry_cols}."
        raise ValueError(
            f"Geography column '{geography_column}' not found in table '{table_id}'. "
            f"Available columns: {all_cols}.{geom_hint}"
        )

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


def _handle_dry_run(
    validated_table_id: str,
    include_list: list[str] | None,
    bbox: str | None,
    bbox_mode: str,
    bbox_threshold: int,
    where: str | None,
    limit: int | None,
) -> None:
    """Handle dry_run mode by printing the SQL query without executing."""
    if include_list:
        select_cols = ", ".join(f'"{c}"' for c in include_list)
    else:
        select_cols = "*"

    query = _build_dry_run_query(validated_table_id, select_cols, bbox, bbox_mode, bbox_threshold)

    # Add DuckDB-side conditions
    if where:
        if "WHERE" in query:
            query += f" AND ({where})"
        else:
            query += f" WHERE ({where})"
    if limit is not None:
        query += f" LIMIT {limit}"

    progress(f"SQL: {query}")
    progress("(Actual query will use ST_AsWKB for geometry columns)")


def _build_dry_run_query(
    table_id: str,
    select_cols: str,
    bbox: str | None,
    bbox_mode: str,
    bbox_threshold: int,
) -> str:
    """Build a dry run query string for display."""
    if not bbox:
        return f"SELECT {select_cols} FROM bigquery_scan('{table_id}')"

    # Show bbox mode info
    if bbox_mode == "auto":
        progress(f"Bbox mode: auto (threshold: {bbox_threshold:,} rows)")
        progress("(Will check table size to determine server vs local filtering)")
    else:
        progress(f"Bbox mode: {bbox_mode}")

    xmin, ymin, xmax, ymax = parse_bbox(bbox)
    wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"

    if bbox_mode == "local":
        query = f"SELECT {select_cols} FROM bigquery_scan('{table_id}')"
        query += f" WHERE ST_Intersects(<geometry_column>, ST_GeomFromText('{wkt}'))"
    else:
        # Server or auto mode - show server-side as example
        bq_filter = f"ST_INTERSECTS(<geometry_column>, ST_GEOGFROMTEXT(''{wkt}''))"
        query = f"SELECT {select_cols} FROM bigquery_scan('{table_id}', filter='{bq_filter}')"

    return query


def _build_column_list(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    include_list: list[str] | None,
    exclude_list: list[str] | None,
    geom_col: str | None,
) -> list[str] | None:
    """
    Build the column list for SELECT based on include/exclude lists.

    Returns:
        List of columns to select, or None for all columns
    """
    if include_list is not None:
        # Ensure geometry column is included unless explicitly excluded
        cols_to_select = list(include_list)
        if geom_col and geom_col not in cols_to_select:
            if exclude_list is None or geom_col not in exclude_list:
                cols_to_select.append(geom_col)
        return cols_to_select

    if exclude_list is not None:
        # Push down exclusions: get all columns, then remove excluded ones
        schema_query = f"DESCRIBE SELECT * FROM bigquery_scan('{table_id}') LIMIT 0"
        schema_result = con.execute(schema_query).fetchall()
        all_schema_cols = [row[0] for row in schema_result]
        return [c for c in all_schema_cols if c not in exclude_list]

    return None  # All columns


def _determine_bbox_strategy(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    bbox_mode: str,
    bbox_threshold: int,
) -> bool:
    """
    Determine whether to use server-side bbox filtering.

    Returns:
        True if server-side filtering should be used, False for local filtering
    """
    if bbox_mode == "server":
        debug("Using server-side bbox filter (forced by --bbox-mode server)")
        return True
    if bbox_mode == "local":
        debug("Using local bbox filter (forced by --bbox-mode local)")
        return False

    # Auto mode - decide based on row count
    row_count = _get_table_row_count(con, table_id)
    if row_count is not None:
        use_server = row_count >= bbox_threshold
        debug(f"Table has {row_count:,} rows, threshold is {bbox_threshold:,}")
        if use_server:
            debug("Using server-side bbox filter (table exceeds threshold)")
        else:
            debug("Using local bbox filter (table below threshold)")
        return use_server

    # Fallback to local if we can't get row count
    debug("Could not determine row count, defaulting to local filter")
    return False


def _build_bbox_filters(
    bbox: str,
    geom_col: str,
    use_server_side: bool,
) -> tuple[list[str], list[str]]:
    """
    Build bbox filter strings for server-side and local filtering.

    Returns:
        Tuple of (bq_filters list, local_conditions list)
    """
    xmin, ymin, xmax, ymax = parse_bbox(bbox)
    wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"

    bq_filters = []
    local_conditions = []

    if use_server_side:
        bbox_filter = f"ST_INTERSECTS({geom_col}, ST_GEOGFROMTEXT(''{wkt}''))"
        bq_filters.append(bbox_filter)
        debug(f"BigQuery filter: {bbox_filter}")
    else:
        bbox_filter = f"ST_Intersects(\"{geom_col}\", ST_GeomFromText('{wkt}'))"
        local_conditions.append(bbox_filter)
        debug(f"DuckDB filter: {bbox_filter}")

    return bq_filters, local_conditions


def extract_bigquery(
    table_id: str,
    output_parquet: str | None = None,
    *,
    project: str | None = None,
    credentials_file: str | None = None,
    where: str | None = None,
    bbox: str | None = None,
    bbox_mode: str = "auto",
    bbox_threshold: int = 500000,
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
        bbox: Bounding box for spatial filter as "minx,miny,maxx,maxy"
        bbox_mode: Filtering mode - "auto" (default), "server", or "local"
        bbox_threshold: Row count threshold for auto mode (default: 500000)
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

    # Parse column lists
    include_list = [c.strip() for c in include_cols.split(",")] if include_cols else None
    exclude_list = [c.strip() for c in exclude_cols.split(",")] if exclude_cols else None

    # Handle dry_run without connecting to BigQuery
    if dry_run:
        _handle_dry_run(
            validated_table_id, include_list, bbox, bbox_mode, bbox_threshold, where, limit
        )
        return None

    # Execute the actual BigQuery extraction
    return _execute_bigquery_extraction(
        validated_table_id=validated_table_id,
        project=project,
        credentials_file=credentials_file,
        geography_column=geography_column,
        include_list=include_list,
        exclude_list=exclude_list,
        bbox=bbox,
        bbox_mode=bbox_mode,
        bbox_threshold=bbox_threshold,
        where=where,
        limit=limit,
        show_sql=show_sql,
        output_parquet=output_parquet,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
    )


def _execute_bigquery_extraction(
    *,
    validated_table_id: str,
    project: str | None,
    credentials_file: str | None,
    geography_column: str | None,
    include_list: list[str] | None,
    exclude_list: list[str] | None,
    bbox: str | None,
    bbox_mode: str,
    bbox_threshold: int,
    where: str | None,
    limit: int | None,
    show_sql: bool,
    output_parquet: str | None,
    compression: str,
    compression_level: int | None,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    geoparquet_version: str | None,
    verbose: bool,
) -> pa.Table | None:
    """Execute the BigQuery extraction with the given parameters."""
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

        # Build column list and SELECT clause
        cols_to_select = _build_column_list(
            con, validated_table_id, include_list, exclude_list, geom_col
        )
        select_cols, _ = _build_select_with_wkb(cols_to_select, geom_col, con, validated_table_id)

        # Build query with bbox and where filters
        query = _build_bigquery_query(
            con=con,
            validated_table_id=validated_table_id,
            select_cols=select_cols,
            bbox=bbox,
            bbox_mode=bbox_mode,
            bbox_threshold=bbox_threshold,
            geom_col=geom_col,
            where=where,
            limit=limit,
        )

        if show_sql:
            progress(f"SQL: {query}")

        # Execute query
        debug(f"Executing BigQuery query: {query}")
        progress("Querying BigQuery...")
        result = con.execute(query).fetch_arrow_table()
        row_count = result.num_rows
        progress(f"Retrieved {row_count:,} rows from BigQuery")

        # Determine if geometry column is in the final result
        final_geom_col = geom_col if geom_col and geom_col in result.column_names else None

        # Write output if path provided
        if output_parquet:
            write_geoparquet_table(
                result,
                output_parquet,
                geometry_column=final_geom_col,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                geoparquet_version=geoparquet_version,
                verbose=verbose,
                edges="spherical" if final_geom_col else None,
            )
            success(f"Extracted {row_count:,} rows to {output_parquet}")
            return None

        return result


def _build_bigquery_query(
    *,
    con: duckdb.DuckDBPyConnection,
    validated_table_id: str,
    select_cols: str,
    bbox: str | None,
    bbox_mode: str,
    bbox_threshold: int,
    geom_col: str | None,
    where: str | None,
    limit: int | None,
) -> str:
    """Build the BigQuery query with filters applied."""
    bq_filters: list[str] = []
    local_conditions: list[str] = []

    # Handle bbox filtering
    if bbox and geom_col:
        use_server_side = _determine_bbox_strategy(
            con, validated_table_id, bbox_mode, bbox_threshold
        )
        bq_filters, local_conditions = _build_bbox_filters(bbox, geom_col, use_server_side)
    elif bbox and not geom_col:
        warn("--bbox specified but no geometry column detected; ignoring spatial filter")

    # Build base query
    if bq_filters:
        filter_str = " AND ".join(bq_filters)
        query = (
            f"SELECT {select_cols} FROM bigquery_scan('{validated_table_id}', "
            f"filter='{filter_str}')"
        )
    else:
        query = f"SELECT {select_cols} FROM bigquery_scan('{validated_table_id}')"

    # Add WHERE clause
    conditions = local_conditions.copy()
    if where:
        conditions.append(f"({where})")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    # Add LIMIT
    if limit is not None:
        query += f" LIMIT {limit}"

    return query
