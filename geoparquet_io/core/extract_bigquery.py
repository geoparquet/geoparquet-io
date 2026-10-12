#!/usr/bin/env python3
"""
BigQuery extraction to GeoParquet.

Uses DuckDB BigQuery extension to read from BigQuery tables,
converting GEOGRAPHY columns to GeoParquet geometry with spherical edges.
"""

from __future__ import annotations

import os
import re

import duckdb
import pyarrow as pa

from geoparquet_io.core.column_selection import (
    resolve_columns_against_schema,
    split_column_list,
)
from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    get_duckdb_connection,
    quote_identifier,
    validate_where_clause,
    where_condition_fragment,
)
from geoparquet_io.core.exceptions import ValidationError
from geoparquet_io.core.extract import parse_bbox
from geoparquet_io.core.file_utils import handle_output_overwrite
from geoparquet_io.core.geometry_repair import repair_arrow_table_geometry
from geoparquet_io.core.logging_config import (
    configure_verbose,
    debug,
    progress,
    success,
    warn,
)
from geoparquet_io.core.memory_limits import validate_memory_limit
from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_table
from geoparquet_io.core.write_funnels import write_geoparquet_table

# Regex patterns for GCP resource validation
# Project IDs: 6-30 chars, lowercase letters, digits, hyphens, must start with letter
_PROJECT_ID_PATTERN = r"^[a-z][a-z0-9\-]{5,29}$"
# Table ID parts: alphanumeric with underscores and hyphens
_TABLE_PART_PATTERN = r"^[a-zA-Z0-9_\-]+$"


def _quote_bigquery_identifier(name: str) -> str:
    """Quote a RAW identifier for **BigQuery** (GoogleSQL), not for DuckDB.

    The two dialects disagree about the delimiter, so they need different
    helpers. GoogleSQL quotes identifiers with backticks and reads ``"..."`` as
    a *string literal*: running :func:`quote_identifier` output through BigQuery
    would silently compare a constant instead of the column. Inside backticks
    GoogleSQL applies the string-literal escape sequences, so a backslash and
    the delimiter itself are backslash-escaped.

    Like :func:`quote_identifier`, this takes a **bare** name exactly as it
    appears in the table's schema and is deliberately not idempotent -- escaping
    an already-quoted name embeds the backticks in the identifier.

    Args:
        name: The identifier to quote, raw and unescaped

    Returns:
        The name as a backtick-quoted GoogleSQL identifier

    Raises:
        ValueError: If the name is empty or contains a NUL byte, neither of
            which has a quoted GoogleSQL spelling.
    """
    if not name:
        raise ValueError("Cannot quote an empty BigQuery identifier")
    if "\x00" in name:
        raise ValueError(f"BigQuery identifier contains a NUL byte: {name!r}")
    escaped = name.replace("\\", "\\\\").replace("`", "\\`")
    return f"`{escaped}`"


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


def _validate_table_part(part: str, part_name: str) -> str:
    """Validate a single part of a BigQuery table ID.

    Args:
        part: Part to validate (project, dataset, or table name)
        part_name: Name of the part for error messages

    Returns:
        The validated part

    Raises:
        ValueError: If part contains invalid characters
    """
    import re

    if not re.match(_TABLE_PART_PATTERN, part):
        raise ValueError(
            f"Invalid BigQuery {part_name}: '{part}'. "
            "Must contain only alphanumeric characters, underscores, and hyphens."
        )
    return part


def _normalize_table_id(table_id: str, project: str | None = None) -> str:
    """Normalize and validate BigQuery table ID.

    Supports both 2-part (dataset.table) and 3-part (project.dataset.table) formats.
    When project is provided, it overrides any project in the table_id.

    Args:
        table_id: BigQuery table ID (dataset.table or project.dataset.table)
        project: Optional project ID to use (overrides table_id project)

    Returns:
        Fully qualified table ID (project.dataset.table)

    Raises:
        ValueError: If table_id format is invalid or project is missing when needed
    """
    parts = table_id.split(".")

    if len(parts) == 3:
        # project.dataset.table format
        table_project, dataset, table = parts
        _validate_table_part(table_project, "project")
        _validate_table_part(dataset, "dataset")
        _validate_table_part(table, "table")

        # Project override takes precedence
        if project:
            _validate_project_id(project)
            return f"{project}.{dataset}.{table}"
        return table_id

    elif len(parts) == 2:
        # dataset.table format - requires project parameter
        dataset, table = parts
        _validate_table_part(dataset, "dataset")
        _validate_table_part(table, "table")

        if not project:
            raise ValueError(
                f"Table ID '{table_id}' uses dataset.table format but no project was specified. "
                "Either use project.dataset.table format or provide --project."
            )
        _validate_project_id(project)
        return f"{project}.{dataset}.{table}"

    else:
        raise ValueError(
            f"Invalid BigQuery table ID: '{table_id}'. "
            "Expected format: dataset.table or project.dataset.table"
        )


class BigQueryConnection:
    """Context manager for DuckDB connection with BigQuery extension.

    Handles proper cleanup of environment variables and connection resources.
    Safely restores state even if setup fails partway through.
    """

    def __init__(
        self,
        project: str | None = None,
        credentials_file: str | None = None,
    ):
        self.project = project
        self.credentials_file = credentials_file
        self._original_creds: str | None = None
        self._creds_was_set: bool = False
        self._creds_modified: bool = False
        self._con: duckdb.DuckDBPyConnection | None = None

    def _restore_credentials(self) -> None:
        """Restore original GOOGLE_APPLICATION_CREDENTIALS state."""
        if not self._creds_modified:
            return
        if self._creds_was_set:
            if self._original_creds is not None:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._original_creds
        else:
            os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        self._creds_modified = False

    def _cleanup(self) -> None:
        """Clean up connection and credentials."""
        if self._con:
            try:
                self._con.close()
            except Exception:
                pass  # Ignore close errors during cleanup
            self._con = None
        self._restore_credentials()

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        # Save original credentials state before any modifications
        self._original_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        self._creds_was_set = "GOOGLE_APPLICATION_CREDENTIALS" in os.environ

        try:
            self._con = _setup_bigquery_connection()

            # Configure authentication via environment variable if credentials file provided
            if self.credentials_file:
                expanded_path = os.path.expanduser(self.credentials_file)
                if not os.path.exists(expanded_path):
                    raise FileNotFoundError(f"Credentials file not found: {expanded_path}")
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = expanded_path
                self._creds_modified = True

            return self._con

        except Exception:
            # Clean up any partial state on failure
            self._cleanup()
            raise

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self._cleanup()
        return False  # Don't suppress exceptions


def _setup_bigquery_connection() -> duckdb.DuckDBPyConnection:
    """Create and configure a DuckDB connection with BigQuery extension.

    Sets up spatial (via get_duckdb_connection), then layers BigQuery on top
    with ZSTD Arrow compression for efficient network transfer.

    DuckDB 1.5+: GEOGRAPHY maps to GEOMETRY automatically (core type),
    so bq_geography_as_geometry is no longer needed.

    Returns:
        Configured DuckDB connection with spatial + BigQuery extensions
    """
    # Use get_duckdb_connection for consistent setup (spatial + geometry_always_xy)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    try:
        # Layer BigQuery extension on top
        # CRITICAL: spatial must be loaded BEFORE bigquery for geography conversion
        try:
            con.execute("FORCE INSTALL bigquery FROM community;")
        except Exception:
            # Ignore race conditions during parallel extension installation
            pass
        con.execute("LOAD bigquery;")

        # Reduce BigQuery Storage API network transfer with ZSTD compression
        con.execute("SET bq_arrow_compression = 'ZSTD';")

        # Note: bq_geography_as_geometry is NOT set — deprecated in DuckDB 1.5.
        # GEOGRAPHY columns map to GEOMETRY automatically (core type).

        return con
    except Exception:
        con.close()
        raise


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


# The Storage Read API backs bigquery_scan, and it serves physical tables
# only. BigQuery refuses a view, a materialized view or an external table in
# these words. Matched lowercased, against the message of the raised error.
_NON_TABLE_ENTITY = "non-table entities cannot be read with the storage api"


#: What ``table_id`` is called in an error message when there is no table id,
#: because the relation came from ``--sql``.
_SQL_SOURCE_LABEL = "the --sql query"

#: Raised when --sql is combined with anything that shapes the query. Shared
#: with the CLI so the flag names a user sees are written once.
_SQL_EXCLUSIVE_MESSAGE = (
    "--sql replaces the table source; express columns, filters and limits in the SQL itself."
)


# One GoogleSQL lexeme the statement check must step over whole, so that a
# ';' or a keyword inside it is not mistaken for code: a triple-quoted, quoted
# or backtick-quoted literal (any r/b prefix; a backslash always escapes the
# next character as far as finding the end is concerned), or a comment in any
# of GoogleSQL's three spellings. Block comments do not nest in GoogleSQL.
# (\x22 is a double quote, spelled out so the pattern reads as no quoting.)
_GOOGLESQL_OPAQUE = re.compile(
    r"(?:[rRbB]{1,2})?(?:'{3}(?:\\.|[^\\])*?'{3}|\x22{3}(?:\\.|[^\\])*?\x22{3})"
    r"|(?:[rRbB]{1,2})?(?:'(?:\\.|[^'\\\n])*'|\"(?:\\.|[^\"\\\n])*\")"
    r"|`(?:\\.|[^`\\])*`"
    r"|(?:--|#)[^\n]*"
    r"|/\*.*?\*/",
    re.S,
)
_READ_ONLY_LEADS = ("SELECT", "WITH")


def _require_single_query(sql: str) -> None:
    """Refuse a ``--sql`` that is not exactly one SELECT (or WITH) query.

    ``extract`` reads, but BigQuery runs whatever it is handed, and it runs it
    while gpio is still only describing the schema. A DELETE, a CREATE, a
    MERGE -- or a script that appends one after a ``;`` -- would change the
    warehouse before a single row came back. So the statement must open with
    SELECT or WITH (after any comments and opening parentheses) and contain no
    ``;`` but an optional trailing one. Literals and comments are stepped over
    with GoogleSQL's own quoting rules, so a ``;`` inside them is data.

    This is a guard against accidents, with the same standing as the --where
    keyword check, not a permission boundary: the credentials decide what the
    job may touch.

    Raises:
        InvalidParameterError: If the statement is not a single query.
    """
    from geoparquet_io.core.exceptions import InvalidParameterError

    # A comment becomes whitespace and a literal becomes an empty one, so what
    # is left is the statement's code alone.
    code = _GOOGLESQL_OPAQUE.sub(lambda m: " " if m.group(0)[0] in "-#/" else "''", sql)
    statements = [part for part in code.split(";") if part.strip()]
    head = code.lstrip(" \t\r\n(")
    lead = head.split(None, 1)[0].upper() if head.strip() else ""
    if len(statements) != 1 or lead not in _READ_ONLY_LEADS:
        raise InvalidParameterError(
            "sql",
            "--sql must be a single SELECT (or WITH ... SELECT) query: extract "
            "only reads, and BigQuery would run any other statement -- or a "
            "second one after a ';' -- against the warehouse before a row came "
            "back.",
        )


def _bigquery_query_relation(project: str, googlesql: str) -> str:
    """Wrap a GoogleSQL statement as a DuckDB ``bigquery_query(...)`` relation.

    The one place GoogleSQL crosses into DuckDB. Both callers -- the view
    fallback in :func:`_bigquery_view_relation` and the ``--sql`` source --
    come through here, so the escaping contract is stated once: the statement
    travels inside a DuckDB **single-quoted string literal** and is escaped
    exactly once, here, at the point of interpolation. ``_escape_sql_string``
    is not idempotent, so no caller may pre-escape.

    Containment, not sanitation, is what this guarantees. ``--sql`` exists to
    run arbitrary BigQuery SQL, so there is nothing to sanitize on the
    BigQuery side; what must hold is that the value cannot close the DuckDB
    literal and become a second *DuckDB* statement, where it would reach
    ``COPY ... TO``, ``ATTACH`` and the local filesystem.
    ``TestHostileSqlCannotBreakTheDuckDbWrapper`` asserts that with
    ``duckdb.extract_statements``.

    Args:
        project: Billing project for the query job, pre-validated by
            :func:`_validate_project_id` or taken from a normalized table id
        googlesql: The statement BigQuery should run, raw and unescaped
    """
    return f"bigquery_query('{_escape_sql_string(project)}', '{_escape_sql_string(googlesql)}')"


def _bigquery_view_relation(
    table_id: str,
    conditions: list[str] | None = None,
    limit: int | None = None,
    columns: list[str] | None = None,
) -> str:
    """Build a ``bigquery_query(...)`` relation for a non-table entity.

    ``bigquery_scan`` reads through the Storage Read API, which serves physical
    tables only. ``bigquery_query`` runs a real query job instead, so it reads
    a view the same way the BigQuery console does. It takes no ``filter=``
    argument, so a server-side predicate becomes a GoogleSQL ``WHERE`` inside
    the statement -- the better place for it anyway, because BigQuery can then
    prune partitions rather than filtering a materialised stream.

    The GoogleSQL travels inside a DuckDB string literal, so it is escaped
    once, here, at the point of interpolation. ``table_id`` is interpolated raw
    for the same reason as in :func:`_schema_rows`: the caller must have run it
    through :func:`_normalize_table_id`, whose ``_TABLE_PART_PATTERN`` admits
    only ``[A-Za-z0-9_-]`` per part and so cannot carry a backtick or a quote.

    Args:
        table_id: Pre-validated, fully qualified ``project.dataset.name``
        conditions: GoogleSQL predicates to AND into the statement
        limit: Row cap to push into the statement, when nothing filters after it
        columns: RAW column names to project, or None for ``*``. A query job is
            billed by the columns it reads, and BigQuery prunes columns through
            a view only when the statement names them.
    """
    # bigquery_query bills the job to its first argument. _normalize_table_id
    # has already made the id fully qualified, so its project is the right one.
    project = table_id.split(".")[0]
    projection = ", ".join(_quote_bigquery_identifier(c) for c in columns) if columns else "*"
    inner = f"SELECT {projection} FROM `{table_id}`"
    if conditions:
        inner += " WHERE " + " AND ".join(conditions)
    if limit is not None:
        inner += f" LIMIT {limit}"
    return _bigquery_query_relation(project, inner)


# The only string literal both dialects lex identically: single quotes, with no
# quote or backslash inside. DuckDB doubles a quote to escape it and treats a
# backslash as a plain character; GoogleSQL backslash-escapes and reads three
# quotes as a triple-quoted string.
_PORTABLE_STRING = re.compile(r"'[^'\\]*'\s*")
# Any other token is one unbroken word or operator; whitespace followed by more
# text inside its span means a comment the tokenizer skipped.
_PORTABLE_TOKEN = re.compile(r"\S+\s*")


def _where_portability_problem(text: str, kind: object) -> str | None:
    """Name what makes one token of a --where read differently in GoogleSQL."""
    if kind == duckdb.token_type.string_const:
        if _PORTABLE_STRING.fullmatch(text):
            return None
        return f"the string literal {text.strip()} (a quote, a backslash or a comment)"
    if kind == duckdb.token_type.identifier and text.startswith('"'):
        return f"the double-quoted name {text.strip()}, which GoogleSQL reads as a string"
    if not _PORTABLE_TOKEN.fullmatch(text):
        return "a comment"
    return None


def _require_where_portable_to_googlesql(where: str) -> None:
    """Refuse a --where that BigQuery would read differently from DuckDB.

    On a view the clause runs inside the GoogleSQL query job, but it was
    validated -- and on a table is evaluated -- as DuckDB SQL. Where the two
    lexers disagree the clause can silently change meaning (``"col"`` is a
    STRING literal in GoogleSQL, so ``"name" = 'x'`` compares two constants),
    and worse, a string boundary DuckDB saw can move: a backslash escape, a
    doubled or tripled quote, or a nested ``/* */`` comment (DuckDB nests them,
    GoogleSQL does not) can turn what DuckDB validated as data into GoogleSQL
    code outside the WHERE. So only the lexical subset both dialects agree on
    is let through: bare names, plain single-quoted strings, no comments.
    Functions that differ between the dialects fail loudly in BigQuery.

    Raises:
        ValidationError: If any token falls outside that subset.
    """
    tokens = duckdb.tokenize(where)
    problem = "a comment" if tokens and where[: tokens[0][0]].strip() else None
    if "\\" in where:
        problem = "a backslash"
    ends = [pos for pos, _ in tokens[1:]] + [len(where)]
    for (start, kind), end in zip(tokens, ends, strict=True):
        problem = problem or _where_portability_problem(where[start:end], kind)
    if problem:
        raise ValidationError(
            f"--where contains {problem}. This relation is a view, so the clause runs "
            "inside BigQuery as GoogleSQL, and only bare column names, plain "
            "single-quoted strings (no quote or backslash inside) and no comments "
            "read the same there as in DuckDB. Simplify the clause, or write the "
            "whole GoogleSQL statement with --sql."
        )


def _schema_rows(
    con: duckdb.DuckDBPyConnection,
    table_id: str | None = None,
    table_source: str = "bigquery",
    relation: str | None = None,
) -> list[tuple[str, str]]:
    """Return ``(column name, column type)`` for every column, in schema order.

    The module's single ``DESCRIBE``. Every function here that needs a schema
    goes through this one, and a caller holding the result can pass it on
    (``schema_rows=``) instead of paying for a second round-trip against
    BigQuery.

    ``table_id`` is interpolated raw on the BigQuery branch, and that is not an
    oversight to be fixed with ``sql_path()`` or ``quote_identifier()``:
    ``bigquery_scan`` takes a ``project.dataset.table`` **string literal**, not
    a path or an identifier. **The caller must have run the id through**
    :func:`_normalize_table_id`, whose ``_TABLE_PART_PATTERN`` admits only
    ``[A-Za-z0-9_-]`` per part and so cannot carry a quote. Consolidating the
    five copies of this query is what makes that contract statable in one
    place; ``tests/test_extract_bigquery.py::TestSchemaIsReadOnce`` keeps it
    there. The local branch takes a real table name and quotes it.

    ``relation`` is the third source: a relation expression the caller has
    already built, which is how a ``--sql`` statement is described. It is
    interpolated verbatim, so it must come from
    :func:`_bigquery_query_relation`, which did the escaping -- never hand
    this a raw string that came from a user.

    Describing a ``--sql`` relation does not cost a second query job, which
    is the thing worth knowing: the DESCRIBE and the read that follows name
    the same relation, and the extension answers both out of one. Measured
    against ``INFORMATION_SCHEMA.JOBS_BY_USER`` -- one ``--sql`` extraction,
    one billed job.

    Args:
        con: DuckDB connection
        table_id: Pre-validated BigQuery table ID, or a local table name.
            Unused, and may be None, when ``relation`` is given.
        table_source: "bigquery" for bigquery_scan, "local" for local tables
        relation: A pre-built, already-escaped relation expression to describe
            in place of one derived from ``table_id``
    """
    if relation is None:
        if table_source == "bigquery":
            relation = f"bigquery_scan('{table_id}')"
        else:
            relation = quote_identifier(table_id)
    schema_query = f"DESCRIBE SELECT * FROM {relation} LIMIT 0"
    return [(row[0], str(row[1])) for row in con.execute(schema_query).fetchall()]


def _detect_geometry_column_from_schema(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    geography_column: str | None = None,
    table_source: str = "bigquery",
    schema_rows: list[tuple[str, str]] | None = None,
) -> str | None:
    """
    Detect native GEOMETRY-typed column from table schema.

    Only detects columns with DuckDB GEOMETRY type (e.g., BigQuery GEOGRAPHY
    columns that are automatically mapped). Does NOT detect VARCHAR columns
    containing WKT/GeoJSON by name - users must specify those explicitly
    with --geography-column.

    Args:
        con: DuckDB connection
        table_id: Table identifier
        geography_column: If provided and matches a native GEOMETRY column,
            returns it. Otherwise returns None (caller handles VARCHAR columns).
        table_source: "bigquery" for bigquery_scan, "local" for local tables
        schema_rows: The schema, when the caller has already read it. Saves a
            DESCRIBE round-trip against BigQuery.

    Returns:
        Name of detected GEOMETRY column, or None
    """
    if schema_rows is None:
        schema_rows = _schema_rows(con, table_id, table_source)

    geometry_cols = []
    all_cols = []
    for col_name, col_type in schema_rows:
        all_cols.append(col_name)
        if "GEOMETRY" in col_type.upper():
            geometry_cols.append(col_name)

    # If explicit column provided and it's a native GEOMETRY column, use it
    if geography_column:
        lower_map = {c.lower(): c for c in geometry_cols}
        if geography_column.lower() in lower_map:
            return lower_map[geography_column.lower()]
        # Check column exists (for better error messages later)
        all_lower = {c.lower() for c in all_cols}
        if geography_column.lower() not in all_lower:
            raise ValueError(
                f"Column '{geography_column}' not found in table '{table_id}'. "
                f"Available columns: {all_cols}."
            )
        # Column exists but isn't GEOMETRY type - return None so caller
        # can handle it as VARCHAR with WKT/GeoJSON parsing
        return None

    # Return first native GEOMETRY column found
    if geometry_cols:
        return geometry_cols[0]

    return None


def _schema_column_names(
    con: duckdb.DuckDBPyConnection,
    table_id: str | None = None,
    table_source: str = "bigquery",
    schema_rows: list[tuple[str, str]] | None = None,
) -> list[str]:
    """Return the table's column names, in schema order.

    Args:
        con: DuckDB connection
        table_id: BigQuery table ID or local table name
        table_source: "bigquery" for bigquery_scan, "local" for local tables
        schema_rows: The schema, when the caller has already read it. Saves a
            DESCRIBE round-trip, and is the only way to answer for a ``--sql``
            relation, which has no table id to describe.
    """
    if schema_rows is None:
        schema_rows = _schema_rows(con, table_id, table_source)
    return [name for name, _ in schema_rows]


def _get_column_type(
    con: duckdb.DuckDBPyConnection,
    table_id: str | None,
    column_name: str,
    schema_rows: list[tuple[str, str]] | None = None,
) -> str:
    """
    Get the DuckDB type of a column in a BigQuery table.

    Args:
        con: DuckDB connection
        table_id: BigQuery table ID
        column_name: Column name to check
        schema_rows: The schema, when the caller has already read it

    Returns:
        Uppercase type string (e.g. "GEOMETRY", "VARCHAR")
    """
    if schema_rows is None:
        schema_rows = _schema_rows(con, table_id)
    for name, col_type in schema_rows:
        if name.lower() == column_name.lower():
            return col_type.upper()
    return "VARCHAR"


def _resolve_column_name(
    con: duckdb.DuckDBPyConnection,
    table_id: str | None,
    column_name: str,
    table_source: str = "bigquery",
    schema_rows: list[tuple[str, str]] | None = None,
) -> str:
    """
    Resolve a column name to its actual schema spelling (case-sensitive).

    Args:
        con: DuckDB connection
        table_id: BigQuery table ID or local table name
        column_name: Column name (possibly with different case)
        table_source: "bigquery" for bigquery_scan, "local" for local tables
        schema_rows: The schema, when the caller has already read it

    Returns:
        The actual column name from the schema, or the input if not found
    """
    for name in _schema_column_names(con, table_id, table_source, schema_rows):
        if name.lower() == column_name.lower():
            return name
    return column_name


def _build_geometry_select_expr(
    column_name: str,
    column_type: str,
    geometry_format: str = "wkt",
) -> str:
    """
    Build the SELECT expression for a geometry column based on its DuckDB type.

    Handles three scenarios:
    - Native GEOMETRY type: ST_AsWKB("col")
    - VARCHAR with WKT: ST_AsWKB(ST_GeomFromText("col"))
    - VARCHAR with GeoJSON: ST_AsWKB(ST_GeomFromGeoJSON("col"))

    Args:
        column_name: Name of the geometry column
        column_type: DuckDB type of the column (e.g. "GEOMETRY", "VARCHAR")
        geometry_format: Format of geometry in VARCHAR columns ("wkt" or "geojson")

    Returns:
        SQL expression string for the SELECT clause
    """
    if "GEOMETRY" in column_type:
        return f"ST_AsWKB({quote_identifier(column_name)}) AS {quote_identifier(column_name)}"
    elif geometry_format == "geojson":
        return f"ST_AsWKB(ST_GeomFromGeoJSON({quote_identifier(column_name)})) AS {quote_identifier(column_name)}"
    else:
        # Default: WKT
        return f"ST_AsWKB(ST_GeomFromText({quote_identifier(column_name)})) AS {quote_identifier(column_name)}"


def _build_select_with_wkb(
    columns: list[str] | None,
    geometry_column: str | None,
    con: duckdb.DuckDBPyConnection,
    table_id: str | None,
    geometry_format: str = "wkt",
    schema_rows: list[tuple[str, str]] | None = None,
) -> tuple[str, list[str]]:
    """
    Build SELECT clause with ST_AsWKB for geometry columns.

    DuckDB's GEOMETRY type uses an internal binary format when exported to Arrow,
    not standard WKB. We must use ST_AsWKB() to convert to proper WKB for GeoParquet.

    Handles native GEOMETRY columns, VARCHAR columns containing WKT, and
    VARCHAR columns containing GeoJSON.

    Args:
        columns: List of columns to select (None = all)
        geometry_column: Name of geometry column (already detected)
        con: DuckDB connection
        table_id: BigQuery table ID
        geometry_format: Format of geometry in VARCHAR columns ("wkt" or "geojson")
        schema_rows: The schema, when the caller has already read it

    Returns:
        Tuple of (SELECT clause string, list of actual column names)
    """
    # Get all column names if selecting all
    if columns is None:
        columns = _schema_column_names(con, table_id, schema_rows=schema_rows)

    # Detect geometry column type if we have one
    geom_col_type = ""
    if geometry_column:
        geom_col_type = _get_column_type(con, table_id, geometry_column, schema_rows)

    # Build SELECT with appropriate geometry conversion
    select_parts = []
    for col in columns:
        if geometry_column and col.lower() == geometry_column.lower():
            select_parts.append(_build_geometry_select_expr(col, geom_col_type, geometry_format))
        else:
            select_parts.append(quote_identifier(col))

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
        select_cols = ", ".join(quote_identifier(c) for c in include_list)
    else:
        select_cols = "*"

    query = _build_dry_run_query(validated_table_id, select_cols, bbox, bbox_mode, bbox_threshold)

    # Add DuckDB-side conditions. where_condition_fragment() closes the paren on
    # its own line so a trailing '--' in the clause cannot comment out the LIMIT.
    if where:
        keyword = " AND " if "WHERE" in query else " WHERE "
        query += keyword + where_condition_fragment(where)
    if limit is not None:
        query += f" LIMIT {limit}"

    progress(f"SQL: {query}")
    progress("(Actual query will use ST_AsWKB for geometry columns)")


def _handle_sql_dry_run(project: str, sql: str) -> None:
    """Print the statement a ``--sql`` run would issue, without connecting.

    There is nothing to decide here, unlike :func:`_handle_dry_run`: no bbox
    strategy to weigh, no filters to place. The statement is the user's, and
    the only gpio adds is the ``bigquery_query`` wrapper and the ``ST_AsWKB``
    the geometry column gets once its type is known -- which needs the
    schema, which needs a connection, which is what --dry-run is avoiding.
    """
    progress(f"SQL: SELECT * FROM {_bigquery_query_relation(project, sql)}")
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
        # Server or auto mode - show server-side as example. Escaped once here,
        # mirroring the real path in _build_bigquery_query.
        bq_filter = f"ST_INTERSECTS(<geometry_column>, ST_GEOGFROMTEXT('{wkt}'))"
        query = (
            f"SELECT {select_cols} FROM bigquery_scan("
            f"'{table_id}', filter='{_escape_sql_string(bq_filter)}')"
        )

    return query


def _build_column_list(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    include_list: list[str] | None,
    exclude_list: list[str] | None,
    geom_col: str | None,
    schema_columns: list[str] | None = None,
) -> list[str] | None:
    """
    Build the column list for SELECT based on include/exclude lists.

    Args:
        schema_columns: The table's columns, when the caller has already read
            them (``_execute_bigquery_extraction`` always has). Saves a DESCRIBE
            round-trip against BigQuery on the exclude branch.

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
        all_schema_cols = (
            schema_columns if schema_columns is not None else _schema_column_names(con, table_id)
        )
        return [c for c in all_schema_cols if c not in exclude_list]

    return None  # All columns


def _determine_bbox_strategy(
    con: duckdb.DuckDBPyConnection,
    table_id: str,
    bbox_mode: str,
    bbox_threshold: int,
    is_view: bool = False,
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

    # Auto mode weighs a row count against the threshold, but a view has no
    # __TABLES__ entry to count. Falling through to the local default would
    # stream the whole view across the wire to discard most of it, so a view
    # filters server-side: BigQuery evaluates the predicate and can prune.
    if is_view:
        debug("Using server-side bbox filter (a view has no row count to weigh)")
        return True

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
    is_native_geometry: bool = True,
    geometry_format: str = "wkt",
) -> tuple[list[str], list[str]]:
    """
    Build bbox filter strings for server-side and local filtering.

    Args:
        bbox: Bounding box string "xmin,ymin,xmax,ymax"
        geom_col: Geometry column name
        use_server_side: Whether to use BigQuery server-side filtering
        is_native_geometry: Whether the column is native GEOMETRY type
        geometry_format: Format of VARCHAR geometry columns ("wkt" or "geojson")

    Returns:
        Tuple of (bq_filters list, local_conditions list). The BigQuery filters
        are returned as plain GoogleSQL -- the caller escapes them once when
        embedding them in the ``filter='...'`` DuckDB string literal.
    """
    xmin, ymin, xmax, ymax = parse_bbox(bbox)
    wkt = f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"

    bq_filters = []
    local_conditions = []

    if use_server_side:
        # BigQuery server-side filter. GoogleSQL dialect: identifiers are
        # backtick-quoted, because "..." is a STRING literal here.
        bq_col = _quote_bigquery_identifier(geom_col)
        if is_native_geometry:
            # Native GEOGRAPHY column - use directly
            bbox_filter = f"ST_INTERSECTS({bq_col}, ST_GEOGFROMTEXT('{wkt}'))"
        elif geometry_format == "geojson":
            # VARCHAR with GeoJSON - parse first
            bbox_filter = f"ST_INTERSECTS(ST_GEOGFROMGEOJSON({bq_col}), ST_GEOGFROMTEXT('{wkt}'))"
        else:
            # VARCHAR with WKT - parse first
            bbox_filter = f"ST_INTERSECTS(ST_GEOGFROMTEXT({bq_col}), ST_GEOGFROMTEXT('{wkt}'))"
        bq_filters.append(bbox_filter)
        debug(f"BigQuery filter: {bbox_filter}")
    else:
        # DuckDB local filter - DuckDB dialect, so quote_identifier applies.
        local_col = quote_identifier(geom_col)
        if is_native_geometry:
            # Native GEOMETRY column - use directly
            bbox_filter = f"ST_Intersects({local_col}, ST_GeomFromText('{wkt}'))"
        elif geometry_format == "geojson":
            # VARCHAR with GeoJSON - parse first
            bbox_filter = (
                f"ST_Intersects(ST_GeomFromGeoJSON({local_col}), ST_GeomFromText('{wkt}'))"
            )
        else:
            # VARCHAR with WKT - parse first
            bbox_filter = f"ST_Intersects(ST_GeomFromText({local_col}), ST_GeomFromText('{wkt}'))"
        local_conditions.append(bbox_filter)
        debug(f"DuckDB filter: {bbox_filter}")

    return bq_filters, local_conditions


def _validate_sql_source(
    table_id: str | None,
    sql: str | None,
    project: str | None,
    query_shaping: dict[str, object],
) -> None:
    """Check the ``--sql`` source against the table source and the other options.

    ``--sql`` *replaces* the table source rather than filtering it, which is
    what makes the exclusivity rule worth enforcing rather than merging: a
    statement can select its own columns, filter its own rows and cap itself,
    and the flags that do those things have nowhere sensible to go once it
    does. Pushing ``--limit`` outside the statement would also be actively
    wrong -- a query job materializes its whole result first, so an outer cap
    saves nothing and an unfiltered one fails with "Response too large to
    return".

    ``--project`` becomes required because ``bigquery_query`` bills the job to
    its first argument, and with no table id there is nothing to take one from.

    Args:
        table_id: The positional table source, if given
        sql: The ``--sql`` statement, if given
        project: The billing project, if given
        query_shaping: Option name -> value, for every option that shapes the
            query. A value the caller judged to be user-supplied raises; the
            caller decides what "supplied" means, because only the CLI can
            tell an explicit ``--bbox-mode auto`` from an untouched default.

    Raises:
        InvalidParameterError: If no source, both sources, a source without a
            billing project, or ``--sql`` with a query-shaping option
    """
    from geoparquet_io.core.exceptions import InvalidParameterError

    # "is None", not truthiness: an empty statement is a (bad) statement, and
    # reading it as "no --sql" would silently fall back to the table.
    if not table_id and sql is None:
        raise InvalidParameterError(
            "TABLE_ID",
            "No BigQuery source given. Pass a TABLE_ID, or --sql with the "
            "GoogleSQL statement to run.",
        )
    if sql is None:
        return
    if table_id:
        raise InvalidParameterError(
            "sql",
            f"{_SQL_EXCLUSIVE_MESSAGE} Drop the TABLE_ID, and name the "
            "relation inside the statement instead.",
        )
    if not project:
        raise InvalidParameterError(
            "project",
            "--sql requires --project: the query job has to be billed to a "
            "project, and with no TABLE_ID there is none to take it from.",
        )
    _require_single_query(sql)
    # "is not None", not truthiness: limit=0 or an empty column list is still
    # an answer to a question the statement already answers.
    supplied = sorted(name for name, value in query_shaping.items() if value is not None)
    if supplied:
        raise InvalidParameterError(
            "sql",
            f"{_SQL_EXCLUSIVE_MESSAGE} Given with sql: {', '.join(supplied)}.",
        )


def extract_bigquery(
    table_id: str | None = None,
    output_parquet: str | None = None,
    *,
    sql: str | None = None,
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
    geometry_format: str = "wkt",
    edges: str | None = None,
    dry_run: bool = False,
    show_sql: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    geoparquet_version: str | None = None,
    overwrite: bool = False,
    repair_geometry: bool = True,
    memory_limit: str | None = None,
) -> pa.Table | None:
    """
    Extract data from BigQuery table to GeoParquet or plain Parquet.

    Uses DuckDB's BigQuery extension with the Storage Read API for
    efficient Arrow-based scanning with filter pushdown.

    If a GEOGRAPHY column is detected (native GEOMETRY type), it is
    automatically converted. If --geography-column is specified, the
    named column is parsed as WKT or GeoJSON geometry.

    If no geometry column is found, the table is written as plain Parquet
    without GeoParquet metadata.

    ``sql`` replaces ``table_id`` as the source: the statement is run as a
    BigQuery query job and its result becomes the relation. It is for what a
    ``where`` clause cannot express -- a window function, a join, a GROUP BY.
    Because the statement already says which columns, which rows and how
    many, it is mutually exclusive with every option that shapes the query
    (see :func:`_validate_sql_source`); the options that shape the output file
    all still apply.

    Args:
        table_id: BigQuery table ID. Supports both formats:
            - project.dataset.table (fully qualified)
            - dataset.table (requires --project parameter)
            Omit it when ``sql`` is given.
        output_parquet: Output GeoParquet file path (None = return table only)
        sql: GoogleSQL to run as the source, in place of ``table_id``.
            Requires ``project``, and excludes every query-shaping argument.
        project: GCP project ID. Required for dataset.table format and for
            ``sql``. Overrides project in table_id if both are specified.
        credentials_file: Path to service account JSON file
        where: SQL WHERE clause for filtering (BigQuery SQL syntax)
        bbox: Bounding box for spatial filter as "minx,miny,maxx,maxy"
        bbox_mode: Filtering mode - "auto" (default), "server", or "local"
        bbox_threshold: Row count threshold for auto mode (default: 500000)
        limit: Maximum rows to extract
        include_cols: Comma-separated columns to include
        exclude_cols: Comma-separated columns to exclude
        geography_column: Name of column containing geometry data.
            Auto-detected for native GEOGRAPHY columns. Use this to
            specify a VARCHAR column containing WKT or GeoJSON geometry.
        geometry_format: Format of geometry in VARCHAR columns ("wkt" or "geojson")
        edges: Edge interpretation for GeoParquet metadata ("spherical" or "planar").
            If None (default), uses "spherical" for native GEOGRAPHY columns (BigQuery
            uses S2 spherical geometry) and "planar" for VARCHAR columns.
        dry_run: Show SQL without executing
        show_sql: Print SQL being executed
        verbose: Enable verbose output
        compression: Output compression type
        compression_level: Compression level
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact rows per row group
        geoparquet_version: GeoParquet version to write
        memory_limit: DuckDB memory_limit for the BigQuery scan (e.g. "4GB").
            The write goes through PyArrow, so this bounds the read side only.

    Returns:
        PyArrow Table if output_parquet is None, otherwise None

    Raises:
        ValueError: If table_id format is invalid, project is missing when needed,
            or project ID doesn't match GCP naming rules
    """
    configure_verbose(verbose)

    # Settle the source before anything else: which of table_id and sql is in
    # play decides what every step below is even about. The dict spells the
    # Python API argument names: the CLI refuses these combinations itself,
    # by flag name, before it gets here, so only an API caller reads them.
    _validate_sql_source(
        table_id,
        sql,
        project,
        {
            "columns": include_cols,
            "exclude_columns": exclude_cols,
            "where": where,
            "bbox": bbox,
            "bbox_mode": bbox_mode if bbox_mode != "auto" else None,
            "bbox_threshold": bbox_threshold if bbox_threshold != 500000 else None,
            "limit": limit,
        },
    )

    # Validate --where before it is interpolated into any query (dry-run or
    # real), and before any network connection is established (gpio #612 parity).
    if where:
        validate_where_clause(where)

    if sql:
        # --sql carries no table id, so the project is the one given, and the
        # statement is its own relation. _validate_project_id is what keeps
        # the id out of the DuckDB string literal's reach.
        validated_table_id = None
        normalized_project = _validate_project_id(project)
    else:
        # Normalize table_id early - validates format and applies project override
        # This ensures validated_table_id is always project.dataset.table format
        validated_table_id = _normalize_table_id(table_id, project)

        # Extract project from normalized table_id for connection setup
        normalized_project = validated_table_id.split(".")[0]

    # Parse column lists
    include_list = split_column_list(include_cols, "--include-cols")
    exclude_list = split_column_list(exclude_cols, "--exclude-cols")

    # Check if output file exists and handle overwrite (fixes issue #278)
    if output_parquet and not dry_run:
        handle_output_overwrite(output_parquet, overwrite)

    # Handle dry_run without connecting to BigQuery
    if dry_run:
        if sql:
            _handle_sql_dry_run(normalized_project, sql)
        else:
            _handle_dry_run(
                validated_table_id, include_list, bbox, bbox_mode, bbox_threshold, where, limit
            )
        return None

    # Execute the actual BigQuery extraction
    return _execute_bigquery_extraction(
        validated_table_id=validated_table_id,
        sql=sql,
        project=normalized_project,
        credentials_file=credentials_file,
        geography_column=geography_column,
        geometry_format=geometry_format,
        edges=edges,
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
        repair_geometry=repair_geometry,
        memory_limit=memory_limit,
    )


def _run_read(
    con: duckdb.DuckDBPyConnection,
    query: str,
    show_sql: bool,
) -> pa.Table:
    """Issue one read and return its Arrow table."""
    if show_sql:
        progress(f"SQL: {query}")
    debug(f"Executing BigQuery query: {query}")
    progress("Querying BigQuery...")
    return con.execute(query).arrow().read_all()


def _read_with_view_fallback(
    con: duckdb.DuckDBPyConnection,
    build_query,
    validated_table_id: str,
    *,
    show_sql: bool,
) -> pa.Table:
    """Read through ``bigquery_scan``, retrying as a query job for a non-table.

    Whether an id is a table or a view is settled here, by BigQuery, rather
    than guessed beforehand: a DESCRIBE answers out of table metadata and so
    succeeds for a view, which leaves the read as the only place the
    difference shows. The refused attempt opens no read session and scans no
    bytes, so a table pays nothing for this and a view pays one wasted
    round-trip. Reacting to the error also covers the external tables the
    Storage Read API refuses in the same words.

    Args:
        con: DuckDB connection
        build_query: ``is_view -> SQL``, i.e. :func:`_build_bigquery_query`
            with everything but that one argument already bound
        validated_table_id: The id being read, for the fallback notice
        show_sql: Echo each statement before issuing it
    """
    try:
        return _run_read(con, build_query(is_view=False), show_sql)
    except duckdb.Error as exc:
        if _NON_TABLE_ENTITY not in str(exc).lower():
            raise
        progress(
            f"{validated_table_id} is a view or external table, which the Storage "
            "Read API cannot serve; reading it with a BigQuery query job instead, "
            "billed by bytes scanned"
        )
        return _run_read(con, build_query(is_view=True), show_sql)


def _execute_bigquery_extraction(
    *,
    validated_table_id: str | None,
    sql: str | None = None,
    project: str | None,
    credentials_file: str | None,
    geography_column: str | None,
    geometry_format: str = "wkt",
    edges: str | None = None,
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
    repair_geometry: bool = True,
    memory_limit: str | None = None,
) -> pa.Table | None:
    """Execute the BigQuery extraction with the given parameters."""
    debug("Connecting to BigQuery...")
    with BigQueryConnection(
        project=project,
        credentials_file=credentials_file,
    ) as con:
        # The BigQuery scan is what actually consumes memory here (the result is
        # materialised as an Arrow table before the PyArrow write), so apply the
        # user's limit to this connection. validate_memory_limit guards the
        # interpolation — a SET value cannot be parameterised.
        if memory_limit is not None:
            con.execute(f"SET memory_limit = '{validate_memory_limit(memory_limit)}'")
            debug(f"DuckDB memory limit: {memory_limit}")

        # --sql is its own relation. Building it once here means every step
        # below -- the DESCRIBE, the geometry detection, the read -- is
        # pointed at the same statement, and that the escaping happens once.
        sql_relation = _bigquery_query_relation(project, sql) if sql else None
        # Everything downstream that wants a name for the source in a message
        # has to say something when there is no table id.
        source_name = validated_table_id if validated_table_id else _SQL_SOURCE_LABEL

        # One DESCRIBE serves everything that needs the schema here: the
        # geometry detection below and the --include-cols/--exclude-cols check
        # further down. Validation therefore costs no BigQuery round-trip at
        # all, on either branch.
        schema_rows = _schema_rows(con, validated_table_id, relation=sql_relation)

        # Detect geometry column from schema (native GEOMETRY type only)
        geom_col = _detect_geometry_column_from_schema(
            con, source_name, geography_column, schema_rows=schema_rows
        )
        is_native_geometry = geom_col is not None  # Track whether geometry is native GEOGRAPHY type
        if geom_col:
            debug(f"Detected geometry column: {geom_col}")
        else:
            if geography_column:
                # User explicitly specified a column that isn't native GEOMETRY -
                # still use it, parsing as WKT or GeoJSON
                # Resolve to actual schema spelling for case-insensitive matching
                geom_col = _resolve_column_name(
                    con, validated_table_id, geography_column, schema_rows=schema_rows
                )
                debug(
                    f"Using '{geom_col}' as geometry column (parsing as {geometry_format.upper()})"
                )
            else:
                warn(
                    "No geometry column detected. Output will be plain Parquet "
                    "without GeoParquet metadata. Use --geography-column to "
                    "specify a column containing WKT or GeoJSON geometry."
                )

        # Determine edge interpretation for GeoParquet metadata
        # - If explicitly set, use that value
        # - Native GEOGRAPHY columns use spherical edges (BigQuery uses S2)
        # - VARCHAR columns default to planar (None) since edge interpretation is unknown
        if edges is not None:
            final_edges = edges
        elif is_native_geometry:
            final_edges = "spherical"
        else:
            final_edges = None  # Planar (no edges metadata)

        # Check the requested columns against the table before any of them is
        # quoted into the SELECT (#969), reusing the schema read above.
        schema_columns = [name for name, _ in schema_rows]
        include_list = resolve_columns_against_schema(
            include_list, schema_columns, "--include-cols"
        )
        exclude_list = resolve_columns_against_schema(
            exclude_list, schema_columns, "--exclude-cols"
        )

        # Build column list and SELECT clause
        cols_to_select = _build_column_list(
            con, validated_table_id, include_list, exclude_list, geom_col, schema_columns
        )
        select_cols, _ = _build_select_with_wkb(
            cols_to_select, geom_col, con, validated_table_id, geometry_format, schema_rows
        )
        # A query job reads only these when it names them; None reads all.
        source_columns = None if cols_to_select is None else list(cols_to_select)

        # Build query with bbox and where filters
        def build_query(is_view: bool) -> str:
            return _build_bigquery_query(
                con=con,
                validated_table_id=validated_table_id,
                select_cols=select_cols,
                bbox=bbox,
                bbox_mode=bbox_mode,
                bbox_threshold=bbox_threshold,
                geom_col=geom_col,
                where=where,
                limit=limit,
                is_native_geometry=is_native_geometry,
                geometry_format=geometry_format,
                is_view=is_view,
                source_columns=source_columns,
            )

        if sql_relation is not None:
            # --sql is already a query job, so there is no Storage Read API
            # attempt to make and nothing to fall back from.
            result = _run_read(con, f"SELECT {select_cols} FROM {sql_relation}", show_sql)
        else:
            result = _read_with_view_fallback(
                con, build_query, validated_table_id, show_sql=show_sql
            )
        row_count = result.num_rows
        progress(f"Retrieved {row_count:,} rows from BigQuery")

        # Determine if geometry column is in the final result
        final_geom_col = geom_col if geom_col and geom_col in result.column_names else None

        # Repair invalid geometry (issue #506). The geometry column is already
        # WKB-encoded by the SELECT; the helper preserves schema metadata.
        if final_geom_col and result.num_rows > 0:
            result, _ = repair_arrow_table_geometry(result, final_geom_col, repair=repair_geometry)

        # Write output if path provided
        if output_parquet:
            if final_geom_col:
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
                    edges=final_edges,
                )
            else:
                # No geometry: plain Parquet, but with the row-group count the
                # geometry branch above gets. This used to hand pq.write_table a
                # byte count where a row count goes -- `--row-group-size-mb 128`
                # asked for 134,217,728 rows per group -- and None when no flag
                # was given, so DEFAULT_ROW_GROUP_ROWS never applied and the
                # output was one row group for the whole file (#1021).
                #
                # It stays a direct write rather than routing through
                # write_geoparquet_table because that helper reads
                # geometry_column=None as "auto-detect by name": it would call a
                # plain VARCHAR column named `geom` or `shape` geometry and
                # stamp a geo key on it, which is precisely what BigQuery's own
                # schema said this table does not have.
                import pyarrow.parquet as pq

                pq.write_table(
                    result,
                    output_parquet,
                    compression=compression,
                    compression_level=compression_level,
                    row_group_size=resolve_row_group_rows_for_table(
                        result, row_group_rows, row_group_size_mb
                    ),
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
    is_native_geometry: bool = True,
    geometry_format: str = "wkt",
    is_view: bool = False,
    source_columns: list[str] | None = None,
) -> str:
    """Build the BigQuery query with filters applied.

    ``source_columns`` are the RAW names ``select_cols`` reads, or None when it
    reads every column; only the view path uses them, to project the query job.
    """
    bq_filters: list[str] = []
    local_conditions: list[str] = []

    # Handle bbox filtering
    if bbox and geom_col:
        use_server_side = _determine_bbox_strategy(
            con, validated_table_id, bbox_mode, bbox_threshold, is_view=is_view
        )
        bq_filters, local_conditions = _build_bbox_filters(
            bbox, geom_col, use_server_side, is_native_geometry, geometry_format
        )
    elif bbox and not geom_col:
        warn("--bbox specified but no geometry column detected; ignoring spatial filter")

    # Build base query
    if is_view:
        return _build_view_query(
            validated_table_id,
            select_cols,
            bq_filters=bq_filters,
            local_conditions=local_conditions,
            where=where,
            limit=limit,
            source_columns=source_columns,
            geom_col=geom_col,
        )
    if bq_filters:
        # The GoogleSQL filter travels inside a DuckDB string literal, so it is
        # escaped once, here, at the point of interpolation. A BigQuery flexible
        # column name may legally contain an apostrophe (gpio #932).
        filter_str = _escape_sql_string(" AND ".join(bq_filters))
        query = (
            f"SELECT {select_cols} FROM bigquery_scan('{validated_table_id}', "
            f"filter='{filter_str}')"
        )
    else:
        query = f"SELECT {select_cols} FROM bigquery_scan('{validated_table_id}')"

    # Add WHERE clause
    conditions = local_conditions.copy()
    if where:
        # Validated upstream in extract_bigquery(); wrapped so a trailing '--'
        # cannot comment out a following condition or the LIMIT.
        conditions.append(where_condition_fragment(where))
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    # Add LIMIT
    if limit is not None:
        query += f" LIMIT {limit}"

    return query


def _build_view_query(
    validated_table_id: str,
    select_cols: str,
    *,
    bq_filters: list[str],
    local_conditions: list[str],
    where: str | None,
    limit: int | None,
    source_columns: list[str] | None,
    geom_col: str | None,
) -> str:
    """Build the DuckDB query that reads a view through a BigQuery query job.

    A view opens no Storage Read API session and so has no filter= argument;
    the predicates ride inside the GoogleSQL instead.

    --where goes down with them rather than staying an outer DuckDB filter. A
    scan streams, so filtering late there only wastes bandwidth, but a query
    job must finish its whole result first: left outside, ``SELECT * FROM
    <view>`` asks BigQuery to materialise the entire relation, which on a
    billion-row view fails outright with "Response too large to return", and
    a partitioned view could not prune. The clause is checked first for the
    DuckDB spellings that would silently change meaning in GoogleSQL.
    """
    view_conditions = list(bq_filters)
    if where:
        _require_where_portable_to_googlesql(where)
        # Validated upstream in extract_bigquery(); wrapped so a trailing
        # '--' cannot comment out the rest of the GoogleSQL either.
        view_conditions.append(where_condition_fragment(where))
    # The outer DuckDB filter reads the geometry column, so the query job must
    # return it even when the user excluded it from the output.
    columns = list(source_columns) if source_columns is not None else None
    if columns is not None and local_conditions and geom_col and geom_col not in columns:
        columns.append(geom_col)
    # LIMIT only goes down when nothing filters after it. With a local bbox
    # condition outside, an inner LIMIT would cut the candidates before that
    # filter ran and return fewer rows than asked for.
    push_limit = limit if not local_conditions else None
    relation = _bigquery_view_relation(validated_table_id, view_conditions, push_limit, columns)
    query = f"SELECT {select_cols} FROM {relation}"
    if local_conditions:
        query += " WHERE " + " AND ".join(local_conditions)
    if limit is not None and push_limit is None:
        query += f" LIMIT {limit}"
    return query
