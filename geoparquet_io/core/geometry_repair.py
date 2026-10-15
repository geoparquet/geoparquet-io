"""Invalid-geometry repair helpers (issue #506).

geoparquet-io repairs invalid geometry by default using DuckDB's ``ST_MakeValid``.
``ST_IsValid``/``ST_MakeValid`` require a *native* ``GEOMETRY`` and reject WKB/BLOB,
so every repair must be applied while the geometry column is still a native
``GEOMETRY`` — i.e. *before* any ``ST_AsWKB(...)`` conversion in a pipeline.

This module provides three injection styles:

* :func:`repair_geometry_sql` — wrap an inline geometry *expression* (a column
  reference or a ``ST_GeomFrom*`` call) with a CASE-guarded repair. Used where a
  query is built around a known native ``GEOMETRY`` expression (e.g. WFS feature
  extraction, ``gpio convert geojson``).
* :func:`repair_query_geometry` — count, warn, and repair the geometry column of
  an arbitrary query, auto-detecting native ``GEOMETRY`` vs WKB encoding. Used by
  the SQL/COPY paths (``gpio extract geoparquet``, the convert pipeline).
* :func:`repair_arrow_table_geometry` — count, warn, and repair a WKB-encoded
  PyArrow table's geometry column. Used by extractors that assemble a table in
  memory (WFS, ArcGIS, BigQuery, Carto).
"""

from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.logging_config import warn


def _warn_invalid(n: int, *, repaired: bool) -> None:
    """Emit a single warning describing invalid geometries found (no-op when n == 0)."""
    if n <= 0:
        return
    verb = "Repaired" if repaired else "Left unrepaired"
    noun = "geometry" if n == 1 else "geometries"
    suffix = "" if repaired else " (--no-repair-geometry)"
    warn(f"{verb} {n} invalid {noun}{suffix}")


def repair_geometry_sql(geom_expr: str) -> str:
    """Return a CASE-guarded ST_MakeValid wrap for a native GEOMETRY expression.

    Only invalid, non-NULL geometry is passed through ``ST_MakeValid``; valid
    geometry is returned unchanged (byte-identical) and NULL is preserved.

    Args:
        geom_expr: A SQL expression yielding a native ``GEOMETRY`` — a quoted
            column reference (e.g. ``"geom"``) or a constructor such as
            ``ST_GeomFromGeoJSON(feature.geometry)``.

    Returns:
        A SQL expression yielding a repaired native ``GEOMETRY``.
    """
    return (
        f"CASE WHEN {geom_expr} IS NULL OR ST_IsValid({geom_expr}) "
        f"THEN {geom_expr} ELSE ST_MakeValid({geom_expr}) END"
    )


def _layered_invalid_count_sql(source_sql: str, parsed_expr: str) -> str:
    """COUNT of invalid geometries, in a shape DuckDB executes without crashing.

    Filtering ``IS NOT NULL`` and evaluating ``ST_IsValid`` over the raw rows in
    one WHERE clause segfaults DuckDB <= 1.5.1's spatial extension when the
    column contains NULLs: its TRY() applies the selection vector twice under
    conditional execution, reading uninitialized vector memory (issues #642 and
    #737; fixed in DuckDB 1.5.2 — duckdb/duckdb-spatial#858 — well below the
    pyproject floor). Verified on issue #642's reproduction: projecting the parsed
    geometry first, filtering NULLs in a middle layer, and running
    ``ST_IsValid`` outermost is logically identical and crash-free.

    Kept for anyone who forces a DuckDB below the pyproject floor, but it is
    not a guarantee there: the layered form only avoids the crash because the
    optimizer happens to keep the two filters in separate operators, which
    nothing promises (duckdb/duckdb-spatial#858). #737's reporter hit SIGSEGV
    through this very code. The version floor is the fix; this shape is only
    slightly better odds.
    """
    return (
        f"SELECT COUNT(*) FROM ("
        f"SELECT __g FROM (SELECT {parsed_expr} AS __g FROM {source_sql}) "
        f"WHERE __g IS NOT NULL"
        f") WHERE NOT ST_IsValid(__g)"
    )


def repair_query_geometry(con, query: str, geometry_column: str, *, repair: bool = True) -> str:
    """Count and optionally repair invalid geometry in an arbitrary query.

    Auto-detects whether the geometry column in ``query`` is a native
    ``GEOMETRY`` or WKB-encoded binary, decoding/re-encoding as needed so the
    output column keeps the same encoding as the input. Emits a single warning
    with the invalid count. Used by paths that emit SQL rather than a PyArrow
    table (``gpio extract geoparquet``, ``gpio convert geojson``).

    The geometry column is left untouched (and the original query returned) when
    its type is neither ``GEOMETRY`` nor binary (e.g. a GeoArrow STRUCT), when no
    invalid geometry is found, or when ``repair=False``.

    Args:
        con: An open DuckDB connection with the spatial extension loaded.
        query: SQL query exposing ``geometry_column``.
        geometry_column: Name of the geometry column.
        repair: Whether to repair (True) or only count and warn (False).

    Returns:
        The original query, or a query that repairs the geometry column in place.
    """
    col = quote_identifier(geometry_column)
    try:
        desc = con.execute(f"DESCRIBE ({query})").fetchall()
    except Exception:
        return query
    col_type = next((d[1].upper() for d in desc if d[0] == geometry_column), None)
    if col_type is None:
        return query

    if "GEOMETRY" in col_type:
        # Native GEOMETRY: no decode needed; it cannot be a malformed-bytes case.
        parsed = col
        repaired_expr = repair_geometry_sql(parsed)
    elif "BLOB" in col_type or "BINARY" in col_type:
        # WKB-encoded: decode defensively. TRY() yields NULL on malformed bytes so
        # we never crash the pipeline; such rows are passed through unchanged.
        parsed = f"TRY(ST_GeomFromWKB({col}))"
        # AND-form (repair in THEN, passthrough in ELSE). The equivalent OR-form
        # with ST_MakeValid in the ELSE branch segfaults DuckDB <= 1.5.1's spatial
        # extension on some real WKB inputs (see repair_arrow_table_geometry).
        repaired_expr = (
            f"CASE WHEN {parsed} IS NOT NULL AND NOT ST_IsValid({parsed}) "
            f"THEN ST_AsWKB(ST_MakeValid({parsed})) ELSE {col} END"
        )
    else:
        # GeoArrow STRUCT or other encoding we don't repair in place.
        return query

    try:
        n = con.execute(_layered_invalid_count_sql(f"({query})", parsed)).fetchone()[0]
    except Exception:
        # Defensive: never let geometry repair break extraction.
        return query
    _warn_invalid(n, repaired=repair)
    if not repair or n == 0:
        return query
    return f"SELECT * REPLACE ({repaired_expr} AS {col}) FROM ({query})"


def repair_arrow_table_geometry(table, geometry_column: str = "geometry", *, repair: bool = True):
    """Count and (optionally) repair invalid geometry in a WKB-encoded Arrow table.

    Used by extraction paths (WFS, ArcGIS, BigQuery, Carto) that assemble a
    PyArrow table whose geometry column is WKB binary. ``ST_IsValid`` and
    ``ST_MakeValid`` require a native ``GEOMETRY``, so the WKB is decoded via
    ``ST_GeomFromWKB`` for the check/repair and re-encoded with ``ST_AsWKB``.

    A single warning with the exact invalid count is emitted when any invalid
    geometry is found (phrasing depends on ``repair``).

    The count and the repair run as ONE DuckDB scan over just the geometry
    column, returning only the repaired rows (NULL elsewhere); the repaired
    column is patched into the caller's table client-side. The table is never
    round-tripped through DuckDB, so its schema metadata, column order,
    attribute types and the valid rows' exact WKB bytes all survive untouched
    (#1268). The original table object is returned unchanged when there is
    nothing to repair (empty, no geometry column, no invalid rows, or
    ``repair=False``).

    Args:
        table: PyArrow table with a WKB-encoded geometry column.
        geometry_column: Name of the WKB geometry column.
        repair: Whether to repair (True) or only count and warn (False).

    Returns:
        Tuple ``(table, n_invalid)``.
    """
    if table.num_rows == 0 or geometry_column not in table.column_names:
        return table, 0

    import pyarrow.compute as pc

    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    col = quote_identifier(geometry_column)
    # Decode WKB defensively: TRY() yields NULL on malformed bytes so a bad row
    # never crashes extraction — such rows pass through unchanged.
    parsed = "TRY(ST_GeomFromWKB(__raw))"
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        # Register only the geometry column (zero-copy) under a fixed name, so
        # the scan never touches the other columns.
        con.register("_gpio_repair_src", table.select([geometry_column]))
        if not repair:
            # Count-only: the layered form — the single-WHERE form segfaults on
            # tables containing NULL geometry rows (#642), see
            # _layered_invalid_count_sql.
            n = con.execute(
                _layered_invalid_count_sql(f"(SELECT {col} AS __raw FROM _gpio_repair_src)", parsed)
            ).fetchone()[0]
            _warn_invalid(n, repaired=False)
            return table, n

        # Count and repair in ONE scan (#1268): each row yields the repaired
        # WKB when the geometry is invalid and NULL otherwise, so the result
        # holds only the repaired rows — not a full copy of the table. The
        # AND-form CASE (repair in THEN, passthrough in ELSE) is the
        # segfault-safe shape, and projecting the parsed geometry in an inner
        # layer keeps the NULL filter and ST_IsValid in separate operators,
        # exactly as _layered_invalid_count_sql does (#642, #737).
        fixed = (
            con.execute(
                f"SELECT CASE WHEN __g IS NOT NULL AND NOT ST_IsValid(__g) "
                f"THEN ST_AsWKB(ST_MakeValid(__g)) END AS __fixed "
                f"FROM (SELECT {parsed} AS __g "
                f"FROM (SELECT {col} AS __raw FROM _gpio_repair_src))"
            )
            .arrow()
            .read_all()
            .column(0)
        )
        n = len(fixed) - fixed.null_count
        _warn_invalid(n, repaired=True)
        if n == 0:
            return table, 0

        # Patch the repaired rows into the original column client-side: valid
        # rows keep their exact original bytes and encoding, and the rest of
        # the table — including its schema metadata — is never rebuilt.
        original = table.column(geometry_column)
        fixed = fixed.combine_chunks()
        if fixed.type != original.type:
            fixed = fixed.cast(original.type)
        patched = pc.if_else(pc.is_valid(fixed), fixed, original.combine_chunks())
        idx = table.column_names.index(geometry_column)
        repaired = table.set_column(idx, table.schema.field(idx), patched)
        return repaired, n
    except Exception:
        # Defensive: never let geometry repair break an extraction pipeline.
        return table, 0
    finally:
        try:
            con.unregister("_gpio_repair_src")
        except Exception:
            pass
        con.close()
