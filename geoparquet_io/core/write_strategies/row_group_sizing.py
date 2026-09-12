"""Shared row-group sizing helpers for the write strategies.

Both DuckDB-backed strategies (``duckdb-kv`` and ``disk-rewrite``) size row
groups through ``COPY ... (ROW_GROUP_SIZE n)``, which is expressed in *rows*.
A caller-supplied ``--row-group-size-mb`` target therefore has to be converted
to a row count first, from a cheap sample of the query. Keeping that conversion
here means the two strategies (and the plain-Parquet paths inside them) resolve
sizing identically instead of each carrying its own copy.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from geoparquet_io.core.logging_config import debug

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

# Rows sampled to estimate average row size when converting an MB target to a
# row count. Large enough to be representative, small enough to stay cheap.
_MB_ESTIMATE_SAMPLE_ROWS = 20000

# Clauses that must not follow a trailing ORDER BY for it to be safely strippable
# (they change which/how many rows a LIMIT would return).
_ORDER_BY_TAIL_STOPWORDS = re.compile(r"(?i)\b(limit|offset|union|except|intersect|fetch)\b")

# Constructs the scanner below cannot reason about. It tracks '' and "" state and
# nothing else, which is the hand-rolled quote walker #657 replaced on the
# --where path: line comments, block comments, dollar quoting and E'' escapes all
# hide (or fake) text from it. Measured against this scanner, each one mangles
# the query --
#     SELECT * FROM t -- keep order by x   ->  SELECT * FROM t --
#     SELECT * FROM t /* order by x */     ->  SELECT * FROM t /*
#     SELECT $$order by x$$ AS s FROM t    ->  SELECT $$
#     ... WHERE s = E'\'order by x'         ->  ... WHERE s = E'\'
# -- and the last is the dangerous one: it stays *valid* SQL with a different
# WHERE clause, so the estimate would be drawn from a different row set instead
# of failing loudly.
#
# There is no public DuckDB AST to gate this properly (extract_statements only
# splits statements), so the scanner stays, and any query carrying one of these
# is left alone: the optimization is skipped and sizing still works.
_UNSCANNABLE_SQL = re.compile(r"--|/\*|\$\$|\$[A-Za-z_][A-Za-z0-9_]*\$|(?<![A-Za-z0-9_])[eE]'")


def _strip_trailing_order_by(query: str) -> str:
    """Drop a trailing top-level ``ORDER BY`` clause for cheap row sampling.

    Estimating bytes-per-row does not need ordered rows, and a ``LIMIT`` layered
    over an ``ORDER BY`` forces DuckDB to scan (and sort) the *entire* source —
    re-downloading remote inputs and recomputing expensive ordering keys (e.g.
    ``ST_Hilbert``) just to size row groups. Without the ordering the sample's
    ``LIMIT`` pushes down, so DuckDB stops after a few row groups.

    Returns the query unchanged when no strippable top-level trailing ORDER BY is
    found, so estimation still works — it just skips the speed-up. The same is
    true for a query carrying any construct this scanner cannot track (see
    ``_UNSCANNABLE_SQL``): skipping the speed-up is always safe, guessing is not.
    """
    if _UNSCANNABLE_SQL.search(query):
        return query

    depth = 0
    in_single = in_double = False
    last_order_by = -1
    i, n = 0, len(query)
    while i < n:
        ch = query[i]
        if in_single:
            in_single = ch != "'"
        elif in_double:
            in_double = ch != '"'
        elif ch == "'":
            in_single = True
        elif ch == '"':
            in_double = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and ch in "oO":
            match = re.match(r"order\s+by\b", query[i:], re.IGNORECASE)
            prev = query[i - 1] if i else " "
            if match and not (prev.isalnum() or prev == "_"):
                last_order_by = i
                i += match.end()
                continue
        i += 1

    if last_order_by == -1:
        return query
    # Bail if anything follows the ORDER BY that would alter the sampled rows.
    if _ORDER_BY_TAIL_STOPWORDS.search(query[last_order_by:]):
        return query
    return query[:last_order_by].rstrip()


def _resolve_row_group_rows(
    con: duckdb.DuckDBPyConnection,
    query: str,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    verbose: bool,
) -> int | None:
    """Resolve an MB row-group target to a row count for DuckDB COPY TO.

    DuckDB's ``ROW_GROUP_SIZE`` is expressed in rows, so a ``--row-group-size-mb``
    target has to be converted before it can take effect on these strategies (the
    bytes-based ``ROW_GROUP_SIZE_BYTES`` option is unusable here because it
    requires disabling insertion-order preservation, which would undo any
    spatial ordering already applied). An explicit row count always wins; when
    only an MB target is given we estimate bytes-per-row from a sample and mirror
    the arrow write path (see ``_write_table_with_settings``).
    """
    if row_group_rows:
        return row_group_rows
    if not row_group_size_mb:
        return None

    # Sample without the ORDER BY so the LIMIT streams (a LIMIT over an ORDER BY
    # would rescan/sort the whole source — re-downloading remote inputs and
    # recomputing the ordering key — just to size row groups).
    sample_query = _strip_trailing_order_by(query)
    try:
        sample = (
            con.execute(f"SELECT * FROM ({sample_query}) LIMIT {_MB_ESTIMATE_SAMPLE_ROWS}")
            .arrow()
            .read_all()
        )
    except Exception as exc:  # pragma: no cover - defensive, fall back to default
        if verbose:
            debug(f"Could not sample rows for --row-group-size-mb estimate: {exc}")
        return None

    if sample.num_rows == 0:
        return None

    return _rows_for_mb_target(sample, row_group_size_mb, verbose)


def _resolve_row_group_rows_for_table(
    table: pa.Table,
    row_group_size_mb: float | None,
    row_group_rows: int | None,
    verbose: bool = False,
) -> int | None:
    """Resolve row-group sizing for an in-memory Arrow table.

    Same contract as :func:`_resolve_row_group_rows`, but the table itself is the
    sample so no query has to be re-run.
    """
    if row_group_rows:
        return row_group_rows
    if not row_group_size_mb or table.num_rows == 0:
        return None

    rows = _rows_for_mb_target(table, row_group_size_mb, verbose)
    return min(rows, table.num_rows) if rows else None


def _rows_for_mb_target(sample: pa.Table, row_group_size_mb: float, verbose: bool) -> int:
    """Convert an MB target into a row count using a sample's bytes-per-row."""
    from geoparquet_io.core.parquet_writer import estimate_row_size

    bytes_per_row = estimate_row_size(sample)
    target_bytes = row_group_size_mb * 1024 * 1024
    rows = max(1, int(target_bytes // bytes_per_row))
    if verbose:
        debug(
            f"Resolved --row-group-size-mb {row_group_size_mb} to {rows:,} rows/group "
            f"(~{bytes_per_row} bytes/row)"
        )
    return rows
