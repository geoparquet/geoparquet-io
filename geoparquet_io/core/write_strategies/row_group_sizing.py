"""Shared row-group sizing helpers for the write strategies.

Both DuckDB-backed strategies (``duckdb-kv`` and ``disk-rewrite``) size row
groups through ``COPY ... (ROW_GROUP_SIZE n)``, which is expressed in *rows*.
A caller-supplied ``--row-group-size-mb`` target therefore has to be converted
to a row count first, from a cheap sample of the query. Keeping that conversion
here means the two strategies (and the plain-Parquet paths inside them) resolve
sizing identically instead of each carrying its own copy.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from geoparquet_io.core.duckdb_utils import _strip_trailing_order_by
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.parquet_writer import estimate_row_size

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

# Rows sampled to estimate average row size when converting an MB target to a
# row count. Large enough to be representative, small enough to stay cheap.
_MB_ESTIMATE_SAMPLE_ROWS = 20000

__all__ = [
    "_resolve_row_group_rows",
    "_resolve_row_group_rows_for_table",
    # Re-exported for the tests and callers that have always imported it from
    # here; it moved to `duckdb_utils` when the derived-stats scan became its
    # second caller (#1177) and a query-text helper could no longer live under
    # a module named for row groups.
    "_strip_trailing_order_by",
]


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

    bytes_per_row = estimate_row_size(sample)
    target_bytes = row_group_size_mb * 1024 * 1024
    rows = max(1, int(target_bytes // bytes_per_row))
    if verbose:
        debug(
            f"Resolved --row-group-size-mb {row_group_size_mb} to {rows:,} rows/group "
            f"(~{bytes_per_row} bytes/row)"
        )
    return rows
