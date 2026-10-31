"""Entry-level validation shared by every ``gpio extract`` backend.

Four backends take ``--include-cols``/``--exclude-cols`` as one comma-separated
string and split it themselves. The Click callback added for #969
(:func:`geoparquet_io.cli.decorators._validate_column_list`) rejects a blank
entry before the value reaches them -- but **the Python API never goes through
Click**. That was the stated reason #973 added a core-level check for BigQuery,
and it applied verbatim to the backends that got only the Click guard (#980):
``ops.from_carto(..., include_cols="id,,name")`` raised a raw
``ValueError: cannot quote an empty SQL identifier`` out of
``quote_identifier()``, and ``ops.from_arcgis(..., include_cols="name,,pop")``
sent ``outFields=name,,pop`` to the server.

The *validation* is the same for all of them, so it lives here once. Only the
schema half genuinely differs -- a DuckDB ``DESCRIBE`` for BigQuery, Carto's
``fields`` JSON block, an ArcGIS layer's advertised fields, an Arrow schema for
parquet -- so each backend reads its own schema and hands the names in.

Two levels, because not every call site has a schema:

- :func:`split_column_list` / :func:`reject_blank_column_entries` need nothing
  but the value, so they run before any network work;
- :func:`resolve_columns_against_schema` additionally checks membership, and is
  called only where the schema is already in hand, never for an extra
  round-trip.

This module is also the one place that answers three further questions about
column names (#1367, concept 12), so no caller carries a private copy:

- **Does this name mean that column?** :func:`fold` is the one case-folding
  rule, :func:`matching_columns` the one lookup (an exact spelling first, then
  every folded one), :func:`find_column` / :func:`has_column` /
  :func:`same_column` its three shapes. Nothing else applies ``.lower()`` to a
  column name (the ``G1.column_fold`` guard).
- **What may a computed column be called?** :func:`unique_name`, the one
  suffixer. Column names are output bytes, so each caller passes the ``start``
  of its own historic numbering.
- **Which columns survive an exclusion?** :func:`exclude_columns`, the one
  guard against dropping a protected column or every column.
"""

from __future__ import annotations

import string
from collections.abc import Iterable

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import warn

__all__ = [
    "bound_column",
    "exclude_columns",
    "find_column",
    "fold",
    "has_column",
    "join_column_list",
    "matching_columns",
    "reject_blank_column_entries",
    "resolve_columns_against_schema",
    "same_column",
    "split_column_list",
    "unique_name",
]

# DuckDB folds identifiers case-insensitively, but only over ASCII: "HEIGHT"
# binds to a `Height` column while "STRASSE" does not bind to `straße`. A lookup
# that follows the same rule accepts exactly what the generated SQL would bind,
# no more and no less.
_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)


def fold(name: str) -> str:
    """``name`` as DuckDB's binder compares it: ASCII letters lowered, the rest kept."""
    return str(name).translate(_ASCII_LOWER)


def matching_columns(name: str, columns: Iterable[str]) -> list[str]:
    """Every column ``name`` could mean: its exact spelling, else each folded match.

    Exact first because folding is not injective -- a table may carry both
    ``id`` and ``ID`` -- so a name that is a column is never second-guessed.
    Order and duplicates follow ``columns``.
    """
    columns = [str(col) for col in columns]
    if name in columns:
        return [name]
    folded = fold(name)
    return [col for col in columns if fold(col) == folded]


def find_column(name: str, columns: Iterable[str], *, option_name: str = "column") -> str | None:
    """The column ``name`` means, in the schema's spelling, or None when there is none.

    Raises:
        InvalidParameterError: ``name`` is not a column, matches several
            differently-cased ones, and so cannot be told apart
    """
    found = matching_columns(name, columns)
    if len(found) > 1:
        raise InvalidParameterError(
            option_name,
            f"Ambiguous column name: {name} matches {', '.join(sorted(found))}. "
            "Use the exact spelling.",
        )
    return found[0] if found else None


def bound_column(name: str, columns: Iterable[str], *, option_name: str) -> str | None:
    """The column a SQL binder would resolve ``name`` to, or None when there is none.

    Stricter than :func:`find_column` where the name goes into generated SQL:
    DuckDB binds ``"HEIGHT"`` to the first of ``height`` and ``HEIGHT``, so an
    exact hit does not make the request unambiguous. Two columns that fold
    together are refused (naming both), instead of reading the wrong one with no
    warning (#1109).

    Raises:
        InvalidParameterError: more than one column folds to ``name``
    """
    found = [col for col in (str(c) for c in columns) if same_column(col, name)]
    if len(found) > 1:
        raise InvalidParameterError(
            option_name,
            f"Ambiguous column name: {name} matches {', '.join(sorted(found))}; "
            "DuckDB cannot tell them apart. Rename one of the columns.",
        )
    return found[0] if found else None


def has_column(columns: Iterable[str], name: str) -> bool:
    """Whether ``name`` resolves to a column, as DuckDB would resolve it."""
    return bool(matching_columns(name, columns))


def same_column(left: str, right: str) -> bool:
    """Whether two spellings name one column to DuckDB's binder."""
    return fold(left) == fold(right)


def unique_name(base: str, taken: Iterable[str], *, start: int = 1) -> str:
    """``base``, or ``base_<n>`` for the first ``n >= start`` that ``taken`` does not hold.

    A computed column aliased ``AS base`` next to ``SELECT *`` does not fail
    when the source already has that name: DuckDB silently renames one of the
    two (``base_1``), and everything that goes on to say ``base`` then binds to
    the *other* column. That is how a computed bbox came to be written under a
    name its ``covering`` did not point at (#1079), and how the geometry built
    from a CSV's WKT column came to be the source's own VARCHAR column of that
    name (#1176). Picking the free name up front makes the alias and every later
    reference one variable, agreeing by construction.

    The comparison is by :func:`fold` because DuckDB binds identifiers that way:
    a source column ``BBOX`` takes the name ``bbox`` just as surely, even though
    Parquet itself would keep the two apart. ``taken`` is not changed; a caller
    suffixing several names in turn adds each result to it.

    Args:
        base: The name the computed column would like
        taken: The names it is emitted beside
        start: The first suffix tried, ``1`` for ``bbox_1``. Output column names
            are bytes, so a caller whose numbering began at ``2`` keeps passing 2.
    """
    held = {fold(name) for name in taken}
    candidate, number = base, start
    while fold(candidate) in held:
        candidate = f"{base}_{number}"
        number += 1
    return candidate


def exclude_columns(
    schema_cols: list[str],
    requested: list[str] | None,
    *,
    protect: str | None = None,
    option_name: str = "--exclude-cols",
) -> list[str]:
    """The columns of ``schema_cols`` left after dropping ``requested``, in schema order.

    The one guard behind every ``--exclude-cols`` that drops columns from a
    table already in hand (``extract``'s Arrow path, ArcGIS, Carto). Names are
    resolved against the schema (:func:`resolve_columns_against_schema`: exact
    first, then a unique folded match; one that is absent or ambiguous raises),
    so a case slip is an error rather than a filter that silently matches
    nothing (#991).

    ``protect`` names a column the output requires (the geometry, on a path that
    writes GeoParquet): asking to drop it warns once and keeps it.

    Raises:
        InvalidParameterError: a name is blank, absent or ambiguous, or the
            exclusion would leave no column at all -- a request that is a typo,
            which used to write a 0-column file without a word (#1176)
    """
    drop = resolve_columns_against_schema(requested, schema_cols, option_name)
    if drop and protect is not None and protect in drop:
        drop = [col for col in drop if col != protect]
        warn(f"Cannot exclude '{protect}' column - it is required for GeoParquet output")
    if not drop:
        return list(schema_cols)
    keep = [col for col in schema_cols if col not in set(drop)]
    if not keep:
        raise InvalidParameterError(
            option_name,
            f"excluding {', '.join(sorted(set(drop)))} would leave no columns at all "
            f"(available: {', '.join(schema_cols)})",
        )
    return keep


def reject_blank_column_entries(
    requested_cols: list[str] | None,
    option_name: str,
) -> None:
    """Reject a blank or whitespace-only entry in a list of column names.

    Args:
        requested_cols: Column names the caller asked for (or None)
        option_name: Option to name in the error message, e.g. "--include-cols"

    Raises:
        InvalidParameterError: If any entry is empty or whitespace-only
    """
    if not requested_cols:
        return
    if any(not col.strip() for col in requested_cols):
        raise InvalidParameterError(option_name, "column names cannot be empty or whitespace-only")


def split_column_list(value: str | None, option_name: str) -> list[str] | None:
    """Split a comma-separated column option into stripped, non-blank entries.

    Replaces the ``[c.strip() for c in v.split(",")] if v else None`` idiom each
    backend used to spell out, so the blank-entry check cannot be forgotten at a
    new call site.

    A **wholly empty** value is not a blank entry, it is an unset option, and
    normalises to None -- ``--include-cols "$COLS"`` expands to ``""`` when
    ``COLS`` is unset, and that has always meant "not given" (#973).

    Args:
        value: The raw option value, e.g. "id,name" (or None)
        option_name: Option to name in the error message

    Returns:
        The entries, stripped; or None when the option was not given

    Raises:
        InvalidParameterError: If any entry is empty or whitespace-only
    """
    if not value:
        return None
    entries = [entry.strip() for entry in value.split(",")]
    reject_blank_column_entries(entries, option_name)
    return entries


def join_column_list(columns: list[str] | None, argument_name: str) -> str | None:
    """Join a list argument into the comma-separated option the core backends take.

    The inverse of :func:`split_column_list`, and it exists because of the seam
    *between* the two. The Python API's BigQuery entry points take ``columns`` as
    a **list** and join it for a core function that takes a string, and ``[""]``
    joins to ``""`` -- which the splitting side correctly reads as "option not
    given", silently widening the request to every column (#992). Validating and
    joining in one call closes that seam so the next list argument cannot reopen
    it by spelling the two steps out again.

    The two shapes have different "unset" values, and that is the whole point:

    - a **string** option is unset when it is wholly empty, so
      ``--include-cols "$COLS"`` keeps working when ``COLS`` is (#973);
    - a **list** argument is unset when it is None or ``[]``. A list holding a
      blank entry is a caller's mistake, never an absent argument.

    Args:
        columns: The column names, or None
        argument_name: The *argument* to name in the error message, e.g.
            "columns" -- these callers have no CLI option to point at

    Returns:
        The comma-separated value, or None when the argument was not given

    Raises:
        InvalidParameterError: If any entry is empty or whitespace-only
    """
    reject_blank_column_entries(columns, argument_name)
    if not columns:
        return None
    return ",".join(columns)


def resolve_columns_against_schema(
    requested_cols: list[str] | None,
    all_columns: list[str],
    option_name: str,
) -> list[str] | None:
    """Check requested column names against a schema and return its spellings.

    The resolving sibling of ``core.extract.validate_columns``, which the
    parquet backend has run since #731. Without it a name the source does not
    carry is passed straight through -- quoted into a SELECT (BigQuery, Carto)
    or sent as ``outFields`` (ArcGIS) -- and fails as a backend error, or
    silently does nothing.

    Blank entries are rejected here too, so a caller with a schema in hand needs
    only this one call.

    Matching is case-insensitive and the **schema spelling is returned**. That
    is not cosmetic: Carto is Postgres, where the delimited identifier
    ``"OWNER"`` does *not* match a column named ``Owner``; and BigQuery's
    ``_build_column_list`` filters ``--exclude-cols`` by string comparison, so
    ``--exclude-cols ID`` against a column ``id`` silently excluded nothing.

    An **exact** match is preferred to a folded one, because folding is not
    injective: DuckDB permits a local table carrying both ``id`` and ``ID``.

    Where folding is genuinely ambiguous -- a third spelling like ``Id`` against
    a schema holding both ``id`` and ``ID`` -- this **raises** rather than
    picking one. A ``{col.lower(): col}`` map silently kept the last, which is
    an arbitrary choice; on ``--exclude-cols`` that arbitrary choice deletes a
    column, so the caller is asked to disambiguate instead.

    Args:
        requested_cols: Column names the caller asked for (or None)
        all_columns: Every column the source's schema carries
        option_name: Option to name in the error message, e.g. "--include-cols"

    Returns:
        The requested columns in their schema spelling, or None

    Raises:
        InvalidParameterError: If any entry is blank or absent from the schema
    """
    if not requested_cols:
        return None

    reject_blank_column_entries(requested_cols, option_name)

    matches = [(col, matching_columns(col, all_columns)) for col in requested_cols]

    ambiguous = {col: cands for col, cands in matches if len(cands) > 1}
    if ambiguous:
        detail = "; ".join(
            f"{col} matches {', '.join(sorted(cands))}" for col, cands in sorted(ambiguous.items())
        )
        raise InvalidParameterError(
            option_name,
            f"Ambiguous column names: {detail}. Use the exact spelling.",
        )

    missing = [col for col, cands in matches if not cands]
    if missing:
        raise InvalidParameterError(
            option_name,
            f"Columns not found in schema: {', '.join(sorted(missing))}. "
            f"Available columns: {', '.join(all_columns)}",
        )

    return [cands[0] for _, cands in matches]
