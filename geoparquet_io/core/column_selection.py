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
"""

from __future__ import annotations

from geoparquet_io.core.exceptions import InvalidParameterError

__all__ = [
    "join_column_list",
    "reject_blank_column_entries",
    "resolve_columns_against_schema",
    "split_column_list",
]


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

    exact = set(all_columns)
    folded: dict[str, list[str]] = {}
    for col in all_columns:
        folded.setdefault(col.lower(), []).append(col)

    def _candidates(col: str) -> list[str]:
        """Every schema spelling this name could mean: the exact one, else the folds."""
        return [col] if col in exact else folded.get(col.lower(), [])

    matches = [(col, _candidates(col)) for col in requested_cols]

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
