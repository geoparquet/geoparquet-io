"""
Convert core exceptions to click exceptions at CLI boundary.

This module bridges the gap between framework-agnostic core exceptions
and click-specific exceptions needed for proper CLI error display.
"""

from __future__ import annotations

import os

import click

from geoparquet_io.core.exceptions import (
    FailureKind,
    GeoParquetError,
    InvalidParameterError,
    classify_failure,
    sanitize_error_message,
)


def handle_core_exception(exc: GeoParquetError) -> click.ClickException:
    """The one GeoParquetError -> Click translation.

    ``InvalidParameterError`` names the option that was wrong, so it becomes a
    ``BadParameter`` (exit 2, a usage-style line); every other gpio error is a
    plain ``ClickException`` (exit 1) carrying its already-sanitized message.
    """
    if isinstance(exc, InvalidParameterError):
        return click.BadParameter(exc.message, param_hint=exc.param_name)
    return click.ClickException(exc.message)


def _error_text(exc: BaseException) -> str:
    """The sanitized text of ``exc``, minus DuckDB's echo of the generated SQL.

    A DuckDB binder/parser error appends ``LINE n: <the SQL gpio built>`` and a
    caret; that excerpt buries the cause and is never the user's to act on.
    """
    import duckdb

    text = exc.message if isinstance(exc, GeoParquetError) else sanitize_error_message(str(exc))
    if isinstance(exc, duckdb.Error):
        text = text.split("\nLINE ")[0].strip()
    return text


def _not_parquet_message(path: str | None) -> str:
    subject = f"The file '{os.path.basename(path)}'" if path else "This file"
    return (
        f"{subject} has a .parquet extension but is not a valid "
        f"Parquet file. It may be a different format (CSV, JSON, etc.) that was "
        f"incorrectly named. Check the file contents or use 'gpio convert' to "
        f"create a proper GeoParquet file."
    )


def cli_error_for(exc: BaseException, *, path: str | None = None) -> click.ClickException | None:
    """The CLI's answer to an exception, or ``None`` when it is not one gpio claims.

    The one translation from an exception to what the terminal shows. It decides
    nothing itself: the taxonomy lives in ``core/`` (``classify_failure``, the
    spill-space and unpublished-extension hints, and
    :data:`~geoparquet_io.core.duckdb_utils.INPUT_FILE_DUCKDB_ERRORS`, which
    carries the reasoning for where the "input file" line falls) so ``api/`` can
    reach the same verdict -- it may not import ``cli/``.

    In order: the two hints that name a fix (a wrapped error keeps them, so they
    are asked first), then a file that is not Parquet, then gpio's own errors,
    then a DuckDB failure the input file caused. Every message is sanitized: DuckDB
    names the URL it failed on with its query string intact and quotes offending
    values back verbatim.

    Args:
        exc: What was caught.
        path: The input the command was reading, to name it in a not-Parquet answer.

    ``None`` means "not mine": the caller re-raises, traceback intact, which is
    what a genuine gpio bug deserves.
    """
    from geoparquet_io.core.duckdb_utils import is_input_file_duckdb_error, spill_space_hint
    from geoparquet_io.core.exceptions import unpublished_extension_hint

    # Click's own exceptions are already the final word (and `Exit`/`Abort` are
    # control flow): translating one again would re-run the taxonomy on its cause.
    if isinstance(exc, (click.ClickException, click.exceptions.Exit, click.Abort)):
        return None

    message = _error_text(exc)

    # Running out of room to spill needs the line naming TMPDIR that DuckDB's own
    # message never mentions (#752); a community extension the registry has no
    # build of would otherwise read as "your input file is unreachable" (#778).
    extra = spill_space_hint(exc) or unpublished_extension_hint(exc)
    if extra is not None:
        return click.ClickException(f"{message}\n\n{extra}")

    if classify_failure(exc).kind is FailureKind.NOT_PARQUET:
        return click.ClickException(_not_parquet_message(path))

    if isinstance(exc, GeoParquetError):
        return handle_core_exception(exc)

    if is_input_file_duckdb_error(exc):
        return click.ClickException(message)

    return None


def click_error_for(
    exc: BaseException, *, path: str | None = None, hint: str | None = None
) -> click.ClickException:
    """:func:`cli_error_for`, but always an answer: any other exception is its sanitized text.

    The replacement for ``click.ClickException(str(e))`` at the sites that already
    decided the failure is the user's to read. ``hint`` is guidance appended to a
    DuckDB failure (e.g. the ``--where`` clause that probably caused a binder error).
    """
    import duckdb

    if hint and isinstance(exc, duckdb.Error):
        return click.ClickException(f"{_error_text(exc)}\n\n{hint}")
    return cli_error_for(exc, path=path) or click.ClickException(_error_text(exc))
