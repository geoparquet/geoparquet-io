"""The one place an exception becomes what the terminal shows.

Framework-agnostic core exceptions are translated here into Click exceptions:
``handle_core_exception`` is the GeoParquetError table, ``cli_error_for`` /
``click_error_for`` the full taxonomy, ``error_boundary`` the single boundary
(translate, debug-log the traceback, re-raise) every command class uses.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

import click

from geoparquet_io.core.exceptions import (
    FailureKind,
    GeoParquetError,
    InvalidParameterError,
    classify_failure,
    clean_error_text,
)


def handle_core_exception(exc: GeoParquetError) -> click.ClickException:
    """The one GeoParquetError -> Click translation.

    ``InvalidParameterError`` names the option that was wrong, so it becomes a
    ``BadParameter`` (exit 2, a usage-style line); every other gpio error is a
    plain ``ClickException`` (exit 1) carrying its already-sanitized message.
    """
    if isinstance(exc, InvalidParameterError):
        return click.BadParameter(exc.reason, param_hint=exc.param_name)
    return click.ClickException(exc.message)


def error_text(exc: BaseException) -> str:
    """The text of ``exc`` for the terminal, cleaned by the one policy.

    Same ``clean_error_text`` the log path uses, minus DuckDB's echo of the
    generated SQL. A DuckDB binder/parser error appends ``LINE n: <the SQL gpio built>`` and a
    caret; that excerpt buries the cause and is never the user's to act on.
    """
    import duckdb

    text = exc.message if isinstance(exc, GeoParquetError) else clean_error_text(str(exc))
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

    message = error_text(exc)

    # Running out of room to spill needs the line naming TMPDIR that DuckDB's own
    # message never mentions (#752); a community extension the registry has no
    # build of would otherwise read as "your input file is unreachable" (#778).
    extra = spill_space_hint(exc) or unpublished_extension_hint(exc)
    if extra is not None:
        return click.ClickException(f"{message}\n\n{extra}")

    # Only with a file to name: a gpio error that already names it (and says what to do) is better as it is.
    if path is not None and classify_failure(exc).kind is FailureKind.NOT_PARQUET:
        return click.ClickException(_not_parquet_message(path))

    if isinstance(exc, GeoParquetError):
        return handle_core_exception(exc)

    if is_input_file_duckdb_error(exc):
        return click.ClickException(message)

    return None


def _answer(
    exc: BaseException, *, path: str | None, hint: str | None, always: bool
) -> click.ClickException | None:
    """The one answering step: translate ``exc``, log its traceback once, return the answer.

    ``cli_error_for`` decides; ``always`` turns "not mine" into the exception's
    cleaned text. ``hint`` is guidance appended to a DuckDB failure whose input
    could not be read as a file -- not to a disk shortage, an unpublished
    extension or a non-Parquet file, which the taxonomy already explained. The
    traceback is hidden, not discarded: ``--verbose`` prints it, because
    ``verbose_option`` raises the package logger to DEBUG as the flag is parsed,
    before the command body runs.
    """
    import duckdb

    from geoparquet_io.core.duckdb_utils import spill_space_hint
    from geoparquet_io.core.exceptions import unpublished_extension_hint
    from geoparquet_io.core.logging_config import get_logger

    error = cli_error_for(exc, path=path)
    if error is None:
        if not always:
            return None
        if isinstance(exc, click.ClickException):
            return exc  # already final: not translated, not logged again
        error = click.ClickException(error_text(exc))
    already_explained = (
        spill_space_hint(exc) is not None
        or unpublished_extension_hint(exc) is not None
        or classify_failure(exc).kind is FailureKind.NOT_PARQUET
    )
    if (
        hint
        and isinstance(exc, duckdb.Error)
        and not already_explained
        and type(error) is click.ClickException
    ):
        error = click.ClickException(f"{error.message}\n\n{hint}")
    get_logger(__name__).debug("Converted a low-level failure to an error line", exc_info=exc)
    return error


def click_error_for(
    exc: BaseException, *, path: str | None = None, hint: str | None = None
) -> click.ClickException:
    """:func:`cli_error_for`, but always an answer: any other exception is its cleaned text.

    The replacement for ``click.ClickException(str(e))`` at the sites that already
    decided the failure is the user's to read; it logs the traceback like the
    boundary does.
    """
    answer = _answer(exc, path=path, hint=hint, always=True)
    assert answer is not None  # always=True
    return answer


@contextmanager
def error_boundary(
    *, path: str | None = None, hint: str | None = None, always: bool = False
) -> Iterator[None]:
    """The one boundary: translate a failure no gpio frame owned, or let it through.

    An exception ``cli_error_for`` does not claim (a genuine gpio bug) is
    re-raised untouched, traceback intact. Both the root group and every command
    class wrap their ``invoke`` in this, so a command run on its own answers
    exactly as it does under the root; a command that knows its input file wraps
    its work in ``error_boundary(path=...)`` so a non-Parquet answer can name it.
    ``always=True`` is for a command that has always shown any failure on its input
    as a plain error line (``inspect``): an unclaimed exception becomes its cleaned
    text instead of a traceback.
    """
    try:
        yield
    except Exception as exc:
        error = _answer(exc, path=path, hint=hint, always=always)
        if error is None:
            raise
        raise error from exc
