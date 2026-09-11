"""
Convert core exceptions to click exceptions at CLI boundary.

This module bridges the gap between framework-agnostic core exceptions
and click-specific exceptions needed for proper CLI error display.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import TypeVar

import click
import duckdb

from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeometryError,
    GeoParquetError,
    InvalidParameterError,
    PartitionError,
    RemoteAccessError,
    ValidationError,
)

F = TypeVar("F", bound=Callable)

#: The DuckDB failures whose cause is the *input*, not gpio.
#:
#: Enumerated one by one rather than written as a base class, because DuckDB's
#: hierarchy does not split along this line. Measured against duckdb 1.5.5:
#: ``InvalidInputException`` -- the one #983 is about -- sits under
#: ``ProgrammingError`` next to ``ParserException``, ``BinderException`` and
#: ``CatalogException``, which are its exact opposite, and ``duckdb.Error``'s
#: only direct subclass is ``DatabaseError``, so every shorthand available is
#: either too wide or splits the wrong set.
#:
#: * ``InvalidInputException`` -- the file's own bytes or metadata are
#:   unreadable. A ``geo`` block with no ``columns`` object raises this, as do
#:   all four of #983's reproductions.
#: * ``IOException`` -- missing, unreachable or unreadable. ``HTTPException``
#:   hangs below it, so a remote URL that will not fetch is covered too; both
#:   are properties of the path the user named.
#: * ``ConversionException`` -- a value the file holds will not cast.
#:
#: What is deliberately *not* here is everything gpio itself can get wrong.
#: gpio authors every SQL string it runs, so ``ParserException``,
#: ``BinderException`` and ``CatalogException`` are never news about the user's
#: file and always a bug in a query we generated -- the defect class behind
#: #700, #718 and #944. Those keep their traceback: answering them with the
#: same ``Error:`` line used for a bad input would tell a user their data is
#: broken when the broken thing is ours.
INPUT_FILE_DUCKDB_ERRORS = (
    duckdb.InvalidInputException,
    duckdb.IOException,
    duckdb.ConversionException,
)


def handle_core_exception(exc: GeoParquetError) -> click.ClickException:
    """Convert a core exception to the appropriate click exception."""
    if isinstance(exc, InvalidParameterError):
        return click.BadParameter(exc.message, param_hint=exc.param_name)
    elif isinstance(exc, FileNotFoundGeoParquetError):
        return click.ClickException(exc.message)
    elif isinstance(exc, RemoteAccessError):
        return click.ClickException(exc.message)
    elif isinstance(exc, GeometryError):
        return click.ClickException(exc.message)
    elif isinstance(exc, PartitionError):
        return click.ClickException(exc.message)
    elif isinstance(exc, ValidationError):
        return click.ClickException(exc.message)
    else:
        # Generic fallback for any GeoParquetError subclass
        return click.ClickException(exc.message)


def cli_error_for(exc: BaseException) -> click.ClickException | None:
    """The CLI's answer to an exception no gpio frame owned, or ``None``.

    ``handle_core_exception`` above translates gpio's own vocabulary. This is
    the other half: exceptions raised *underneath* gpio, by DuckDB, which reach
    the boundary with nothing in between willing to claim them.

    One of those is worth answering without a traceback: DuckDB's Parquet reader
    refusing the input. It refuses a ``geo`` block it cannot parse -- no
    ``columns`` object, a non-string ``version``, a column entry with no
    ``encoding`` -- and that block is arbitrary JSON written by somebody else's
    tool, so the refusal is news about the file rather than a gpio bug (#983).
    DuckDB's message says exactly what is wrong, so it is kept verbatim; only
    the traceback around it was noise.

    That is the whole claim, and ``INPUT_FILE_DUCKDB_ERRORS`` is what makes it
    literally true rather than merely usual: a DuckDB failure gpio *caused*, by
    generating SQL that will not parse or bind, is not in the tuple and so keeps
    its traceback.

    ``None`` means "not mine": the caller re-raises, traceback intact, which is
    what a genuine gpio bug deserves.
    """
    from geoparquet_io.core.duckdb_utils import spill_space_hint

    # Running out of room to spill is a DuckDB error too, and it needs the extra
    # line naming TMPDIR that DuckDB's own message never mentions. Ask first, so
    # the translation below cannot swallow it.
    hint = spill_space_hint(exc)
    if hint is not None:
        return click.ClickException(f"{exc}\n\n{hint}")

    if isinstance(exc, INPUT_FILE_DUCKDB_ERRORS):
        return click.ClickException(str(exc))

    return None


def core_exception_handler(func: F) -> F:
    """
    Decorator to catch core exceptions and convert to click exceptions.

    Use this decorator on CLI command functions to automatically convert
    framework-agnostic core exceptions to click exceptions.

    Note:
        This decorator is provided for external integrations and custom Click
        commands that use geoparquet-io core functions. The main CLI commands
        handle exceptions through Click's built-in error handling, but this
        decorator enables consistent error display for third-party extensions.

    Example:
        @click.command()
        @core_exception_handler
        def my_command():
            # If this raises InvalidParameterError, it becomes click.BadParameter
            do_something_that_might_fail()
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except GeoParquetError as e:
            raise handle_core_exception(e) from e

    return wrapper  # type: ignore
