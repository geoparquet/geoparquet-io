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

    A ``duckdb.Error`` on a read is almost always a property of the input file.
    DuckDB's Parquet reader refuses a ``geo`` block it cannot parse -- no
    ``columns`` object, a non-string ``version``, a column entry with no
    ``encoding`` -- and that block is arbitrary JSON written by somebody else's
    tool, so the refusal is news about the file rather than a gpio bug (#983).
    DuckDB's message says exactly what is wrong, so it is kept verbatim; only
    the traceback around it was noise.

    ``None`` means "not mine": the caller re-raises, traceback intact, which is
    what a genuine gpio bug deserves.
    """
    import duckdb

    from geoparquet_io.core.duckdb_utils import spill_space_hint

    # Running out of room to spill is a DuckDB error too, and it needs the extra
    # line naming TMPDIR that DuckDB's own message never mentions. Ask first, so
    # the generic translation below cannot swallow it.
    hint = spill_space_hint(exc)
    if hint is not None:
        return click.ClickException(f"{exc}\n\n{hint}")

    if isinstance(exc, duckdb.Error):
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
