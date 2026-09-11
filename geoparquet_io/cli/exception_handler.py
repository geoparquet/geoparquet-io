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

    It decides nothing itself. Three questions already have an answer in
    ``core/`` -- is this a spill-volume shortage, an unpublished community
    extension, or a failure the *input file* caused
    (:data:`~geoparquet_io.core.duckdb_utils.INPUT_FILE_DUCKDB_ERRORS`, which
    carries the reasoning for where that line falls) -- and this asks them in
    order, most specific first, then wraps. Keeping the taxonomy in ``core/`` is
    what lets ``api/`` reach the same verdict, which it could never do from
    here: ``api/`` may not import ``cli/``.

    Every message is sanitized before it is shown. DuckDB names the URL it
    failed on with its query string intact, and quotes offending values back
    verbatim, so a presigned signature or an ANSI escape can ride in from
    outside gpio -- see
    :func:`~geoparquet_io.core.exceptions.sanitize_error_message`.

    ``None`` means "not mine": the caller re-raises, traceback intact, which is
    what a genuine gpio bug deserves.
    """
    from geoparquet_io.core.duckdb_utils import is_input_file_duckdb_error, spill_space_hint
    from geoparquet_io.core.exceptions import (
        sanitize_error_message,
        unpublished_extension_hint,
    )

    message = sanitize_error_message(str(exc))

    # Running out of room to spill is a DuckDB error too, and it needs the extra
    # line naming TMPDIR that DuckDB's own message never mentions (#752).
    hint = spill_space_hint(exc)
    if hint is not None:
        return click.ClickException(f"{message}\n\n{hint}")

    # A community extension the registry has no build of arrives as an
    # HTTPException, which hangs below IOException -- so without this it would
    # read as "your input file is unreachable" and lose #778's guidance.
    hint = unpublished_extension_hint(exc)
    if hint is not None:
        return click.ClickException(f"{message}\n\n{hint}")

    if is_input_file_duckdb_error(exc):
        return click.ClickException(message)

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
