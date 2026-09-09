"""CLI helpers shared between ``cli/main.py`` and the per-group command modules.

``cli/main.py`` owns the root ``gpio`` group and imports each command group from
:mod:`geoparquet_io.cli.commands`. Anything a group module also needs therefore
cannot keep living in ``main.py``: importing it back would make the dependency
cyclic. It lives here instead, so both sides import downwards.

The split against the neighbouring modules:

* :mod:`geoparquet_io.cli.decorators` - reusable Click *option* decorators,
  the ``click.Command`` subclasses commands declare with ``cls=``
  (``GlobAwareCommand``, ``SingleFileCommand``), and small option-parsing
  helpers such as ``parse_row_group_options``. That is still their home;
  nothing was moved out of it.
* this module - the group-neutral runtime plumbing that is not a decorator and
  not a Click option: S3 activation, the shared write-command prologue, and the
  default-subcommand group factory.

Only helpers used by more than one command group belong here. A helper used by a
single group travels with that group into ``cli/commands/<group>.py``.
"""

import os
from contextlib import contextmanager

import click


@contextmanager
def _activate_s3(ctx, aws_profile=None, s3_endpoint=None, s3_region=None, s3_no_ssl=False):
    """Resolve and activate S3 config from ctx + per-command overrides.

    Properly saves/restores AWS_PROFILE env var to avoid credential leaks.

    ``ctx.obj`` is primed here rather than relied upon. The root ``gpio`` group
    fills it, so every real CLI path already arrives with a dict -- but a group
    object invoked on its own (``CliRunner().invoke(process, [...])``, or any
    programmatic caller) never runs the root callback, and the read below then
    raised ``AttributeError: 'NoneType' object has no attribute 'get'`` (#922).
    Enforcing the precondition in the one helper that has it keeps every group
    correct, instead of depending on each group callback to remember.
    """
    from geoparquet_io.core.duckdb_utils import s3_config_scope
    from geoparquet_io.core.remote import resolve_s3_config

    ctx.ensure_object(dict)
    config = resolve_s3_config(
        s3_endpoint=s3_endpoint or ctx.obj.get("s3_endpoint"),
        s3_region=s3_region or ctx.obj.get("s3_region"),
        s3_no_ssl=s3_no_ssl or ctx.obj.get("s3_no_ssl", False),
        aws_profile=aws_profile or ctx.obj.get("aws_profile"),
    )
    previous_profile = os.environ.get("AWS_PROFILE")
    try:
        if config["profile"]:
            os.environ["AWS_PROFILE"] = config["profile"]
        with s3_config_scope(config):
            yield config
    finally:
        if previous_profile is None:
            os.environ.pop("AWS_PROFILE", None)
        else:
            os.environ["AWS_PROFILE"] = previous_profile


def prepare_output(
    output_path: str | None,
    any_extension: bool,
    row_group_size: int | None,
    row_group_size_mb: str | None,
) -> float | None:
    """Run the three checks every write command performs before it does any work.

    In order:

    1. :func:`~geoparquet_io.core.streaming.validate_output` - refuses a missing
       output when stdout is a terminal, and warns when binary Arrow IPC would
       be written to one. Its ``StreamingError`` is re-raised as a
       ``ClickException`` with ``from None``: the message is already written for
       a user, and the traceback behind it is noise.
    2. :func:`~geoparquet_io.core.file_utils.validate_parquet_extension` -
       rejects a non-``.parquet`` output unless ``--any-extension`` was given.
    3. :func:`~geoparquet_io.cli.decorators.parse_row_group_options` - enforces
       that ``--row-group-size`` and ``--row-group-size-mb`` are mutually
       exclusive and converts the size string to MB.

    The order matters and is part of what this helper pins: a user who typed
    neither an output nor a ``.parquet`` name should get the streaming hint,
    which tells them what to do, rather than the extension complaint.

    Args:
        output_path: The command's output argument. ``None`` means "stream to
            stdout" and ``"-"`` means it explicitly.
        any_extension: Value of ``--any-extension``.
        row_group_size: Value of ``--row-group-size`` (rows).
        row_group_size_mb: Value of ``--row-group-size-mb`` (size string).

    Returns:
        The row group size in MB, or ``None`` when it was not requested -- pass
        it to the core writer as ``row_group_size_mb``.

    Raises:
        click.ClickException: No output was given and stdout is a terminal.
        click.UsageError: Both row-group options were given, or the size string
            is invalid.
        InvalidParameterError: The output has a non-``.parquet`` extension and
            ``--any-extension`` was not given.
    """
    from geoparquet_io.cli.decorators import parse_row_group_options
    from geoparquet_io.core.file_utils import validate_parquet_extension
    from geoparquet_io.core.streaming import StreamingError, validate_output

    try:
        validate_output(output_path)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # ``validate_parquet_extension`` returns immediately on ``None``, but is
    # annotated ``str``. Skipping the call keeps the None-handling visible here
    # rather than relying on an annotation that does not admit it.
    if output_path is not None:
        validate_parquet_extension(output_path, any_extension)

    return parse_row_group_options(row_group_size, row_group_size_mb)


def create_default_group(default_subcommand: str, description: str) -> type:
    """Factory to create a click.Group subclass that defaults to a specific subcommand.

    Args:
        default_subcommand: The subcommand to invoke when none is provided
        description: The docstring for the generated class

    Returns:
        A click.Group subclass with the configured default behavior
    """

    class _DefaultGroup(click.Group):
        def parse_args(self, ctx, args):
            # Handle --help for group
            if "--help" in args and (not args or args[0] not in self.commands):
                return super().parse_args(ctx, [a for a in args if a != "--help"] + ["--help"])

            # If first arg is a known subcommand, use it
            if args and not args[0].startswith("-") and args[0] in self.commands:
                return super().parse_args(ctx, args)

            # Default to configured subcommand
            return super().parse_args(ctx, [default_subcommand] + args)

    _DefaultGroup.__doc__ = description
    return _DefaultGroup
