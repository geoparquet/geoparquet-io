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
  not a Click option: S3 activation, and the default-subcommand group factory.

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
