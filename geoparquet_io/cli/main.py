import sys
from importlib.metadata import entry_points

import click
from click_plugins import with_plugins

from geoparquet_io.cli.commands.add import add
from geoparquet_io.cli.commands.benchmark import benchmark
from geoparquet_io.cli.commands.check import check
from geoparquet_io.cli.commands.convert import convert
from geoparquet_io.cli.commands.extract import extract
from geoparquet_io.cli.commands.inspect import inspect
from geoparquet_io.cli.commands.partition import partition
from geoparquet_io.cli.commands.pmtiles import pmtiles
from geoparquet_io.cli.commands.process import process
from geoparquet_io.cli.commands.publish import publish
from geoparquet_io.cli.commands.sort import sort
from geoparquet_io.cli.decorators import (
    ErrorBoundaryGroup,
    GlobAwareCommand,
    handle_geoparquet_errors,
)
from geoparquet_io.core.logging_config import setup_cli_logging


class OptionalIntCommand(GlobAwareCommand):
    """Custom Command that supports options with optional integer values.

    Inherits from GlobAwareCommand to also detect shell-expanded glob patterns
    and provide helpful error messages.

    Options listed in optional_int_options can be used as flags (defaulting to 10)
    or with an explicit integer value. For example:
        --head           -> uses default value of 10
        --head 5         -> uses value 5
        (no --head)      -> uses None
    """

    # Options that support optional integer values and their defaults
    optional_int_options = {"--head": 10, "--tail": 10}

    def make_context(self, info_name, args, parent=None, **extra):
        """Preprocess args to insert default values for optional int options."""
        args = list(args)  # Make a mutable copy
        for opt, default_val in self.optional_int_options.items():
            if opt in args:
                idx = args.index(opt)
                # Check if next arg exists and looks like an integer
                if idx + 1 < len(args):
                    next_arg = args[idx + 1]
                    # If next arg starts with - (another option) or doesn't look like int
                    if next_arg.startswith("-") or not next_arg.lstrip("-").isdigit():
                        args.insert(idx + 1, str(default_val))
                else:
                    # Option at end of args
                    args.insert(idx + 1, str(default_val))
        return super().make_context(info_name, args, parent=parent, **extra)


@with_plugins(entry_points(group="gpio.plugins"))
@click.group(cls=ErrorBoundaryGroup)
@click.version_option(prog_name="geoparquet-io")
@click.option("--timestamps", is_flag=True, help="Show timestamps in output messages")
@click.option(
    "--s3-endpoint",
    default=None,
    help="Custom S3-compatible endpoint (e.g., 'minio.example.com:9000')",
)
@click.option(
    "--s3-region",
    default=None,
    help="S3 region for custom endpoints",
)
@click.option(
    "--s3-no-ssl",
    is_flag=True,
    default=False,
    help="Disable SSL for S3 endpoint (use HTTP instead of HTTPS)",
)
@click.option(
    "--aws-profile",
    default=None,
    help="AWS profile name for S3 operations",
)
@click.pass_context
def cli(ctx, timestamps, s3_endpoint, s3_region, s3_no_ssl, aws_profile):
    """Fast I/O and transformation tools for GeoParquet files."""
    # Ensure stdout/stderr can emit UTF-8 even on Windows, where the default
    # codec (cp1252) raises UnicodeEncodeError on non-ASCII GeoJSON content.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8")
            except Exception:
                pass
    ctx.ensure_object(dict)
    ctx.obj["timestamps"] = timestamps
    ctx.obj["s3_endpoint"] = s3_endpoint
    ctx.obj["s3_region"] = s3_region
    ctx.obj["s3_no_ssl"] = s3_no_ssl
    ctx.obj["aws_profile"] = aws_profile
    # Setup logging for CLI output (default level INFO, verbose commands will set DEBUG)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


# ---------------------------------------------------------------------------
# Command groups extracted into geoparquet_io/cli/commands/
#
# Each of those modules declares a standalone `@click.group()` and is attached
# here, explicitly, one line per group. Registration is deliberately not done by
# the group module itself (no `@cli.group()` there, no import-time
# self-registration): that would need `cli.main`, and `cli.main` imports the
# group -- a cycle. Keeping it here also means `grep add_command` lists the
# whole tree.
# ---------------------------------------------------------------------------
cli.add_command(add)
cli.add_command(benchmark)
cli.add_command(check)
cli.add_command(convert)
cli.add_command(extract)
cli.add_command(inspect)
cli.add_command(partition)
cli.add_command(pmtiles)
cli.add_command(process)
cli.add_command(publish)
cli.add_command(sort)


# Skills command (for LLM integration)
@cli.command()
@handle_geoparquet_errors
@click.option("--show", is_flag=True, help="Print skill content to stdout")
@click.option("--copy", "copy_to", type=click.Path(), help="Copy skill to directory")
@click.option("--name", default="geoparquet", help="Skill name (default: geoparquet)")
def skills(show: bool, copy_to: str | None, name: str):
    """List and access LLM skills for gpio.

    Skills are markdown files that help LLMs (ChatGPT, Claude, etc.) work
    effectively with the gpio CLI tool.

    \b
    Examples:
      gpio skills              # List available skills
      gpio skills --show       # Print skill to stdout (for piping to LLM)
      gpio skills --copy .     # Copy skill to current directory

    \b
    Using with LLMs:
      # Paste skill content into a conversation
      gpio skills --show | pbcopy

      # Or reference the installed file
      gpio skills  # Shows file path
    """
    from geoparquet_io.skills import get_skill_content, get_skill_path, list_skills

    try:
        if show:
            # Print content to stdout
            click.echo(get_skill_content(name))
        elif copy_to:
            # Copy skill to directory
            from pathlib import Path
            from shutil import copy2

            dest_dir = Path(copy_to)
            if not dest_dir.is_dir():
                raise click.ClickException(f"Not a directory: {copy_to}")

            src = get_skill_path(name)
            dest = dest_dir / f"{name}.md"
            copy2(src, dest)
            click.echo(f"Copied skill to: {dest}")
        else:
            # List available skills
            available = list_skills()
            click.echo("Available gpio skills:\n")
            for skill_name in available:
                skill_path = get_skill_path(skill_name)
                click.echo(f"  {skill_name}")
                click.echo(f"    Path: {skill_path}")
            click.echo("\nUsage:")
            click.echo("  gpio skills --show       # Print to stdout")
            click.echo("  gpio skills --copy .     # Copy to current directory")
    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e


if __name__ == "__main__":
    cli()
