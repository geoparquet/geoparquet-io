"""Every option that names a column rejects an empty or whitespace-only value.

``--bbox-name``, ``--h3-name``, ``--a5-name``, ``--s2-name``,
``--quadkey-name``, ``--kdtree-name`` and ``partition string --column`` all end
up as a delimited SQL identifier. Before #933 an empty value walked all the way
into ``quote_identifier()`` and surfaced as a raw
``ValueError: cannot quote an empty SQL identifier`` traceback -- after the
command had already read the input file -- and a whitespace-only value was
accepted outright, writing a column literally named ``'   '``.

The guard is a shared Click callback wired up by
:func:`geoparquet_io.cli.decorators.column_name_option`, so it fires during
parameter processing, before any work starts, and reports a usage error the way
every other bad option value does.

``test_every_column_name_option_is_guarded`` walks the live command tree, so a
new ``--*-name`` option declared with a bare ``@click.option`` fails here rather
than shipping unguarded.
"""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.decorators import _validate_column_name
from geoparquet_io.cli.main import cli

# Options whose value names a column in a generated SQL statement. Keyed by the
# command path, valued by the option flag.
#
# Deliberately excluded, and why:
#   * ``convert geopackage --layer-name`` -- a GDAL layer name, not a SQL
#     identifier; an empty value is handled by GDAL, not by quote_identifier().
#   * ``skills --name`` -- a skill lookup key; an unknown name already reports
#     cleanly.
GUARDED_OPTIONS = {
    ("add", "bbox"): "--bbox-name",
    ("add", "h3"): "--h3-name",
    ("add", "a5"): "--a5-name",
    ("add", "s2"): "--s2-name",
    ("add", "kdtree"): "--kdtree-name",
    ("add", "quadkey"): "--quadkey-name",
    ("partition", "h3"): "--h3-name",
    ("partition", "a5"): "--a5-name",
    ("partition", "s2"): "--s2-name",
    ("partition", "kdtree"): "--kdtree-name",
    ("partition", "string"): "--column",
    ("sort", "quadkey"): "--quadkey-name",
}


def _positional_args(path: tuple[str, ...], tmp_path) -> list[str]:
    """Return the positional arguments for one command under test.

    Every required argument is supplied: a *missing* one is itself a usage
    error, which would let these tests pass for the wrong reason.
    """
    source = "tests/data/places_test.parquet"
    destination = "out_dir" if path[0] == "partition" else "out.parquet"
    return [source, str(tmp_path / destination)]


def _iter_options(command: click.Command, path: tuple[str, ...]):
    """Yield ``(path, option)`` for every option in the command tree."""
    if isinstance(command, click.Group):
        for name, sub in command.commands.items():
            yield from _iter_options(sub, (*path, name))
        return
    for param in command.params:
        if isinstance(param, click.Option):
            yield path, param


def _column_name_options():
    """Every option in the tree whose flag looks like it names a column."""
    found = {}
    for path, option in _iter_options(cli, ()):
        flags = [opt for opt in option.opts if opt.startswith("--")]
        if any(flag.endswith("-name") or flag == "--column" for flag in flags):
            found[path] = flags
    return found


class TestGuardCoverage:
    """The declared family matches the live command tree."""

    def test_every_column_name_option_is_guarded(self):
        """Each option in GUARDED_OPTIONS carries the shared validator."""
        for path, flag in GUARDED_OPTIONS.items():
            command = cli
            for part in path:
                command = command.commands[part]
            option = next(opt for opt in command.params if flag in opt.opts)
            assert option.callback is _validate_column_name, (
                f"{' '.join(path)} {flag} must be declared with column_name_option()"
            )

    def test_no_unreviewed_name_option_appeared(self):
        """A newly added ``--*-name`` option must be guarded or excluded here."""
        reviewed = {("convert", "geopackage"): ["--layer-name"], ("skills",): ["--name"]}
        reviewed.update({path: [flag] for path, flag in GUARDED_OPTIONS.items()})
        assert _column_name_options() == reviewed


class TestEmptyColumnNameIsRejected:
    """An empty or blank value is a usage error, and nothing is written."""

    @pytest.mark.parametrize("path,flag", sorted(GUARDED_OPTIONS.items()))
    @pytest.mark.parametrize("value", ["", "   ", "\t"])
    def test_blank_value_is_a_usage_error(self, path, flag, value, tmp_path):
        runner = CliRunner()
        result = runner.invoke(cli, [*path, *_positional_args(path, tmp_path), flag, value])

        assert result.exit_code == 2, result.output
        assert flag in result.output
        assert "cannot be empty" in result.output
        # Refused during parameter processing: no output file was produced.
        assert not list(tmp_path.iterdir())

    def test_a_real_name_still_passes_the_guard(self):
        """The guard returns the value untouched for a normal column name."""
        param = click.Option(["--bbox-name"])
        assert _validate_column_name(None, param, "bbox") == "bbox"

    def test_none_is_passed_through(self):
        """An option left unset reaches the command as ``None``."""
        param = click.Option(["--bbox-name"])
        assert _validate_column_name(None, param, None) is None
