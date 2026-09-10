"""Every option that names a column rejects an empty or whitespace-only value.

``--bbox-name``, ``--h3-name``, ``--a5-name``, ``--s2-name``,
``--quadkey-name``, ``--kdtree-name``, ``--geometry-column``,
``--quadkey-column`` and ``partition string --column`` all end up as a delimited
SQL identifier. Before #933 an empty value walked all the way into
``quote_identifier()`` and surfaced as a raw
``ValueError: cannot quote an empty SQL identifier`` traceback -- after the
command had already read the input file -- and a whitespace-only value was
accepted outright, writing a column literally named ``'   '``.

The guard is a shared Click callback wired up by
:func:`geoparquet_io.cli.decorators.column_name_option`, so it fires during
parameter processing, before any work starts, and reports a usage error the way
every other bad option value does.

``test_no_unreviewed_name_option_appeared`` walks the live command tree, so a
new column-naming option declared with a bare ``@click.option`` fails here
rather than shipping unguarded. Two properties make that ratchet actually bite,
and both were once absent:

* it **accumulates** every matching flag per command instead of overwriting, so
  a second option on a command cannot hide behind the first; without this,
  whether the ratchet fired depended on decorator ordering.
* it matches ``--*-column`` as well as ``--*-name``, which is how the family was
  found to be incomplete in the first place.

Neither is a substitute for tracing the value: membership in
``GUARDED_OPTIONS`` / ``REVIEWED_UNGUARDED`` below is decided by following the
option into ``core/``, and the spelling filter only decides what has to be
*classified*.
"""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.decorators import _validate_column_name
from geoparquet_io.cli.main import cli

# Options whose value becomes a SQL identifier. Keyed by the command path,
# valued by the option flag.
#
# Membership is decided by tracing the value to ``quote_identifier()``, never by
# the flag's spelling: ``--geometry-column`` and ``--quadkey-column`` are the
# same exposure as ``--bbox-name`` and were missed by a sweep that matched
# ``--*-name``.
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
    ("partition", "quadkey"): "--quadkey-column",
    ("sort", "quadkey"): "--quadkey-name",
    ("sort", "hilbert"): "--geometry-column",
    ("sort", "str"): "--geometry-column",
}

# Options the sweep matches but that deliberately carry no Click-layer guard.
# Each entry records why a blank value is already someone else's problem; drop an
# option in here only after tracing where its value actually goes.
REVIEWED_UNGUARDED = {
    # A GDAL layer name, not a SQL identifier: emitted as a string *literal*
    # through _escape_sql_string() in core/format_writers.py.
    ("convert", "geopackage"): ["--layer-name"],
    # Validated against the CSV's own header before use: an empty value is
    # falsy and falls back to auto-detection, a blank one reports
    # "column '   ' not found in CSV" (core/convert.py).
    ("convert", "geoparquet"): ["--lat-column", "--lon-column", "--wkt-column"],
    # Resolved against the BigQuery table's schema first, so a blank value
    # reports "Column '   ' not found in table" (core/extract_bigquery.py).
    ("extract", "bigquery"): ["--geography-column"],
    # A GeoJSON property key handed to tippecanoe, not SQL. A name the data does
    # not carry yields a single "_unknown" layer (core/pmtiles.py).
    ("pmtiles", "create"): ["--layer-by-column"],
    # Checked for membership in the input schema before it is quoted:
    # "bbox column '   ' not found in the input"
    # (core/process/aggregate/grid_common.py).
    ("process", "aggregate", "a5"): ["--bbox-column"],
    ("process", "aggregate", "admin"): ["--bbox-column"],
    ("process", "aggregate", "h3"): ["--bbox-column"],
    # Same shape: "column '   ' not found in the input"
    # (core/process/overview/detect.py).
    ("process", "overview"): ["--cell-column"],
    # A skill lookup key on disk, never SQL; an unknown name already reports
    # cleanly.
    ("skills",): ["--name"],
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
    """Every option in the tree whose flag looks like it names a column or table.

    ``--*-column`` counts as well as ``--*-name``: ``sort hilbert
    --geometry-column`` and ``partition quadkey --quadkey-column`` reach
    ``quote_identifier()`` by exactly the same route, and a filter that only
    matched ``--*-name`` could not see them.

    Boolean flags are skipped -- ``--keep-h3-column`` and friends carry a
    ``True``/``False``, which can never become an identifier.

    Every match is *accumulated*, not assigned: keying by command path and
    overwriting kept only one option per command, so whether the ratchet fired
    for an unguarded option depended on where its decorator sat in the stack
    rather than on whether it was guarded.
    """
    found: dict[tuple[str, ...], list[str]] = {}
    for path, option in _iter_options(cli, ()):
        if option.is_flag:
            continue
        flags = [opt for opt in option.opts if opt.startswith("--")]
        if any(flag.endswith(("-name", "-column")) for flag in flags):
            found.setdefault(path, []).extend(flags)
    return {path: sorted(flags) for path, flags in found.items()}


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
        """A newly added column-naming option must be guarded or excluded here."""
        reviewed = {path: sorted(flags) for path, flags in REVIEWED_UNGUARDED.items()}
        for path, flag in GUARDED_OPTIONS.items():
            reviewed.setdefault(path, []).append(flag)
        assert _column_name_options() == {path: sorted(f) for path, f in reviewed.items()}


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
