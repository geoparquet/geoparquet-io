"""Every option carrying a comma-separated column list rejects a blank entry.

The sibling of ``tests/test_cli_column_name_guard.py``. #959 closed the #933
sweep for options carrying **one** column name; the list-valued options were
left out because a callback validating "the value" is the wrong shape when the
value is ``"a,b,c"`` -- each *entry* needs checking (#969).

``gpio extract bigquery ... --include-cols '   '`` split to ``['']``, and the
empty string reached ``quote_identifier()`` in ``_handle_dry_run`` /
``_build_select_with_wkb``, surfacing as a raw
``ValueError: cannot quote an empty SQL identifier`` traceback at exit 1.
``gpio extract carto`` reached the same call through ``_build_carto_query``,
after a network round-trip. The guard is a shared Click callback wired up by
:func:`geoparquet_io.cli.decorators.column_list_option`, so it fires during
parameter processing, before any file is read or any service is contacted.

``test_no_unreviewed_cols_option_appeared`` walks the live command tree, so a
new list-valued column option declared with a bare ``@click.option`` fails here
rather than shipping unguarded. Its sweep matches the **spelling** ``--*-cols``,
which is a filter on what has to be *classified*, not a claim of coverage: a
list-valued option spelled some other way is invisible to it. Membership is
decided by tracing the value, exactly as in the sibling module, and the
list-shaped values that fall outside the spelling filter are recorded in
``LIST_VALUED_OUTSIDE_THE_SWEEP`` with the reason each is already safe.

An entirely empty value is **unset**, not a blank entry. Every backend reads
these options as ``[c.strip() for c in v.split(",")] if v else None``
(``core/extract.py``, ``core/extract_bigquery.py``, ``core/carto.py``,
``core/pmtiles.py``), so ``""`` has always meant "option not given" -- which is
what ``--include-cols "$COLS"`` with an unset variable expands to. The guard
preserves that and rejects only a value that carries a blank *entry*: ``"   "``
and ``"id,,name"`` are truthy on main, split to a list containing ``""``, and
are the actual #969 defect.

The Click layer owns *blank entries only*. Whether a non-blank name actually
exists is a schema question, so it belongs to whichever backend can see the
schema -- ``validate_columns`` for parquet, ``validate_bigquery_columns`` for
BigQuery -- and is covered in those backends' own tests.
"""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.decorators import _validate_column_list
from geoparquet_io.cli.main import cli

# Options whose value is a comma-separated list of column names. Keyed by
# command path, valued by the flags on that command.
GUARDED_LIST_OPTIONS = {
    ("extract", "geoparquet"): ["--exclude-cols", "--include-cols"],
    ("extract", "arcgis"): ["--exclude-cols", "--include-cols"],
    ("extract", "bigquery"): ["--exclude-cols", "--include-cols"],
    ("extract", "carto"): ["--exclude-cols", "--include-cols"],
    # Forwarded verbatim to a `gpio extract` subprocess (core/pmtiles.py), so a
    # blank entry used to fail there, one process away from the user.
    ("pmtiles", "create"): ["--include-cols"],
}

# Options the sweep matches but that deliberately carry no guard. Empty: every
# option in the tree spelled ``--*-cols`` is in GUARDED_LIST_OPTIONS above.
# That is a statement about this dict, not about coverage -- see
# LIST_VALUED_OUTSIDE_THE_SWEEP for what the spelling filter cannot see.
REVIEWED_UNGUARDED_LISTS: dict[tuple[str, ...], list[str]] = {}

# List-shaped values the ``--*-cols`` sweep does **not** match, traced and found
# already validated where the names are used. Recorded so the limit of the
# spelling filter is written down rather than implied, and pinned by
# ``test_out_of_sweep_params_still_exist`` so a rename cannot leave the note
# stale. Keyed by command path, valued by parameter name as Click knows it.
LIST_VALUED_OUTSIDE_THE_SWEEP = {
    # Positional COLUMNS, comma-separated. Every one of the three code paths
    # checks each name against the schema before quoting it: the Arrow path
    # (core/sort_by_column.py sort_table_by_column), the file path
    # (_build_sort_query) and the streaming path (_build_streaming_sort_query)
    # all raise "column '   ' not found" first.
    ("sort", "column"): ["columns"],
    # ``--metric`` carries comma-separated ``func:column`` pairs. parse_metrics()
    # (core/process/aggregate/common.py) skips an entry that strips to empty, so
    # a blank one is dropped rather than quoted.
    ("process", "aggregate", "a5"): ["metric"],
    ("process", "aggregate", "h3"): ["metric"],
    ("process", "aggregate", "admin"): ["metric"],
    # Not identifiers, so no exposure to quote_identifier() at all:
    # ``--levels`` is a list of integers, ``--converters`` names conversion
    # backends, and ``--metric-nodata`` carries numeric fill values.
}

# Leading positional arguments per command, so a *missing* argument is never
# what makes these assertions pass. The output path is appended per test.
LEADING_ARGS = {
    ("extract", "geoparquet"): ["tests/data/places_test.parquet"],
    ("extract", "arcgis"): ["https://example.com/arcgis/rest/services/x/FeatureServer/0"],
    ("extract", "bigquery"): ["project-name.dataset.table"],
    ("extract", "carto"): ["https://gcp-us-east1.api.carto.com", "carto-demo.public.table"],
    ("pmtiles", "create"): ["tests/data/places_test.parquet"],
}

OUTPUT_NAME = {("pmtiles", "create"): "out.pmtiles"}


def _iter_options(command: click.Command, path: tuple[str, ...]):
    """Yield ``(path, option)`` for every option in the command tree."""
    if isinstance(command, click.Group):
        for name, sub in command.commands.items():
            yield from _iter_options(sub, (*path, name))
        return
    for param in command.params:
        if isinstance(param, click.Option):
            yield path, param


def _column_list_options():
    """Every option in the tree whose flag looks like a column list.

    Matches ``--*-cols``, accumulating per command so a second list option on
    one command cannot hide behind the first.
    """
    found: dict[tuple[str, ...], list[str]] = {}
    for path, option in _iter_options(cli, ()):
        if option.is_flag:
            continue
        flags = [opt for opt in option.opts if opt.startswith("--")]
        if any(flag.endswith("-cols") for flag in flags):
            found.setdefault(path, []).extend(flags)
    return {path: sorted(flags) for path, flags in found.items()}


class TestListGuardCoverage:
    """The declared family matches the live command tree."""

    def test_every_column_list_option_is_guarded(self):
        for path, flags in GUARDED_LIST_OPTIONS.items():
            command = cli
            for part in path:
                command = command.commands[part]
            for flag in flags:
                option = next(opt for opt in command.params if flag in opt.opts)
                assert option.callback is _validate_column_list, (
                    f"{' '.join(path)} {flag} must be declared with column_list_option()"
                )

    def test_no_unreviewed_cols_option_appeared(self):
        reviewed = {path: list(flags) for path, flags in REVIEWED_UNGUARDED_LISTS.items()}
        for path, flags in GUARDED_LIST_OPTIONS.items():
            reviewed.setdefault(path, []).extend(flags)
        assert _column_list_options() == {path: sorted(f) for path, f in reviewed.items()}

    @pytest.mark.parametrize(
        ("path", "param_name"),
        sorted(
            (path, name) for path, names in LIST_VALUED_OUTSIDE_THE_SWEEP.items() for name in names
        ),
    )
    def test_out_of_sweep_params_still_exist(self, path, param_name):
        """The recorded reasons name real parameters, so a rename fails here."""
        command = cli
        for part in path:
            command = command.commands[part]
        assert param_name in {param.name for param in command.params}


class TestBlankEntryIsRejected:
    """A blank entry is a usage error, and nothing is read or contacted."""

    @pytest.mark.parametrize(
        ("path", "flag"),
        sorted((path, flag) for path, flags in GUARDED_LIST_OPTIONS.items() for flag in flags),
    )
    @pytest.mark.parametrize("value", ["   ", "\t", "id,,name", "id, ,name", "id,", ","])
    def test_blank_entry_is_a_usage_error(self, path, flag, value, tmp_path):
        runner = CliRunner()
        output = tmp_path / OUTPUT_NAME.get(path, "out.parquet")
        args = [*path, *LEADING_ARGS[path], str(output), flag, value]
        result = runner.invoke(cli, args)

        assert result.exit_code == 2, result.output
        assert flag in result.output
        assert "empty or whitespace-only" in result.output
        # Refused during parameter processing: nothing was written.
        assert not list(tmp_path.iterdir())

    def test_blank_entry_is_refused_even_in_dry_run(self, tmp_path):
        """``--dry-run`` builds the SELECT too, so it hit the same ValueError."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "bigquery",
                "project-name.dataset.table",
                str(tmp_path / "out.parquet"),
                "--include-cols",
                "   ",
                "--dry-run",
            ],
        )
        assert result.exit_code == 2, result.output
        assert "cannot quote an empty SQL identifier" not in result.output


class TestWhollyEmptyMeansUnset:
    """``--include-cols ""`` is "option not given", the way it always was.

    Every backend reads the option as ``... if v else None``, so the empty
    string was already unset before any guard existed. ``--include-cols
    "$COLS"`` with an unset ``COLS`` expands to exactly this, so rejecting it
    would break working invocations for no gain: there is no blank *entry* to
    protect against, because there is no entry at all.
    """

    @pytest.mark.parametrize(
        ("path", "flag"),
        sorted((path, flag) for path, flags in GUARDED_LIST_OPTIONS.items() for flag in flags),
    )
    def test_empty_string_is_accepted_as_unset(self, path, flag):
        param = click.Option([flag])
        assert _validate_column_list(None, param, "") is None

    @pytest.mark.parametrize("flag", ["--include-cols", "--exclude-cols"])
    def test_empty_string_extracts_every_row_and_column(self, flag, tmp_path):
        """End to end: the option is inert, not an error."""
        runner = CliRunner()
        output = tmp_path / "out.parquet"
        result = runner.invoke(
            cli,
            ["extract", "geoparquet", "tests/data/places_test.parquet", str(output), flag, ""],
        )

        assert result.exit_code == 0, result.output
        assert output.exists()

        import pyarrow.parquet as pq

        written = pq.read_table(output)
        source = pq.read_table("tests/data/places_test.parquet")
        assert written.num_rows == source.num_rows
        assert set(written.column_names) == set(source.column_names)


class TestValidValuesPassThrough:
    """The callback is a guard, not a transform."""

    @pytest.mark.parametrize("value", ["id", "id,name", "id, name", "weird name,other"])
    def test_a_real_list_is_returned_unchanged(self, value):
        param = click.Option(["--include-cols"])
        assert _validate_column_list(None, param, value) == value

    def test_none_is_passed_through(self):
        """An option left unset reaches the command as ``None``."""
        param = click.Option(["--include-cols"])
        assert _validate_column_list(None, param, None) is None
