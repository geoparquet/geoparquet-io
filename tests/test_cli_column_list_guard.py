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
rather than shipping unguarded.

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

# Options the sweep matches but that deliberately carry no guard. Empty for
# now: every ``--*-cols`` option in the tree is a column list.
REVIEWED_UNGUARDED_LISTS: dict[tuple[str, ...], list[str]] = {}

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


class TestBlankEntryIsRejected:
    """A blank entry is a usage error, and nothing is read or contacted."""

    @pytest.mark.parametrize(
        ("path", "flag"),
        sorted((path, flag) for path, flags in GUARDED_LIST_OPTIONS.items() for flag in flags),
    )
    @pytest.mark.parametrize("value", ["", "   ", "\t", "id,,name", "id, ,name", "id,"])
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
