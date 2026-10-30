"""One wording for "these two options cannot be given together" (#1367, concept 13).

Seven sites raised ``"--a and --b are mutually exclusive"`` with their own copy of
the sentence; ``cli.decorators.refuse_together`` is the one copy now, and the
messages users see are unchanged.
"""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.decorators import refuse_together
from geoparquet_io.cli.main import cli


def test_both_given_is_a_usage_error():
    with pytest.raises(click.UsageError, match=r"^--a and --b are mutually exclusive$"):
        refuse_together(("--a", True), ("--b", 1))


@pytest.mark.parametrize("given", [(False, True), (True, None), (0, "")])
def test_one_or_none_given_passes(given):
    refuse_together(("--a", given[0]), ("--b", given[1]))


def test_the_ending_is_the_callers():
    with pytest.raises(click.UsageError, match=r"exclusive\.$"):
        refuse_together(("--a", True), ("--b", True), end=".")


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["inspect", "summary", "{f}", "--json", "--markdown"], "--json and --markdown"),
        (["inspect", "head", "{f}", "--json", "--markdown"], "--json and --markdown"),
        (["inspect", "stats", "{f}", "--json", "--markdown"], "--json and --markdown"),
        (
            ["convert", "{f}", "{o}", "--row-group-size", "1000", "--row-group-size-mb", "1MB"],
            "--row-group-size and --row-group-size-mb",
        ),
        (
            ["add", "kdtree", "{f}", "{o}", "--partitions", "4", "--auto", "100"],
            "--partitions and --auto",
        ),
        (
            ["add", "kdtree", "{f}", "{o}", "--approx", "1000", "--exact"],
            "--approx and --exact",
        ),
    ],
)
def test_the_cli_messages_are_unchanged(buildings_test_file, tmp_path, args, message):
    out = str(tmp_path / "out.parquet")
    argv = [a.format(f=buildings_test_file, o=out) for a in args]
    result = CliRunner().invoke(cli, argv)
    assert result.exit_code == 2, result.output
    assert f"Error: {message} are mutually exclusive\n" in result.output
