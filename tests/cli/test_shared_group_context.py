"""Unit tests for :func:`geoparquet_io.cli._shared.init_group_context`.

Every ``gpio`` subgroup opened with the same three lines. The two behaviours
worth pinning are that ``ctx.obj`` is primed even when the root ``gpio``
callback never ran -- which is what a ``CliRunner().invoke(add, ...)`` in a test
does -- and that ``--timestamps``, set by the root callback, reaches
``setup_cli_logging``.
"""

from __future__ import annotations

from unittest import mock

import click
import pytest
from click.testing import CliRunner

from geoparquet_io.cli._shared import init_group_context
from geoparquet_io.cli.main import cli


@pytest.fixture
def logging_spy():
    with mock.patch("geoparquet_io.cli._shared.setup_cli_logging") as spy:
        yield spy


def test_primes_a_missing_context_object(logging_spy):
    ctx = click.Context(click.Command("x"))
    assert ctx.obj is None

    init_group_context(ctx)

    assert ctx.obj == {}
    logging_spy.assert_called_once_with(verbose=False, show_timestamps=False)


def test_leaves_an_existing_context_object_alone(logging_spy):
    ctx = click.Context(click.Command("x"))
    ctx.obj = {"timestamps": True, "aws_profile": "prod"}

    init_group_context(ctx)

    assert ctx.obj == {"timestamps": True, "aws_profile": "prod"}
    logging_spy.assert_called_once_with(verbose=False, show_timestamps=True)


GROUPS_USING_THE_HELPER = [
    "add",
    "check",
    "convert",
    "extract",
    "inspect",
    "partition",
    "publish",
    "sort",
]


@pytest.mark.parametrize("group", GROUPS_USING_THE_HELPER)
def test_group_invoked_standalone_does_not_crash(group):
    # A group object reached without the root callback has ctx.obj None, so the
    # group callback is the first thing that touches it (cf. #922). ``--help``
    # on a *subcommand* is used rather than on the group, because Click prints
    # a group's own help during parsing, before the callback ever runs.
    command = cli.commands[group]
    subcommand = sorted(command.commands)[0]

    result = CliRunner().invoke(command, [subcommand, "--help"])

    assert result.exit_code == 0, result.output


def test_timestamps_flag_reaches_the_logger_through_a_group(logging_spy):
    result = CliRunner().invoke(cli, ["--timestamps", "add", "bbox", "--help"])

    assert result.exit_code == 0, result.output
    assert logging_spy.call_args_list == [mock.call(verbose=False, show_timestamps=True)]
