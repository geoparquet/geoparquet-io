"""Every test must start from a known-clean ``geoparquet_io`` logger.

``--verbose`` raises that logger to DEBUG as the flag is parsed
(:func:`geoparquet_io.cli.decorators.enable_verbose_logging`, #995) and
:func:`~geoparquet_io.core.logging_config.configure_verbose` is deliberately
one-way, so nothing lowers it again. That is right for a CLI -- one command per
process -- and wrong under pytest, where a whole file of tests shares one xdist
worker process: the first ``--verbose`` invocation, or the first core call made
with ``verbose=True``, leaves DEBUG on for every test that follows it on that
worker.

The victim is any test that asserts on the *absence* of debug output.
``ErrorBoundaryGroup.invoke`` logs "Converted a low-level failure to an error
line" at DEBUG with ``exc_info``, so a leaked DEBUG level -- together with a
leaked stream handler -- puts a traceback in ``result.output`` and
``tests/test_cli_error_boundary.py`` fails on ``assert "Traceback" not in
result.output``.

Two shapes of that leak are pinned below, because the first fix only handled
one of them:

* **Left behind by an earlier test**, which a snapshot/restore fixture catches.
* **Already in place before the fixture can look** -- a module- or
  session-scoped fixture is set up *before* any function-scoped one, so
  ``test_add.py``'s ``bbox_optioned_run`` (a module-scoped fixture that runs
  ``gpio add bbox --verbose``) had raised the level before the snapshot was
  taken. Putting that snapshot back after every test made the poison permanent
  for the rest of the worker rather than fixing it (#1016).

So ``tests/conftest.py`` sets a known state rather than restoring what it
found. These tests pin that distinction: a restore-only fixture fails
:class:`TestAPoisonedWorkerStillStartsClean`, which is poisoned from a
module-scoped fixture exactly as ``test_add.py`` poisons a real worker.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

import click
import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.decorators import ErrorBoundaryGroup
from geoparquet_io.core.logging_config import configure_verbose, setup_cli_logging
from tests.conftest import pristine_package_logging

PACKAGE_LOGGER = logging.getLogger("geoparquet_io")
ROOT_LOGGER = logging.getLogger()

REPO_ROOT = Path(__file__).resolve().parent.parent


def _boundary_result():
    """Invoke a throwaway ``ErrorBoundaryGroup`` that fails the way #983 did.

    A throwaway group rather than the real ``cli`` on purpose: ``cli``'s own
    callback calls ``setup_cli_logging``, which resets the level on every
    invocation and hides the leak. The two tests that actually went red are the
    two that build their own group, and this is why.
    """

    @click.group(cls=ErrorBoundaryGroup)
    def root():
        pass

    @root.command()
    def boom():
        raise duckdb.InvalidInputException(
            "Invalid Input Error: Geoparquet metadata does not have a columns object"
        )

    return CliRunner().invoke(root, ["boom"])


# =============================================================================
# The guard itself
# =============================================================================


class TestTheGuardSetsAKnownState:
    """What it puts in place, and why those are the pieces.

    Read off ``core/logging_config.py`` rather than guessed: ``setup_cli_logging``
    assigns the level, clears ``handlers`` in place and installs its own, and
    sets ``propagate``; ``configure_verbose`` raises the level and may add a
    handler through ``_bootstrap_default_handler``. No module-level flag records
    verbosity -- the logger's level *is* the state.
    """

    def test_a_raised_level_is_gone_on_entry_not_merely_on_exit(self):
        """The difference from a snapshot: entering cleans, it does not record.

        This is the whole bug in #1016. A fixture that only restores hands back
        whatever a module-scoped fixture already did.
        """
        configure_verbose(True)
        assert PACKAGE_LOGGER.level == logging.DEBUG

        with pristine_package_logging():
            assert PACKAGE_LOGGER.level == logging.NOTSET

    def test_handlers_are_gone_on_entry(self):
        setup_cli_logging(verbose=True)
        assert PACKAGE_LOGGER.handlers

        with pristine_package_logging():
            assert PACKAGE_LOGGER.handlers == []

    def test_a_child_logger_is_cleaned_too(self):
        """``get_logger(__name__)`` hands out children, and the record that
        broke the boundary tests is logged on ``geoparquet_io.cli.decorators``."""
        child = logging.getLogger("geoparquet_io.cli.decorators")
        child.setLevel(logging.DEBUG)
        child.propagate = False

        with pristine_package_logging():
            assert child.level == logging.NOTSET
            assert child.propagate is True

    def test_a_root_level_that_would_switch_debug_back_on_is_clamped(self):
        """The package loggers sit at ``NOTSET``, so they inherit from the root.

        A root left at DEBUG would re-enable every gpio debug record through
        that inheritance, which is the one way a snapshot of *our* loggers can
        still be wrong.
        """
        ROOT_LOGGER.setLevel(logging.DEBUG)
        try:
            with pristine_package_logging():
                assert PACKAGE_LOGGER.getEffectiveLevel() > logging.DEBUG
        finally:
            ROOT_LOGGER.setLevel(logging.WARNING)

    def test_the_root_level_is_handed_back(self):
        """The root belongs to pytest, not to us."""
        before = ROOT_LOGGER.level
        with pristine_package_logging():
            pass
        assert ROOT_LOGGER.level == before

    def test_an_explicit_log_level_is_not_clamped_away(self):
        """``--log-level=DEBUG`` is a developer asking for output, not a leak."""
        ROOT_LOGGER.setLevel(logging.DEBUG)
        try:
            with pristine_package_logging(guard_root=False):
                assert ROOT_LOGGER.level == logging.DEBUG
        finally:
            ROOT_LOGGER.setLevel(logging.WARNING)

    def test_the_guard_cleans_up_even_when_the_body_raises(self):
        with pytest.raises(RuntimeError), pristine_package_logging():
            setup_cli_logging(verbose=True)
            raise RuntimeError("a failing test still has to leave the logger clean")

        assert PACKAGE_LOGGER.level == logging.NOTSET
        assert PACKAGE_LOGGER.handlers == []


# =============================================================================
# The leak, in the shape that reached CI
# =============================================================================


@pytest.fixture(scope="module", autouse=True)
def _poison_before_any_test_in_this_module():
    """Poison the loggers from a *module-scoped* fixture, as ``test_add.py`` does.

    Module-scoped setup runs before every function-scoped fixture, so this is
    in place before the autouse guard in ``tests/conftest.py`` gets to look --
    the exact ordering that made #1016's snapshot/restore fixture re-apply the
    poison after every test instead of clearing it. Both the package logger
    (DEBUG plus a live stream handler, which is what ``gpio ... --verbose``
    leaves) and the root logger are poisoned, so neither route is left untested.

    Undone afterwards so the poisoning cannot escape this module if the guard
    under test is ever removed.
    """
    root_level = ROOT_LOGGER.level
    setup_cli_logging(verbose=True, use_colors=False)  # DEBUG + a stream handler
    ROOT_LOGGER.setLevel(logging.DEBUG)
    yield
    PACKAGE_LOGGER.setLevel(logging.NOTSET)
    PACKAGE_LOGGER.handlers[:] = []
    PACKAGE_LOGGER.propagate = True
    ROOT_LOGGER.setLevel(root_level)


class TestAPoisonedWorkerStillStartsClean:
    """Every test in this module runs on a worker poisoned before it started."""

    def test_the_level_did_not_survive_into_this_test(self):
        assert PACKAGE_LOGGER.level == logging.NOTSET
        assert PACKAGE_LOGGER.getEffectiveLevel() > logging.DEBUG

    def test_the_stream_handler_did_not_survive_into_this_test(self):
        assert PACKAGE_LOGGER.handlers == []

    def test_the_boundary_still_answers_with_an_error_line_and_no_traceback(self):
        """The failure from CI run 34683461451, reproduced by construction.

        With a snapshot/restore guard this fails with
        ``assert 'Traceback' not in 'Converted a low-level failure to an error
        line\\nTraceback (most recent call last): ...'`` -- the boundary's own
        DEBUG record, in a test that never passed ``--verbose``.
        """
        result = _boundary_result()

        assert result.exit_code == 1
        assert "Error: Invalid Input Error: Geoparquet metadata" in result.output
        assert "Traceback" not in result.output


def test_this_test_did_not_inherit_a_raised_level():
    """The invariant the autouse guard buys, asserted from inside a test."""
    assert PACKAGE_LOGGER.level != logging.DEBUG
    assert PACKAGE_LOGGER.getEffectiveLevel() > logging.DEBUG


# =============================================================================
# The same thing end to end, in the order that failed
# =============================================================================


@pytest.mark.integration
def test_the_poisoning_orders_no_longer_fail_the_error_boundary_tests():
    """Both measured orders, run for real in a subprocess.

    ``tests/test_add.py`` before ``tests/test_cli_error_boundary.py`` is the CI
    failure itself: two module-scoped ``--verbose`` fixtures in ``test_add.py``
    poison the worker, and the two boundary tests that build their own group --
    rather than invoking ``cli``, whose callback resets the level -- go red. The
    three-node order is the minimal form of the same leak.

    A subprocess is what makes the order deterministic: under ``-n auto`` the
    scheduler, not this file, decides which tests share a worker.
    """
    boundary = "tests/test_cli_error_boundary.py"
    orders = [
        # `TestAddBboxCLI` is the smallest real poisoner: its module-scoped
        # `bbox_optioned_run` runs `gpio add bbox --verbose`, which leaves DEBUG
        # and a live stream handler behind before any function-scoped fixture runs.
        [
            "tests/test_add.py::TestAddBboxCLI",
            f"{boundary}::TestTheRootGroupIsTheBoundary"
            "::test_a_duckdb_error_reaches_the_user_as_an_error_line",
            f"{boundary}::TestInnerHandlersStillRunFirst"
            "::test_a_refusal_the_inner_handler_declines_still_becomes_an_error_line",
        ],
        [
            "tests/test_cli_error_boundary.py::test_a_rejected_file_gets_an_error_line_and_exit_one[inspect head]",
            "tests/test_decorators.py::TestVerboseOption::test_verbose_option_adds_flag",
            "tests/test_cli_error_boundary.py::TestTheRootGroupIsTheBoundary"
            "::test_a_duckdb_error_reaches_the_user_as_an_error_line",
        ],
    ]
    # The child must not inherit the parent run's coverage hooks, xdist worker
    # identity or extra flags -- CI supplies --cov and -n auto to the outer run.
    dropped = ("COV_CORE", "PYTEST_XDIST", "PYTEST_ADDOPTS", "PYTEST_CURRENT_TEST")
    env = {k: v for k, v in os.environ.items() if not k.startswith(dropped)}

    for order in orders:
        run = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-p",
                "no:cacheprovider",
                "-p",
                "no:randomly",
                "-n",
                "0",
                "-q",
                "--no-header",
                *order,
            ],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=900,
        )
        assert run.returncode == 0, f"{order}\n{run.stdout}{run.stderr}"
