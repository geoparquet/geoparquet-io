"""One test must not leave the package logger raised for the next one.

``--verbose`` raises the ``geoparquet_io`` logger to DEBUG as the flag is parsed
(:func:`geoparquet_io.cli.decorators.enable_verbose_logging`, #995) and
:func:`~geoparquet_io.core.logging_config.configure_verbose` is deliberately
one-way, so nothing ever lowers it again. That is right for a CLI -- one command
per process -- and wrong under pytest, where a whole file of tests shares one
xdist worker process: the first ``--verbose`` invocation, or the first core call
made with ``verbose=True``, leaves DEBUG on for every test that follows it on
that worker.

The victim is any test that asserts on the *absence* of debug output. The one
that actually went red on ``main`` is
``tests/test_cli_error_boundary.py::TestTheRootGroupIsTheBoundary::test_a_duckdb_error_reaches_the_user_as_an_error_line``:
``ErrorBoundaryGroup.invoke`` logs "Converted a low-level failure to an error
line" at DEBUG with ``exc_info``, so a leaked DEBUG level puts a traceback in
``result.output`` and the test's ``assert "Traceback" not in result.output``
fails -- on Windows first, only because ``-n auto`` happened to schedule the
tests in the poisoning order there.

The CLI behaviour is correct and is not what these tests pin. What they pin is
the test harness: ``tests/conftest.py`` snapshots the package logger around
every test, so no test can inherit another's level or handlers.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from geoparquet_io.core.logging_config import configure_verbose, setup_cli_logging
from tests.conftest import pristine_package_logging

PACKAGE_LOGGER = logging.getLogger("geoparquet_io")

REPO_ROOT = Path(__file__).resolve().parent.parent


class TestTheGuardPutsBackWhatConfigureVerboseChanges:
    """The three pieces of global state the logging entry points mutate.

    Read off ``core/logging_config.py`` rather than guessed: ``setup_cli_logging``
    assigns ``logger.setLevel``, ``logger.handlers.clear()`` +
    ``addHandler``, and ``logger.propagate``; ``configure_verbose`` raises the
    level and, through ``_bootstrap_default_handler``, may add a handler. No
    module-level flag records verbosity -- the logger's own level *is* the
    state -- and nothing mutates a handler that was already attached, so putting
    back the same handler objects puts back their levels and formatters too.
    """

    def test_a_raised_level_is_put_back(self):
        before = PACKAGE_LOGGER.level

        with pristine_package_logging():
            setup_cli_logging(verbose=False)
            configure_verbose(True)
            assert PACKAGE_LOGGER.level == logging.DEBUG

        assert PACKAGE_LOGGER.level == before

    def test_replaced_handlers_are_put_back_as_the_same_objects(self):
        before = list(PACKAGE_LOGGER.handlers)

        with pristine_package_logging():
            setup_cli_logging(verbose=True)
            assert PACKAGE_LOGGER.handlers != before

        assert PACKAGE_LOGGER.handlers == before

    def test_propagate_is_put_back(self):
        before = PACKAGE_LOGGER.propagate

        with pristine_package_logging():
            PACKAGE_LOGGER.propagate = not before

        assert PACKAGE_LOGGER.propagate is before

    def test_the_guard_puts_state_back_even_when_the_body_raises(self):
        before = PACKAGE_LOGGER.level

        with pytest.raises(RuntimeError), pristine_package_logging():
            configure_verbose(True)
            raise RuntimeError("a failing test still has to hand the logger back")

        assert PACKAGE_LOGGER.level == before


def test_this_test_did_not_inherit_a_raised_level():
    """The invariant the autouse fixture buys, asserted from inside a test.

    Nothing in this process can have left DEBUG set, whatever ran before and on
    whichever worker, because every test hands the level back.
    """
    assert PACKAGE_LOGGER.level != logging.DEBUG


@pytest.mark.integration
def test_the_poisoning_order_no_longer_fails_the_error_boundary_test():
    """The measured reproduction, pinned in the order that used to fail.

    Two things have to be true at once for the boundary test to see a
    traceback, which is why it took an unlucky schedule to surface:

    1. a stream handler is attached to the package logger -- any invocation of
       the real ``cli`` does that, via ``setup_cli_logging``; and
    2. the level is DEBUG -- any ``--verbose`` invocation does that, and never
       undoes it.

    Neither test below is unusual; ``test_verbose_option_adds_flag`` is a
    four-line decorator test. Run in this order on ``main`` the third test fails
    with ``assert 'Traceback' not in ...``. A subprocess is what makes the order
    deterministic: under ``-n auto`` the scheduler, not this file, decides which
    tests share a worker.
    """
    order = [
        "tests/test_cli_error_boundary.py::test_a_rejected_file_gets_an_error_line_and_exit_one[inspect head]",
        "tests/test_decorators.py::TestVerboseOption::test_verbose_option_adds_flag",
        "tests/test_cli_error_boundary.py::TestTheRootGroupIsTheBoundary"
        "::test_a_duckdb_error_reaches_the_user_as_an_error_line",
    ]
    # The child must not inherit the parent run's coverage hooks, xdist worker
    # identity or extra flags -- CI supplies --cov and -n auto to the outer run.
    dropped = ("COV_CORE", "PYTEST_XDIST", "PYTEST_ADDOPTS", "PYTEST_CURRENT_TEST")
    env = {k: v for k, v in os.environ.items() if not k.startswith(dropped)}

    run = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "no:cacheprovider",
            "-n",
            "0",
            "-q",
            "--no-header",
            "-p",
            "no:randomly",
            *order,
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )

    assert run.returncode == 0, run.stdout + run.stderr
