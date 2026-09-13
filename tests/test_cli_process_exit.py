"""The ``gpio`` process's exit status is gpio's answer, not its teardown's.

A gpio run loads DuckDB, its spatial extension, Arrow, GEOS and PROJ. When the
command is over, CPython's interpreter finalization unloads all of them and
destroys their process-global state while their worker threads are being torn
down -- and *that* code can fail after gpio's work is done and its last line is
printed. Measured on Linux under CPU contention (8 concurrent loops, 4 CPUs),
roughly one run in twenty of ``gpio add geometry-metrics`` printed

    Added metrics:area and metrics:perimeter to: .../geometry-metrics.parquet
    terminate called without an active exception

and exited 134. The file was correct; a shell's ``set -e`` still saw a failure,
and so did CI (#1053). The same loop with the process leaving via ``os._exit``
after flushing was clean in 48 of 48 runs.

So the root group's ``__call__`` -- Click's console-script entry point, which
``CliRunner`` and the Python API never reach -- decides the status and leaves.
What that costs is interpreter finalization, and these tests pin both halves:
the status a command chose survives, everything it printed still arrives, and
finalization is genuinely skipped.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1053
"""

from __future__ import annotations

import os
import subprocess
import sys

import click
import pytest

from geoparquet_io.cli.decorators import (
    ErrorBoundaryGroup,
    _leave_process,
    _process_status,
)

#: The console script's own program, plus a marker that only interpreter
#: finalization can print. ``gpio = "geoparquet_io:cli"`` calls the group, so
#: this is the entry point under test and not an approximation of it.
_PROGRAM = (
    "import atexit, sys\n"
    "atexit.register(lambda: sys.stderr.write('FINALIZATION-RAN\\n'))\n"
    "from geoparquet_io.cli.main import cli\n"
    "cli()\n"
)


def _run_entry_point(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _PROGRAM, *args],
        capture_output=True,
        text=True,
        timeout=300,
    )


def test_a_successful_command_exits_zero_and_skips_finalization():
    completed = _run_entry_point("--version")

    assert completed.returncode == 0
    assert "geoparquet-io" in completed.stdout
    assert "FINALIZATION-RAN" not in completed.stderr


@pytest.mark.parametrize(
    ("args", "status", "needle"),
    [
        # A usage error is 2 and a reported failure is 1: two different
        # statuses, so a wrapper that flattened everything to "nonzero" -- or
        # to 1 -- would be caught here.
        (("--no-such-flag",), 2, "No such option"),
        (("inspect", "meta", "no-such-file.parquet"), 1, "no-such-file.parquet"),
    ],
)
def test_a_failing_command_keeps_the_status_click_chose(args, status, needle):
    completed = _run_entry_point(*args)

    assert completed.returncode == status
    assert needle in completed.stdout + completed.stderr
    assert "FINALIZATION-RAN" not in completed.stderr


def test_everything_a_command_printed_arrives_before_the_process_leaves():
    """``os._exit`` discards whatever is still buffered, so the flush is load-bearing.

    ``gpio skills --show`` writes tens of kilobytes in one go -- comfortably more
    than a pipe's buffer, and more than one write -- so a missing flush truncates
    it rather than merely reordering it.
    """
    from geoparquet_io.skills import get_skill_content

    expected = get_skill_content("geoparquet")
    completed = _run_entry_point("skills", "--show")

    assert completed.returncode == 0
    assert len(expected) > 8192, "the fixture stopped being big enough to test buffering"
    assert completed.stdout.rstrip("\n") == expected.rstrip("\n")


# ---------------------------------------------------------------------------
# The pieces, in process. The subprocess tests above are the end-to-end oracle;
# these pin the branches a subprocess cannot be steered into -- a non-integer
# ``SystemExit`` argument, a stream whose flush raises.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "status"),
    [
        (None, 0),  # `sys.exit()` and a command that just returned
        (0, 0),
        (1, 1),
        (2, 2),
        (-1, 255),  # what exit(3) does with it, so `gpio || echo $?` is unchanged
        (300, 44),
    ],
)
def test_a_status_survives_the_trip_through_os_exit(code, status):
    assert _process_status(code) == status


def test_a_non_integer_exit_code_is_printed_and_becomes_one(capsys):
    """``sys.exit("message")`` prints the message and exits 1 -- CPython's rule."""
    assert _process_status("something went wrong") == 1
    assert capsys.readouterr().err.strip() == "something went wrong"


def test_a_broken_downstream_does_not_change_the_status(monkeypatch):
    """``gpio ... | head`` closes the pipe; the flush raises and gpio still exits 0."""

    class _Broken:
        def flush(self):
            raise BrokenPipeError(32, "Broken pipe")

    statuses: list[int] = []
    monkeypatch.setattr(os, "_exit", statuses.append)
    monkeypatch.setattr(sys, "stdout", _Broken())
    monkeypatch.setattr(sys, "stderr", _Broken())

    _leave_process(0)

    assert statuses == [0]


def test_both_streams_are_flushed_before_the_process_leaves(monkeypatch):
    flushed: list[str] = []

    class _Recorder:
        def __init__(self, name):
            self._name = name

        def flush(self):
            flushed.append(self._name)

    monkeypatch.setattr(os, "_exit", lambda status: None)
    monkeypatch.setattr(sys, "stdout", _Recorder("stdout"))
    monkeypatch.setattr(sys, "stderr", _Recorder("stderr"))

    _leave_process(3)

    assert flushed == ["stdout", "stderr"]


@pytest.mark.parametrize(("args", "status"), [(["noop"], 0), (["--no-such-flag"], 2)])
def test_calling_a_group_hands_the_status_to_the_exit(monkeypatch, capsys, args, status):
    """``__call__`` is the seam, and it is the *only* one that exits.

    ``CliRunner`` and the Python API reach a group through
    ``main(standalone_mode=False)``, which is why the assertion below is that
    ``main`` leaves the process alone while ``__call__`` ends it.
    """
    left: list[int] = []

    class _Left(BaseException):
        """Stands in for ``os._exit``, which does not return."""

    def _fake_exit(code: int):
        left.append(code)
        raise _Left

    monkeypatch.setattr(os, "_exit", _fake_exit)

    @click.group(cls=ErrorBoundaryGroup)
    def throwaway():
        pass

    @throwaway.command()
    def noop():
        pass

    with pytest.raises(_Left):
        throwaway(args=args, prog_name="throwaway")
    assert left == [status]

    capsys.readouterr()
    throwaway.main(args=["noop"], prog_name="throwaway", standalone_mode=False)
    assert left == [status], "main(standalone_mode=False) must not end the process"


def test_a_click_that_returns_instead_of_exiting_still_leaves_with_zero(monkeypatch):
    """Nothing here leans on Click raising ``SystemExit`` to end the process.

    It does, in standalone mode -- but a ``__call__`` that only handled the
    exception would fall off its own end and hand the process back to the
    finalization this exists to skip.
    """
    left: list[int] = []

    class _Left(BaseException):
        pass

    def _fake_exit(code: int):
        left.append(code)
        raise _Left

    monkeypatch.setattr(os, "_exit", _fake_exit)

    @click.group(cls=ErrorBoundaryGroup)
    def throwaway():
        pass

    @throwaway.command()
    def noop():
        pass

    with pytest.raises(_Left):
        throwaway(args=["noop"], prog_name="throwaway", standalone_mode=False)

    assert left == [0]
