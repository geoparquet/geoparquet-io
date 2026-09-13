"""The ``gpio`` process's exit status is gpio's answer, not its teardown's.

``geoparquet_io.cli.main.main`` (the console script) ends the process with
``os._exit`` after running the atexit handlers and flushing the streams, so
CPython never unloads DuckDB, Arrow, GEOS and PROJ: that teardown aborted about
one run in twenty on Linux under CPU contention, after the command had already
succeeded (#1053). These tests pin what that must and must not change: the
status Click chose, everything the command printed, the atexit handlers, and
that ``cli`` itself stays a plain Click group for embedders.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1053
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from geoparquet_io.cli.main import _leave_process, _process_status

#: The console script's own program, plus two probes: an atexit handler, which
#: must still run, and a module-level ``__del__``, which only interpreter
#: finalization (the part being skipped) would trigger.
_PROGRAM = (
    "import atexit, sys\n"
    "atexit.register(lambda: sys.stderr.write('ATEXIT-RAN\\n'))\n"
    "class _Probe:\n"
    "    def __del__(self): sys.stderr.write('FINALIZATION-RAN\\n')\n"
    "_keep = _Probe()\n"
    "from geoparquet_io.cli.main import main\n"
    "main()\n"
)


#: Windows keeps the interpreter's exit (see ADR-0007), so finalization runs there.
_WINDOWS = sys.platform == "win32"


def _run_entry_point(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", _PROGRAM, *args],
        capture_output=True,
        text=True,
        timeout=300,
        env={**os.environ, **(env or {})},
    )


@pytest.mark.parametrize(
    ("args", "status", "needle"),
    [
        # A usage error is 2 and a reported failure is 1: two different
        # statuses, so a wrapper that flattened everything to 1 is caught.
        (("--no-such-flag",), 2, "No such option"),
        (("inspect", "meta", "no-such-file.parquet"), 1, "no-such-file.parquet"),
    ],
)
def test_a_failing_command_keeps_the_status_click_chose(args, status, needle):
    completed = _run_entry_point(*args)

    assert completed.returncode == status
    assert needle in completed.stdout + completed.stderr
    assert "ATEXIT-RAN" in completed.stderr
    assert ("FINALIZATION-RAN" in completed.stderr) is _WINDOWS


def test_a_successful_command_delivers_its_output_and_skips_finalization():
    """``os._exit`` discards whatever is still buffered, so the flush is load-bearing:
    ``gpio skills --show`` writes tens of kilobytes, more than one pipe buffer."""
    from geoparquet_io.skills import get_skill_content

    expected = get_skill_content("geoparquet")
    assert len(expected) > 8192, "the fixture stopped being big enough to test buffering"

    completed = _run_entry_point("skills", "--show")

    assert completed.returncode == 0
    assert completed.stdout.rstrip("\n") == expected.rstrip("\n")
    assert "ATEXIT-RAN" in completed.stderr
    assert ("FINALIZATION-RAN" in completed.stderr) is _WINDOWS


def test_the_escape_hatch_leaves_through_the_interpreter():
    completed = _run_entry_point("--version", env={"GPIO_INTERPRETER_EXIT": "1"})

    assert completed.returncode == 0
    assert "FINALIZATION-RAN" in completed.stderr


def test_the_group_itself_stays_a_plain_click_group():
    """Embedders call ``cli(..., standalone_mode=False)`` and expect it to return."""
    from geoparquet_io.cli.main import cli

    assert cli(["--version"], standalone_mode=False) == 0


class _Left(BaseException):
    """Stands in for ``os._exit``, which does not return."""

    def __init__(self, status: int):
        self.status = status


def _fake_exit(status: int):
    raise _Left(status)


@pytest.mark.parametrize(("argv", "status"), [(["--version"], 0), (["--no-such-flag"], 2)])
def test_main_hands_clicks_status_to_the_exit(monkeypatch, capsys, argv, status):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(os, "_exit", _fake_exit)
    monkeypatch.setattr("atexit._run_exitfuncs", lambda: None)
    monkeypatch.setattr(sys, "argv", ["gpio", *argv])
    from geoparquet_io.cli.main import main

    with pytest.raises(_Left) as left:
        main()

    assert left.value.status == status
    capsys.readouterr()


@pytest.mark.parametrize("how", ["env", "windows"])
def test_the_escape_hatch_uses_sys_exit_in_process(monkeypatch, how):
    if how == "env":
        monkeypatch.setenv("GPIO_INTERPRETER_EXIT", "1")
    else:
        monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(os, "_exit", _fake_exit)

    with pytest.raises(SystemExit) as left:
        _leave_process(3)

    assert left.value.code == 3


@pytest.mark.parametrize(("code", "status"), [(None, 0), (0, 0), (2, 2)])
def test_a_status_survives_the_trip_through_os_exit(code, status):
    assert _process_status(code) == status


def test_a_non_integer_exit_code_is_printed_and_becomes_one(capsys):
    """``sys.exit("message")`` prints the message and exits 1, CPython's rule."""
    assert _process_status("something went wrong") == 1
    assert capsys.readouterr().err.strip() == "something went wrong"


class _Stream:
    def __init__(self, name: str, error: Exception | None = None):
        self.name = name
        self.error = error

    def flush(self):
        if self.error is not None:
            raise self.error


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (BrokenPipeError(32, "Broken pipe"), 3),  # `| head` went away: status stands
        (ValueError("I/O operation on closed file"), 3),
        (OSError(28, "No space left on device"), 120),  # CPython's own status for this
    ],
)
def test_a_flush_failure_is_a_broken_pipe_or_a_real_error(monkeypatch, error, status):
    monkeypatch.setattr(sys, "platform", "linux")
    statuses: list[int] = []
    monkeypatch.setattr(os, "_exit", statuses.append)
    monkeypatch.setattr(sys, "stdout", _Stream("<stdout>", error))
    monkeypatch.setattr(sys, "stderr", _Stream("<stderr>"))
    monkeypatch.setattr("atexit._run_exitfuncs", lambda: None)

    _leave_process(3)

    assert statuses == [status]


def test_a_missing_stream_is_not_an_error(monkeypatch):
    """``pythonw`` and a closed fd 1 leave ``sys.stdout`` as None."""
    monkeypatch.setattr(sys, "platform", "linux")
    statuses: list[int] = []
    monkeypatch.setattr(os, "_exit", statuses.append)
    monkeypatch.setattr(sys, "stdout", None)
    monkeypatch.setattr("atexit._run_exitfuncs", lambda: None)

    _leave_process(0)

    assert statuses == [0]
