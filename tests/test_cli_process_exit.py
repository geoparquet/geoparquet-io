"""The ``gpio`` process's exit status is gpio's answer, not its teardown's.

``geoparquet_io.cli.main.main`` (the console script) ends the process with
``os._exit`` after running the atexit handlers and flushing the streams, so
CPython never unloads DuckDB, Arrow, GEOS and PROJ: that teardown aborted about
one run in twenty on Linux under CPU contention, after the command had already
succeeded (#1053). These tests pin what that must and must not change: the
status Click chose, everything the command printed, the atexit handlers, and
that ``cli`` itself stays a plain Click group for embedders.

``gpio add geometry-metrics`` was not special: the same teardown killed
``gpio convert geoparquet <native-geo file> -`` -- stdout streaming, a whole
Arrow IPC stream already on the pipe -- with SIGABRT or SIGSEGV in 5 of 832
runs of the same Linux loop, which is #1028. It is the command whose *output*
skipping finalization can damage, so it has a test of its own below.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1053
Refs: https://github.com/geoparquet/geoparquet-io/issues/1028
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


#: Windows keeps the interpreter's exit (see `_leave_process`), so finalization runs there.
_WINDOWS = sys.platform == "win32"

#: Arrow's end-of-stream marker: a zero-length continuation, and the last eight
#: bytes of every well-formed IPC stream.
_IPC_END_OF_STREAM = b"\xff\xff\xff\xff\x00\x00\x00\x00"


def _run_entry_point(
    *args: str, env: dict[str, str] | None = None, text: bool = True
) -> subprocess.CompletedProcess:
    """Run the console script's own program. ``text=False`` for a binary payload."""
    return subprocess.run(
        [sys.executable, "-c", _PROGRAM, *args],
        capture_output=True,
        text=text,
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


def test_a_binary_stream_written_past_click_is_terminated(projected_conus):
    """The flush also has to carry bytes gpio never handed to Click.

    ``gpio convert geoparquet in.parquet -`` is the other command #1028 caught
    aborting at teardown, and it is the one whose output ``os._exit`` can
    silently damage. Its payload is an Arrow IPC stream that pyarrow's
    ``RecordBatchStreamWriter`` writes straight into ``sys.stdout.buffer`` --
    past Click, past the text layer -- and it ends in an eight-byte
    end-of-stream marker (``ff ff ff ff 00 00 00 00``) small enough to sit in
    the buffer indefinitely. The test above cannot see a missing flush, because
    ``click.echo`` flushes the stream itself; this one can, and the damage it
    sees is not a lost log line but an unterminated stream.

    Sabotaged (``_leave_process`` flushing nothing) this command emits 27,944
    bytes instead of 27,952: pyarrow's reader tolerates the truncation and
    still yields 200 rows, so the assertion has to be on the marker rather than
    on whether the stream parses.

    Refs: https://github.com/geoparquet/geoparquet-io/issues/1028
    """
    completed = _run_entry_point("convert", "geoparquet", str(projected_conus), "-", text=False)
    stderr = completed.stderr.decode("utf-8", "replace")

    assert completed.returncode == 0, stderr
    # What #1028 is: the work done, the stream written, and then a native
    # teardown killing the process with SIGABRT or SIGSEGV. Measured on Linux
    # under CPU contention, 5 of 832 runs on `main` died this way.
    assert "terminate called" not in stderr, stderr
    assert "ATEXIT-RAN" in stderr, stderr
    assert ("FINALIZATION-RAN" in stderr) is _WINDOWS, stderr
    assert completed.stdout.endswith(_IPC_END_OF_STREAM), (
        f"the Arrow IPC stream lost its tail: {completed.stdout[-16:].hex()}"
    )


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
