"""Classified failures reach the user as one error line, never a traceback.

#1031 (a failed single-file upload; Ctrl-C during a directory upload) and
#1266 item 3 (an interrupt swallowed by geometry-column detection).
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.commands import publish as publish_module
from geoparquet_io.cli.main import cli
from geoparquet_io.core import upload as upload_module
from geoparquet_io.core.geometry_detection import _detect_geometry_from_query


def _assert_error_line(result, needle):
    """Exit 1 through Click's own error path: an ``Error:`` line, no escaped exception."""
    assert result.exit_code == 1, result.output
    assert result.exception is None or isinstance(result.exception, SystemExit), repr(
        result.exception
    )
    assert "Traceback" not in result.output
    assert f"Error: {needle}" in result.output or needle in result.output, result.output


def _run_upload(source, fake_put, destination="s3://example-bucket/out/", extra=()):
    with (
        patch.object(publish_module, "check_credentials", return_value=(True, "")),
        patch.object(upload_module.obs, "put", fake_put),
        patch.object(upload_module, "_setup_store_and_kwargs", lambda *a, **k: (object(), {})),
    ):
        return CliRunner().invoke(cli, ["publish", "upload", str(source), destination, *extra])


def test_a_failed_single_file_upload_is_an_error_line_not_a_traceback(tmp_path):
    """#1031: obstore's RuntimeError is not a GeoParquetError; it used to escape raw."""
    source = tmp_path / "one.parquet"
    source.write_bytes(b"x" * 100)

    def failing_put(store, key, path, **kwargs):
        raise RuntimeError("AccessDenied: bucket is read-only")

    result = _run_upload(source, failing_put)

    _assert_error_line(result, "")
    assert "AccessDenied" in result.output
    assert "s3://example-bucket/out/" in result.output


def test_ctrl_c_during_a_directory_upload_stops_the_queue(tmp_path, monkeypatch):
    """#1031: an interrupt sets the stop flag, in-flight uploads finish, the rest never start."""
    source = tmp_path / "dataset"
    source.mkdir()
    for i in range(10):
        (source / f"part-{i:02d}.parquet").write_bytes(b"x" * 100)
    arrived: list[str] = []
    lock = threading.Lock()

    def slow_put(store, key, path, **kwargs):
        time.sleep(0.05)
        with lock:
            arrived.append(Path(path).name)

    real_as_completed = upload_module.as_completed

    def interrupted(futures):
        iterator = real_as_completed(futures)
        yield next(iterator)
        raise KeyboardInterrupt

    monkeypatch.setattr(upload_module, "as_completed", interrupted)

    result = _run_upload(source, slow_put, extra=["--max-files", "1"])

    assert len(arrived) < 10, f"all {len(arrived)} files reached the store after Ctrl-C"
    assert "file(s) uploaded successfully" in result.output, "no summary after the interrupt"
    assert result.exit_code != 0


@pytest.mark.parametrize(
    "interrupt",
    [RuntimeError("Query interrupted"), duckdb.InterruptException("interrupted")],
    ids=["runtime-error", "interrupt-exception"],
)
def test_geometry_detection_lets_an_interrupt_through(interrupt):
    """#1266 item 3: the broad ``except`` fell back to a default and carried on."""

    class _Conn:
        def execute(self, *_a, **_k):
            raise interrupt

    with pytest.raises(type(interrupt)):
        _detect_geometry_from_query(_Conn(), "SELECT 1")


def test_geometry_detection_still_falls_back_on_an_ordinary_failure():
    class _Conn:
        def execute(self, *_a, **_k):
            raise RuntimeError("some other failure")

    assert _detect_geometry_from_query(_Conn(), "SELECT 1") is None
