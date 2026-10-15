"""Ctrl-C stops ``gpio check`` instead of becoming a "could not run" verdict.

DuckDB 1.5.6 surfaces a real SIGINT in Python as ``RuntimeError("Query
interrupted")``, not as ``duckdb.InterruptException``. A stage guard that
catches ``Exception`` would print it as a failed check, carry on, and in
``check all --fix`` go on to rewrite the file the user asked it to stop on.
"""

from __future__ import annotations

import json
import os
import random
import signal
import struct
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import validate
from geoparquet_io.core.check_spatial_order import check_spatial_order
from geoparquet_io.core.duckdb_utils import is_interrupt

QUERY_INTERRUPTED = "Query interrupted"

_GEO = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
}


def _wkb_point(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


def _write(path: Path) -> str:
    """A small GeoParquet 1.1 file with no bbox column, so ``--fix`` has work to do."""
    table = pa.table(
        {
            "id": pa.array(range(20)),
            "geometry": pa.array([_wkb_point(i, -i) for i in range(20)], pa.binary()),
        }
    ).replace_schema_metadata({b"geo": json.dumps(_GEO).encode()})
    pq.write_table(table, path)
    return str(path)


def _write_unordered(path: Path) -> str:
    """Like :func:`_write`, but shuffled, so ``check spatial --fix`` has work to do."""
    order = list(range(200))
    random.Random(7).shuffle(order)
    table = pa.table(
        {
            "id": pa.array(order),
            "geometry": pa.array([_wkb_point(i, -i) for i in order], pa.binary()),
        }
    ).replace_schema_metadata({b"geo": json.dumps(_GEO).encode()})
    pq.write_table(table, path)
    return str(path)


@pytest.fixture
def fixable_file(tmp_path) -> str:
    return _write(tmp_path / "in.parquet")


def _interrupted(*_args, **_kwargs):
    raise RuntimeError(QUERY_INTERRUPTED)


# =============================================================================
# The helper
# =============================================================================


@pytest.mark.parametrize(
    "exc",
    [
        KeyboardInterrupt(),
        duckdb.InterruptException("INTERRUPT Error: Interrupted!"),
        RuntimeError(QUERY_INTERRUPTED),
        duckdb.Error(QUERY_INTERRUPTED),
    ],
    ids=["keyboard", "duckdb-interrupt", "runtime", "duckdb-error"],
)
def test_is_interrupt_recognises_every_shape(exc):
    assert is_interrupt(exc)


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("something else"),
        duckdb.InvalidInputException("Invalid Input Error: Query interrupted by bad WKB"),
        ValueError(QUERY_INTERRUPTED),
    ],
    ids=["other-runtime", "other-duckdb", "other-type"],
)
def test_is_interrupt_ignores_other_errors(exc):
    assert not is_interrupt(exc)


def _wrapped(inner: BaseException) -> Exception:
    """``inner`` re-raised the way ``apply_all_fixes`` wraps it."""
    try:
        try:
            raise inner
        except BaseException as e:
            raise ValueError(f"Failed to apply fixes: {e}") from e
    except ValueError as outer:
        return outer


@pytest.mark.parametrize(
    "inner",
    [RuntimeError(QUERY_INTERRUPTED), duckdb.InterruptException("INTERRUPT Error: Interrupted!")],
    ids=["runtime", "duckdb-interrupt"],
)
def test_is_interrupt_sees_a_wrapped_interrupt(inner):
    assert is_interrupt(_wrapped(inner))


def test_is_interrupt_sees_an_interrupt_in_the_context_chain():
    try:
        try:
            raise RuntimeError(QUERY_INTERRUPTED)
        except RuntimeError:
            raise ValueError("cleanup failed")  # noqa: B904, the implicit context is the point
    except ValueError as outer:
        assert is_interrupt(outer)


def test_is_interrupt_ignores_a_wrapped_ordinary_error():
    assert not is_interrupt(_wrapped(RuntimeError("disk full")))


def test_is_interrupt_survives_a_cause_cycle():
    first, second = RuntimeError("a"), RuntimeError("b")
    first.__cause__ = second
    second.__cause__ = first
    assert not is_interrupt(first)


# =============================================================================
# check all and check spatial
# =============================================================================


def test_check_all_fix_writes_output_when_not_interrupted(fixable_file, tmp_path):
    """Control for the test below. Without an interrupt, --fix does write."""
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["check", "all", fixable_file, "--fix", "--fix-output", str(out)]
    )
    assert out.exists(), result.output


@pytest.mark.parametrize("stage", ["check_spatial_impl", "check_structure_impl"])
def test_check_all_stops_on_query_interrupted(fixable_file, tmp_path, monkeypatch, stage):
    from geoparquet_io.cli.commands import check as cli_check

    monkeypatch.setattr(cli_check, stage, _interrupted)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["check", "all", fixable_file, "--fix", "--fix-output", str(out)]
    )
    assert result.exit_code == 1, result.output
    assert "Aborted!" in result.output
    assert "could not run" not in result.output
    assert "Spec Validation:" not in result.output
    assert not out.exists()


def test_check_all_stops_on_keyboard_interrupt(fixable_file, tmp_path, monkeypatch):
    """Ctrl-C outside a query arrives as KeyboardInterrupt, through both guards."""
    from geoparquet_io.cli.commands import check as cli_check

    def ctrl_c(*_args, **_kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli_check, "check_spatial_impl", ctrl_c)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["check", "all", fixable_file, "--fix", "--fix-output", str(out)]
    )
    assert result.exit_code == 1, result.output
    assert "Aborted!" in result.output
    assert "Spec Validation:" not in result.output
    assert not out.exists()


def test_fix_summary_is_printed_once_on_interrupt(tmp_path, monkeypatch):
    """The stage guard and the file guard both see the interrupt, one summary."""
    from geoparquet_io.cli.commands import check as cli_check

    files = [_write(tmp_path / name) for name in ("a.parquet", "b.parquet")]

    def second_file_interrupted(file_path, *args, **kwargs):
        if Path(file_path).name == "b.parquet":
            raise RuntimeError(QUERY_INTERRUPTED)
        return real_spatial(file_path, *args, **kwargs)

    real_spatial = cli_check.check_spatial_impl
    monkeypatch.setattr(cli_check, "check_spatial_impl", second_file_interrupted)
    result = CliRunner().invoke(
        cli, ["check", "all", str(tmp_path / "*.parquet"), "--all-files", "--fix"]
    )
    assert result.exit_code == 1, result.output
    assert result.output.count("Fixed 1 file:") == 1, result.output
    assert files[0] in result.output


@pytest.mark.parametrize(
    "interrupt",
    [lambda: RuntimeError(QUERY_INTERRUPTED), KeyboardInterrupt],
    ids=["query-interrupted", "keyboard"],
)
@pytest.mark.parametrize(
    ("command", "patched"),
    [("all", "_apply_bbox_column_fix"), ("spatial", "fix_spatial_ordering")],
)
def test_multi_file_fix_stops_on_an_interrupted_fix_write(
    tmp_path, monkeypatch, interrupt, command, patched
):
    """A Ctrl-C during one file's fix write stops the run, even when wrapped.

    ``apply_all_fixes`` re-raises it as ``GeoParquetError(...) from e``, which
    used to read as "Fix failed" so the next file was rewritten. The
    interrupted file is put back and left with no ``.bak``.
    """
    from geoparquet_io.core import check_fixes

    first = _write_unordered(tmp_path / "a.parquet")
    second = _write_unordered(tmp_path / "b.parquet")
    before = {path: Path(path).read_bytes() for path in (first, second)}
    fixed = []

    def interrupted_write(current_file, *args, **kwargs):
        fixed.append(current_file)
        raise interrupt()

    monkeypatch.setattr(check_fixes, patched, interrupted_write)
    result = CliRunner().invoke(
        cli, ["check", command, str(tmp_path / "*.parquet"), "--all-files", "--fix"]
    )
    assert result.exit_code == 1, result.output
    assert "Aborted!" in result.output
    assert "Fix failed" not in result.output
    assert len(fixed) == 1, fixed
    assert {path: Path(path).read_bytes() for path in (first, second)} == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["a.parquet", "b.parquet"]


def test_check_all_stops_when_spec_validation_is_interrupted(fixable_file, tmp_path, monkeypatch):
    monkeypatch.setattr(validate, "validate_geoparquet", _interrupted)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["check", "all", fixable_file, "--fix", "--fix-output", str(out)]
    )
    assert result.exit_code == 1, result.output
    assert "Aborted!" in result.output
    assert "could not run" not in result.output
    assert not out.exists()


def test_multi_file_check_spatial_stops_on_query_interrupted(tmp_path, monkeypatch):
    from geoparquet_io.cli.commands import check as cli_check

    _write(tmp_path / "a.parquet")
    _write(tmp_path / "b.parquet")
    calls = []

    def interrupted(*args, **kwargs):
        calls.append(args)
        raise RuntimeError(QUERY_INTERRUPTED)

    monkeypatch.setattr(cli_check, "check_spatial_impl", interrupted)
    result = CliRunner().invoke(
        cli, ["check", "spatial", str(tmp_path / "*.parquet"), "--all-files"]
    )
    assert result.exit_code == 1, result.output
    assert "Aborted!" in result.output
    # The second file is never started.
    assert len(calls) == 1


def test_structure_stage_failure_fails_the_file_and_fixes_nothing(
    fixable_file, tmp_path, monkeypatch
):
    """The structure check is the gate. A file it cannot read is not checked further."""
    from geoparquet_io.cli.commands import check as cli_check

    def boom(*_args, **_kwargs):
        raise RuntimeError("structure exploded")

    monkeypatch.setattr(cli_check, "check_structure_impl", boom)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(
        cli, ["check", "all", fixable_file, "--fix", "--fix-output", str(out)]
    )
    # Reported as a check that could not run, not a traceback (#1252).
    assert result.exit_code == 1, result.output
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert "Check could not run, structure exploded" in result.output
    assert "Spec Validation:" not in result.output
    assert not out.exists()


# =============================================================================
# core
# =============================================================================


@pytest.mark.parametrize(
    "exc",
    [RuntimeError(QUERY_INTERRUPTED), duckdb.Error(QUERY_INTERRUPTED)],
    ids=["runtime", "duckdb-error"],
)
def test_spatial_order_lets_query_interrupted_through(fixable_file, monkeypatch, exc):
    from geoparquet_io.core import check_spatial_order as spatial_mod

    def interrupted(*_args, **_kwargs):
        raise exc

    monkeypatch.setattr(spatial_mod, "_sample_distances", interrupted)
    with pytest.raises(type(exc), match=QUERY_INTERRUPTED):
        check_spatial_order(fixable_file, 10, 1000, False, return_results=True)


class _InterruptingConnection:
    """Forwards to a real connection, but a WKB scan is "interrupted"."""

    def __init__(self, inner):
        self._inner = inner

    def execute(self, query, *args):
        if "octet_length" in query:
            raise RuntimeError(QUERY_INTERRUPTED)
        return self._inner.execute(query, *args)

    def close(self):
        self._inner.close()


def test_validation_stops_when_a_check_is_interrupted(fixable_file, monkeypatch):
    """A check's own ``except Exception`` must not turn Ctrl-C into a verdict."""
    real_open = validate._open_validation_connection
    monkeypatch.setattr(
        validate,
        "_open_validation_connection",
        lambda parquet_file: _InterruptingConnection(real_open(parquet_file)),
    )
    with pytest.raises(KeyboardInterrupt):
        validate.validate_geoparquet(fixable_file, validate_data=True, sample_size=0)


# =============================================================================
# A real SIGINT
# =============================================================================


def _write_large(path: Path, rows: int) -> None:
    con = duckdb.connect()
    con.execute("LOAD spatial")
    con.execute(
        f"""
        COPY (
            SELECT i AS id,
                   ST_Point(random() * 360 - 180, random() * 180 - 90) AS geometry
            FROM range({rows}) t(i)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.mark.slow
@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT to a child is POSIX only")
def test_real_sigint_during_check_all_writes_no_fix(tmp_path):
    """SIGINT lands inside the spatial order query, which takes about 20s here.

    The sample size, not the file size, sets that duration (the distance query
    grows with its square), so the signal reliably arrives mid-query even on a
    loaded runner. Output is unbuffered so the stage header is seen when printed.
    """
    source = tmp_path / "big.parquet"
    _write_large(source, 1_000_000)
    out = tmp_path / "out.parquet"
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "geoparquet_io.cli.main",
            "check",
            "all",
            str(source),
            "--random-sample-size",
            "30000",
            "--fix",
            "--fix-output",
            str(out),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    assert proc.stdout is not None
    deadline = time.monotonic() + 120
    for line in proc.stdout:
        if "Spatial Order Analysis:" in line or time.monotonic() > deadline:
            break
    time.sleep(1)
    os.kill(proc.pid, signal.SIGINT)
    output, _ = proc.communicate(timeout=120)
    assert proc.returncode != 0, output
    assert "Aborted!" in output
    assert "Traceback" not in output
    assert "Spec Validation:" not in output
    assert not out.exists()
