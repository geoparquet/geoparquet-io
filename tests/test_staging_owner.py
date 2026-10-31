"""One staging owner (#1367 concept 11, #1076): what a rewrite leaves behind.

Every command that writes over a destination stages beside it and publishes
with one move (``file_utils.atomic_write``). These tests hold the commands to
what that owner promises, through the real CLI:

* a write that fails at the publish step -- an ``OSError`` or a Ctrl-C --
  leaves the destination as it was and no temp file anywhere (#1076's family:
  each command used to clean up its own temp, some on ``Exception`` only, some
  not at all);
* a rewrite keeps the destination's mode, and a new output gets the umask's
  mode, not ``mkstemp``'s 0600 (HANDOFFS row 34);
* an in-place ``check --fix`` of a symlinked input rewrites the link's target
  and keeps the link (#1076 item 2), and a second in-place fix keeps the first
  fix's backup of the original (#1076 item 1).
"""

from __future__ import annotations

import gc
import os
import shutil
import stat
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

DATA = Path(__file__).parent / "data"
BUILDINGS = DATA / "buildings_test.parquet"


def _cells(work: Path) -> Path:
    """An H3 r9 aggregate of the buildings fixture: an overview ladder's base."""
    from geoparquet_io.api.table import read

    path = work / "cells.parquet"
    read(str(BUILDINGS)).aggregate_h3(9).write(str(path))
    return path


def _command(name: str, work: Path, sibs: Path) -> tuple[list[str], Path]:
    """The CLI arguments for *name* and the existing file it writes over."""
    src = work / "in.parquet"
    out = work / "out.parquet"
    if name == "process_overview":
        cells = _cells(sibs)
        out.write_bytes(b"precious")
        args = ["process", "overview", str(cells), "--levels", "8", "--overview-out", str(out)]
        return [*args, "--output-dir", str(sibs), "--force"], out
    shutil.copy(BUILDINGS, src)
    if name == "process_simplify":
        out.write_bytes(b"precious")
        return ["process", "simplify", str(src), str(out), "--tolerance", "0.00001"], out
    if name == "convert_reproject":
        return ["convert", "reproject", str(src), str(src), "--overwrite", "-d", "EPSG:3857"], src
    if name == "check_bbox_fix":
        return ["check", "bbox", str(src), "--fix"], src
    assert name == "add_geometry_metrics"
    out.write_bytes(b"precious")
    return ["add", "geometry-metrics", str(src), str(out), "--overwrite"], out


COMMANDS = (
    "process_overview",
    "process_simplify",
    "convert_reproject",
    "check_bbox_fix",
    "add_geometry_metrics",
)

#: (test, case) -> reason, for a cell that fails before the staging owner. Empty
#: since the owner landed: every command stages through file_utils.atomic_write.
XFAIL: dict[tuple[str, str], str] = {}


def _cases(test: str, *values) -> list:
    """*values* as params, each strictly xfailed where ``XFAIL`` names it."""
    params = []
    for value in values:
        ident = (
            "-".join(v if isinstance(v, str) else v[0] for v in value)
            if isinstance(value, tuple)
            else value
        )
        reason = XFAIL.get((test, ident))
        marks = [pytest.mark.xfail(strict=True, reason=reason)] if reason else []
        params.append(
            pytest.param(*(value if isinstance(value, tuple) else (value,)), marks=marks, id=ident)
        )
    return params


_FAILURES = (("oserror", OSError), ("ctrl_c", KeyboardInterrupt))


@pytest.mark.parametrize(
    ("name", "failure"),
    _cases(
        "publish",
        *((name, (label, failure)) for name in COMMANDS for label, failure in _FAILURES),
    ),
)
def test_a_failed_publish_leaves_the_destination_and_no_temp(name, failure, tmp_path, monkeypatch):
    """The publish step fails; the destination is untouched and nothing is left over."""
    work = tmp_path / "work"
    sibs = tmp_path / "sibs"
    scratch = tmp_path / "scratch"
    for d in (work, sibs, scratch):
        d.mkdir()
    args, destination = _command(name, work, sibs)
    listing = sorted(p.name for p in work.iterdir())
    before = destination.read_bytes()

    failure = failure[1]

    def refuse(*_args, **_kwargs):
        raise failure("injected publish failure")

    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    for module, attr in ((os, "replace"), (os, "rename"), (shutil, "move")):
        monkeypatch.setattr(module, attr, refuse)
    result = CliRunner().invoke(cli, args)
    monkeypatch.undo()

    assert result.exit_code != 0, result.output
    # The captured exception retains closed connection frames. End that
    # lifetime before checking connection-owned spill marker cleanup.
    del result
    gc.collect()
    assert destination.read_bytes() == before, "the destination was written although the run failed"
    assert sorted(p.name for p in work.iterdir()) == listing
    assert not list(scratch.rglob("*")), "a temp file outlived the failed run"


def _umask_mode() -> int:
    mask = os.umask(0)
    os.umask(mask)
    return 0o666 & ~mask


@pytest.mark.parametrize("name", _cases("mode", *COMMANDS))
def test_a_rewrite_keeps_the_destinations_mode(name, tmp_path):
    work = tmp_path / "work"
    sibs = tmp_path / "sibs"
    work.mkdir()
    sibs.mkdir()
    args, destination = _command(name, work, sibs)
    os.chmod(destination, 0o640)
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert stat.S_IMODE(destination.stat().st_mode) == 0o640


NEW_OUTPUTS = {
    "process_overview": lambda src, out, sibs: [
        "process",
        "overview",
        str(_cells(sibs)),
        "--levels",
        "8",
        "--overview-out",
        str(out),
        "--output-dir",
        str(sibs),
    ],
    "process_simplify": lambda src, out, _sibs: [
        "process",
        "simplify",
        str(src),
        str(out),
        "--tolerance",
        "0.00001",
    ],
    "convert_reproject": lambda src, out, _sibs: ["convert", "reproject", str(src), str(out)],
    "add_geometry_metrics": lambda src, out, _sibs: ["add", "geometry-metrics", str(src), str(out)],
    "check_bbox_fix_output": lambda src, out, _sibs: [
        "check",
        "bbox",
        str(src),
        "--fix",
        "--fix-output",
        str(out),
    ],
}


@pytest.mark.parametrize("name", _cases("new", *sorted(NEW_OUTPUTS)))
def test_a_new_output_gets_the_umask_mode(name, tmp_path):
    sibs = tmp_path / "sibs"
    sibs.mkdir()
    src = tmp_path / "in.parquet"
    shutil.copy(BUILDINGS, src)
    out = tmp_path / "out.parquet"
    result = CliRunner().invoke(cli, NEW_OUTPUTS[name](src, out, sibs))
    assert result.exit_code == 0, result.output
    assert stat.S_IMODE(out.stat().st_mode) == _umask_mode()


@pytest.mark.parametrize("command", _cases("symlink", "bbox", "compression"))
def test_an_in_place_fix_of_a_symlink_rewrites_its_target(command, tmp_path):
    """#1076 item 2: the link stays a link, and the file it names is the one fixed."""
    real = tmp_path / "real.parquet"
    pq.write_table(pq.read_table(BUILDINGS), real, compression="snappy")
    link = tmp_path / "link.parquet"
    link.symlink_to(real)
    before = real.read_bytes()

    result = CliRunner().invoke(cli, ["check", command, str(link), "--fix"])

    assert result.exit_code == 0, result.output
    assert link.is_symlink(), "the in-place fix replaced the link with a regular file"
    assert real.read_bytes() != before, "the link's target was not fixed"


def test_a_second_in_place_fix_keeps_the_first_backup(tmp_path):
    """#1076 item 1: the ``.bak`` holding the original is never written over."""
    target = tmp_path / "f.parquet"
    pq.write_table(pq.read_table(BUILDINGS), target, compression="snappy")
    original = target.read_bytes()

    first = CliRunner().invoke(cli, ["check", "compression", str(target), "--fix"])
    assert first.exit_code == 0, first.output
    fixed_once = target.read_bytes()
    second = CliRunner().invoke(cli, ["check", "bbox", str(target), "--fix"])
    assert second.exit_code == 0, second.output

    assert Path(f"{target}.bak").read_bytes() == original
    backups = sorted(p.name for p in tmp_path.iterdir() if p.name != target.name)
    assert backups == ["f.parquet.bak", "f.parquet.bak.1"], backups
    assert Path(f"{target}.bak.1").read_bytes() == fixed_once
    assert f"Created backup: {target}.bak.1" in second.output


def test_scratch_dir_retries_a_released_leftover(monkeypatch):
    from geoparquet_io.core.file_utils import scratch_dir

    real_unlink = os.unlink
    failures = []

    def unlink(path, *args, **kwargs):
        if os.path.basename(path) == "response.gfs" and not failures:
            failures.append(path)
            raise PermissionError("handle still closing")
        return real_unlink(path, *args, **kwargs)

    with scratch_dir() as directory:
        leftover = Path(directory) / "response.gfs"
        leftover.write_text("schema")
        monkeypatch.setattr(os, "unlink", unlink)
    assert failures
    assert not leftover.exists()
    assert not Path(directory).exists()
