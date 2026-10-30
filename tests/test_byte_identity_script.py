"""``scripts/byte_identity.py``: the per-track byte-identity harness (#1367).

Two properties make the harness evidence: a tree compared with itself is
all-identical (so a difference it reports is not noise), and a real change on
one side is reported as differing, with a footer diff that names it. The second
is shown with the script's own test hook, ``GPIO_BYTE_IDENTITY_FAULT``, which
adds ``--compression-level 9`` on the branch side and is honoured only under
pytest.

Both run this checkout against itself, through its own ``.venv``, so they need
no second worktree. Slow: every case is two ``uv run`` subprocesses.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.slow

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "byte_identity.py"
SELF_CASES = "convert_parquet_default,sort_hilbert,table_write_in-memory_places"


def _run(tmp_path: Path, only: str, **env) -> tuple[subprocess.CompletedProcess, str]:
    if not (ROOT / ".venv").is_dir():
        pytest.skip(f"{ROOT} has no .venv for the harness to run gpio from")
    report = tmp_path / "report.md"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--base",
            str(ROOT),
            "--branch",
            str(ROOT),
            "--out",
            str(report),
            "--only",
            only,
            "--jobs",
            "3",
        ],
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    assert report.exists(), proc.stdout + proc.stderr
    return proc, report.read_text(encoding="utf-8")


def _verdicts(report: str) -> dict[str, str]:
    rows = [line.split("|") for line in report.splitlines() if line.startswith("| `")]
    return {cells[1].strip().strip("`"): cells[2].strip() for cells in rows}


def test_a_tree_against_itself_is_identical(tmp_path, monkeypatch):
    monkeypatch.delenv("GPIO_BYTE_IDENTITY_FAULT", raising=False)
    proc, report = _run(tmp_path, SELF_CASES)
    assert _verdicts(report) == dict.fromkeys(SELF_CASES.split(","), "identical"), report
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_an_injected_compression_level_is_reported_with_its_footer_diff(tmp_path):
    proc, report = _run(
        tmp_path,
        "convert_parquet_default,table_write_in-memory_places,to_geojson_places",
        # 9, not 3: an unset level on the DuckDB COPY paths already *is* 3
        # (#1335), so a level-3 fault leaves `convert` byte-identical.
        GPIO_BYTE_IDENTITY_FAULT="compression_level:9",
    )
    verdicts = _verdicts(report)
    assert verdicts["convert_parquet_default"] == "differs", report
    assert verdicts["table_write_in-memory_places"] == "differs", report
    # A case that takes no compression level is untouched by the fault.
    assert verdicts["to_geojson_places"] == "identical", report
    assert proc.returncode == 1

    detail = report.split("## `table_write_in-memory_places`", 1)[1].split("\n## ", 1)[0]
    assert "compressed size `geometry`" in detail, detail
    assert "data tables equal: yes" in detail, detail
    assert "codec" not in detail, "a level change must not read as a codec change"
    assert "injected fault (branch side)" in report


def test_the_fault_hook_is_ignored_outside_pytest(tmp_path):
    env = {k: v for k, v in os.environ.items() if k != "PYTEST_CURRENT_TEST"}
    env["GPIO_BYTE_IDENTITY_FAULT"] = "compression_level:9"
    report = tmp_path / "report.md"
    if not (ROOT / ".venv").is_dir():
        pytest.skip(f"{ROOT} has no .venv")
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--base",
            str(ROOT),
            "--branch",
            str(ROOT),
            "--out",
            str(report),
            "--only",
            "convert_parquet_default",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _verdicts(report.read_text()) == {"convert_parquet_default": "identical"}
