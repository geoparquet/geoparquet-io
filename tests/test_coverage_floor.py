"""Tests for scripts/coverage_floor.py, the one-command coverage ratchet.

The floor rule (measured combined figure minus two, rounded down; never
lowered) lived only in a comment, and the move was a hand edit that left a
stale dated comment behind once already (#827 -> #1038). These tests pin the
arithmetic, the rewrite of every site the number appears in, the refusals,
and the drift check that ``tests/test_coverage_job.py`` runs against the real
tree on every PR.
"""

from __future__ import annotations

import datetime as dt
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts import coverage_floor

pytestmark = pytest.mark.meta

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _totals(**overrides):
    totals = {
        "covered_lines": 22_000,
        "num_statements": 25_000,
        "percent_covered": 87.4,
        "missing_lines": 3_000,
        "excluded_lines": 10,
        "num_branches": 9_500,
        "num_partial_branches": 1_100,
        "covered_branches": 7_600,
        "missing_branches": 1_900,
    }
    totals.update(overrides)
    return totals


def _write_report(path: Path, totals: dict, files: dict | None = None) -> Path:
    path.write_text(json.dumps({"totals": totals, "files": files or {}}), encoding="utf-8")
    return path


@pytest.fixture()
def repo_copy(tmp_path: Path) -> Path:
    """A copy of just the files the script rewrites, so --apply can be exercised."""
    for rel in ("pyproject.toml", *{rel for rel, _ in coverage_floor.ECHOES}):
        dest = tmp_path / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(PROJECT_ROOT / rel, dest)
    return tmp_path


class TestArithmetic:
    @pytest.mark.parametrize(
        ("combined", "floor"),
        [(87.069, 85), (87.999, 85), (88.0, 86), (90.5, 88), (86.2, 84)],
    )
    def test_measured_minus_two_rounded_down(self, combined, floor):
        assert coverage_floor.propose(combined) == floor

    def test_measurement_reads_all_three_figures(self, tmp_path: Path):
        report = _write_report(tmp_path / "cov.json", _totals())
        m = coverage_floor.measurement_from_json(report)
        assert m.combined == 87.4
        assert m.line == pytest.approx(88.0)
        assert m.branch == pytest.approx(80.0)
        assert m.proposed_floor == 85

    def test_report_without_branch_data_is_refused(self, tmp_path: Path):
        """A line-only report would propose a floor several points too high."""
        report = _write_report(tmp_path / "cov.json", _totals(num_branches=0))
        with pytest.raises(SystemExit, match="no branch data"):
            coverage_floor.measurement_from_json(report)


class TestApply:
    def test_rewrites_home_comment_and_every_echo(self, repo_copy: Path):
        m = coverage_floor.measurement_from_json(
            _write_report(repo_copy / "cov.json", _totals(percent_covered=91.234))
        )
        coverage_floor.apply(m.proposed_floor, m, dt.date(2030, 1, 2), root=repo_copy)

        pyproject = (repo_copy / "pyproject.toml").read_text(encoding="utf-8")
        assert coverage_floor.current_floor(pyproject) == 89
        dated = coverage_floor.DATED.search(pyproject)
        assert dated is not None
        assert (dated["measured"], dated["date"]) == ("91.234", "2030-01-02")
        for rel, pattern in coverage_floor.ECHOES:
            match = pattern.search((repo_copy / rel).read_text(encoding="utf-8"))
            assert match is not None, rel
            assert int(match["value"]) == 89, rel
        assert coverage_floor.drift(root=repo_copy) == []

    def test_rewrite_is_one_line_per_site(self, repo_copy: Path):
        """Nothing but the numbers moves: the surrounding prose is untouched."""
        before = {
            rel: (repo_copy / rel).read_text(encoding="utf-8").splitlines()
            for rel, _ in coverage_floor.ECHOES
        }
        before["pyproject.toml"] = (
            (repo_copy / "pyproject.toml").read_text(encoding="utf-8").splitlines()
        )
        m = coverage_floor.measurement_from_json(
            _write_report(repo_copy / "cov.json", _totals(percent_covered=95.0))
        )
        coverage_floor.apply(93, m, dt.date(2030, 1, 2), root=repo_copy)
        for rel, old_lines in before.items():
            new_lines = (repo_copy / rel).read_text(encoding="utf-8").splitlines()
            changed = sum(a != b for a, b in zip(old_lines, new_lines, strict=True))
            expected = (
                2 if rel == "pyproject.toml" else sum(r == rel for r, _ in coverage_floor.ECHOES)
            )
            assert changed == expected, f"{rel}: {changed} lines changed, expected {expected}"


class TestDrift:
    def test_clean_copy_has_no_drift(self, repo_copy: Path):
        assert coverage_floor.drift(root=repo_copy) == []

    def test_echo_that_disagrees_is_reported(self, repo_copy: Path):
        rel, pattern = coverage_floor.ECHOES[0]
        path = repo_copy / rel
        text = path.read_text(encoding="utf-8")
        match = pattern.search(text)
        assert match is not None
        path.write_text(
            text[: match.start("value")] + "42" + text[match.end("value") :], encoding="utf-8"
        )
        problems = coverage_floor.drift(root=repo_copy)
        assert len(problems) == 1
        assert "says 42" in problems[0]

    def test_stale_dated_comment_is_reported(self, repo_copy: Path):
        """The bug #1038 had to clean up: a floor moved without its measurement."""
        pyproject = repo_copy / "pyproject.toml"
        text = pyproject.read_text(encoding="utf-8")
        floor = coverage_floor.current_floor(text)
        text = coverage_floor.HOME.sub(f"fail_under = {floor + 3}", text)
        pyproject.write_text(text, encoding="utf-8")
        problems = coverage_floor.drift(root=repo_copy)
        assert any("comment records a measurement" in p for p in problems)


class TestMain:
    def test_from_json_reports_and_proposes_without_writing(
        self, repo_copy: Path, capsys, monkeypatch
    ):
        monkeypatch.setattr(coverage_floor, "REPO_ROOT", repo_copy)
        report = _write_report(repo_copy / "cov.json", _totals(percent_covered=99.5))
        assert coverage_floor.main(["--from-json", str(report)]) == 0
        out = capsys.readouterr().out
        assert "combined (line+branch): 99.500%" in out
        assert "proposed fail_under:    97" in out
        assert coverage_floor.drift(root=repo_copy) == [], "a report-only run must not write"

    def test_apply_refuses_to_lower_the_floor(self, repo_copy: Path, capsys, monkeypatch):
        monkeypatch.setattr(coverage_floor, "REPO_ROOT", repo_copy)
        floor = coverage_floor.current_floor(
            (repo_copy / "pyproject.toml").read_text(encoding="utf-8")
        )
        report = _write_report(repo_copy / "cov.json", _totals(percent_covered=floor - 1.0))
        assert coverage_floor.main(["--from-json", str(report), "--apply"]) == 1
        assert "regression to fix" in capsys.readouterr().err
        assert (
            coverage_floor.current_floor((repo_copy / "pyproject.toml").read_text(encoding="utf-8"))
            == floor
        )

    def test_apply_refuses_after_a_run_with_failures(self, repo_copy: Path, capsys, monkeypatch):
        """Timed-out subprocess tests under-report; a red run cannot move the floor."""
        monkeypatch.setattr(coverage_floor, "REPO_ROOT", repo_copy)

        def fake_run(json_path: Path) -> int:
            _write_report(json_path, _totals(percent_covered=99.0))
            return 1

        monkeypatch.setattr(coverage_floor, "run_fast_suite", fake_run)
        assert coverage_floor.main(["--apply"]) == 1
        assert "had failures" in capsys.readouterr().err

    def test_apply_moves_the_floor_after_a_green_run(self, repo_copy: Path, capsys, monkeypatch):
        monkeypatch.setattr(coverage_floor, "REPO_ROOT", repo_copy)

        def fake_run(json_path: Path) -> int:
            _write_report(json_path, _totals(percent_covered=99.0))
            return 0

        monkeypatch.setattr(coverage_floor, "run_fast_suite", fake_run)
        assert coverage_floor.main(["--apply"]) == 0
        assert "-> 97" in capsys.readouterr().out
        assert (
            coverage_floor.current_floor((repo_copy / "pyproject.toml").read_text(encoding="utf-8"))
            == 97
        )

    def test_top_prints_modules_by_missed_statements(self, repo_copy: Path, capsys, monkeypatch):
        monkeypatch.setattr(coverage_floor, "REPO_ROOT", repo_copy)
        files = {
            "a.py": {
                "summary": {
                    "missing_lines": 5,
                    "num_statements": 50,
                    "covered_lines": 45,
                    "num_branches": 10,
                    "covered_branches": 8,
                }
            },
            "b.py": {
                "summary": {
                    "missing_lines": 40,
                    "num_statements": 100,
                    "covered_lines": 60,
                    "num_branches": 0,
                    "covered_branches": 0,
                }
            },
        }
        report = _write_report(repo_copy / "cov.json", _totals(), files)
        assert coverage_floor.main(["--from-json", str(report), "--top", "1"]) == 0
        out = capsys.readouterr().out
        assert "b.py" in out and "a.py" not in out

    def test_check_on_the_real_tree(self):
        """End to end through the console: the committed tree is consistent."""
        result = subprocess.run(
            ["uv", "run", "python", str(PROJECT_ROOT / "scripts" / "coverage_floor.py"), "--check"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 0, result.stderr
        assert "agree" in result.stdout
