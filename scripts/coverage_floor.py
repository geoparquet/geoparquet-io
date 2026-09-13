#!/usr/bin/env python3
"""Measure fast-suite coverage and move the trailing-ratchet floor.

The coverage floor is ``[tool.coverage.report] fail_under`` in ``pyproject.toml``:
the measured full-fast-suite figure minus two, rounded down. Because
``[tool.coverage.run] branch = true``, "measured" means coverage.py's COMBINED
line+branch figure, which sits several points under the line-only figure, so
the floor is re-measured, never converted from a line percentage.

Before this script the ratchet was a hand procedure -- run the suite, read
``TOTAL``, subtract, edit the TOML, update the dated comment -- and the dated
comment went stale at the previous ratchet (#827, fixed by #1038). Here the
whole move is one command, and ``--check`` (also run by the test suite) fails
when the number's echoes in prose drift from the home value.

Usage::

    uv run python scripts/coverage_floor.py                     # run the fast suite, propose
    uv run python scripts/coverage_floor.py --from-json cov.json  # reuse a coverage.json
    uv run python scripts/coverage_floor.py --apply             # move the floor and its echoes
    uv run python scripts/coverage_floor.py --check             # the echoes agree with fail_under
    uv run python scripts/coverage_floor.py --from-json cov.json --top 25  # per-module misses

The floor only ratchets upward: ``--apply`` refuses to lower it, because a
lower measurement means a regression to fix, not a floor to admit it.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MARGIN = 2

# The fast suite exactly as CI's coverage leg runs it (see tests.yml COV_ARGS).
FAST_SUITE_MARKERS = "not slow and not network and not meta"

# Where the floor lives: the one value pytest-cov reads.
HOME = re.compile(r"^fail_under = (?P<value>\d+)$", re.MULTILINE)

# The dated measurement in the comment above it, e.g. "(87.069% on 2026-09-12,".
DATED = re.compile(r"\((?P<measured>\d+\.\d+)% on (?P<date>\d{4}-\d{2}-\d{2}),")

# Prose that repeats the number for readers. Each pattern has one ``value``
# group; ``--apply`` rewrites it and ``--check`` asserts it equals the home.
# Keep this list short: prose that can say "the floor" without a number should.
ECHOES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("docs/contributing.md", re.compile(r"sets `fail_under = (?P<value>\d+)`")),
    ("docs/contributing.md", re.compile(r"\((?P<value>\d+) at the time of writing\)")),
)


@dataclass(frozen=True)
class Measurement:
    combined: float
    line: float
    branch: float
    statements: int
    missed_statements: int
    branches: int
    partial_branches: int

    @property
    def proposed_floor(self) -> int:
        return propose(self.combined)


def propose(combined: float) -> int:
    """The trailing-ratchet rule: measured minus two, rounded down."""
    return math.floor(combined - MARGIN)


def measurement_from_json(path: Path) -> Measurement:
    """Read the totals of a ``--cov-report=json`` file."""
    totals = json.loads(path.read_text(encoding="utf-8"))["totals"]
    branches = totals.get("num_branches", 0)
    if not branches:
        raise SystemExit(
            f"{path}: no branch data. The floor is the combined line+branch figure, "
            "so the report must come from a run with [tool.coverage.run] branch = true."
        )
    statements = totals["num_statements"]
    return Measurement(
        combined=totals["percent_covered"],
        line=100.0 * totals["covered_lines"] / statements,
        branch=100.0 * totals["covered_branches"] / branches,
        statements=statements,
        missed_statements=totals["missing_lines"],
        branches=branches,
        partial_branches=totals["num_partial_branches"],
    )


def run_fast_suite(json_path: Path) -> int:
    """Run the fast suite under pytest-cov and return pytest's exit status."""
    cmd = [
        "uv",
        "run",
        "pytest",
        "-n",
        "auto",
        "-m",
        FAST_SUITE_MARKERS,
        "--cov=geoparquet_io",
        f"--cov-report=json:{json_path}",
        "--cov-fail-under=0",
        "-p",
        "no:cacheprovider",
        "-q",
    ]
    print("$", " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=REPO_ROOT, check=False).returncode


def current_floor(pyproject_text: str) -> int:
    match = HOME.search(pyproject_text)
    if match is None:
        raise SystemExit("pyproject.toml: no `fail_under = N` line under [tool.coverage.report]")
    return int(match["value"])


def per_file_table(json_path: Path, top: int) -> str:
    """The modules with the most missed statements, for deciding where tests go."""
    files = json.loads(json_path.read_text(encoding="utf-8"))["files"]
    rows = []
    for name, data in files.items():
        s = data["summary"]
        branches = s.get("num_branches", 0)
        branch_pct = 100.0 * s.get("covered_branches", 0) / branches if branches else float("nan")
        line_pct = (
            100.0 * s["covered_lines"] / s["num_statements"] if s["num_statements"] else 100.0
        )
        rows.append((s["missing_lines"], s["num_statements"], line_pct, branch_pct, name))
    rows.sort(key=lambda r: (-r[0], r[4]))
    lines = ["missed  stmts  line%  branch%  module"]
    for missed, stmts, line_pct, branch_pct, name in rows[:top]:
        b = "   -" if math.isnan(branch_pct) else f"{branch_pct:5.1f}"
        lines.append(f"{missed:6d} {stmts:6d} {line_pct:6.1f}   {b}   {name}")
    return "\n".join(lines)


def _sub_once(text: str, pattern: re.Pattern[str], group: str, new: str, where: str) -> str:
    match = pattern.search(text)
    if match is None:
        raise SystemExit(f"{where}: pattern {pattern.pattern!r} not found")
    if pattern.search(text, match.end()) is not None:
        raise SystemExit(f"{where}: pattern {pattern.pattern!r} matches more than once")
    return text[: match.start(group)] + new + text[match.end(group) :]


def apply(floor: int, measurement: Measurement, today: dt.date, root: Path | None = None) -> None:
    """Rewrite the home value, its dated comment, and every prose echo."""
    root = root or REPO_ROOT
    pyproject = root / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    text = _sub_once(text, HOME, "value", str(floor), str(pyproject))
    text = _sub_once(text, DATED, "measured", f"{measurement.combined:.3f}", str(pyproject))
    text = _sub_once(text, DATED, "date", today.isoformat(), str(pyproject))
    pyproject.write_text(text, encoding="utf-8")
    for rel, pattern in ECHOES:
        path = root / rel
        path.write_text(
            _sub_once(path.read_text(encoding="utf-8"), pattern, "value", str(floor), str(path)),
            encoding="utf-8",
        )


def drift(root: Path | None = None) -> list[str]:
    """Every way the floor's echoes can disagree with its home; empty when consistent."""
    root = root or REPO_ROOT
    pyproject = root / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")
    floor = current_floor(text)
    problems: list[str] = []
    dated = DATED.search(text)
    if dated is None:
        problems.append(
            f"{pyproject}: the fail_under comment has no '(NN.NNN% on YYYY-MM-DD,' measurement"
        )
    elif propose(float(dated["measured"])) != floor:
        problems.append(
            f"{pyproject}: fail_under = {floor} but the comment records a measurement of "
            f"{dated['measured']}% ({dated['date']}), whose floor is {propose(float(dated['measured']))}"
        )
    for rel, pattern in ECHOES:
        path = root / rel
        match = pattern.search(path.read_text(encoding="utf-8"))
        if match is None:
            problems.append(f"{path}: pattern {pattern.pattern!r} not found")
        elif int(match["value"]) != floor:
            problems.append(f"{path}: says {match['value']}, fail_under is {floor}")
    return problems


def _report(m: Measurement, floor: int) -> None:
    print(f"combined (line+branch): {m.combined:.3f}%")
    print(
        f"line-only:              {m.line:.3f}%  ({m.statements - m.missed_statements}/{m.statements} statements)"
    )
    print(
        f"branch-only:            {m.branch:.3f}%  ({m.branches} branches, {m.partial_branches} partial)"
    )
    print(f"current fail_under:     {floor}")
    print(f"proposed fail_under:    {m.proposed_floor}  (floor({m.combined:.3f} - {MARGIN}))")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--from-json",
        type=Path,
        help="reuse an existing --cov-report=json file instead of running the suite",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="move fail_under, its dated comment and the prose echoes",
    )
    parser.add_argument(
        "--check", action="store_true", help="exit 1 if the echoes disagree with fail_under"
    )
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        metavar="N",
        help="also print the N modules with the most missed statements",
    )
    args = parser.parse_args(argv)

    if args.check:
        problems = drift()
        for problem in problems:
            print(problem, file=sys.stderr)
        if not problems:
            floor = current_floor((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
            print(f"fail_under = {floor}: home, comment and echoes agree")
        return 1 if problems else 0

    if args.from_json is not None:
        json_path = args.from_json
        status = 0
    else:
        json_path = Path(tempfile.mkdtemp(prefix="coverage-floor-")) / "coverage.json"
        status = run_fast_suite(json_path)
        print(f"pytest exit status {status}; report at {json_path}")

    measurement = measurement_from_json(json_path)
    floor = current_floor((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    _report(measurement, floor)
    if args.top:
        print()
        print(per_file_table(json_path, args.top))
    if not args.apply:
        return 0

    if status != 0:
        print(
            "refusing --apply: the run had failures, so the measurement under-reports",
            file=sys.stderr,
        )
        return 1
    if measurement.proposed_floor < floor:
        print(
            f"refusing --apply: measured {measurement.combined:.3f}% proposes {measurement.proposed_floor}, "
            f"below the current floor of {floor}. That is a regression to fix, not a floor to lower.",
            file=sys.stderr,
        )
        return 1
    apply(measurement.proposed_floor, measurement, dt.date.today())
    print(f"fail_under: {floor} -> {measurement.proposed_floor}; comment and echoes updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
