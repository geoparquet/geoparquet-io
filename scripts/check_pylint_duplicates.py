#!/usr/bin/env python3
"""Run pylint's duplicate-code check (R0801) and hold it to the pinned pairs (guard G3).

pylint is configured in pyproject.toml to run R0801 alone. The module pairs it
reports similar lines between today are pinned in scripts/owner_exemptions.toml
under ``[[G3.pylint]]`` (``modules = "a|b"``, optional ``count``, ``reason``).
pylint exits non-zero while any remain, so this wrapper decides instead: it
fails when a pair is reported that is not pinned, or when a pinned pair's
report count differs from the tree (a removed copy needs its entry lowered or
deleted, so the list only falls). Editing ``# pylint: disable`` into
geoparquet_io/ is not how a pair goes away.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib  # type: ignore[no-redef]

_REPORT = re.compile(r"\n(?=\S+: R0801:)")
_MODULE = re.compile(r"^==([\w.]+):\[\d+:\d+\]", re.M)
PREFIX = "geoparquet_io."


def parse_report(output: str) -> Counter[str]:
    """``"a|b"`` (sorted module names, package prefix dropped) -> report count."""
    pairs: Counter[str] = Counter()
    for block in _REPORT.split(output):
        if "R0801" not in block:
            continue
        modules = sorted({m.removeprefix(PREFIX) for m in _MODULE.findall(block)})
        pairs["|".join(modules)] += 1
    return pairs


def pinned(exemptions: Path) -> Counter[str]:
    with exemptions.open("rb") as fh:
        entries = tomllib.load(fh).get("G3", {}).get("pylint", [])
    return Counter({e["modules"]: e.get("count", 1) for e in entries})


def run_pylint(repo: Path, target: str) -> str:
    result = subprocess.run(
        [sys.executable, "-m", "pylint", target],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode & 1 or result.returncode & 32:  # fatal or usage error
        sys.stderr.write(result.stdout + result.stderr)
        raise SystemExit(f"pylint failed to run (exit {result.returncode})")
    return result.stdout


def differences(found: Counter[str], expected: Counter[str]) -> list[str]:
    problems = []
    for key in sorted(set(found) | set(expected)):
        have, want = found.get(key, 0), expected.get(key, 0)
        if want == 0:
            problems.append(
                f"new duplicate code between {key.replace('|', ' and ')} ({have} report(s)): "
                "call the existing code instead of copying it"
            )
        elif have != want:
            problems.append(
                f"[[G3.pylint]] {key}: pinned count={want}, pylint reports {have}"
                + ("; stale: delete the entry" if have == 0 else "; update the count")
            )
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--target", default="geoparquet_io", help="what pylint scans")
    parser.add_argument("--exemptions", type=Path, default=None)
    parser.add_argument(
        "--report", type=Path, default=None, help="read a saved pylint report instead of running it"
    )
    args = parser.parse_args(argv)
    repo = args.repo.resolve()
    output = (
        args.report.read_text(encoding="utf-8") if args.report else run_pylint(repo, args.target)
    )
    exemptions = args.exemptions or repo / "scripts" / "owner_exemptions.toml"
    problems = differences(parse_report(output), pinned(exemptions))
    for line in problems:
        print(line)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
