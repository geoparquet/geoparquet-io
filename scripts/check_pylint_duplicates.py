#!/usr/bin/env python3
"""Run pylint's duplicate-code check (R0801) and hold it to the pinned pairs (guard G3).

pylint is configured in pyproject.toml to run R0801 alone. The blocks it
reports today are pinned in scripts/owner_exemptions.toml under
``[[G3.pylint]]`` (``block = "modA:funcA|modB:funcB"``, optional ``count``,
``reason``). Each side is the module and the function (or class, or
``<module>``) the similar lines start in, so swapping one copy for another
between the same two modules is a different block and fails.
pylint exits non-zero while any remain, so this wrapper decides instead: it
fails when a pair is reported that is not pinned, or when a pinned pair's
report count differs from the tree (a removed copy needs its entry lowered or
deleted, so the list only falls). Editing ``# pylint: disable`` into
geoparquet_io/ is not how a pair goes away.
"""

from __future__ import annotations

import argparse
import ast
import functools
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
_SIDE = re.compile(r"^==([\w.]+):\[(\d+):(\d+)\]", re.M)
PREFIX = "geoparquet_io."


@functools.cache
def _defs(repo: Path, module: str) -> tuple[tuple[int, int, str], ...]:
    """``(start, end, qualname)`` of every def and class in *module*'s file."""
    base = repo / Path(*module.split("."))
    path = base.with_suffix(".py") if base.with_suffix(".py").is_file() else base / "__init__.py"
    if not path.is_file():
        return ()
    found = []

    def walk(node: ast.AST, scope: list[str]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                qual = [*scope, child.name]
                found.append((child.lineno, child.end_lineno or child.lineno, ".".join(qual)))
                walk(child, qual)

    walk(ast.parse(path.read_text(encoding="utf-8")), [])
    return tuple(found)


def _enclosing(repo: Path | None, module: str, start: int) -> str:
    """The innermost def or class holding the block's first line (pylint
    reports 0-based starts), or ``<module>``."""
    line = start + 1
    holders = [d for d in (_defs(repo, module) if repo else ()) if d[0] <= line <= d[1]]
    return max(holders, key=lambda d: d[0])[2] if holders else "<module>"


def parse_report(output: str, repo: Path | None = None) -> Counter[str]:
    """``"modA:funcA|modB:funcB"`` (sorted, package prefix dropped) -> report count."""
    blocks: Counter[str] = Counter()
    for block in _REPORT.split(output):
        if "R0801" not in block:
            continue
        sides = sorted(
            {
                f"{m.removeprefix(PREFIX)}:{_enclosing(repo, m, int(a))}"
                for m, a, _ in _SIDE.findall(block)
            }
        )
        blocks["|".join(sides)] += 1
    return blocks


def pinned(exemptions: Path) -> Counter[str]:
    with exemptions.open("rb") as fh:
        entries = tomllib.load(fh).get("G3", {}).get("pylint", [])
    return Counter({e["block"]: e.get("count", 1) for e in entries})


def run_pylint(repo: Path, target: str) -> str:
    # Pylint picks representative pairs from overlapping copies in input order.
    # Filesystem traversal order differs between checkouts and platforms.
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pylint",
            "--jobs=1",
            "--min-similarity-lines=8",
            *(
                [
                    str(p.relative_to(repo))
                    for p in sorted((repo / target).rglob("*.py"), reverse=True)
                ]
                if (repo / target).is_dir()
                else [target]
            ),
        ],
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
                f"call the existing code instead of copying it\n  block: {key}"
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
    problems = differences(parse_report(output, repo), pinned(exemptions))
    for line in problems:
        print(line)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
