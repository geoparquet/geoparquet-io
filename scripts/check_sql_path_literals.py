#!/usr/bin/env python3
"""No hand-written ``FROM '{path}'`` SQL interpolations.

Writing the quotes by hand is what let ``gpio add admin-divisions`` interpolate
an unescaped CLI argument and die with a ``ParserException`` on an output path
containing an apostrophe -- after it had already written the file (issue #718).
The fix is :func:`geoparquet_io.core.duckdb_utils.sql_path`, which returns a
complete, quoted, escaped literal::

    FROM {sql_path(path)}          # good -- one escape, no hand-written quotes
    FROM '{path}'                  # bad  -- the escape is the author's problem

This started as a **ratchet**: 118 existing sites across 34 files were correct
but hand-written (they interpolated an already-escaped ``safe_file_url``
result), and rewriting them in one go would have been an unreviewable diff, so
each file's count was recorded in the baseline and the check failed only when a
count went up. Issue #802 finished the migration, so the baseline is now empty
and the mechanism is a flat ban: any occurrence at all fails. The per-file
machinery stays because a stalled migration is the normal way this file gets
reused, and ``--update`` still records one -- but a non-empty baseline is a
deliberate exception that needs saying out loud in review, not a default.

Only real call sites are counted: comments and docstrings are skipped, so a
file's baseline is not a budget that a reworded docstring can free up for a
genuine new violation.

Usage::

    uv run python scripts/check_sql_path_literals.py            # check
    uv run python scripts/check_sql_path_literals.py --update   # rewrite baseline
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BASELINE = REPO_ROOT / "scripts" / "sql_path_literals_baseline.txt"
SCAN_ROOTS = ("geoparquet_io", "plugins")

# A file path interpolated straight into a SQL string literal: the opening quote
# and the ``{`` are adjacent, so the author -- not a helper -- owns the escape.
PATTERN = re.compile(
    r"""(?ix)
    (?:
        \b (?: FROM | TO | ATTACH ) \s
      | \b (?: read_parquet
             | read_csv(?:_auto)?
             | read_json(?:_auto)?
             | read_ndjson(?:_auto|_objects)?
             | read_text
             | read_blob
             | parquet_scan
             | ST_Read(?:_Meta)?
             | parquet_schema
             | parquet_metadata
             | parquet_file_metadata
             | parquet_kv_metadata
             | glob
          ) \(
    )
    '\{
    """
)


def source_files() -> list[Path]:
    files: list[Path] = []
    for root in SCAN_ROOTS:
        files.extend(sorted((REPO_ROOT / root).rglob("*.py")))
    return files


def docstring_lines(text: str) -> set[int]:
    """Line numbers covered by a docstring.

    Prose is not a call site, and counting it made each file's baseline a budget
    that documentation could free up: shortening the docstring that quotes the
    bad form would pay for a real new interpolation with no change in the count.
    """
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return set()

    covered: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Expr):
            continue
        value = node.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            covered.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
    return covered


def count_in(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    skip = docstring_lines(text)
    return sum(
        1
        for lineno, line in enumerate(text.splitlines(), start=1)
        if lineno not in skip and not line.lstrip().startswith("#") and PATTERN.search(line)
    )


def scan() -> dict[str, int]:
    counts = {}
    for path in source_files():
        n = count_in(path)
        if n:
            counts[path.relative_to(REPO_ROOT).as_posix()] = n
    return counts


def read_baseline() -> dict[str, int]:
    if not BASELINE.exists():
        return {}
    baseline = {}
    for line in BASELINE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        count, _, rel = line.partition(" ")
        baseline[rel.strip()] = int(count)
    return baseline


def write_baseline(counts: dict[str, int]) -> None:
    header = (
        "# Hand-written `FROM '{path}'` SQL interpolations per file (issues #718, #802).\n"
        "# EMPTY ON PURPOSE: the migration finished in #802, so any occurrence now\n"
        "# fails. Use sql_path() from geoparquet_io/core/duckdb_utils.py instead.\n"
        "# Adding a line back here exempts a file -- say why in review; do not run\n"
        "# `--update` just to get a red check green.\n"
    )
    body = "".join(f"{n} {rel}\n" for rel, n in sorted(counts.items()))
    BASELINE.write_text(header + body, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update", action="store_true", help="rewrite the baseline")
    args = parser.parse_args()

    counts = scan()
    if args.update:
        write_baseline(counts)
        print(f"Wrote {BASELINE.relative_to(REPO_ROOT)} ({sum(counts.values())} occurrences).")
        return 0

    baseline = read_baseline()
    regressions = {
        rel: (baseline.get(rel, 0), n) for rel, n in counts.items() if n > baseline.get(rel, 0)
    }
    if not regressions:
        return 0

    print("ERROR: new hand-written SQL path interpolation.")
    print("       Use sql_path(path) from core/duckdb_utils.py:")
    print("           FROM {sql_path(path)}      instead of      FROM '{path}'")
    print("       It quotes and escapes in one step, so the escape cannot be forgotten.")
    print("       Pass it the RAW path, never one already escaped -- escaping is not")
    print("       idempotent, so an escaped path is escaped twice (#718).")
    print()
    for rel, (was, now) in sorted(regressions.items()):
        print(f"  {rel}: {was} -> {now}")
    print()
    print("       The baseline is empty on purpose (#802 migrated all 118 old sites).")
    print("       `--update` would exempt the file above rather than fix it, so reach")
    print("       for it only when a reviewer has agreed the site cannot migrate.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
