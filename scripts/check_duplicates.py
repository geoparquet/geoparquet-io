#!/usr/bin/env python3
"""Fail on a new near-duplicate function pair under geoparquet_io/ (guard G3 of #1367).

Every function with at least 8 statements (counted recursively, the docstring
left out) is normalised: every ``Name`` and ``arg`` in its body is renamed
``v0``, ``v1``, ... in order of first occurrence *in the body*, so two
functions that differ only in their identifiers read the same. The signature
(parameter names, annotations, defaults) and the decorators are not part of
the body and do not shift the numbering: a copy with an annotation or an extra
leading parameter still reads the same. Attributes and constants are kept. The
bodies are compared as token sequences with ``difflib.SequenceMatcher``
(``autojunk=False``), and a pair at or above 0.90 is a near-duplicate. Two
upper bounds of the ratio prune first, so most pairs never reach the matcher:
the body lengths (``real_quick_ratio``: only bodies within a length window of
each other are paired at all) and token counts per class (a coarse
``quick_ratio``); a pair either bound puts below the floor cannot reach it.

The pairs that exist today are listed in ``scripts/duplicate_pairs_baseline.txt``
as ``fileA:qualnameA|fileB:qualnameB`` (``#`` starts a comment). A pair that is
not in the baseline fails the check: call the existing function instead of
copying it. A baseline pair that has gone only warns; ``--update`` drops it, so
the baseline can only fall. ``--update`` never adds a pair.

Standard library only, so the pre-commit hook needs nothing installed. Method
from the #1367 diagnosis scan (Codex Sol, ``dup_scan.py``).
"""

from __future__ import annotations

import argparse
import ast
import bisect
import difflib
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

MIN_STATEMENTS = 8
FLOOR = 0.90
SCAN_ROOT = "geoparquet_io"
DEFAULT_BASELINE = "scripts/duplicate_pairs_baseline.txt"
_TOKEN = re.compile(r"\w+|[^\w\s]", re.UNICODE)
_FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef)


def _without_docstring(body: list[ast.stmt]) -> list[ast.stmt]:
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        return body[1:]
    return body


def _rename(body: list[ast.stmt]) -> list[tuple[ast.AST, str, str]]:
    """Rename every ``Name`` and ``arg`` in *body* in place, ``v0``, ``v1``, ...
    by first occurrence in AST field order (a pre-order walk, as
    ``ast.NodeVisitor`` would visit them). Returns what to put back."""
    names: dict[str, str] = {}
    renamed: list[tuple[ast.AST, str, str]] = []
    stack: list[ast.AST] = list(reversed(body))
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Name):
            renamed.append((node, "id", node.id))
            node.id = names.setdefault(node.id, f"v{len(names)}")
        elif isinstance(node, ast.arg):
            renamed.append((node, "arg", node.arg))
            node.arg = names.setdefault(node.arg, f"v{len(names)}")
        stack.extend(reversed(list(ast.iter_child_nodes(node))))
    return renamed


def normalized_tokens(func: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    """The body's tokens with identifiers renamed by first occurrence in the
    body. Nothing from the signature seeds the numbering: numbering parameter
    annotations and defaults first let one annotation shift every later name
    and hid byte-identical twins (T0c audit). The names are renamed in place
    and put back (a deep copy of every function cost more than the scan)."""
    body = _without_docstring(func.body)
    renamed = _rename(body)
    try:
        return _TOKEN.findall(ast.unparse(ast.Module(body=body, type_ignores=[])))
    finally:
        for node, field, name in reversed(renamed):
            setattr(node, field, name)


def statement_count(func: ast.FunctionDef | ast.AsyncFunctionDef) -> int:
    """Statements in the body, nested ones included. An expression never holds
    a statement, so the count does not descend into one."""
    count = 0
    stack: list[ast.AST] = list(_without_docstring(func.body))
    while stack:
        node = stack.pop()
        count += isinstance(node, ast.stmt)
        stack.extend(c for c in ast.iter_child_nodes(node) if not isinstance(c, ast.expr))
    return count


@dataclass(frozen=True)
class Function:
    path: str  # POSIX path from the repo root
    qualname: str
    lineno: int
    statements: int
    tokens: tuple[str, ...]

    @property
    def key(self) -> str:
        return f"{self.path}:{self.qualname}"


@dataclass(frozen=True)
class Pair:
    a: Function
    b: Function
    ratio: float

    @property
    def key(self) -> str:
        return pair_key(self.a.key, self.b.key)


def pair_key(a: str, b: str) -> str:
    """Stable ``fileA:qualA|fileB:qualB``, the two sides in sorted order."""
    first, second = sorted((a, b))
    return f"{first}|{second}"


def collect(repo: Path, minimum: int = MIN_STATEMENTS) -> list[Function]:
    """Every function (methods and nested functions too) with *minimum* statements."""
    found: list[Function] = []
    for source in sorted((repo / SCAN_ROOT).rglob("*.py")):
        rel = source.relative_to(repo).as_posix()
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=rel)

        def walk(node: ast.AST, scope: list[str], rel: str = rel) -> None:
            for child in ast.iter_child_nodes(node):
                if isinstance(child, _FUNCTIONS):
                    qualified = [*scope, child.name]
                    count = statement_count(child)
                    if count >= minimum:
                        found.append(
                            Function(
                                rel,
                                ".".join(qualified),
                                child.lineno,
                                count,
                                tuple(normalized_tokens(child)),
                            )
                        )
                    walk(child, qualified)
                elif isinstance(child, ast.ClassDef):
                    walk(child, [*scope, child.name])
                elif not isinstance(child, ast.expr):  # no def is ever inside an expression
                    walk(child, scope)

        walk(tree, [])
    return found


#: How many of the most frequent tokens get a class of their own in each count
#: profile (the rest share one): a coarse one that is cheap to compare, then a
#: fine one for the pairs the coarse one lets through.
_COARSE_CLASSES = 7
_FINE_CLASSES = 63


def _profiles(token_lists: list[list[str]], classes: int) -> list[tuple[int, ...]]:
    """Each body's token counts per class: the most frequent tokens one class
    each, every other token one more. Matching tokens share a class, so the sum
    of per-class minimums of two bodies bounds the multiset intersection
    ``quick_ratio`` counts."""
    totals = Counter(t for tokens in token_lists for t in tokens)
    ranked = sorted(totals, key=lambda t: (-totals[t], t))[:classes]
    index = {t: i for i, t in enumerate(ranked)}
    other = len(ranked)
    profiles = []
    for tokens in token_lists:
        counts = [0] * (other + 1)
        for t in tokens:
            counts[index.get(t, other)] += 1
        profiles.append(tuple(counts))
    return profiles


def _length_window(length: int, floor: float) -> tuple[float, float]:
    """The other body's lengths whose ``real_quick_ratio`` can reach *floor*
    (``2 * min / (a + b) >= floor``), widened a little; the exact test follows."""
    if floor <= 0:
        return float("-inf"), float("inf")
    if floor >= 2:
        return float("inf"), float("-inf")
    return length * floor / (2 - floor) - 1, length * (2 - floor) / floor + 1


def scan(functions: list[Function], floor: float = FLOOR) -> list[Pair]:
    """Every pair at or above *floor*, highest ratio first.

    Same pairs and ratios as comparing every pair with ``SequenceMatcher``
    (earlier function as the first sequence); a pair is skipped only when an
    upper bound of its ratio is below *floor*.
    """
    tokens = [list(f.tokens) for f in functions]
    lengths = [len(t) for t in tokens]
    coarse = _profiles(tokens, _COARSE_CLASSES)
    fine = _profiles(tokens, _FINE_CLASSES)
    by_length = sorted(range(len(functions)), key=lengths.__getitem__)
    sorted_lengths = [lengths[i] for i in by_length]
    pairs = []
    for j, right in enumerate(functions):
        lb = lengths[j]
        low, high = _length_window(lb, floor)
        window = by_length[
            bisect.bisect_left(sorted_lengths, low) : bisect.bisect_right(sorted_lengths, high)
        ]
        matcher = None
        for i in sorted(i for i in window if i < j):
            total = lengths[i] + lb
            if (2.0 * min(lengths[i], lb) / total if total else 1.0) < floor:
                continue  # real_quick_ratio
            if total and (
                2.0 * sum(map(min, coarse[i], coarse[j])) / total < floor
                or 2.0 * sum(map(min, fine[i], fine[j])) / total < floor
            ):
                continue  # each at or above quick_ratio
            if matcher is None:
                # autojunk=False: the default junks tokens frequent in a long body and
                # lowered real pairs below the floor (T0c audit).
                matcher = difflib.SequenceMatcher(None, [], tokens[j], autojunk=False)
            matcher.set_seq1(tokens[i])
            if matcher.quick_ratio() < floor:
                continue
            ratio = matcher.ratio()
            if ratio >= floor:
                pairs.append(Pair(functions[i], right, ratio))
    return sorted(pairs, key=lambda p: (-p.ratio, p.key))


def read_baseline(path: Path) -> list[str]:
    if not path.exists():
        return []
    keys = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            keys.append(line)
    return keys


def drop_from_baseline(path: Path, gone: set[str]) -> None:
    """Rewrite *path* without the *gone* pairs, keeping comments and order."""
    kept = [
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.split("#", 1)[0].strip() not in gone
    ]
    path.write_text("\n".join(kept) + "\n", encoding="utf-8")


def _location(f: Function) -> str:
    return f"{f.path}:{f.lineno} {f.qualname} ({f.statements} stmts)"


def write_json(path: Path, pairs: list[Pair]) -> None:
    rows = [
        {
            "key": p.key,
            "ratio": round(p.ratio, 8),
            "a": {"path": p.a.path, "qualname": p.a.qualname, "lineno": p.a.lineno},
            "b": {"path": p.b.path, "qualname": p.b.qualname, "lineno": p.b.lineno},
        }
        for p in pairs
    ]
    path.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")


def write_md(path: Path, functions: list[Function], pairs: list[Pair], baseline: set[str]) -> None:
    lines = [
        "# Near-duplicate functions (guard G3)",
        "",
        f"{len(functions)} functions with at least {MIN_STATEMENTS} statements; "
        f"{len(pairs)} pairs at or above {FLOOR:.2f}.",
        "",
        "| Ratio | A | B | In baseline |",
        "| ---: | --- | --- | --- |",
    ]
    lines += [
        f"| {p.ratio:.4f} | {_location(p.a)} | {_location(p.b)} | "
        f"{'yes' if p.key in baseline else '**no**'} |"
        for p in pairs
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="tree to scan")
    parser.add_argument(
        "--baseline",
        type=Path,
        default=None,
        help=f"baseline file (default: <repo>/{DEFAULT_BASELINE}); missing means empty",
    )
    parser.add_argument("--update", action="store_true", help="drop gone pairs from the baseline")
    parser.add_argument("--json", type=Path, help="write the pairs as JSON")
    parser.add_argument("--md", type=Path, help="write the pairs as a Markdown table")
    args = parser.parse_args(argv)

    repo = args.repo.resolve()
    baseline_file = args.baseline or repo / DEFAULT_BASELINE
    functions = collect(repo)
    pairs = scan(functions)
    baseline = read_baseline(baseline_file)
    found = {p.key for p in pairs}
    new = [p for p in pairs if p.key not in set(baseline)]
    gone = sorted(set(baseline) - found)

    if args.json:
        write_json(args.json, pairs)
    if args.md:
        write_md(args.md, functions, pairs, set(baseline))
    for p in new:
        print(
            f"near-duplicate ({p.ratio:.2f}): {_location(p.a)} and {_location(p.b)}; "
            "call the existing function instead of copying it\n"
            f"  key: {p.key}"
        )
    if gone and args.update and new:
        print("not updating the baseline: this run found new pairs")
    elif gone and args.update:
        drop_from_baseline(baseline_file, set(gone))
        print(f"dropped {len(gone)} gone pair(s) from {baseline_file.name}")
    elif gone:
        for key in gone:
            print(f"warning: baseline pair gone (run with --update to drop it): {key}")
    return 1 if new else 0


if __name__ == "__main__":
    sys.exit(main())
