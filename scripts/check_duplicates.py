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
(``autojunk=False``; ``real_quick_ratio`` and ``quick_ratio`` prefilter), and a
pair at or above 0.90 is a near-duplicate.

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
import copy
import difflib
import json
import re
import sys
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


class _Rename(ast.NodeTransformer):
    """Rename identifiers by first occurrence in the body, in AST field order."""

    def __init__(self) -> None:
        self.names: dict[str, str] = {}

    def identifier(self, name: str) -> str:
        return self.names.setdefault(name, f"v{len(self.names)}")

    def visit_Name(self, node: ast.Name) -> ast.AST:
        node.id = self.identifier(node.id)
        return self.generic_visit(node)

    def visit_arg(self, node: ast.arg) -> ast.AST:
        node.arg = self.identifier(node.arg)
        return self.generic_visit(node)


def normalized_tokens(func: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    """The body's tokens with identifiers renamed by first occurrence in the
    body. Nothing from the signature seeds the numbering: numbering parameter
    annotations and defaults first let one annotation shift every later name
    and hid byte-identical twins (T0c audit)."""
    func = copy.deepcopy(func)
    body = _without_docstring(func.body)
    rename = _Rename()
    for statement in body:
        rename.visit(statement)
    return _TOKEN.findall(ast.unparse(ast.Module(body=body, type_ignores=[])))


def statement_count(func: ast.FunctionDef | ast.AsyncFunctionDef) -> int:
    return sum(isinstance(n, ast.stmt) for s in _without_docstring(func.body) for n in ast.walk(s))


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
                else:
                    walk(child, scope)

        walk(tree, [])
    return found


def scan(functions: list[Function], floor: float = FLOOR) -> list[Pair]:
    """Every pair at or above *floor*, highest ratio first."""
    pairs = []
    for j, right in enumerate(functions):
        # autojunk=False: the default junks tokens frequent in a long body and
        # lowered real pairs below the floor (T0c audit).
        matcher = difflib.SequenceMatcher(None, [], list(right.tokens), autojunk=False)
        for left in functions[:j]:
            matcher.set_seq1(list(left.tokens))
            if matcher.real_quick_ratio() < floor or matcher.quick_ratio() < floor:
                continue
            ratio = matcher.ratio()
            if ratio >= floor:
                pairs.append(Pair(left, right, ratio))
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
