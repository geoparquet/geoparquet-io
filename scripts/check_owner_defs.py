#!/usr/bin/env python3
"""Owner-only definitions (guard G2 of #1367).

A function whose name says which concept it answers -- ``is_geographic*``,
``resolve_*version*``, ``parse_size*``, ``atomic_write`` ... -- may be defined
only in the module that owns that concept. A second definition elsewhere is how
gpio grew four is-geographic tests, six version resolvers, three covering
builders and three ``parse_bbox`` copies that each answered the same question
slightly differently (#1367, "Copies that exist on main today").

The name-to-owner table is ``[G2.owners]`` in ``scripts/owner_exemptions.toml``:
each key is a ``fnmatch`` glob over the bare ``def`` name, each value the list
of modules (paths from the repo root) allowed to define a matching name. Every
``def`` in ``geoparquet_io/`` -- module level, nested, or a method -- is
checked; a definition is allowed when its module owns *any* glob it matches.

The copies that exist today are listed one by one under ``[[G2.exemptions]]``
(``path``, ``name``, ``reason``). An exemption that no longer matches a
definition is stale and fails the check too, so the list can only shrink as the
copies are deleted.

This grew out of the ``forbid-bespoke-schema-reconciliation`` hook (now
``owner-only-defs``, which runs this script), which was a
``grep`` for ``def _(align|unify|reconcile)*schema`` in ``core/``. That rule is
the three ``_align*schema`` / ``_unify*schema`` / ``_reconcile*schema`` rows of
the table. It is an AST walk rather than a grep so that a comment, a string or
a docstring naming a function is never mistaken for a definition of one.

Usage::

    uv run python scripts/check_owner_defs.py                   # check this tree
    uv run python scripts/check_owner_defs.py --repo ../base \\
        --exemptions scripts/owner_exemptions.toml --no-exemptions   # count a tree
"""

from __future__ import annotations

import argparse
import ast
import sys
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EXEMPTIONS = Path("scripts") / "owner_exemptions.toml"
SCAN_ROOT = "geoparquet_io"


@dataclass(frozen=True)
class Definition:
    path: str  # POSIX path from the repo root
    name: str
    lineno: int
    patterns: tuple[str, ...]  # the owner globs the name matches


def load_table(exemptions_file: Path) -> dict:
    with exemptions_file.open("rb") as fh:
        return tomllib.load(fh).get("G2", {})


def _owners(table: dict) -> dict[str, tuple[str, ...]]:
    owners = table.get("owners", {})
    return {
        glob: (owner,) if isinstance(owner, str) else tuple(owner) for glob, owner in owners.items()
    }


def owned_definitions(repo: Path, owners: dict[str, tuple[str, ...]]) -> list[Definition]:
    """Every ``def`` under ``geoparquet_io/`` whose name matches an owner glob."""
    found: list[Definition] = []
    for source in sorted((repo / SCAN_ROOT).rglob("*.py")):
        rel = source.relative_to(repo).as_posix()
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=rel)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            patterns = tuple(g for g in owners if fnmatchcase(node.name, g))
            if patterns:
                found.append(Definition(rel, node.name, node.lineno, patterns))
    return found


def violations(repo: Path, owners: dict[str, tuple[str, ...]]) -> list[Definition]:
    """Definitions that live outside every owner of every glob they match."""
    return [
        d
        for d in owned_definitions(repo, owners)
        if not any(d.path in owners[glob] for glob in d.patterns)
    ]


def apply_exemptions(
    found: list[Definition], exemptions: list[dict]
) -> tuple[list[Definition], list[dict]]:
    """Split *found* into the unexempted, and return the exemptions that matched nothing."""
    keys = {(e["path"], e["name"]) for e in exemptions}
    unexempted = [d for d in found if (d.path, d.name) not in keys]
    hit = {(d.path, d.name) for d in found}
    stale = [e for e in exemptions if (e["path"], e["name"]) not in hit]
    return unexempted, stale


def _owner_hint(d: Definition, owners: dict[str, tuple[str, ...]]) -> str:
    homes = sorted({o for glob in d.patterns for o in owners[glob]})
    return ", ".join(homes)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="tree to scan")
    parser.add_argument(
        "--exemptions",
        type=Path,
        default=None,
        help="exemptions file (default: <repo>/scripts/owner_exemptions.toml)",
    )
    parser.add_argument(
        "--no-exemptions",
        action="store_true",
        help="ignore [[G2.exemptions]] and report every definition outside its owner",
    )
    args = parser.parse_args(argv)
    repo = args.repo.resolve()
    exemptions_file = args.exemptions or repo / DEFAULT_EXEMPTIONS
    table = load_table(exemptions_file)
    owners = _owners(table)
    if not owners:
        print(f"ERROR: no [G2.owners] table in {exemptions_file}")
        return 2

    found = violations(repo, owners)
    exemptions = [] if args.no_exemptions else table.get("exemptions", [])
    unexempted, stale = apply_exemptions(found, exemptions)

    for d in unexempted:
        print(
            f"{d.path}:{d.lineno}: def {d.name} matches {', '.join(d.patterns)}; "
            f"only {_owner_hint(d, owners)} may define it"
        )
    for e in stale:
        print(
            f"{exemptions_file.name}: stale [[G2.exemptions]] entry "
            f"{e['path']} {e['name']}: no such definition any more; delete the entry"
        )
    if args.no_exemptions:
        print(f"{len(found)} definition(s) outside their owner")
    if unexempted or stale:
        if unexempted:
            print(
                "ERROR: define these in their owner module and import them from there "
                "(table: [G2.owners] in scripts/owner_exemptions.toml)."
            )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
