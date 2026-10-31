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

A leading underscore does not take a name out of its family
(``_is_geographic_crs`` is an ``is_geographic*``), and ``[G2.known_copies]``
lists, per glob, the renamed copies no glob can see.

The copies that exist today are listed under ``[[G2.exemptions]]`` (``path``,
``name``, optional ``count`` of definitions, ``reason``). An entry whose count
differs from the tree fails the check: a stale one (0) so the list shrinks as
copies are deleted, and an extra one so a second copy under an exempted name
is still caught.

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
import tomllib
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path

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
    return tomllib.loads(exemptions_file.read_text(encoding="utf-8")).get("G2", {})


def _owners(table: dict) -> dict[str, tuple[str, ...]]:
    owners = table.get("owners", {})
    return {
        glob: (owner,) if isinstance(owner, str) else tuple(owner) for glob, owner in owners.items()
    }


def _known_copies(table: dict) -> dict[str, frozenset[str]]:
    """``[G2.known_copies]``: glob -> exact names that answer the same question
    under a name the glob cannot see (``_detect_version_from_table`` for
    ``resolve_*version*``)."""
    return {glob: frozenset(names) for glob, names in table.get("known_copies", {}).items()}


def matches(name: str, glob: str, known: frozenset[str] = frozenset()) -> bool:
    """*name* is in *glob*'s family: by the glob, by the glob with a leading
    underscore either side ignored (``_is_geographic_crs`` is an
    ``is_geographic*``, ``get_query_columns`` a ``_get_query_columns``), or by
    being listed as a known copy."""
    return (
        name in known or fnmatchcase(name, glob) or fnmatchcase(name.lstrip("_"), glob.lstrip("_"))
    )


def owned_definitions(
    repo: Path,
    owners: dict[str, tuple[str, ...]],
    known: dict[str, frozenset[str]] | None = None,
) -> list[Definition]:
    """Every ``def`` under ``geoparquet_io/`` whose name is in an owner family."""
    known = known or {}
    found: list[Definition] = []
    for source in sorted((repo / SCAN_ROOT).rglob("*.py")):
        rel = source.relative_to(repo).as_posix()
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=rel)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            patterns = tuple(g for g in owners if matches(node.name, g, known.get(g, frozenset())))
            if patterns:
                found.append(Definition(rel, node.name, node.lineno, patterns))
    return found


def violations(
    repo: Path,
    owners: dict[str, tuple[str, ...]],
    known: dict[str, frozenset[str]] | None = None,
) -> list[Definition]:
    """Definitions that live outside every owner of every glob they match."""
    return [
        d
        for d in owned_definitions(repo, owners, known)
        if not any(d.path in owners[glob] for glob in d.patterns)
    ]


def apply_exemptions(
    found: list[Definition], exemptions: list[dict]
) -> tuple[list[Definition], list[dict]]:
    """Split *found* into the unexempted, and return the exemptions whose
    ``count`` (default 1) is not the number of definitions they cover: a stale
    entry covers none, and a second copy under an exempted name is one too many."""
    keys = {(e["path"], e["name"]) for e in exemptions}
    unexempted = [d for d in found if (d.path, d.name) not in keys]
    miscounted = [
        e
        for e in exemptions
        if sum((d.path, d.name) == (e["path"], e["name"]) for d in found) != e.get("count", 1)
    ]
    return unexempted, miscounted


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

    found = violations(repo, owners, _known_copies(table))
    exemptions = [] if args.no_exemptions else table.get("exemptions", [])
    unexempted, stale = apply_exemptions(found, exemptions)

    for d in unexempted:
        print(
            f"{d.path}:{d.lineno}: def {d.name} matches {', '.join(d.patterns)}; "
            f"only {_owner_hint(d, owners)} may define it"
        )
    for e in stale:
        have = sum((d.path, d.name) == (e["path"], e["name"]) for d in found)
        print(
            f"{exemptions_file.name}: [[G2.exemptions]] entry {e['path']} {e['name']} "
            f"says count={e.get('count', 1)}, the tree has {have}"
            + ("; stale: delete the entry" if have == 0 else "")
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
