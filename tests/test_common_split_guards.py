"""Guards for the end state of the common.py split (#1083).

import-linter's ``core-common-split-layers`` contract sees modules. These two
see names: after stage D nothing reaches an owned name *through*
``core/common.py``, and common.py carries no compatibility re-export that
would let it start again.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
COMMON = REPO / "geoparquet_io" / "core" / "common.py"


def _common_tree() -> ast.Module:
    return ast.parse(COMMON.read_text(encoding="utf-8"))


def _names_common_defines() -> set[str]:
    """Names common.py binds itself: functions, classes and module constants."""
    defined: set[str] = set()
    for node in _common_tree().body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        elif isinstance(node, ast.Assign):
            defined.update(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            defined.add(node.target.id)
    return defined


def _imports_from_common() -> list[tuple[Path, int, str]]:
    """Every ``from geoparquet_io.core.common import NAME`` outside common.py."""
    found: list[tuple[Path, int, str]] = []
    for top in ("geoparquet_io", "tests", "scripts", "docs"):
        for path in (REPO / top).rglob("*.py"):
            if path == COMMON:
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module == "geoparquet_io.core.common":
                    found.extend((path, node.lineno, alias.name) for alias in node.names)
    return found


def test_common_has_no_re_export_shims():
    """``import X as X`` was the shim form (#1010); stage D deleted the last one."""
    shims = [
        alias.name
        for node in _common_tree().body
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("geoparquet_io")
        for alias in node.names
        if alias.asname == alias.name
    ]
    assert shims == [], f"common.py re-exports {shims}; import these from their owner"


def test_nothing_imports_an_owned_name_through_common():
    """A name common.py only *imports* has an owner; go to the owner.

    This is what kept the split from finishing: common.py imported
    ``get_duckdb_connection`` for its own use and 28 test files picked it up
    from there, so the module could never be emptied.
    """
    defined = _names_common_defines()
    assert defined, "common.py defines nothing? the walk is broken"
    routed_through = [
        f"{path.relative_to(REPO)}:{lineno} {name}"
        for path, lineno, name in _imports_from_common()
        if name not in defined
    ]
    assert routed_through == [], (
        "these import a name through common.py that common.py does not define; "
        "import it from the module that does:\n  " + "\n  ".join(routed_through)
    )


def test_the_residue_is_the_named_stage_c_list():
    """What is left in common.py is #1010's stage-C table and nothing else.

    A new public helper landing here is the grab-bag starting again. Move it
    to a module named for the question it answers, or add it to a resident's
    future home, and only then extend this list.
    """
    public = {n for n in _names_common_defines() if not n.startswith("_")}
    assert public == {
        "add_bbox",
        "add_computed_column",
        "calculate_file_bounds",
        "compute_geometry_dimensions_via_sql",
        "compute_geometry_types_via_sql",
        "get_dataset_bounds",
        "get_parquet_metadata",
        "resolve_geoparquet_version_from_file",
        "resolve_geoparquet_version_from_table",
        "should_skip_bbox",
        "split_zm_suffix",
        "zm_suffix_sql",
    }
