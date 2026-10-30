"""Guards for the end state of the common.py split (#1083).

import-linter's ``core-common-split-layers`` contract sees modules. These see
names: after stage D nothing reaches an owned name *through*
``core/common.py``, and common.py carries no compatibility re-export that
would let it start again.

One of them reads the source as text rather than as a tree, because two of the
importers stage D had to repoint were not in any tree: the subprocess programs
``tests/test_spill_strategy.py`` and ``tests/test_secondary_geometry_carriers.py``
carry as string literals. An AST walk of the test file cannot see them, and
they were fixed by hand -- which is exactly the kind of fix that is not made
the next time.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
COMMON = REPO / "geoparquet_io" / "core" / "common.py"


def _common_tree(common: Path = COMMON) -> ast.Module:
    return ast.parse(common.read_text(encoding="utf-8"))


def _module_level(body: list[ast.stmt]) -> list[ast.stmt]:
    """The module's own statements, looking through ``try``/``if``/``with``/
    loops at module level (a ``def`` under ``try: ... except ImportError`` is
    still the module's) but not into a function or class."""
    found: list[ast.stmt] = []
    for node in body:
        found.append(node)
        if isinstance(node, (ast.If, ast.For, ast.AsyncFor, ast.While)):
            found += _module_level(node.body + node.orelse)
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            found += _module_level(node.body)
        elif isinstance(node, ast.Try) or type(node).__name__ == "TryStar":
            handlers = [s for h in node.handlers for s in h.body]
            found += _module_level(node.body + handlers + node.orelse + node.finalbody)
    return found


def _names_common_defines(common: Path = COMMON) -> set[str]:
    """Names common.py binds itself: functions, classes and module constants,
    plain or annotated, including those bound under a module-level ``try`` or
    ``if``."""
    defined: set[str] = set()
    for node in _module_level(_common_tree(common).body):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                defined.update(n.id for n in ast.walk(target) if isinstance(n, ast.Name))
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


#: ``from geoparquet_io.core.common import a, b`` on one line, wherever it
#: appears -- in code, or inside a string literal holding a program.
_COMMON_IMPORT_LINE = re.compile(
    r"^[ \t]*from geoparquet_io\.core\.common import[ \t]+(?P<names>[^(\n]+)$",
    re.MULTILINE,
)


def _textual_imports_from_common() -> list[tuple[Path, int, str]]:
    """The same imports, found by reading rather than by parsing."""
    found: list[tuple[Path, int, str]] = []
    for top in ("geoparquet_io", "tests", "scripts", "docs"):
        for path in (REPO / top).rglob("*.py"):
            if path == COMMON:
                continue
            text = path.read_text(encoding="utf-8")
            for match in _COMMON_IMPORT_LINE.finditer(text):
                lineno = text.count("\n", 0, match.start()) + 1
                for name in match["names"].split(","):
                    name = name.strip().split(" as ")[0].strip()
                    if name:
                        found.append((path, lineno, name))
    return found


def test_nothing_imports_an_owned_name_through_common_in_an_embedded_program():
    """The half the AST cannot reach: a program that lives in a string.

    ``_DETERMINISM_SCRIPT`` and ``_READ_ONLY_CWD_SCRIPT`` are run in a fresh
    interpreter, so an import through ``common.py`` inside them is a real
    import that a shim deletion breaks -- and nothing else in this file, or in
    import-linter, would say so.
    """
    defined = _names_common_defines()
    routed_through = [
        f"{path.relative_to(REPO)}:{lineno} {name}"
        for path, lineno, name in _textual_imports_from_common()
        if name not in defined
    ]
    assert routed_through == [], (
        "these import a name through common.py that common.py does not define "
        "(source read as text, so string-embedded programs count too); import "
        "it from the module that does:\n  " + "\n  ".join(routed_through)
    )


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
        "get_parquet_metadata",
        "should_skip_bbox",
    }


#: The private names common.py binds today (#1367, G5). The public pin above
#: let #1211 add ``_declares_geoarrow_encoding`` here unseen. This is a ceiling,
#: not an exact pin: a name may leave (T2b is emptying the file) but none may
#: arrive. When one leaves, drop it here too so it cannot come back.
PRIVATE_NAMES_CEILING: frozenset[str] = frozenset()

#: Sizes measured on the tree G5 landed on: 789 lines (778 after T2a, 772 after
#: T2e, 704 after T2b moved version resolution to parquet_writer, 396 after T3b moved
#: the stats, bounds and dimension helpers to geo_metadata), 15 module-level
#: functions and classes (11 after T2b, 4 after T3b). These are pins, not ceilings: growth fails, and so
#: does shrinking until the number here is lowered to match, so slack left by a
#: deletion never becomes a budget for the next helper (T0b audit).
LINE_PIN = 389
DEF_PIN = 4


def _private_names(common: Path = COMMON) -> set[str]:
    return {n for n in _names_common_defines(common) if n.startswith("_")}


def new_private_names(common: Path = COMMON) -> set[str]:
    return _private_names(common) - PRIVATE_NAMES_CEILING


def gone_private_names(common: Path = COMMON) -> set[str]:
    """Pinned private names common.py no longer binds: drop them from the pin."""
    return PRIVATE_NAMES_CEILING - _private_names(common)


def size_overruns(common: Path = COMMON) -> list[str]:
    """Every way the measured size differs from the pins, either direction."""
    lines = len(common.read_text(encoding="utf-8").splitlines())
    defs = sum(
        isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        for n in _module_level(_common_tree(common).body)
    )
    overruns = []
    if lines > LINE_PIN:
        overruns.append(f"{lines} lines > pin {LINE_PIN}")
    elif lines < LINE_PIN:
        overruns.append(f"{lines} lines < pin {LINE_PIN}: lower LINE_PIN to {lines}")
    if defs > DEF_PIN:
        overruns.append(f"{defs} module-level defs > pin {DEF_PIN}")
    elif defs < DEF_PIN:
        overruns.append(f"{defs} module-level defs < pin {DEF_PIN}: lower DEF_PIN to {defs}")
    return overruns


def test_no_new_private_helper_lands_in_common():
    """#1211's ``_declares_geoarrow_encoding`` is the regrowth this catches: a
    private helper is still the grab-bag starting again. Put it in the module
    that owns its question."""
    assert new_private_names() == set()


def test_the_private_name_pin_has_no_gone_names():
    """A name deleted from common.py leaves the pin too, so the set only shrinks."""
    assert gone_private_names() == set(), "remove from PRIVATE_NAMES_CEILING"


def test_common_does_not_grow():
    assert size_overruns() == []


def test_g12_the_private_name_guard_fails_on_a_new_helper(tmp_path):
    planted = tmp_path / "common.py"
    planted.write_text(
        COMMON.read_text(encoding="utf-8") + "\n\ndef _new_helper():\n    return 1\n",
        encoding="utf-8",
    )
    assert new_private_names(planted) == {"_new_helper"}


def test_g12_the_size_ceilings_fail_on_growth(tmp_path):
    planted = tmp_path / "common.py"
    extra = "".join(f"\n\ndef _pad_{i}():\n    return {i}\n" for i in range(DEF_PIN))
    planted.write_text(COMMON.read_text(encoding="utf-8") + extra, encoding="utf-8")
    overruns = size_overruns(planted)
    assert len(overruns) == 2, overruns


def test_g12_the_size_pins_fail_on_shrinking(tmp_path):
    planted = tmp_path / "common.py"
    source = COMMON.read_text(encoding="utf-8")
    tree = ast.parse(source)
    last = [n for n in tree.body if isinstance(n, ast.FunctionDef)][-1]
    lines = source.splitlines(keepends=True)
    del lines[last.lineno - 1 - len(last.decorator_list) : last.end_lineno]
    planted.write_text("".join(lines), encoding="utf-8")
    overruns = size_overruns(planted)
    assert len(overruns) == 2 and all("lower" in o for o in overruns), overruns


def test_g12_the_private_name_pin_fails_on_a_deleted_name(monkeypatch):
    # common.py binds no private name any more, so pin one it lacks.
    monkeypatch.setitem(globals(), "PRIVATE_NAMES_CEILING", frozenset({"_gone_helper"}))
    assert gone_private_names() == {"_gone_helper"}


@pytest.mark.parametrize(
    ("label", "extra", "name"),
    [
        ("annotated constant", "\n_NEW_MAP: dict = {}\n", "_NEW_MAP"),
        (
            "def under try",
            "\ntry:\n    def _h1():\n        return 1\nexcept ImportError:\n    pass\n",
            "_h1",
        ),
        (
            "def in except",
            "\ntry:\n    pass\nexcept ImportError:\n    def _h2():\n        return 1\n",
            "_h2",
        ),
        ("def under if", "\nif True:\n    def _h3():\n        return 1\n", "_h3"),
        ("lambda assigned", "\n_f = lambda x: x\n", "_f"),
        ("tuple target", "\n_a, _b = 1, 2\n", "_a"),
    ],
)
def test_g12_the_private_name_guard_sees_nested_module_bindings(tmp_path, label, extra, name):
    planted = tmp_path / "common.py"
    planted.write_text(COMMON.read_text(encoding="utf-8") + extra, encoding="utf-8")
    assert name in new_private_names(planted), label
