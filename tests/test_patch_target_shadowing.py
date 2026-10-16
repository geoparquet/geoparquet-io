"""Forbid dotted ``mock.patch`` targets that walk through ``geoparquet_io.cli``.

``geoparquet_io/__init__.py`` resolves the name ``cli`` lazily (PEP 562, #1307)
to the Click ``Group`` object, shadowing the ``geoparquet_io.cli``
**subpackage** as an attribute of its own parent — exactly as the old eager
``from geoparquet_io.cli.main import cli`` did. In a fresh interpreter::

    >>> import geoparquet_io
    >>> type(geoparquet_io.cli)
    <class 'click.core.Group'>
    >>> hasattr(geoparquet_io.cli, "_shared")
    False

Since #1307 the shadowing is lazier: a process that imports the subpackage
directly (``import geoparquet_io.cli.main``) *before* anything resolves the
``cli`` attribute sees the subpackage there instead. That makes the hazard
below rarer, not gone — which order a given process hits is exactly the kind
of thing that differs between test runs.

``mock.patch`` resolves a dotted string target by importing the longest prefix
it can and then walking the rest with ``getattr``. When the walk reaches
``geoparquet_io.cli`` it finds the ``Group``, and the next step fails::

    AttributeError: 'Group' object has no attribute '_shared'

Whether that happens depends on how much of the target ``mock`` manages to
import before it starts walking, which differs across Python versions -- so a
test written this way can pass locally and on most of the CI matrix while
failing on exactly one interpreter. It has cost this repo three times now
(``test_write_memory_forwarding.py`` and ``test_check_cli_guards.py`` both
carry warning comments about it; ``test_shared_group_context.py`` hit it on
Python 3.10 only, after passing on 3.11, 3.12 and 3.13).

``mock.patch.object(module, "name")`` takes the module object directly, so there
is no name to walk and no version-dependent behaviour. Import the module and use
that instead.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.meta

TESTS_DIR = Path(__file__).parent

#: Any patch target starting with this prefix walks through the shadowed name.
SHADOWED_PREFIX = "geoparquet_io.cli."

_PATCH_FUNCS = {"patch", "patch.object", "mock.patch"}


def _patch_string_targets(tree: ast.AST):
    """Yield ``(lineno, target)`` for every ``patch("...")``-style call."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue

        func = node.func
        # `patch(...)`, `mock.patch(...)`, `unittest.mock.patch(...)` -- but NOT
        # `patch.object(...)`, which takes a module object as its first argument.
        if isinstance(func, ast.Name):
            name = func.id
        elif isinstance(func, ast.Attribute):
            name = func.attr
        else:
            continue
        if name != "patch":
            continue

        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            yield node.lineno, first.value


def test_no_dotted_patch_target_walks_through_the_cli_package():
    offenders = []

    for path in sorted(TESTS_DIR.rglob("test_*.py")):
        if path.name == Path(__file__).name:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for lineno, target in _patch_string_targets(tree):
            if target.startswith(SHADOWED_PREFIX):
                offenders.append(f"{path.relative_to(TESTS_DIR.parent)}:{lineno}: {target}")

    assert not offenders, (
        "These dotted patch targets walk through `geoparquet_io.cli`, which "
        "`geoparquet_io/__init__.py` rebinds to a Click Group. They resolve on "
        "some Python versions and raise `AttributeError: 'Group' object has no "
        "attribute ...` on others.\n\n"
        + "\n".join(offenders)
        + '\n\nImport the module and use `mock.patch.object(module, "name")`.'
    )


def test_the_shadowing_this_guard_exists_for_is_real():
    """If the shadowing ever goes away, this guard can go with it.

    Since the package root went lazy (#1307) the shadowing fires only when the
    ``cli`` attribute is resolved before the subpackage is imported directly.
    This pytest process imported ``geoparquet_io.cli.main`` long ago, so the
    proof needs a fresh interpreter.
    """
    import subprocess
    import sys

    code = (
        "import click\n"
        "import geoparquet_io\n"
        "assert isinstance(geoparquet_io.cli, click.Group), type(geoparquet_io.cli)\n"
        "import geoparquet_io.cli\n"
        "assert isinstance(geoparquet_io.cli, click.Group), type(geoparquet_io.cli)\n"
        "print('shadowed')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0 and result.stdout.strip() == "shadowed", (
        "`geoparquet_io.cli` no longer resolves to the Click Group, so the "
        "patch-target hazard this module guards may be gone. Re-check before "
        "deleting: the guard is cheap and the failure mode is version-specific.\n"
        f"{result.stderr}"
    )
