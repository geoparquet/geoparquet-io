"""Ratchets for fast collection and explicit timeout exceptions (#1402)."""

import ast
import os
import re
import subprocess
import tomllib
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
BUDGET = REPO / "tests" / "suite_budget.toml"


def collected_count(output):
    """Read pytest's selected count, including deselected-suite summaries."""
    match = re.search(r"(?:^|\n)(\d+)(?:/\d+)? tests? collected\b", output)
    if not match:
        raise AssertionError(f"No collection summary in pytest output:\n{output}")
    return int(match[1])


def assert_budget(count, maximum):
    assert count <= maximum, (
        f"Suite budget exceeded: {count} > {maximum}; reduce tests or justify raising the ratchet in the PR"
    )


def timeout_marks(root):
    """Count literal pytest.mark.timeout calls in Python files under root.

    The budget scans tests/ only, including the docs-example collector; each
    source call counts once, regardless of how many test items receive it.
    """
    count = 0
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        aliases = {"pytest": "pytest"}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                aliases.update({a.asname or a.name: a.name for a in node.names})
            elif isinstance(node, ast.ImportFrom) and node.module:
                aliases.update({a.asname or a.name: node.module + "." + a.name for a in node.names})

        def resolved(node, aliases=aliases):
            if isinstance(node, ast.Name):
                return aliases.get(node.id, node.id)
            if isinstance(node, ast.Attribute):
                return resolved(node.value) + "." + node.attr
            return ""

        # Resolve simple assigned marker aliases too (timeout = mark.timeout).
        for _ in range(len(tree.body)):
            changed = False
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    origin = resolved(node.value)
                    for target in node.targets:
                        if isinstance(target, ast.Name) and origin.startswith("pytest."):
                            if aliases.get(target.id) != origin:
                                aliases[target.id] = origin
                                changed = True
            if not changed:
                break

        count += sum(
            isinstance(n, ast.Call)
            and resolved(n.func) in {"pytest.mark.timeout", "pytest.mark.timeout.with_args"}
            for n in ast.walk(tree)
        )
    return count


def test_default_timeout_is_pinned():
    options = tomllib.loads((REPO / "pyproject.toml").read_text())["tool"]["pytest"]["ini_options"]
    assert options["timeout"] == 60
    assert "timeout" not in str(options.get("addopts", "")).lower()


def test_fast_collection_budget():
    env = os.environ.copy()
    # Drop inherited -n/verbosity/selection; retain only explicit plugin disables.
    env["PYTEST_ADDOPTS"] = " ".join(
        option
        for option in os.environ.get("PYTEST_ADDOPTS", "").split()
        if option.startswith("-pno:")
    )
    result = subprocess.run(
        [
            "uv",
            "run",
            "--no-sync",
            "python",
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
            "-p",
            "no:rerunfailures",
            "-o",
            "addopts=",
            "-m",
            "not slow and not network and not meta",
            # addopts normally excludes the corpus submodule; clearing it must
            # not accidentally collect the submodule's own independent tests.
            "--ignore=tests/data/geoparquet-testing",
            "tests",
        ],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert_budget(
        collected_count(result.stdout),
        tomllib.loads(BUDGET.read_text())["fast_suite"]["max_collected"],
    )


def test_timeout_mark_budget():
    assert_budget(
        timeout_marks(REPO / "tests"),
        tomllib.loads(BUDGET.read_text())["fast_suite"]["max_timeout_marks"],
    )


def test_budget_rejects_growth(tmp_path):
    output = tmp_path / "collection.txt"
    output.write_text("tests/test_example.py::test_one\n2 tests collected in 0.01s\n")
    assert collected_count(output.read_text()) == 2
    assert_budget(2, 2)
    with pytest.raises(AssertionError, match="2 > 1"):
        assert_budget(collected_count(output.read_text()), 1)
    with pytest.raises(AssertionError, match="No collection summary"):
        collected_count("collection failed")
    assert collected_count("\n2/3 tests collected (1 deselected) in 0.01s") == 2

    (tmp_path / "test_marks.py").write_text(
        "pytestmark = pytest.mark.timeout(600)\n"
        "@pytest.mark.timeout(120)\ndef test_pipeline():\n    pass\n"
        'description = "pytest.mark.timeout(999)"\n'
    )
    assert timeout_marks(tmp_path) == 2
    (tmp_path / "aliases.py").write_text(
        "import pytest as pt\nfrom pytest import mark as m\n"
        "a = m.timeout.with_args(90)\nb = pt.mark.timeout(80)\n"
        "timeout_alias = m.timeout\nc = timeout_alias.with_args(70)\n"
    )
    assert timeout_marks(tmp_path) == 5
    with pytest.raises(AssertionError, match="5 > 1"):
        assert_budget(timeout_marks(tmp_path), 1)
