"""Detect identical test bodies with the same execution context (#1402)."""

import ast
import json
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _dump(node):
    return ast.dump(node, include_attributes=False)


def _marks(nodes):
    marks = []
    for node in nodes:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(
                isinstance(target, ast.Name) and target.id == "pytestmark" for target in targets
            ):
                marks.append(_dump(node.value))
    return tuple(sorted(marks))


def _decorators(node):
    return tuple(
        sorted(
            _dump(mark)
            for mark in node.decorator_list
            if ast.unparse(mark).startswith(("pytest.mark.", "pytest.fixture"))
        )
    )


def duplicate_groups(root):
    """Return duplicate node IDs without importing or executing tests."""
    groups = defaultdict(list)

    def visit(nodes, path, classes, marks):
        for node in nodes:
            if isinstance(node, ast.ClassDef):
                visit(
                    node.body,
                    path,
                    (*classes, node.name),
                    (*marks, _decorators(node), _marks(node.body)),
                )
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and (
                node.name.startswith("test_")
                or any(
                    ast.unparse(d).split("(")[0] == "pytest.fixture" for d in node.decorator_list
                )
            ):
                body = node.body
                if (
                    body
                    and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)
                ):
                    body = body[1:]
                args = node.args
                parameters = tuple(
                    arg.arg for arg in [*args.posonlyargs, *args.args, *args.kwonlyargs]
                )
                parameters += (
                    args.vararg.arg if args.vararg else None,
                    args.kwarg.arg if args.kwarg else None,
                )
                key = (
                    tuple(map(_dump, body)),
                    marks,
                    _decorators(node),
                    isinstance(node, ast.AsyncFunctionDef),
                    parameters,
                )
                groups[key].append("::".join((path, *classes, node.name)))

    for path in sorted(root.rglob("*.py")):
        if "geoparquet-testing" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        visit(tree.body, path.relative_to(root).as_posix(), (), (_marks(tree.body),))
    return sorted(sorted(members) for members in groups.values() if len(members) > 1)


def test_no_duplicate_test_bodies():
    groups = duplicate_groups(REPO / "tests")
    baseline = json.loads((REPO / "tests/fixture_duplicate_baseline.json").read_text())
    assert groups == baseline, "Duplicate test bodies:\n" + "\n".join(
        " = ".join(group) for group in groups
    )


def test_pre_hygiene_duplicates(tmp_path):
    for path in (REPO / "tests" / "fixtures" / "suite_budget").glob("*.txt"):
        (tmp_path / ("test_" + path.stem + ".py")).write_text(path.read_text())
    assert duplicate_groups(tmp_path) == [
        [
            "test_common_utils.py::TestExtractCrsIdentifier::test_projjson_with_id",
            "test_crs_utils.py::TestExtractCrsIdentifier::test_projjson_dict_with_id",
        ],
        [
            "test_common_utils.py::TestExtractCrsIdentifier::test_projjson_without_id_returns_none",
            "test_crs_utils.py::TestExtractCrsIdentifier::test_dict_without_id_returns_none",
        ],
    ]


def test_duplicate_execution_context(tmp_path):
    first = tmp_path / "test_first.py"
    second = tmp_path / "test_second.py"
    first.write_text('def test_first(value):\n    "first docstring"\n    pass\n')
    second.write_text('def test_second(value):\n    "different docstring"\n    pass\n')
    assert duplicate_groups(tmp_path) == [
        ["test_first.py::test_first", "test_second.py::test_second"]
    ]
    for source in (
        "def test_second(other):\n    pass\n",
        "async def test_second(value):\n    pass\n",
        "@pytest.mark.slow\ndef test_second(value):\n    pass\n",
        "pytestmark = pytest.mark.network\ndef test_second(value):\n    pass\n",
        "@pytest.mark.slow\nclass TestOuter:\n    class TestInner:\n        def test_second(value):\n            pass\n",
        "class TestOuter:\n    pytestmark = pytest.mark.network\n    def test_second(value):\n        pass\n",
    ):
        second.write_text(source)
        assert duplicate_groups(tmp_path) == [], source


def test_duplicate_fixtures_are_scanned(tmp_path):
    (tmp_path / "conftest.py").write_text(
        "@pytest.fixture\ndef first(value):\n    return value\n"
        "@pytest.fixture\ndef second(value):\n    return value\n"
    )
    assert duplicate_groups(tmp_path) == [["conftest.py::first", "conftest.py::second"]]
