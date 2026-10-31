"""Guard CI lane policy for the integration marker (#667, #1402).

Integration tests run in the fast lane unless also marked slow/network (meta
checks have their own lane). Whole-pipeline checks in test_journeys,
test_pipe_integration, and test_geoparquet_corpus are slow by policy (#1402),
keeping the fast matrix within its twenty-minute budget. The existing fast,
slow, and network selections route the tests. This lightweight wiring guard
stays in the fast lane.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from tests import test_geoparquet_corpus, test_pipe_integration
from tests.e2e import test_journeys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = PROJECT_ROOT / ".github" / "workflows" / "tests.yml"
PYPROJECT = PROJECT_ROOT / "pyproject.toml"

LANE_MARKERS = {"slow", "network", "meta", "integration", "corpus"}

# `-m "a and not b"`, `-m 'a and not b'`, or a bare `-m network`.
_SELECTION = re.compile(r"-m\s+(?:\"([^\"]+)\"|'([^']+)'|(?!-)(\S+))")


def _selections() -> list[str]:
    """Distinct pytest marker selections used by tests.yml.

    Deduplicated: each job repeats its selection once per OS branch (xdist
    flags differ), and those repeats are the same lane.
    """
    text = WORKFLOW.read_text(encoding="utf-8")
    found = list(
        dict.fromkeys(double or single or bare for double, single, bare in _SELECTION.findall(text))
    )
    assert found, "tests.yml has no `-m` marker selections at all"
    return found


_ALLOWED_NODES = (
    ast.Expression,
    ast.BoolOp,
    ast.UnaryOp,
    ast.And,
    ast.Or,
    ast.Not,
    ast.Name,
    ast.Load,
)


def selects(expression: str, marks: set[str]) -> bool:
    """Evaluate a pytest ``-m`` expression against a set of marker names.

    Walks the parsed expression instead of calling ``eval``: marker expressions
    are only names combined with and/or/not, so a tiny evaluator covers them,
    and nothing in this guard can execute whatever a future edit puts in
    tests.yml. Anything outside that grammar raises rather than being guessed at.
    """
    tree = ast.parse(expression, mode="eval")
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ValueError(f"unsupported marker expression {expression!r}: {type(node).__name__}")

    def evaluate(node: ast.AST) -> bool:
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Name):
            return node.id in marks
        if isinstance(node, ast.UnaryOp):  # only `not`, per _ALLOWED_NODES
            return not evaluate(node.operand)
        if isinstance(node, ast.BoolOp):
            results = [evaluate(value) for value in node.values]
            return all(results) if isinstance(node.op, ast.And) else any(results)
        raise ValueError(f"unsupported node {type(node).__name__}")  # pragma: no cover

    return evaluate(tree)


def _marks_of(func) -> set[str]:
    marks = {mark.name for mark in getattr(test_journeys, "pytestmark", [])}
    marks |= {mark.name for mark in getattr(func, "pytestmark", [])}
    return marks & LANE_MARKERS


def _journeys() -> dict[str, set[str]]:
    return {
        name: _marks_of(getattr(test_journeys, name))
        for name in dir(test_journeys)
        if name.startswith("test_")
    }


def test_selection_evaluator_is_not_vacuous():
    """The evaluator this module reasons with must actually discriminate."""
    assert selects("not slow and not network and not meta", {"integration"})
    assert not selects("not slow and not network and not meta", {"integration", "slow"})
    assert selects("(slow or meta) and not network", {"slow"})
    assert not selects("(slow or meta) and not network", {"slow", "network"})
    assert selects("network", {"network"})


def test_every_journey_carries_the_integration_marker():
    journeys = _journeys()
    assert len(journeys) >= 10, f"expected the ten journeys plus guards, got {journeys}"
    for name, marks in journeys.items():
        assert "integration" in marks, f"{name} is not integration-marked"


def test_no_ci_selection_excludes_integration_tests():
    """No job may deselect the marker; otherwise the journeys run nowhere."""
    for expression in _selections():
        assert "integration" not in expression, (
            f"tests.yml selection {expression!r} mentions `integration`. The journeys are "
            "meant to ride the existing lanes; naming the marker here (especially as "
            "`not integration`) would silently strand them."
        )


def test_each_journey_lands_in_exactly_one_ci_job():
    """Every journey is selected by exactly one of the fast / slow / network jobs.

    Scope caveat: this counts marker SELECTIONS, not whether the job that owns
    the selection actually gates a PR. The network job is `continue-on-error`
    and does not run on pull requests, and the slow job runs on merge/schedule
    or behind the `run-slow-tests` label -- so "selected" is not "blocking".
    Adding `network` to a journey moves it from a blocking lane to an advisory
    one while this test stays green (that is why journey 8 is `slow` only).
    """
    selections = _selections()
    for name, marks in _journeys().items():
        matched = [expression for expression in selections if selects(expression, marks)]
        assert len(matched) == 1, (
            f"{name} (marks={sorted(marks)}) is selected by {len(matched)} CI jobs: {matched}"
        )


@pytest.mark.parametrize("module", [test_journeys, test_pipe_integration, test_geoparquet_corpus])
def test_whole_pipeline_modules_run_in_slow_lane(module):
    """Whole-pipeline checks are slow to bound the fast matrix (#1402)."""
    marks = module.pytestmark
    if not isinstance(marks, list):
        marks = [marks]
    names = {mark.name for mark in marks}
    assert "slow" in names, f"{module.__name__} must remain slow (#1402)"
    assert not selects("not slow and not network and not meta", names)
    assert selects("(slow or meta) and not network", names)


def test_plain_integration_tests_still_run_in_fast_lane():
    assert "not slow and not network and not meta" in _selections()
    assert selects("not slow and not network and not meta", {"integration"})


def test_marker_description_documents_the_lane():
    """`--strict-markers` is on; the description is where the lane is documented."""
    description = None
    for line in PYPROJECT.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith('"integration:'):
            description = line
            break
    assert description is not None, "pyproject.toml lost its `integration` marker"
    assert "fast suite" in description, (
        "the `integration` marker description must say which lane it rides, "
        f"got: {description.strip()}"
    )


@pytest.mark.parametrize("path", [WORKFLOW, PYPROJECT])
def test_guarded_files_exist(path: Path):
    assert path.exists(), f"{path} is missing; this guard would silently pass"
