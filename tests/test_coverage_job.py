"""Meta-tests for the coverage-reporting leg of the `test` matrix in tests.yml.

Exactly one matrix combination reports coverage: it is the only job that
measures lines at all, the only one that uploads to Codecov, the only one that
enforces the 85% floor, and the only one that runs the 90% diff-cover gate.

Four separate places key off that decision (checkout depth, the pytest flag
list, the Codecov upload, the diff-cover gate). While each re-derived
`matrix.os == ... && matrix.python-version == ...` for itself, moving the
baseline -- dropping 3.11, adding 3.14 -- turned all four false simultaneously:
no coverage measured, nothing uploaded, and BOTH gates silently gone with every
required check green. The decision therefore lives in ONE job-level env flag,
and these tests keep it that way.
"""

import itertools
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

PROJECT_ROOT = Path(__file__).parent.parent

# The coverage floor, repeated in three places that must agree: this pin,
# `[tool.coverage.report] fail_under` in pyproject.toml, and `--cov-fail-under`
# inside COV_ARGS in tests.yml. It is a trailing ratchet — the measured
# full-fast-suite number minus two, rounded down — and since
# `[tool.coverage.run] branch = true` that measured number is the COMBINED
# line+branch figure (87.069% on 2026-09-12), not the line figure.
FLOOR = 85

# The job-level env var that decides which matrix leg reports coverage.
COVERAGE_FLAG = "COVERAGE_JOB"

# How the four consumers must refer to that decision.
FLAG_REFERENCE = f"env.{COVERAGE_FLAG}"


@pytest.fixture()
def tests_workflow() -> dict[str, Any]:
    """Load and return the parsed tests.yml workflow."""
    workflow_path = PROJECT_ROOT / ".github" / "workflows" / "tests.yml"
    assert workflow_path.exists(), "tests.yml workflow missing"
    # encoding is explicit: tests.yml contains em-dashes, and Windows defaults to
    # cp1252, which raises UnicodeDecodeError on the first non-ASCII byte.
    with open(workflow_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture()
def test_job(tests_workflow: dict[str, Any]) -> dict[str, Any]:
    """Return the `test` job (the fast-test matrix)."""
    assert "test" in tests_workflow["jobs"], "tests.yml lost its `test` job"
    return tests_workflow["jobs"]["test"]


@pytest.fixture()
def coverage_expression(test_job: dict[str, Any]) -> str:
    """Return the job-level expression that selects the coverage leg."""
    job_env = test_job.get("env", {})
    assert COVERAGE_FLAG in job_env, (
        f"The `test` job must define a job-level `{COVERAGE_FLAG}` env flag. It is "
        "the single place that decides which matrix leg measures coverage; "
        "without it, four steps re-derive the baseline independently and a "
        "version bump can delete both coverage gates while CI stays green."
    )
    return job_env[COVERAGE_FLAG]


def _matrix_combinations(matrix: dict[str, Any]) -> list[dict[str, str]]:
    """Expand a matrix into its combinations, applying `exclude`."""
    dimensions = {k: v for k, v in matrix.items() if k not in ("include", "exclude")}
    combos = [
        dict(zip(dimensions, values, strict=True))
        for values in itertools.product(*dimensions.values())
    ]
    for excluded in matrix.get("exclude", []):
        combos = [c for c in combos if not all(c.get(k) == v for k, v in excluded.items())]
    return combos


def _selected_combination(expression: str) -> dict[str, str]:
    """Parse `matrix.<key> == '<value>'` pairs out of the coverage expression."""
    pairs = re.findall(r"matrix\.([A-Za-z0-9_-]+)\s*==\s*'([^']*)'", expression)
    assert pairs, (
        f"Could not find any `matrix.<key> == '<value>'` comparison in the "
        f"{COVERAGE_FLAG} expression: {expression!r}"
    )
    keys = [key for key, _ in pairs]
    assert len(keys) == len(set(keys)), (
        f"{COVERAGE_FLAG} compares the same matrix key twice, so it can never "
        f"select a real combination: {expression!r}"
    )
    return dict(pairs)


def _steps_text(job: dict[str, Any]) -> str:
    """Serialize a job's steps back to YAML for substring assertions."""
    return yaml.safe_dump(job["steps"])


def _step_by_name(job: dict[str, Any], fragment: str) -> dict[str, Any]:
    matches = [s for s in job["steps"] if fragment.lower() in str(s.get("name", "")).lower()]
    assert len(matches) == 1, f"Expected exactly one step matching {fragment!r}, got {len(matches)}"
    return matches[0]


class TestCoverageLegIsSingleSourced:
    """The coverage decision is made once and consumed four times."""

    def test_flag_selects_exactly_one_combination(
        self, test_job: dict[str, Any], coverage_expression: str
    ):
        """The flag must pin every matrix dimension, i.e. name one combination."""
        matrix = test_job["strategy"]["matrix"]
        dimensions = {k for k in matrix if k not in ("include", "exclude")}
        selected = _selected_combination(coverage_expression)

        assert set(selected) == dimensions, (
            f"{COVERAGE_FLAG} constrains {sorted(selected)} but the matrix has "
            f"dimensions {sorted(dimensions)}. Leaving one unconstrained would "
            "enable coverage on several legs at once (duplicate Codecov uploads, "
            "duplicate diff-cover runs); over-constraining names a nonexistent key."
        )

    def test_selected_combination_survives_the_excludes(
        self, test_job: dict[str, Any], coverage_expression: str
    ):
        """The named combination must actually be scheduled as a job."""
        matrix = test_job["strategy"]["matrix"]
        combos = _matrix_combinations(matrix)
        selected = _selected_combination(coverage_expression)

        assert selected in combos, (
            f"{COVERAGE_FLAG} points at {selected}, which is not one of the "
            f"{len(combos)} combinations this matrix actually schedules "
            f"({combos}). No job would measure coverage, so the {FLOOR}% floor and "
            "the 90% diff-cover gate would both vanish with all checks green."
        )

    def test_matrix_has_no_include_entries(self, test_job: dict[str, Any]):
        """`include:` is banned here: it renames the generated job.

        A key contributed by `include` is appended to the auto-generated job
        name, so `test (ubuntu-latest, 3.11)` would be reported as
        `test (ubuntu-latest, 3.11, true)` and the required status check would
        never arrive. That is why the coverage flag is a job-level `env`.
        """
        matrix = test_job["strategy"]["matrix"]
        assert "include" not in matrix, (
            "The `test` matrix must not use `include:`. An include-contributed "
            "key is appended to the default job name, which changes the required "
            "status-check context. Use the job-level env flag instead."
        )

    def test_checkout_depth_uses_the_flag(self, test_job: dict[str, Any]):
        """Full history is fetched only for the diff-cover leg."""
        checkout = next(
            s for s in test_job["steps"] if "actions/checkout" in str(s.get("uses", ""))
        )
        fetch_depth = str(checkout["with"]["fetch-depth"])
        assert FLAG_REFERENCE in fetch_depth, (
            f"checkout fetch-depth must key off {FLAG_REFERENCE}, got {fetch_depth!r}"
        )

    def test_pytest_flags_use_the_flag(self, test_job: dict[str, Any]):
        """COV_ARGS is derived from the flag, not from a re-derived condition."""
        step = _step_by_name(test_job, "Run fast tests")
        cov_args = step["env"]["COV_ARGS"]
        assert FLAG_REFERENCE in cov_args, (
            f"COV_ARGS must key off {FLAG_REFERENCE}, got {cov_args!r}"
        )

    def test_codecov_upload_uses_the_flag(self, test_job: dict[str, Any]):
        step = _step_by_name(test_job, "Upload coverage to Codecov")
        assert FLAG_REFERENCE in str(step["if"]), (
            f"The Codecov upload must key off {FLAG_REFERENCE}, got {step['if']!r}"
        )

    def test_diff_cover_gate_uses_the_flag(self, test_job: dict[str, Any]):
        step = _step_by_name(test_job, "Diff coverage gate")
        assert FLAG_REFERENCE in str(step["if"]), (
            f"The diff-cover gate must key off {FLAG_REFERENCE}, got {step['if']!r}"
        )

    def test_no_step_re_derives_the_coverage_condition(self, test_job: dict[str, Any]):
        """Nothing below the job level may compare matrix values itself.

        This is the regression guard: the bug was four independent copies of
        the same condition, not any one of them being wrong.
        """
        steps = _steps_text(test_job)
        comparisons = (
            "matrix.os ==",
            "matrix.os !=",
            "matrix.python-version ==",
            "matrix.python-version !=",
        )
        for forbidden in comparisons:
            assert forbidden not in steps, (
                f"A step re-derives the coverage condition ({forbidden!r}). Key "
                f"off {FLAG_REFERENCE} instead so there is one place to change."
            )


class TestCoverageLegStillEnforcesTheGates:
    """The one leg that measures coverage still carries every gate."""

    def test_coverage_leg_passes_the_floor_and_reports(self, test_job: dict[str, Any]):
        """`addopts` no longer carries coverage, so these flags are the gate."""
        step = _step_by_name(test_job, "Run fast tests")
        cov_args = step["env"]["COV_ARGS"]
        for flag in ("--cov=geoparquet_io", "--cov-report=xml", f"--cov-fail-under={FLOOR}"):
            assert flag in cov_args, (
                f"{flag} missing from COV_ARGS. Coverage flags are no longer in "
                "pyproject `addopts`, so this env var is the only thing enforcing "
                "the floor and producing coverage.xml for Codecov/diff-cover."
            )

    def test_non_coverage_legs_opt_out_explicitly(self, test_job: dict[str, Any]):
        """The other legs must pass --no-cov, not an empty string."""
        step = _step_by_name(test_job, "Run fast tests")
        assert "--no-cov" in step["env"]["COV_ARGS"], (
            "Non-reporting matrix legs must pass --no-cov explicitly"
        )

    def test_diff_cover_threshold_unchanged(self, test_job: dict[str, Any]):
        step = _step_by_name(test_job, "Diff coverage gate")
        assert "--fail-under=90" in step["run"], "diff-cover must gate changed lines at 90%"
