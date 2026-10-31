"""Meta-tests for the CI wiring: the `all-checks` aggregator, the branch
ruleset that requires it, and CODEOWNERS (#1367, G10 and G11).

Before `all-checks`, the branch ruleset named 13 contexts one by one
(`lint`, `security`, `notebooks` and ten `test` cells) while
`scripts/apply_branch_protection.sh` said 14. Every job added since --
`embeddable-import`, `test-duckdb-2`, `docs-examples` -- could fail with the
merge button still green, because nobody remembered to add its context to the
ruleset. One aggregator job that `needs:` every other job, plus a ruleset that
requires only that job and `pr-title`, makes a new job a gate the moment it is
added to tests.yml, and these tests stop the aggregator's `needs:` list from
drifting away from the job list.

`pr-title` stays in its own workflow and its own required context: it must
rerun when a title is edited (`pull_request: edited`), and re-triggering the
whole suite on a title edit would let a trivially re-run `all-checks` overwrite
the real one.

These run in the fast lane: they only parse YAML and text, and the gate step
test runs a short bash script.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

PROJECT_ROOT = Path(__file__).parent.parent
WORKFLOWS = PROJECT_ROOT / ".github" / "workflows"
BRANCH_PROTECTION = PROJECT_ROOT / "scripts" / "apply_branch_protection.sh"
CODEOWNERS = PROJECT_ROOT / ".github" / "CODEOWNERS"

AGGREGATOR = "all-checks"

# Jobs whose `if:` skips them by design on some events (slow-tests runs on
# push/schedule/dispatch or a labelled PR; network-tests never runs on a PR).
# For these alone `skipped` counts as a pass; any other skipped job fails
# `all-checks`, because a required job that silently stops running is exactly
# the hole the aggregator exists to close.
SKIPPABLE_JOBS = {"slow-tests", "network-tests"}

# The only contexts the ruleset may require.
REQUIRED_CONTEXTS = ["all-checks", "pr-title"]

# CODEOWNERS paths for owner modules that a later #1367 track creates. Each
# entry must NOT exist yet; the track that creates the file deletes its entry
# here in the same commit. This list only shrinks, and ends empty.
PENDING_CODEOWNERS_PATHS: set[str] = set()

CODEOWNER = "@nlebovits"

# A line of the gate step: `check <job-id> "$<VAR>" <skip-ok|required>`.
CHECK_LINE = re.compile(r'^\s*check\s+(\S+)\s+"\$(\w+)"\s+(skip-ok|required)\s*$', re.M)


def _load_workflow(name: str) -> dict[str, Any]:
    path = WORKFLOWS / name
    assert path.exists(), f"{name} workflow missing"
    # encoding is explicit: the workflows contain em-dashes, and Windows
    # defaults to cp1252.
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture()
def tests_jobs() -> dict[str, Any]:
    return _load_workflow("tests.yml")["jobs"]


@pytest.fixture()
def aggregator(tests_jobs: dict[str, Any]) -> dict[str, Any]:
    assert AGGREGATOR in tests_jobs, (
        f"tests.yml has no `{AGGREGATOR}` job. The branch ruleset requires it; "
        "without it no PR can merge, and without the ruleset requiring it every "
        "job not named one by one in the ruleset is advisory."
    )
    return tests_jobs[AGGREGATOR]


@pytest.fixture()
def gate_step(aggregator: dict[str, Any]) -> dict[str, Any]:
    steps = aggregator.get("steps", [])
    assert len(steps) == 1, f"`{AGGREGATOR}` must have exactly one step, the result check"
    return steps[0]


def _needs(job: dict[str, Any]) -> list[str]:
    needs = job.get("needs", [])
    return [needs] if isinstance(needs, str) else list(needs)


class TestAllChecksJob:
    def test_runs_even_when_a_needed_job_failed(self, aggregator: dict[str, Any]):
        # Without always(), a failed dependency SKIPS all-checks, and GitHub
        # treats a skipped required check as passing.
        condition = str(aggregator.get("if", "")).strip()
        assert condition in ("always()", "${{ always() }}"), (
            f"`{AGGREGATOR}` must run with `if: always()`, got {condition!r}"
        )

    def test_needs_every_other_job(self, tests_jobs: dict[str, Any], aggregator: dict[str, Any]):
        needs = _needs(aggregator)
        assert len(needs) == len(set(needs)), f"duplicate entries in needs: {needs}"
        expected = set(tests_jobs) - {AGGREGATOR}
        assert set(needs) == expected, (
            f"`{AGGREGATOR}` needs must equal every other job in tests.yml. "
            f"missing: {sorted(expected - set(needs))}, "
            f"unknown: {sorted(set(needs) - expected)}"
        )

    def test_no_other_job_depends_on_another(self, tests_jobs: dict[str, Any]):
        # A `needs:` edge between two gated jobs would turn a failure upstream
        # into a `skipped` downstream, which the gate step reads as a failure
        # for most jobs but as a pass for SKIPPABLE_JOBS.
        chained = {
            job_id: _needs(job)
            for job_id, job in tests_jobs.items()
            if job_id != AGGREGATOR and _needs(job)
        }
        assert not chained, f"jobs other than `{AGGREGATOR}` declare needs: {chained}"

    def test_skippable_jobs_are_exactly_the_conditional_ones(self, tests_jobs: dict[str, Any]):
        conditional = {
            job_id for job_id, job in tests_jobs.items() if job_id != AGGREGATOR and "if" in job
        }
        assert conditional == SKIPPABLE_JOBS, (
            "The jobs allowed to be skipped must be exactly the jobs with a "
            f"job-level `if:`. conditional: {sorted(conditional)}, "
            f"allowed to skip: {sorted(SKIPPABLE_JOBS)}"
        )

    def test_gate_step_checks_every_needed_job_once(
        self, aggregator: dict[str, Any], gate_step: dict[str, Any]
    ):
        env = gate_step.get("env", {})
        script = gate_step.get("run", "")
        checks = CHECK_LINE.findall(script)
        checked_jobs = [job for job, _var, _mode in checks]
        assert sorted(checked_jobs) == sorted(_needs(aggregator)), (
            "the gate step must run `check` exactly once for every needed job; "
            f"checked: {sorted(checked_jobs)}"
        )
        for job, var, mode in checks:
            assert env.get(var, "").replace(" ", "") == f"${{{{needs.{job}.result}}}}", (
                f"check {job} reads ${var}, which must be set from needs.{job}.result"
            )
            expected_mode = "skip-ok" if job in SKIPPABLE_JOBS else "required"
            assert mode == expected_mode, f"check {job} must be {expected_mode}, got {mode}"

    @pytest.mark.skipif(
        sys.platform == "win32" or shutil.which("bash") is None,
        reason="the gate step is a bash script that runs on ubuntu-latest",
    )
    @pytest.mark.parametrize(
        ("overrides", "passes"),
        [
            ({}, True),
            ({"slow-tests": "skipped", "network-tests": "skipped"}, True),
            ({"lint": "failure"}, False),
            ({"test": "cancelled"}, False),
            ({"docs-examples": "skipped"}, False),
            ({"slow-tests": "failure"}, False),
            ({"network-tests": "cancelled"}, False),
            ({"embeddable-import": ""}, False),
        ],
    )
    def test_gate_step_verdict(
        self,
        aggregator: dict[str, Any],
        gate_step: dict[str, Any],
        tmp_path: Path,
        overrides: dict[str, str],
        passes: bool,
    ):
        """Run the gate script with the results GitHub would hand it."""
        results = dict.fromkeys(_needs(aggregator), "success")
        results.update(overrides)
        env = dict(os.environ)
        for var, expression in gate_step["env"].items():
            job = re.fullmatch(r"\$\{\{\s*needs\.([\w-]+)\.result\s*\}\}", expression).group(1)
            env[var] = results[job]
        script = tmp_path / "gate.sh"
        script.write_text(gate_step["run"], encoding="utf-8")
        proc = subprocess.run(
            ["bash", "-euo", "pipefail", str(script)],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert (proc.returncode == 0) is passes, (
            f"results {overrides or 'all success'}: exit {proc.returncode}\n{proc.stdout}{proc.stderr}"
        )


class TestTriggers:
    def test_push_runs_on_defrag_integration_branches(self):
        # #1367's tracks merge into a defrag/* integration branch, not main;
        # without this pattern those merges get no CI run at all.
        workflow = _load_workflow("tests.yml")
        # PyYAML reads the bare key `on` as the boolean True.
        triggers = workflow.get("on", workflow.get(True))
        branches = triggers["push"]["branches"]
        assert "defrag/**" in branches, f"tests.yml push branches lack defrag/**: {branches}"
        assert triggers["pull_request"]["branches"] == ["main"]


def _ruleset_contexts() -> list[str]:
    text = BRANCH_PROTECTION.read_text(encoding="utf-8")
    return re.findall(r'\{\s*context:\s*"([^"]+)"\s*\}', text)


class TestBranchRuleset:
    def test_requires_exactly_all_checks_and_pr_title(self):
        contexts = _ruleset_contexts()
        assert contexts == REQUIRED_CONTEXTS, (
            f"apply_branch_protection.sh must require exactly {REQUIRED_CONTEXTS}; "
            f"it lists {len(contexts)}: {contexts}"
        )

    def test_comments_do_not_count_a_stale_number_of_checks(self):
        text = BRANCH_PROTECTION.read_text(encoding="utf-8")
        stale = re.findall(r"\b(\d+) (?:status )?checks\b", text)
        assert all(int(n) == len(REQUIRED_CONTEXTS) for n in stale), (
            f"apply_branch_protection.sh counts checks as {stale}, "
            f"but requires {len(REQUIRED_CONTEXTS)}"
        )

    def test_pr_title_workflow_reports_the_pr_title_context(self):
        workflow = _load_workflow("pr-title.yml")
        jobs = workflow["jobs"]
        assert "pr-title" in jobs, "pr-title.yml lost its `pr-title` job"
        assert jobs["pr-title"].get("name", "pr-title") == "pr-title"
        # PyYAML reads the bare key `on` as the boolean True.
        triggers = workflow.get("on", workflow.get(True))
        assert "edited" in triggers["pull_request"]["types"], (
            "pr-title must rerun when the title is edited; that is why it is "
            "its own workflow and its own required context"
        )


def _codeowners_entries() -> list[tuple[str, list[str]]]:
    assert CODEOWNERS.exists(), ".github/CODEOWNERS missing"
    entries = []
    for line in CODEOWNERS.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        path, *owners = line.split()
        entries.append((path, owners))
    return entries


class TestCodeowners:
    def test_every_path_exists_or_is_pending(self):
        missing = [
            path
            for path, _owners in _codeowners_entries()
            if not (PROJECT_ROOT / path.lstrip("/")).exists()
            and path.lstrip("/") not in PENDING_CODEOWNERS_PATHS
        ]
        assert not missing, f"CODEOWNERS names paths that do not exist: {missing}"

    def test_pending_paths_are_still_pending(self):
        # Once a later track creates the file, its entry leaves the allowlist.
        created = sorted(p for p in PENDING_CODEOWNERS_PATHS if (PROJECT_ROOT / p).exists())
        assert not created, (
            f"these files now exist; remove them from PENDING_CODEOWNERS_PATHS: {created}"
        )
        listed = {path.lstrip("/") for path, _owners in _codeowners_entries()}
        assert PENDING_CODEOWNERS_PATHS <= listed

    def test_every_entry_is_owned(self):
        unowned = [path for path, owners in _codeowners_entries() if owners != [CODEOWNER]]
        assert not unowned, f"CODEOWNERS entries without exactly {CODEOWNER}: {unowned}"

    def test_paths_are_files_not_patterns(self):
        # One line per owner module: a glob would quietly widen ownership.
        patterns = [p for p, _owners in _codeowners_entries() if any(c in p for c in "*?[")]
        assert not patterns, f"CODEOWNERS should name files, not patterns: {patterns}"


def test_every_documented_concept_owner_has_codeowners():
    """G11 covers the final owner table, including owners added after T0a."""
    content = (PROJECT_ROOT / "CLAUDE.md").read_text(encoding="utf-8")
    table = content.split("## One owner per concept\n", 1)[1].split("\n## ", 1)[0]
    documented = set(re.findall(r"`(geoparquet_io/[^`]+\.py)`", table))
    assert documented, "the concept owner table is empty"
    listed = {path.lstrip("/") for path, _ in _codeowners_entries()}
    assert not PENDING_CODEOWNERS_PATHS, "all owners must exist at close-out"
    assert documented <= listed, f"concept owners missing CODEOWNERS: {documented - listed}"
