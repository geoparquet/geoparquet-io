# ADR-0008: The Coverage Floor Is a Trailing Ratchet on the Combined Figure

## Status

Accepted

## Context

The project enforces two coverage gates in CI: a whole-suite floor and a 90%
gate on the lines a pull request changes. Both were shaped by a sequence of
problems rather than designed once, and the reasoning was spread over a
`pyproject.toml` comment, a workflow comment, a test docstring and issue
#1018. ADR-0005 carried the floor's number in its text and was edited at
every ratchet (`27611fa`, `299d4fc`, #827, #1038), against this directory's
rule that accepted ADRs are not edited.

What the problems were:

- **A stale floor.** The floor was set once and left; by #1018 it sat 6.6
  points under the measured number, so a regression of six points would have
  passed. The previous ratchet (#827) also left a dated comment that no longer
  described the number next to it.
- **Line coverage accepted half-tested branches.** With line-only coverage a
  new `if` whose else-arm never ran was 100% covered on its changed lines, so
  the diff-cover gate accepted it. The measured line figure was 88.6% while
  the branch figure was 79.9%, with 1,195 partial branches (#1018 §2).
- **The number lived in three places.** `fail_under`, a `--cov-fail-under`
  flag in the workflow, and a value pinned by a test had to move together, and
  the PR that moved them missed two prose sites. An explicit flag silently
  overrides the config value, so the copies were not a cross-check.
- **Nine of ten matrix jobs instrumented coverage and discarded it**, which
  obscured where the gate lived and slowed every job (tests.yml comment).
- **Partial local runs failed on a whole-suite gate.** With coverage flags in
  `addopts`, a single-file run exited 1 against a floor it could never clear,
  and single-file runs were 39-49% slower.

## Decision

1. **Branch coverage is on** (`[tool.coverage.run] branch = true`), so every
   coverage figure the project quotes or gates on is coverage.py's *combined*
   line+branch figure. It is several points under the line figure; the floor
   is re-measured, never converted from a line percentage.

2. **The floor is a trailing ratchet**: the measured full-fast-suite combined
   figure (`-m "not slow and not network and not meta"`) minus two, rounded
   down. Two points is the margin for run-to-run noise. It is raised when the
   measured number moves up and never lowered to admit a regression.

3. **The floor has one home**, `[tool.coverage.report] fail_under` in
   `pyproject.toml`, with the measurement and its date in the comment above
   it. pytest-cov reads it on the coverage leg because that leg passes no
   `--cov-fail-under`; `tests/test_coverage_job.py` asserts that stays so, and
   that `branch = true` stays on. Prose that repeats the number is limited to
   the sites listed in `scripts/coverage_floor.py`, which `--apply` rewrites
   and the fast suite checks for drift.

4. **The ratchet is one command**: `scripts/coverage_floor.py` runs the fast
   suite under pytest-cov, prints the combined, line-only and branch-only
   figures and the proposed floor, and with `--apply` moves the home value,
   the dated comment and the prose echoes together. It refuses to lower the
   floor and refuses to apply a measurement from a run that had failures,
   because timed-out subprocess tests under-report.

5. **One CI leg measures.** A single job-level `COVERAGE_JOB` flag in
   `tests.yml` selects one matrix combination; that leg alone instruments,
   enforces the floor, uploads to Codecov and runs the diff-cover gate. Every
   other leg passes `--no-cov`. Local runs are uninstrumented; a partial
   `--cov` run adds `--cov-fail-under=0` because the config floor re-arms on
   any `--cov` run.

6. **Changed lines are gated at 90% by the fast suite**, with
   `--branch-coverage` so a partial branch on a changed line counts as
   uncovered. New code ships with tests that run on every PR, not only in the
   post-merge slow lane.

## Consequences

### Positive
- A regression of more than the two-point margin fails CI; a floor that has
  fallen behind the measured number is visible as a proposed ratchet.
- A new `if` with only one arm exercised no longer passes the diff-cover gate.
- Moving the floor is one command with no prose to remember, and the fast
  suite catches a hand edit that moved one site and not the rest.
- ADR-0005 no longer carries a number, so it stops being edited.

### Negative
- The combined figure is lower than the line figure users of other projects
  expect; the numbers are not comparable across projects without saying which
  one is quoted.
- `--branch-coverage` on the changed-lines gate is stricter than anything
  measured before it: a legitimate defensive branch on a changed line needs a
  test for its other arm. If that proves too strict, the threshold or the flag
  is the knob, not the tests (#1047).
- The measurement depends on an idle machine: subprocess tests time out under
  load and the floor script refuses a red run, so the ratchet cannot be run
  from a busy session.

### Neutral
- The floor moves only when someone runs the script; it is not raised by CI.
- Codecov still receives the report from the one measuring leg, as
  information, not as a gate.

## Alternatives Considered

### A line-only floor
Cheaper to reason about and the number most projects quote. Rejected because
it is what let the 8.7-point line/branch gap open: the diff-cover gate on a
line-only report accepted every half-tested branch, and a line-only floor is
a looser gate wearing the same number (`test_branch_coverage_stays_on`).

### Setting the floor at the measured number
Catches every regression, but flakes on run-to-run noise from test ordering
and timed-out subprocess tests; each flake would then be "fixed" by lowering
the floor. The two-point margin is the price of a floor that is never lowered.

### The floor as a `--cov-fail-under` flag in the workflow
The first version had it in three places (flag, config, a test constant). An
explicit flag silently wins over `fail_under`, so the config value everybody
reads locally becomes a lie the moment they drift, and the drift is invisible
to CI. One home, asserted by a test, replaces the copies.

### Measuring on every matrix leg
Instrumenting nine jobs to discard the result did not speed the matrix
measurably (5926s vs 5980s wall, n=1) and hid which leg's number the floor
referred to. One flag selecting one leg is what `test_coverage_job.py` pins.

### Raising the floor automatically in CI
Would ratchet on whichever run happened to be highest, including a run where
a flaky test's extra retries covered more lines. The floor moves when a person
runs the measurement and reads its failures.

## References

- Issue #1018 (measured survey and the ratchet schedule), #1038 (branch
  coverage, combined floor, one home, `--branch-coverage`), #1047 (floor
  script and this ADR), #827 (the previous ratchet)
- `pyproject.toml`: `[tool.coverage.run]`, `[tool.coverage.report]`
- `.github/workflows/tests.yml`: `COVERAGE_JOB`, `COV_ARGS`, "Diff coverage gate"
- `scripts/coverage_floor.py`, `tests/test_coverage_floor.py`,
  `tests/test_coverage_job.py`
- `CLAUDE.md` "Testing", `docs/contributing.md` "Coverage gates"
- ADR-0005 (test fixture strategy; its coverage bullet now points here)
