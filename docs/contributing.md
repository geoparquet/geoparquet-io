# Contributing

Thanks for contributing! This guide explains how we work, what a finished
pull request looks like, and how to get set up. The short version: send a
PR, test it against real data, get it reviewed hard, and let CI do its job.

## How we work

**Prefer pull requests to issues.** If you can describe a problem clearly,
you can usually also open a PR that fixes it. Open an issue instead only
when the change genuinely needs discussion first (a design decision, a
breaking change, a new dependency) or when you can't attempt the fix
yourself.

**Start from a reproducible example.** A failing test that demonstrates a
bug is a better bug report than any amount of prose. If you hit a bug, try
to capture it as a small test case first. From there, an AI coding
assistant (Claude, Copilot, or similar) can often carry you the rest of the
way to a working PR — we encourage that. AI-assisted PRs are welcome here;
they're held to the same bar as any other PR, and the CI is built so we can
trust them.

**The CI is strict on purpose.** Every gate described below exists so that
maintainers don't have to manually re-verify each change. Strict, automated
checks are what make it safe for us to accept fast, autonomous
contributions — including ones written largely by AI. The guardrails aren't
there to slow you down; they're what let us say yes quickly.

## What a finished PR looks like

Before you ask for a merge, check that your PR clears this bar:

- [ ] **Tests run against real data.** This project reads and writes real
      GeoParquet files, so tests should too. Use actual (small) files
      rather than mocks or toy fixtures wherever that's feasible.
- [ ] **Integration tests for anything that crosses a boundary.** If your
      change spans modules, file formats, or an external service, add a
      test that exercises the whole path — unit tests alone aren't enough.
- [ ] **At least one adversarial review.** Someone (or something) whose job
      is to *break* the change: feed it wrong data, hit the edge cases,
      probe the failure modes. An AI review counts, but it has to be
      adversarial in intent, not a rubber stamp.
- [ ] **All CI checks green.** The required checks are the contract. A red
      check blocks the merge, by design — fix it rather than working around
      it.
- [ ] **CodeRabbit comments addressed.** Every automated review comment is
      either fixed or answered with a reason. Silence isn't an answer.
- [ ] **Docs updated** if you changed user-facing behavior (guides, CLI
      reference, Python API docs).

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
git clone https://github.com/geoparquet/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
uv run pre-commit install
```

## Tests

```bash
uv run pytest                                              # Full suite
uv run pytest tests/test_yourfile.py -v                   # Single file
uv run pytest -m "not slow and not network and not meta"  # Fast tests only
uv run pytest -m meta                                     # Repo tooling checks
uv run pytest --cov=geoparquet_io --cov-report=term-missing --cov-fail-under=0  # With coverage
```

Local runs are **not** instrumented for coverage — pass `--cov` yourself when you
want a report. Instrumenting every run slowed single-file runs by 39-49%, and a
partial run can never clear a whole-suite floor, so the default invocation used to
exit 1 on a gate that measured nothing. Both gates below still run in CI.

Add `--cov-fail-under=0` when you opt in on a subset: `[tool.coverage.report]`
sets `fail_under = 80`, so coverage re-arms the floor on any `--cov` run even
though `addopts` no longer requests one.

CI runs three test tiers:

- **Fast tests** (`not slow and not network and not meta`) — run on every PR
  across the full OS/Python matrix and **block merging**.
- **Slow tests** (`(slow or meta) and not network`) — run after merge to main
  and nightly; opt in on a PR by adding the `run-slow-tests` label. This tier
  also carries the `meta` lane: repo-tooling checks (codespell, commitizen,
  doc-sync, mutmut, mypy, validate-claude-md, security tool availability) that
  shell out to subprocesses. Most re-check something the lint job already runs
  via pre-commit, so `uv run pytest -m meta` is mainly for when you skip
  pre-commit — except commitizen (a `commit-msg`-stage hook, which
  `pre-commit run --all-files` never runs) and mutmut (no hook at all), where
  it is the only local check.
- **Network tests** (`network`) — hit live third-party services (ArcGIS, WFS,
  Carto, ...). They are **non-blocking**: failures open/update a tracking issue
  instead of failing CI, because a remote server's behavior is not something a
  PR author can fix. They run serially with per-test timeouts, retries, and
  process isolation (`--forked`).

Coverage gates (both enforced in CI):

- Global floor: 80% total coverage.
- **Changed lines: 90%** — `diff-cover` checks that the lines your PR touches
  are covered *by fast tests*. New code ships with tests that run on every PR,
  not only in the post-merge slow suite.

<!-- BEGIN GENERATED: test-markers -->
### Test Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.slow` | marks tests as slow (deselect with '-m "not slow"') |
| `@pytest.mark.network` | marks tests requiring network access (deselect with '-m "not network"') |
| `@pytest.mark.integration` | marks end-to-end integration tests; runs in the fast suite unless also marked slow/network (see tests/e2e/test_integration_lane.py) |
| `@pytest.mark.corpus` | tests against the official geoparquet-testing corpus (requires git submodule) |
| `@pytest.mark.meta` | repo tooling checks, excluded from the fast suite |
| `@pytest.mark.docs_example` | a fenced example block executed out of docs/guide/*.md |
<!-- END GENERATED: test-markers -->

### Documentation examples are tests

Every fenced `bash` and `python` block in `docs/guide/*.md` is collected as a
test and executed against a seeded scratch directory, so an example you add to
a guide runs in CI. If a block cannot run, mark it in the doc with an HTML
comment on the line above the fence — `<!-- doctest: skip="why" -->`,
`network`, `setup="…"`, `needs-tippecanoe`, and friends. The reason is
mandatory and has to be true of the whole fence.

**[`tests/docs_examples/README.md`](https://github.com/geoparquet/geoparquet-io/blob/main/tests/docs_examples/README.md)
is the reference**: the full directive vocabulary, how the seeding works, what
the meta-test rejects, and how to write a reason that will still mean something
to the next person.

```bash
uv run pytest docs/guide -n 4 -m "not network"   # run the guides' examples
```

Keep the `-m "not network"`. Without it the run also picks up the 29 blocks
marked `network`, each of which downloads hundreds of megabytes of
administrative boundaries from a live service; they belong to the network CI
lane, and locally they mostly just time out.

## Code Quality

`.pre-commit-config.yaml` is the **single source of truth** for every quality
rule. CI runs the exact same hooks (`pre-commit run --all-files`, plus the
pre-push stage), so a check can never pass locally and fail in CI for a
different rule set — and skipping local hook install just moves the same
failure to CI.

```bash
uv run pre-commit run --all-files                        # commit-stage hooks
uv run pre-commit run --all-files --hook-stage pre-push  # deptry, vulture,
                                                         # xenon, mypy,
                                                         # import-linter, menard
```

Two ratchet/ignore files gate quality over time:

- `.pip-audit-ignores` — self-expiring CVE ignore list shared by the CI
  security job and the daily security audit. Each entry needs an expiry date
  and a reason; expired entries automatically re-fail CI.
- `.mutation-baseline` — minimum mutation kill rate enforced by the nightly
  mutation-testing run. Ratchet it up as test quality improves.

## How the CI is set up (and why)

Merging to `main` requires these status checks to pass: `lint`, `security`,
`notebooks`, and the `test` matrix (Ubuntu on Python 3.10–3.13, macOS and
Windows on 3.11–3.13). That list lives in code —
`scripts/apply_branch_protection.sh` declares the required checks and review
rules as GitHub rulesets, and an admin re-runs it whenever the list changes.
If your PR is blocked on a check, that's the system working as intended:
fix the check.

Two heavier gates exist but don't run on every PR, so they won't slow down
day-to-day contributions:

- **Benchmark regression** — label-triggered; fails if performance drops
  10% or more against the baseline.
- **Mutation testing** — nightly; enforces the kill-rate floor in
  `.mutation-baseline`.

The repository also maintains itself where it can:

- The **daily security audit** opens an automated fix PR when a dependency
  vulnerability has a clean upgrade path.
- **Dependabot PRs auto-merge** once all required checks pass.

Bot-opened PRs go through the exact same required checks as human ones.
That's the deal across the board: nothing merges without the checks, and
because of that, a lot can merge without a human bottleneck.

## Documentation

```bash
uv run mkdocs serve
```

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): message`

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

## Architecture

New CLI commands need corresponding Python API:

1. Core logic in `geoparquet_io/core/<feature>.py`
2. CLI wrapper in `geoparquet_io/cli/main.py`
3. Python API in `geoparquet_io/api/table.py` and `api/ops.py`

See `CLAUDE.md` for full architecture details.

### External extractors

Extractors that page through a remote service (`wfs`, `arcgis`, `carto`, `bigquery`) keep
re-discovering the same edge cases. Reuse the shared schema helpers in `core/common.py`
(`_compute_unified_schema`, `_cast_table_to_schema`, `_promote_numeric_type`) rather than
hand-rolling schema reconciliation — the `forbid-bespoke-schema-reconciliation` pre-commit hook
enforces this. Known edge cases to handle:

- **Pagination**: detect server `startIndex`/page-size limits; supply a stable sort
  (`sortBy`/`orderByFields`) for layers without a primary key.
- **Schema across pages**: never trust the first page's inferred schema. Where a source has an
  authoritative schema (ArcGIS layer metadata), cast each page to it via `_cast_table_to_schema`.
  Where it does not (WFS), unify with `_compute_unified_schema` (handles int/float/decimal mixes,
  uint64 overflow).
- **Empty/null responses**: handle empty result sets and null properties without crashing.
- **CRS/SR**: normalize WKID/SR; resolve native WKT → EPSG; unset CRS for unresolvable codes; honor
  `--output-crs` by reprojecting when the server returns a different CRS.
- **Network**: retry transient errors; surface upstream stderr.

## Releasing

(Maintainers only)

Uses commitizen + automated CI workflow:

```bash
# 1. Create bump PR (updates version + changelog)
uv run cz bump --changelog
git push origin HEAD

# 2. Open PR, merge to main
# 3. CI automatically: creates tag, publishes to PyPI, creates GitHub Release
```

**Recovery** (if release fails after merge):
```bash
git push origin :refs/tags/vX.Y.Z  # Delete orphan tag
gh workflow run release.yml --ref main  # Retry
```

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
