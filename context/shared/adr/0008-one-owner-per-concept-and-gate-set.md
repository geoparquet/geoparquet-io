# ADR-0008: One owner per concept and the gate set

## Status

Accepted

Maintainer decisions for #1367 recorded 2026-10-30.

## Context

Write policy, geo-block decoding, bounds, CRS, geometry identity, HTTP retry and
file staging had several implementations. Call-site fixes left other copies
unchanged and the same defect returned. Existing grep gates recognized a past
spelling rather than enforcing ownership. CI's required contexts also omitted
whole jobs, so a green merge gate did not mean every required lane passed.

## Decision

Each of the twenty concepts has one policy owner. The exact module and public
entry-point table is maintained outside generated blocks in
[CLAUDE.md](../../../CLAUDE.md) and in the
[migration reference](../../../docs/guide/one-owner-migration.md).
Callers supply explicit inputs to an owner; they do not keep local decisions,
wrappers with independent defaults, or compatibility re-exports for deleted names.
Leaf primitives stay below orchestration: `parquet_writer` owns write policy,
`write_funnels` orchestrates, and `file_utils` owns staging so footer patching can
share it without a cycle. Derived metadata is rebuilt from output data through
`geo_metadata.compose_geo_block` in one gate order.

Enforce that decision with the complete gate set:

| Gate | Contract and implementation |
|---|---|
| G1 | Banned imports (ruff TID251) and AST owner-call arms in `tests/test_owner_guards.py` |
| G2 | Owner-only definitions, `scripts/check_owner_defs.py` |
| G3 | Normalized function similarity, `scripts/check_duplicates.py`; token duplication, `scripts/check_pylint_duplicates.py` |
| G4 | Vulture at confidence 60, production and tests scanned separately, dynamic dispatch whitelists only |
| G5 | Exact public/private names and size pins for `common.py`, `tests/test_common_split_guards.py` |
| G6 | Public API snapshot, `tests/test_api.py` and `tests/data/snapshots/public_api.json` |
| G7 | Real CLI/API cross-path write invariants, `tests/test_write_path_invariants.py` |
| G8 | Derived metadata compared to a fresh rebuild, `tests/test_derived_state_rebuild.py` |
| G9 | Issue repros with strict issue-keyed expected failures until fixed, `tests/test_defrag_g9_repros.py` |
| G10 | `all-checks` needs every test-workflow job; wiring asserted by `tests/test_ci_wiring.py` |
| G11 | Explicit owner-file entries in `.github/CODEOWNERS`; no pending paths |
| G12 | Planted faults prove G1–G5 fail, in the owner/common guard tests |
| G13 | Shared Click option consistency, `tests/test_cli_shared_options.py` |

All gates block. The owner-definition, both duplicate and both vulture hooks run
at commit and push. Exemptions live only in `scripts/owner_exemptions.toml`, match
real sites exactly and carry enduring reasons. Counts only go down. Remaining
allowances describe semantic false positives (scratch spools, benchmark baselines,
metadata serialization and validators reading claims), never deferred track work.
The AST duplicate baseline is empty; remaining pylint allowances are forwarded
argument lists, each with a reason. G12 plants remain active with blocking gates.

`all-checks` fails failed/cancelled/required-skipped jobs. Only conditional slow and
network jobs may skip. `pr-title` stays a separate required context because its
`edited` event must rerun without replacing the test workflow's verdict. The live
ruleset must require both; `scripts/apply_branch_protection.sh` applies that policy.
The nightly mutation job and its unused baseline are removed.

G7 policy: a patchable metadata-only operation keeps its input data pages/layout;
an unpatchable/remote fallback re-encodes through the facade. Geometry-free outputs
keep carried sidecar keys like geometry outputs. Epoch and carried metadata survive
both funnels; the former five expected-failure epoch cells now pass normally.

## Consequences

### Positive

A fix at an owner reaches every caller, and planted faults prove the gates block
regrowth. Reviewers see intentional API and byte-level changes in the same diff.

### Negative

Core imports break without shims. Exact baselines require maintenance when copies
are removed, and a semantic false positive needs a reviewed, specific reason.

### Neutral

Breaking core-name moves are intentional, without shims; the migration reference
lists the old names and owners. Snapshots make further public changes reviewable.
The eleven approved output changes are: default COPY ZSTD level 15; facade row
groups for geometry-free aggregate/overview; facade row groups for bbox-metadata
fallback; canonical uncompressed aliases and rejection of LZ4_RAW; decimal MB/GB
and bare-number MB; malformed geo JSON/UTF-8 warns and becomes absent; add bbox
upgrades 1.x to 1.1; pyproj geographic classification including None/EPSG:4171;
structural CRS equality fallback; GeoArrow without geo writes 2.0 on both paths;
and label-to-PROJJSON resolution fails closed.

Defect fixes and additional maintainer-visible consequences are recorded separately
in the migration guide and PR #1372 body: mode preservation, numbered backups,
carried kv/epoch, row-group layout and sample measurement, explicit-null CRS
precedence and datum validation, planar-key omission, dateline filters, fallback
codec, canonical geo-block key order, STAC re-encoding, ASCII-only name folding,
root-only geometry detection, and the native-geometry extension of approved change
7. These are disclosed for review rather than described as byte-identical.

Spill ownership on POSIX uses a held `fcntl.flock` marker, independent of PID
namespaces. Creation and sweeping share a base lock so an incomplete marker cannot
be swept during publication. Windows falls back to conservative PID checks.

Follow-up: the eleven `"OGC:CRS84"` spellings flagged in the handoff outside `crs_utils` are
recorded for a later owner-vocabulary pass; this ADR does not authorize sweeping
them. Geometry-metrics still ignores edges for area/length (handoff row 72).
Kdtree/admin deliberately request planar centroids; whether their keys should
respect non-planar input remains a maintainer policy question (row 68).
Cross-build A5 IDs, the spatial-order heuristic, and performance investigations
remain outside this consolidation. The aarch64 fiona lockfile/build prerequisite
and live AWS upload endpoint validation remain maintainer environment items.

## Alternatives Considered

- Keep per-command patches and grep known bad spellings: rejected because renamed,
  aliased and slightly varied copies escaped them.
- Keep compatibility wrappers at old paths: rejected because they preserve another
  policy/default surface and hide breaking moves from review.
- Replace behavioral tests with structural gates: rejected; both are required.
  Agreement matrices are preservation pins, while repros and injected faults
  distinguish the change from the old implementation.
- Require every individual CI matrix name in branch protection: rejected because
  matrix changes silently leave the live ruleset behind. One aggregator plus the
  independently triggered title check expresses the actual requirement.

## References

- [Issue #1367](https://github.com/geoparquet/geoparquet-io/issues/1367)
- [Integration PR #1372](https://github.com/geoparquet/geoparquet-io/pull/1372)
- [Track ledger](../documentation/TRACKS-1367.md)
- [Owner guard maintenance](../../../docs/contributing.md#owner-guards)
- [ADR-0001: CLI/Core Separation](0001-cli-core-separation.md)
- [ADR-0002: DuckDB as Processing Engine](0002-duckdb-as-processing-engine.md)

## The fast suite's budget and the matrix rule

### Status

Accepted maintainer rule for the #1367 close-out, 2026-10-31.

### Context

A concept having one production owner does not justify repeating its complete input matrix through every caller.
Those repetitions obscure which test owns a failure mode and consume the fast lane's budget.
Run 41218224680 had completed legs at 19, 35, 38, 56, 62 and 66 minutes on the same commit.
The suite also repeatedly created DuckDB databases that verified the same extension signatures.
Query thread limits did not limit the verifier's hashing threads.

### Decision

Keep the owner's unit tests, G7 cross-path invariants and one routing test per path.
Delete other repetitions only with a ledger line naming the survivor and the failure mode it still catches.
An axis can collapse only where the implementation has no branch that distinguishes its values.
Smaller fixtures must still fail for an incorrect implementation.
G7 and G8 keep their invariant cells.
The decision ledgers are `scratchpad/diet/ledger-hygiene.md`, `ledger-check.md`, `ledger-write.md` and `ledger-guards.md`.

Enforce the fast suite's budget with three gates. They run in the fast lane, so every PR's `Tests` run checks them:

| Gate | Owner | Budget or evidence |
|---|---|---|
| Fast collection | `tests/test_suite_budget.py` | `tests/suite_budget.toml`, `max_collected`; reviewed ratchet 14,199 |
| Per-test wall and timeout exceptions | `pyproject.toml`, `timeout = 60`, thread method; `tests/test_suite_budget.py` counts marks | `tests/suite_budget.toml`, `max_timeout_marks = 4` after review |
| Duplicate tests and fixtures | `tests/test_duplicate_tests.py` | Live normalized AST groups over test bodies and fixtures; historical regression excerpts under `tests/fixtures/suite_budget/` |

The TOML budgets only go down unless the PR gives an explicit reason for an increase.
The collection gate uses the CI fast marker expression and fails closed on invalid collection output.
The duplicate gate preserves parametrization values so different input sets remain distinct tests.
The timeout mark budget counts source calls within `tests/`, including the docs collector call once.
A timeout override needs a reason.
The three slow modules and docs examples have justified 600-second overrides; the four-mark budget accounts for these source sites.

Ordinary `integration` tests run in fast CI unless marked `slow` or `network`.
The whole-pipeline journeys, real subprocess pipelines and corpus suite belong to the slow lane.
`tests/e2e/test_integration_lane.py` pins that module list.
The slow job has three OS legs and selects `(slow or meta) and not network`.
Docs examples are slow and also have their separate docs execution path.
Moving a test between lanes transfers its cost and preserves its assertions.

Use one root DuckDB database per xdist worker for eligible test connections in `tests/conftest.py`.
Return cursors, reapply session settings and isolate ordinary catalog names with unique schemas.
Register the database-global UDF once.
Keep real fresh connections, selected by the `fresh_duckdb` marker, for tests of extension loading, custom connection settings, memory/spill/finalizer behavior, profiling and GDAL encoding isolation.
Signature verification stays enabled.

This fixture is test-only.
It does not decide the lifecycle of a database in a long-running library process.
Production reuse remains [issue #1410](https://github.com/geoparquet/geoparquet-io/issues/1410).
The issue names four constraints a production design has to meet, because `SET memory_limit` and `SET threads` are database-wide in DuckDB, not per cursor:

- the memory policy (`core/memory_limits.py`, `scoped_write_memory_limit`, the pinned `threads=1` for duckdb-kv writes) scopes settings per statement on the assumption that each caller owns its database; with a shared database a scoped SET lands on every live cursor in the process;
- a cursor shares the catalog, so callers that register a view or temp table under the same name are no longer isolated;
- the spill directory and its lock (`_spill_lock`, the `weakref.finalize` release) are per database and get a per-process lifetime;
- the module owns several G1 arms in `tests/test_owner_guards.py`, so exemption counts may move.

### Consequences

#### Positive

The suite has an explicit owner for each failure mode and blocking checks against regrowth.
Repeated signature verification is amortized without disabling verification.
The measured reductions distinguish the matrix diet from connection reuse.

| Measurement | Before | After |
|---|---:|---:|
| Diet, reported worker-seconds | 1,731.39 | 1,301 |
| One database per worker, reported worker-seconds | 1,301 | 509.05 |
| Full suite pinned to four cores, lead measurement | 1,362 s | 277 s |
| Full suite, 12 workers | 139 s | 63.58 s |
| Pinned slice, query threads 64 / 4 / 1 | 317 / 321 / 315 s | No material change |
| Diagnostic signature bypass, pinned slice | 437.39 s | 52.43 s |

The diet cut about 25% of reported worker-seconds.
Connection reuse removed the rest of the identified verification bottleneck.
The profile recorded 3,577 extension loads across 1,676 tests and 1,044.52 CPU-seconds in extension loading.
The separate unprofiled control used 1,166.36 user CPU-seconds.
These are separate runs, not a same-run percentage.

#### Negative

A removed matrix cell needs a reviewable survivor rather than an assumption of coverage.
The shared test database needs explicit isolation and fresh-database exceptions.
The independent review in `scratchpad/exp/one-db-review.log` identifies limit restoration, schema cleanup and explicit isolation selection as requirements for that fixture; each was fixed before #1411 merged (as 2a6b890d). The final audit of #1372 (`scratchpad/reports/audit-final-1372.md`) was fixed and its review (`scratchpad/reports/review-audit-fixes.md`) left one finding, fixed in the close-out commit.
GDAL encoding tests demonstrated that unsafe sharing can cause decoding failures and crashes.
Budgets require review when a new failure mode needs additional tests.

#### Neutral

The reported worker-seconds sum setup/call/teardown durations at or above 0.05 seconds.
They are not complete CPU accounting.
The local full-suite measurement had 3 socket failures and 30 socket setup errors; excluding exactly those cases passed.
The close-out `Tests` run's per-leg walls are recorded in the [track ledger](../documentation/TRACKS-1367.md).
The signature bypass was diagnostic and does not authorize disabling production verification.

### Alternatives Considered

- Keep every caller matrix: rejected because owner behavior and routing can be established without repeating indistinguishable axes.
- Cap DuckDB query threads: the pinned measurements showed no material improvement.
- Disable signature verification: useful for diagnosis, rejected as the fixture's remedy.
- Apply test database sharing to production immediately: deferred to #1410's explicit lifecycle constraints.

### References

- `scratchpad/pr-1404-body.md`, `pr-1406-body.md`, `pr-wp4a-body.md`, `pr-wp3-body.md`, `pr-1409-body.md`.
- The WP6 suite-budget branch, merged into the integration branch locally without a PR (ab38fb8c); `scratchpad/reports/review-wp6.md`; `scratchpad/briefs/diet/wp6-r2-report.md` and `wp6-r3-report.md`.
- `scratchpad/exp/profile-report.md`, `profile/report.md`, `threads-ab-report.md` and `one-db-report.md`.
- PR #1411, test-only connection reuse, merged as 2a6b890d; production follow-up #1410.
