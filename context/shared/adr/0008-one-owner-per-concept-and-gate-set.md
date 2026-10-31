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
