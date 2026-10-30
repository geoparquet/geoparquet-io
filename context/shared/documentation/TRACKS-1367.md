# Tracks for #1367 — one owner per concept

Lead-maintained ledger for the integration branch `defrag/1367-one-owner`. Base:
`origin/main` @ 3185269f (the ticket was diagnosed at 22584e5f; #1357, #1358 and #1362
merged in between and are absorbed by T1a and T1c). Agents never merge to `main`.

## Tracks, models, worktrees

| Track | Concepts | Model | Worktree / branch |
|---|---|---|---|
| T0 gates and CI | G1–G13, remove mutation testing | Opus 5.5 | `/home/ubuntu/gpio-wt/t0` · `defrag/1367-t0-gates` |
| T1 DuckDB layer | 9, 10, 16, 17, 20 | Opus 5.5 | `/home/ubuntu/gpio-wt/t1` · `defrag/1367-t1-duckdb` |
| T2 output-file lifecycle | 1, 2, 11 | Opus 5.5 | `/home/ubuntu/gpio-wt/t2` · `defrag/1367-t2-output` |
| T3 geo block + geometry identity | 3, 4, 5, 6, 7 | Opus 5.5 | `/home/ubuntu/gpio-wt/t3` · `defrag/1367-t3-geo` |
| T4 extractors | 8, 14, 18 | Sonnet 5.5 | `/home/ubuntu/gpio-wt/t4` · `defrag/1367-t4-extractors` |
| T5 commands | 12, 13, 15, 19 | Codex Sol 6.1 (T5b), Sonnet 5.5 | `/home/ubuntu/gpio-wt/t5` · `defrag/1367-t5-commands` |

Reference worktrees: `/home/ubuntu/gpio-wt/base` (detached at the base commit, for
byte-identity runs) and `/home/ubuntu/gpio-wt/integ` (this branch).

## Sub-phase order

```
T0a..d ∥ T1a..e ∥ T4a,T4b  →  T3a..e ∥ T2a..f ∥ T5b,T5d  →  T4c ∥ T5c,T5a  →  T0e flip, docs, ADR
```

One PR per sub-phase onto this branch. Shared-file order rules:

- `write_funnels.py`: T3a (301) → T2a/T2b → T3d (edges out) → T2d (footer) → T2f.
- `add/bbox.py`: T1c (125, 541) → T3a (235, 308) → T3c (544-548) → T3e (249) → T2b (deletes 292-319).
- `overview_file.py`: T3a (293) → T3e (`_output_schema`) → T2f (369-392).
- `simplify.py`: T3b (527, 554) → T3e (413, 600-621) → T2e (846) → T2f (623-639, 702-735, 974-1002).
- `stream_io.py`: T1c (220) → T3d (221, 242) → T3e (254) → T2b (475).
- `reproject.py`: T3a (435) → T2b (680, 887) → T2f (whole `_sanitize_null_crs_file`).
- `arcgis/carto/wfs`: T3e block → T4c CRS.
- `cli/decorators.py`: T1b → T2a/T2e → T5c.

## Boundary corrections to the ticket's track table

Line numbers are from 22584e5f; functions are the unit of ownership.

| File | Owner |
|---|---|
| `core/stream_io.py` | T1 `_file_stream_metadata`; T3 `_stream_input_metadata`; T2 the version call at 475 |
| `core/geo_metadata.py` | T1 `_get_query_columns` (1350) and `output_query_schema` (1873) + their callers 913/1398/1480/1928 and imports 26-32; T3 everything else |
| `core/process/aggregate/common.py` | T3 `aggregate_source_relation`, `geometry_to_geom_expr` (and its 5 callers), `_PLAIN_EXTENT_SQL`/`antimeridian_aware_bbox`; T1 `sql_literal`; T5 `_fold`/`_has_column`/`validate_agg_columns`/`_unique_name` |
| `core/process/aggregate/grid_common.py` | T3 `_BBOX_STRUCT_FIELDS` + `_validate_bbox_struct_column`; T1 `_relation_columns`, DESCRIBE at 520; T2 the write block; T5 `_resolve_column_names` |
| `core/reproject.py` | `_sanitize_null_crs_file`: T3a one-line decode swap first, T2f rewrites the function last; T2 680-682, 887-889 |
| `core/process/simplify.py` | `_footer_kv_via_funnel`: T3e the block 600-621, T2f the temp handling 623-639; 893-898 is T2 (version); T3 `_geometry_column_of` whole |
| `core/add/bbox.py` | T2 `_write_options_requested` + `_passthrough_version`; T3 223-260 and 544-548; T1 the `LIMIT 0` probes at 125, 541 |
| `core/extract.py` | T4 `is_geographic_crs` and the except at 1245; T3 `_get_data_bounds`, 703; T1 probe 168-171 and 471; T5 `parse_bbox`, `build_column_selection` |
| `core/exceptions.py` | T1 186-216 only; T4 the rest |
| `core/partition/auto_resolution.py` | T3 `_geom_sql`; T5 the rest |
| `core/preview.py` | T1 `_row_group_spatial_stats`; T3 `_covering_bbox_column`/`_primary_geometry_column`; T5 `_validated_bbox` |
| `cli/decorators.py` | T1 100-134, 569-622, 893-903; T2 223-240, 254-263, 293-318, 405-412; T5 the rest after T1b/T2a/T2e |
| `core/arcgis.py`, `core/carto.py`, `core/wfs.py` | T3e block builders first; T4 the rest; T5 column guards after T4; `arcgis:1895` overwrite is T2e |
| `core/arrow_geo_metadata.py` | T3, except `_detect_version_from_table` 68-110 and caller 856-860 (T2b after T3a) |
| `core/streaming.py` | T3 183, 569, 485, 615; T2b 598-700 otherwise |
| `core/write_funnels.py` | T2, except the edges code 127-330, 1635-1695, 1808 (T3d) and the decode at 301 (T3a) |
| `core/benchmark.py` 733-752, 811 | T1a |
| `api/ops.py` `get_row_group_geo_stats` | T1c (whole function) |
| `tests/test_common_split_guards.py` residue pin, pyproject `ignore_imports` parquet_writer→common | T2b |

## Interfaces agreed up front

- T3 `geo_metadata.compose_geo_block(source, options) -> dict`; stub in T3a; T2 swaps `build_geo_metadata` callers against it.
- T1a `duckdb_utils.duckdb_capabilities()` (frozen dataclass, cached, `major: int | None` parsed defensively) and `complete_without_fetch(result)`.
- T1c `duckdb_utils.query_schema(con, sql) -> pa.Schema` (raises); `duckdb_metadata.is_native_geo_column` (GEOMETRY and GEOGRAPHY).
- T3c `geometry_detection.primary_geometry_column(source) -> str | None` (never the literal `"geometry"`).
- T3d `core/edges.py`; one hook kept inside `write_geoparquet_table`.
- T2f `atomic_write` lives in `core/file_utils.py` (leaf; moving it into `write_funnels` would cycle through `parquet_footer`), re-exported from `write_funnels`; `write_strategies.base.atomic_write` deleted.
- T5a `column_selection.unique_name(base, taken, *, start=1)`.
- `compression.resolve_process_compression_level` stays a thin wrapper until T2f deletes it.

## Dispositions policy

No new issues are opened. Every linked defect is fixed in-track (failing-first test, then
the owner fix), or recorded in the PR body as stale (repro output on the base) or out of
scope per the ticket (perf; the spatial-order heuristic #618/#755/#774).

## Ledger

| Date | Event |
|---|---|
| 2026-10-30 | Integration branch cut from origin/main @ 3185269f; eight worktrees created; hooks installed. |
| 2026-10-30 | Draft PR #1372 opened. Launched T0a (Opus, t0), T0d (Opus, t0h: G7/G8/G9 + byte-identity harness), T1a (Opus, t1), T4a (Sonnet, t4), Codex Sol duplicate scan (read-only). |
| 2026-10-30 | T0a merged (#1373). T4a PR #1374 open: verifier + consolidation auditor running (dedicated worktrees `verify`, `audit`). Launched T0b (Opus, t0 on `defrag/1367-t0b-guards`), T4b (Sonnet, t4 on `defrag/1367-t4b-errors`, stacked on T4a). Environment fix: geopandas/pyogrio installed in every venv; pytest runs with `env -u AWS_PROFILE -u AWS_REGION`. |
| 2026-10-30 | T4a PR #1374: verifier says the arcgis side has no discriminating test (wrapper deletion only); consolidation audit says FRAGMENTATION REMAINS (9: per-caller retry defaults/timeout constants in wfs/carto, dead wrapper, stale comments, inline page-refusal check). Fixes assigned to the T4a agent in worktree `t4a-fix`; merge blocked until a clean re-audit. Asides forwarded: 401/403/404 mapping x3 to T4b; guard gap (status sets, Retry-After parsing, default constants) to T0b. |
| 2026-10-30 | T1a+T1b done on one branch → PR #1375; verifier + auditor running. T4a fixes (10) pushed to #1374 → re-audit running. T1c started (branch `defrag/1367-t1c-schema`). Hand-offs collected in scratchpad `briefs/HANDOFFS.md`. |
| 2026-10-30 | T0d merged (#1376): G7/G8/G9 + `scripts/byte_identity.py` (94 cases; base-vs-base clean). T4a re-audit: 3 remain (wfs client wrapper, wfs `_make_request` body copy → T4b concept 18, API/CLI timeout literals) + 31 docs files reformatted by a hook → fixes assigned. Policy taken for #1367-keyed G7 xfails: metadata-only rewrites take the facade codec/row-group policy; geometry-free aggregate carries sidecar keys like the geometry branch (maintainer may overrule in the PR). |
| 2026-10-30 | T1a/T1b (PR #1375): verifier found 3 non-discriminating call-site tests; audit FRAGMENTATION REMAINS (13: two MB definitions on `--row-group-size-mb`, decimal/binary `format_size`, third memory-limit apply copy, size regex in duckdb_utils, unused `result_complete`, geojson wrapper, dead `hasattr(duckdb, "HTTPException")`). Fresh Opus agent fixing on the T1 branch in worktree `t1-fix`; T1c continues on its stacked branch and is rebased afterwards. |
| 2026-10-30 | T4a merged (#1374) after three audit passes; deferred: wfs `_make_request` body → T4b. T0d G7 policy decisions, T4a review record in PR body. |
| 2026-10-30 | T0b done (rebasing onto integ, reconciling baselines vs T4a). T4b done (#1034, #1031, #1266, #1344 fixed; rebasing onto integ). |
| 2026-10-30 | T0b PR #1378 open (audit running). T4b PR #1377: audit FRAGMENTATION REMAINS (14: remote.py hint classifier, wfs page error copy, duplicate httpx type switch, double-sanitizing, two CLI translations outside exception_handler, four "not parquet" text tests, logging not sanitized once, two Click translation tables) → fixes assigned. T1c done, parked for rebase; T1d started. |
| 2026-10-30 | T0b PR #1378: audit gaps found (G2 misses `_`-prefixed and renamed copies; geo_parse misses key-read-then-loads; json_dumps cli-only; no urllib ban; vulture counts tests as use; 23/31 varied faults missed; G5 caps not pins) → fixes assigned. T1a/T1b fixes done + rebased (12577 passed) → re-audit running; T1c rebase in progress; T1d done (#543 fixed, output change recorded as defect fix; #942 second half + docs in progress). |
| 2026-10-30 | T1a+T1b merged (#1375) after two audit passes. Next: rebase T1c/T1d onto the tip. |
| 2026-10-30 | T4b: 14 audit findings + verifier CLI gap fixed (new tests/test_cli_error_routing.py, 19 cases); rebased onto tip (12669 passed), pushed → re-audit running. T2a launched (Opus, t2). T5b launched (Codex Sol, t5). |
| 2026-10-30 | T1d follow-up done (aeb527b1: #942 fully fixed, `admin_datasets.build_admin_subquery` the one builder; docs updated). T1e done (d140f9c6: `sql_literal` → duckdb_utils, BigQuery row-count quoting, CSV WKT probe quoted; menard review mark committed). T1 track complete pending T1c rebase → PRs T1c, then T1d+T1e. |
| 2026-10-30 | T0b audit fixes done (8832e8ac; probes 31/31; G4 package-only + tests whitelist; G5 exact pins; G2 `_`-insensitive + known_copies + counts). Rebasing onto tip to reconcile vs T1. Host note: tests/test_spill_strategy.py (3 tests) fails on base too when several worktrees run suites concurrently — `/tmp/gpio-spill-<pid>-…` vanishes mid-test; likely a cross-process stale-spill sweep. Recorded, not a track defect. |
| 2026-10-30 | T0b merged (#1378): owner guards G1/G2/G4/G5 live (warn-only baselines). Every open branch reconciles `scripts/owner_exemptions.toml` + `EXPECTED_COUNTS` on rebase from here. |
| 2026-10-30 | T0b follow-up merged (#1379; #1378 had merged the pre-fix head). T2a done (3123c317, 687a803b → rebased 7833dbeb, 041aae88): `resolve_codec` owner, 14 copies deleted, #1335 and #1026 fixed; byte-identity 65/94 differ only in compressed size (approved 1). One guard failure after rebase: common.py 791 > pin 789 → T2a fixing. |
| 2026-10-30 | T2a PR #1380 open (verifier + auditor running). T4b round three done (A–L fixed); rebased onto the guard tip → 3 owner-guard hits (httpx/requests import in exceptions.py, stale whitelist line, a retry_wrappers shape) assigned. |
| 2026-10-30 | T5b (Codex Sol) finished implementation (duplicate pairs 22 → 6; six byte comparisons identical; 286 targeted tests) but could not commit: `-s workspace-write` sandbox cannot write the shared `.git` of a worktree. Sonnet finalizer committing, rebasing, reconciling guards, dispositioning #1361. |
| 2026-10-30 | T2a PR #1380: audit FRAGMENTATION REMAINS (28: hand-built pyarrow codec kwargs in arrow_streaming/write_funnels, three footer-codec translations, ~30 literal `15` defaults, `ResolvedCodec` constructor skips validation → SQL injection via disk_rewrite, wrapper/owner disagree on process default, dry-run displays rebuild codec) → fixes assigned. |
| 2026-10-30 | PRs opened: #1381 T5d (12773 pass), #1382 T1c (12785 pass), #1383 T0c (12792 pass, meta 215), #1384 T5b (12795 pass). Verifiers (T5d, T1c) and auditors (T5d, T1c, T0c, T5b) launched. T4b third audit: A–L resolved except G half-done (GlobAwareCommand.invoke still a second boundary tail) + 4 minor → assigned. T2a verifier: 7 codec sites unpinned by behavioural tests → folded into the T2a audit-fix round. |
| 2026-10-30 | T5d PR #1381: verifier OK except untested native-geometry error; audit FRAGMENTATION REMAINS (8: local BLOB decode/native check instead of `resolve_geometry_read`, duplicated to_json builder, a 13th partial-file cleanup that can delete a pre-existing output, docs/config residue) → fixes assigned. |
| 2026-10-30 | T4b round four (830e957d: one `error_boundary()`), pushed; fourth audit pass running. T5b audit: FRAGMENTATION REMAINS (14: second quadkey file driver, streaming fork, bounds in 13 places, 25 `spec.name ==` branches, sub_partition and aggregate GridScheme registries) → Codex re-run (workspace-write; a Claude agent commits). T1c audit: FRAGMENTATION REMAINS (47: ~70 unrouted DESCRIBE/LIMIT 0 probes pinned as "other", 8 native-type probes, bbox-first stats readers, owner readers not sharing `_parquet_metadata_rows`) → assigned. |
| 2026-10-30 | T3a done on `defrag/1367-t3-geo` (tip c186ad26, carries pre-fix T1c ba10e591): two decoders `read_raw_geo`/`sanitized_carried_geo`, 18 sites routed, 5 names deleted, #913/#982/#957/#1080 fixed, `compose_geo_block` stub + `edges_for_version`; 12870 pass, byte-identity 93 identical/1 same-error. Lead: #982 test change accepted; vulture whitelist line for the stub rejected → stub gets its first caller (`write_strategies/base.build_geo_metadata`). Verifier + auditor launched. PR waits for T1c merge. |
| 2026-10-30 | T0c PR #1383 audit: FRAGMENTATION REMAINS (14: normaliser numbers annotations so byte-identical bodies score 0.56–0.72 and 4 pairs escape; autojunk; 4 exemptions wrongly permanent; 2 wrong tracks; G13/G6 gaps) → assigned. T4b fourth audit: 4 findings (process/pmtiles/inspect translate before the boundary → Usage preamble; 4 `truncate=True` sites; auth predicate ×2) → assigned. |
| 2026-10-30 | T3a audit: FRAGMENTATION REMAINS (13: decode-then-sanitize copies in crs_utils/base.py/streaming/write_funnels, a second owner-internal copy, five two-step file readers that swallow bad JSON without the approved warning, dead except arms, validate not on `read_raw_geo`) → assigned. |
| 2026-10-30 | T2a round two (ceab3a8a): `ResolvedCodec` validates in `__post_init__`, 91 second defaults → None, `codec_from_footer`, `validate_compression_settings` deleted, 18-site write test, new codec-policy guard (149 hits on the old tip → 0); process with no codec now writes level 9. T5d round two (619b0bd4): geometry via `resolve_geometry_read`, staging via `atomic_write` (Shapefile sidecar set staged in a temp dir), one `_attribute_expr`. Both pushed; second audits running. |
| 2026-10-30 | T2a second audit: 28/28 resolved, 3 minor (docstrings, `_compression_level_help` recomputes defaults) + GZIP/BROTLI described as 6 while COPY emits no level → final round assigned. T5d second audit: 8/8 resolved, 2 left (Shapefile staging is a second staging implementation → multi-file mode on `atomic_write`; `sample_output` repeats `extension`) → assigned. |
| 2026-10-30 | T0c round two (8c713c73: body-only normaliser → 27 pairs base=tip, autojunk off, per-function pylint pins, bbox-parser G13 arm, G6 25 modules) pushed, second audit running. T4b round five (593bbded: no translation in front of `error_boundary`, `sanitize_error_message` deleted, `is_auth_status`) pushed, fifth confirmation pass running. T3a round two (d4db8ac6: `sanitized_carried_geo_of_file`, base.py copies gone, `build_geo_metadata` composes through the stub with `CarriedSource`/`defer_bboxless_gate` for byte-identity; 93 identical) second audit running. |
| 2026-10-30 | T4b fifth audit: 2 findings, one root cause (only `error_boundary` logs the traceback; inspect keeps `_inspect_error` because the boundary carries no file name) → one answering step + `error_boundary(path=, hint=)` assigned. T5b Codex round two done uncommitted (8/8 byte-identical, 0 index pairs); Sonnet finalizer committing. |
| 2026-10-30 | T0c second audit: guard code clean, 3 tag findings → retag (publish pair → T2e; admin-join pair → T1d/T1e PR; `_print_dry_run_header` + benchmark twins → t5c brief); merge on report. T3a second audit: 10/13 resolved, 8 new — lead ruling: findings 5–7 (CarriedSource/defer_bboxless/build_geo_metadata steps) are T3e composer-folding (brief updated verbatim), 1–4 + 8 + verifier gaps fixed now. T5d round three (91b30434: `atomic_write(sidecar_suffixes=)` one path) pushed, 12780 pass; third audit running. |
| 2026-10-30 | **Merged T0c PR #1383** (G3/G6/G13/G12 gates; 27 G3 pairs all tracked, 56 pylint entries, 13 G13 options, 3 bbox parsers). Note for every open branch: rebase will hit `check-duplicates`/`pylint-duplicates` hooks — run `scripts/check_duplicates.py --update` to drop pairs you removed; the T3 branch adds `_pyarrow_get_kv_metadata|_pyarrow_get_geo_metadata` (0.95) and T2 adds an admin_divisions/country_codes pylint block. |
| 2026-10-30 | T5d round four (4d917741: one shapefile sidecar list) rebased onto the T0c tip; G6 snapshot re-record assigned. T4b round six: `always=True` on the boundary rejected (second policy behind a flag) → fix the `inspect meta` AttributeError at source, rebase onto T0c. T5b second audit: 14/14 resolved, 9 new (swapped a5/s2 specs, name-derived spec fields, api/ops + api/table + grid_common own `--auto`/bounds/extension-load rules, sort_quadkey ensure copies, quadkey literals in the owner) → Codex round 3 running. T1c audit fixes done (47228bc1: ~60 probes routed, 3 type-name owners in duckdb_utils, 9/9 metadata readers shared, G1 `parquet_metadata` arm) — rulings: route check_spatial_order native-first (report label change), stats-fold disagreement → T3b. T3a round three (0f06a3df) third audit running. pylint synced into every worktree venv after the T0c merge. |
| 2026-10-30 | **Merged T5d PR #1381** (format_writers defects #1161/#1320/#1333 at the owner; `atomic_write(sidecar_suffixes=)`; `shapefile_members`; geometry via `resolve_geometry_read`). |
| 2026-10-30 | T2a round three (53bc412b: BROTLI default 11 so COPY and pyarrow agree, `copy_level`, codec guard widened with `JUSTIFIED_CALL_SITES`, one `print_spatial_join_dry_run`, four forwarding pylint blocks pinned permanent per lead ruling, G6 re-recorded) rebased onto the T5d tip, 13042 pass; third audit running. T4b round seven (8172f428: `always` removed, `inspect meta` crash fixed in metadata_utils via `sanitize_geo_metadata`, G6 re-recorded) rebased, 12923 pass; sixth audit running. T3a holds at 7b1f8588 for the T1c merge. |
| 2026-10-30 | T1c round two (40fc3a6b, rebased onto the T5d tip, 12856 pass): check_spatial_order native-first (lead ruling; `check_spatial_order_bbox_stats` deleted), geo_parse guard no longer reads callee names, GEOGRAPHY + VARIANT behavioural tests, G3 removals only; second audit running. T2a third audit: 2 findings (two COPY dry-run displays unrouted in common/extract; admin_datasets:507 cache write with no codec) → fixing. T4b sixth audit: 3 findings (inspect inner boundary prints Usage; two raw geo reads in inspect_utils; gpio-fiboa plugin imports the deleted decorator) → fixing. |
| 2026-10-30 | T1c second audit: 46/47 closed, 9 new (three TYPEOF probes, preview stats projection copy, numeric type lists behind a mis-justified pylint pin, geo_parse callee exclusion blinding the retry arms, two stats folds) → folds F5/F6 ruled T3B hand-off, rest fixing. T2a round four (83a4a0e1: `print_copy_dry_run` one body ×4, admin cache ZSTD 3, warn once, effective level in display) pushed; fourth-pass confirmation running. |
| 2026-10-30 | **Merged T2a PR #1380** (one codec policy `resolve_codec`/`ResolvedCodec`; BROTLI 11; `print_copy_dry_run`; `JUSTIFIED_CALL_SITES` allowlist in the codec guard). |
