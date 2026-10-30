# Tracks for #1367 — one owner per concept

Lead-maintained ledger for the integration branch `defrag/1367-one-owner`. Base:
`origin/main` @ 3185269f (the ticket was diagnosed at 22584e5f; #1357, #1358 and #1362
merged in between and are absorbed by T1a and T1c). Agents never merge to `main`.

## Tracks, models, worktrees

| Track | Concepts | Model | Worktree / branch |
|---|---|---|---|
| T0 gates and CI | G1–G13, drop mutmut | Opus 5.5 | `/home/ubuntu/gpio-wt/t0` · `defrag/1367-t0-gates` |
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
