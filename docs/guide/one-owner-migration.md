# One owner per concept: migration reference

The #1367 refactor removes duplicate core helpers without compatibility shims.
Use `geoparquet_io.read`, `Table` and `geoparquet_io.api.ops` for application code;
embedding code can call the owners below. Old convenience imports are not a
compatibility surface. See [Embedding](embedding.md) and the [Python API](../api/python-api.md).

## Owners

| Concept | Owner module | Public entry points |
|---|---|---|
| Final Parquet write policy | `geoparquet_io/core/parquet_writer.py`, `geoparquet_io/core/write_funnels.py`, `geoparquet_io/core/compression.py` | `WriteOptions`, `resolve_output_geoparquet_version`, `resolve_row_group_rows`, `resolve_codec`, `write_parquet_with_metadata`, `write_geoparquet_table` |
| Footer patch or rewrite | `geoparquet_io/core/parquet_footer.py` | `patch_or_rewrite` |
| Geo block decoding | `geoparquet_io/core/geo_metadata.py` | `read_raw_geo`, `sanitized_carried_geo`, `declared_version` |
| Geo block composition and covering | `geoparquet_io/core/geo_metadata.py`, `geoparquet_io/core/bbox_structure.py` | `compose_geo_block`, `GeoBlockOptions`, `build_bbox_covering`, `bbox_struct_child_names_match`, `covering_bbox_refs` |
| Geometry statistics and bounds | `geoparquet_io/core/geo_metadata.py` | `geometry_stats_of`, `bounds_of`, `geometry_types_of`, `merged_geometry_stats`, `bbox_xy` |
| Non-planar edges | `geoparquet_io/core/edges.py` | `detect_edges`, `edges_for_version`, `carry_edges`, `representative_point_sql`, `geoarrow_type_edges` |
| Geometry identity and read expressions | `geoparquet_io/core/geometry_detection.py` | `primary_geometry_column`, `resolve_geometry_column`, `resolve_geometry_read`, `require_geometry_column`, `create_geometry_view` |
| CRS | `geoparquet_io/core/crs_utils.py` | `crs_of`, `is_geographic`, `is_projected`, `crs_equal`, `projjson_for_label`, `load_crs` |
| Memory ceilings and byte sizes | `geoparquet_io/core/memory_limits.py`, `geoparquet_io/core/sizing.py` | `bound_open_connection`, `scoped_write_memory_limit`, `parse_size_string`, `format_size`, `format_memory_limit`, `mb_to_bytes` |
| SQL escaping and native type probes | `geoparquet_io/core/duckdb_utils.py`, `geoparquet_io/core/duckdb_metadata.py` | `sql_literal`, `sql_path`, `quote_identifier`, `native_geo_type`, `is_native_geo_column` |
| Paths, overwrite and staging | `geoparquet_io/core/remote.py`, `geoparquet_io/core/file_utils.py` | `is_remote_url`, `has_glob_pattern`, `is_partition_path`, `handle_output_overwrite`, `atomic_write`, `scratch_file`, `scratch_path`, `scratch_dir` |
| Column-name resolution | `geoparquet_io/core/column_selection.py` | `fold`, `find_column`, `matching_columns`, `unique_name`, `exclude_columns`, `resolve_columns_against_schema` |
| Index add and partition workflows | `geoparquet_io/core/index_ops.py` | `IndexSpec`, `add_index_column`, `ensure_index_column`, `index_expression`, `index_point` |
| HTTP retries | `geoparquet_io/core/http_retry.py` | `make_request_with_retry`, `run_with_retry`, `retry_wait`, `parse_retry_after` |
| CLI options and report formatting | `geoparquet_io/cli/decorators.py`, `geoparquet_io/core/bbox_parse.py`, `geoparquet_io/core/json_output.py`, `geoparquet_io/cli/commands/check.py` | `overwrite_option`, `refuse_together`, `parse_bbox`, `BBox`, `dumps_json`, `MultiFileCheckRunner` |
| Spatial joins | `geoparquet_io/core/duckdb_utils.py`, `geoparquet_io/core/admin_datasets.py` | `build_spatial_join_condition`, `build_spatial_join_query`, `build_admin_subquery`, `admin_match_order` |
| Query schema and row-group statistics | `geoparquet_io/core/duckdb_utils.py`, `geoparquet_io/core/duckdb_metadata.py` | `query_schema`, `describe_query`, `get_row_group_bbox_stats`, `get_row_group_sizes` |
| Error classification and CLI translation | `geoparquet_io/core/exceptions.py`, `geoparquet_io/cli/exception_handler.py` | `classify_failure`, `redact_credentials`, `status_message`, `error_boundary` |
| GDAL format writes | `geoparquet_io/core/format_writers.py` | `write_gdal_format` |
| DuckDB capabilities | `geoparquet_io/core/duckdb_utils.py` | `duckdb_capabilities`, `DuckDBCapabilities`, `complete_without_fetch`, `resolve_load_spatial` |

## Removed and moved names

Each row names the old module and every removed public definition or constant
found against the pre-refactor tree. The replacement column points to the owner;
these are policy migrations, not necessarily argument-for-argument substitutes.
Some former constants now arrive as owner imports; do not rely on those incidental
re-exports. Unused reporting, benchmark and logging helpers were retired outright.

| Old module | Removed or moved names | Replacement |
|---|---|---|
| `benchmarks.config` | `DEFAULT_THRESHOLDS`, `RegressionThresholds` | Retired unused helper; use the supported CLI or public Python API |
| `benchmarks.profile_report` | `format_profile_stats` | Retired unused helper; use the supported CLI or public Python API |
| `cli.commands.add` | `add_country_codes` | Retired unused helper; use the supported CLI or public Python API |
| `cli.decorators` | `handle_geoparquet_errors` | cli.exception_handler.error_boundary |
| `cli.exception_handler` | `F`, `core_exception_handler` | error_boundary / click_error_for |
| `cli.main` | `OptionalIntCommand`, `OptionalIntCommand.make_context` | Retired unused helper; use the supported CLI or public Python API |
| `core.add.bbox_metadata` | `require_geo_metadata_for_covering` | require_geo_metadata_for_cover |
| `core.add.country_codes` | `add_country_codes`, `find_country_code_column`, `find_subdivision_code_column` | add.admin_divisions.add_admin_divisions with the country level |
| `core.admin_datasets` | `AdminDataset.prepare_data_source` | AdminDataset.table_reference |
| `core.arcgis` | `WKID_TO_EPSG` | crs_utils.canonical_crs_code / AUTHORITY_CODE_CRS |
| `core.arrow_geo_metadata` | `describe_wkb_geometry` | geometry_detection and geo_metadata geometry primitives |
| `core.benchmark` | `benchmark_geopandas_fiona`, `benchmark_geopandas_pyogrio` | benchmark_geopandas(engine=) |
| `core.benchmark_report` | `format_comparison_table` | Retired unused helper; use the supported CLI or public Python API |
| `core.benchmark_suite` | `ComparisonResult`, `RegressionStatus`, `compare_results` | Retired unused helper; use the supported CLI or public Python API |
| `core.carto` | `DEFAULT_MAX_RETRIES`, `DEFAULT_RETRY_DELAY`, `DEFAULT_TIMEOUT` | http_retry policy constants |
| `core.check_fixes` | `fix_bboxless_covering`, `get_geoparquet_version_from_check_results` | fix_bboxless_cover; parquet_writer.resolve_output_geoparquet_version(preserve_input=True) |
| `core.check_parquet_structure` | `check_covering_encoding` | check_cover_encoding |
| `core.check_spatial_order` | `check_spatial_order_bbox_stats` | duckdb_metadata.get_row_group_bbox_stats |
| `core.common` | `calculate_file_bounds`, `compute_geometry_dimensions_via_sql`, `compute_geometry_types_via_sql`, `get_dataset_bounds`, `resolve_geoparquet_version_from_file`, `resolve_geoparquet_version_from_table`, `split_zm_suffix`, `zm_suffix_sql` | geo_metadata (statistics) or parquet_writer (version policy) |
| `core.compression` | `resolve_process_compression_level`, `validate_compression_settings` | compression.resolve_codec (context="process" for process defaults) |
| `core.constants` | `DEFAULT_QUADKEY_PARTITION_RESOLUTION`, `DEFAULT_S2_COMPRESSION_LEVEL`, `DEFAULT_S2_TARGET_ROWS`, `FIBOA_CORE_SCHEMA` | Retired unused helper; use the supported CLI or public Python API |
| `core.crs_utils` | `NULL_CRS_NO_FLAG_ERROR`, `WGS84_TRANSFORM_TARGET`, `apply_target_crs_to_geo_meta`, `crs_string_from_geo_meta`, `crs_string_from_table`, `crs_transform_sql_expr`, `extract_crs_from_parquet`, `extract_crs_from_table`, `is_geographic_crs`, `parse_crs_string_to_projjson`, `parse_geo_metadata_from_schema` | crs_utils.crs_of / is_geographic / crs_equal / projjson_for_label; compose_geo_block(crs=) for writes |
| `core.duckdb_metadata` | `find_primary_geometry_column_duckdb`, `get_bbox_from_row_group_stats`, `get_geo_metadata`, `get_native_geo_statistics`, `get_num_row_groups` | geometry_detection.primary_geometry_column; geo_metadata.merged_geometry_stats; duckdb_metadata.get_file_metadata / get_native_geo_stats_by_row_group |
| `core.duckdb_utils` | `free_column_name`, `geoarrow_field_edges`, `has_core_geometry`, `validate_compression_level` | column_selection.unique_name; edges.geoarrow_type_edges; duckdb_capabilities().core_geometry; compression.resolve_codec |
| `core.exceptions` | `sanitize_error_message` | redact_credentials |
| `core.extract` | `is_geographic_crs`, `parse_bbox`, `validate_columns` | crs_utils.is_geographic; bbox_parse.parse_bbox; column_selection.resolve_columns_against_schema |
| `core.extract_bigquery` | `extract_bigquery_table` | Retired unused helper; use the supported CLI or public Python API |
| `core.file_utils` | `safe_file_url` | file_utils.resolve_file_url, then duckdb_utils.sql_path exactly once |
| `core.format_writers` | `ERROR_FILE_EXISTS`, `ERROR_NO_COMPATIBLE_COLUMNS`, `ERROR_NO_GEOMETRY`, `write_format` | write_gdal_format or the format-specific writer |
| `core.geo_metadata` | `GEOGRAPHY_EDGE_ALGORITHMS`, `carried_geometry_column`, `compute_bbox_via_sql`, `compute_geo_stats_via_sql`, `compute_geometry_types_via_sql`, `create_geo_metadata`, `declare_carried_bbox_column`, `decode_carried_geo`, `gate_carried_covering`, `gate_illegal_bbox_covering`, `output_query_schema`, `parse_geo_metadata` | geo_metadata.read_raw_geo / sanitized_carried_geo / compose_geo_block / geometry_stats_of / bounds_of; geometry_detection for identity; duckdb_utils.query_schema for probes |
| `core.geoarrow_encoding` | `detect_wkb_dimensions` | geo_metadata.geoarrow_dimension |
| `core.geodesic_bbox` | `COUNTERCLOCKWISE`, `ELLIPSOIDAL_ALGORITHMS`, `SPHERICAL`, `geodesic_bbox_of_wkb` | edges vocabulary; row_group_geography_bboxes for geodesic bounds |
| `core.geometry_detection` | `detect_geometry_column_from_names`, `detect_parquet_geometry_column`, `find_primary_geometry_column` | geometry_detection.primary_geometry_column / resolve_geometry_column |
| `core.inspect` | `get_primary_geometry_column` | geometry_detection.primary_geometry_column |
| `core.inspect_utils` | `extract_columns_info` | Retired unused helper; use the supported CLI or public Python API |
| `core.logging_config` | `LibraryFormatter`, `verbose_logging` | Retired unused helper; use the supported CLI or public Python API |
| `core.memory_limits` | `get_default_memory_limit`, `parse_size` | sizing.parse_size_string / format_memory_limit; an explicit memory_limit on the connection factory |
| `core.parquet_footer` | `patch_footer`, `patch_footer_kv` | parquet_footer.patch_or_rewrite |
| `core.partition.reader` | `get_files_to_check` | Retired unused helper; use the supported CLI or public Python API |
| `core.process.aggregate.common` | `antimeridian_aware_bbox`, `sql_literal` | geo_metadata (statistics) or parquet_writer (version policy) |
| `core.process.aggregate.grid_common` | `BUCKET_POINT_GEOMETRY` | process.aggregate.options.BUCKET_POINT_GEOMETRY |
| `core.process.overview.detect` | `detect_aggregate_file` | Retired unused helper; use the supported CLI or public Python API |
| `core.process.overview.rollup` | `GRID_PARENT_TEMPLATES`, `GRID_SCHEMES` | index_ops.GRID_SCHEMES / IndexSpec parent template |
| `core.process.simplify` | `strip_stale_geometry_stats` | Retired unused helper; use the supported CLI or public Python API |
| `core.streaming` | `apply_geo_metadata`, `detect_version_for_output`, `extract_crs_from_table`, `extract_geo_metadata`, `extract_version_from_metadata`, `find_geometry_column_from_metadata`, `find_geometry_column_from_table`, `has_geoarrow_extension_in_table`, `is_stdout` | parquet_writer.resolve_output_geoparquet_version; geo_metadata.sanitized_carried_geo / compose_geo_block; crs_utils.crs_of; geometry_detection.primary_geometry_column |
| `core.sub_partition` | `SUB_PARTITION_COLUMN_OPTIONS`, `quadkey_resolution_error` | IndexSpec resolution validation |
| `core.validate` | `ORIENTATION_COUNTERCLOCKWISE`, `VALID_EDGES_GEOPARQUET`, `VALID_EDGES_PARQUET_GEO`, `WKB_TYPE_CODES` | edges vocabulary and geo_metadata geometry-type primitives |
| `core.wfs` | `DEFAULT_TIMEOUT` | http_retry.DEFAULT_TIMEOUT |
| `core.write_strategies.__init__` | `WriteStrategyFactory.list_strategies` | Retired unused helper; use the supported CLI or public Python API |
| `core.write_strategies.arrow_streaming` | `DEFAULT_BATCH_SIZE` | parquet_writer.regroup / sized_stream |
| `core.write_strategies.base` | `atomic_write`, `build_geo_metadata`, `merge_secondary_geometry_metadata` | file_utils.atomic_write; geo_metadata.compose_geo_block |
| `core.write_strategies.duckdb_kv` | `VALID_COMPRESSIONS` | compression.ALLOWED_CODECS |

Names introduced and retired within the refactor also have no shims:
`write_funnels.staged_replacement` and `write_strategies.atomic_write` move to
`file_utils.atomic_write`; `write_funnels.file_relation` moves to
`duckdb_utils.file_relation`; `parquet_writer.declared_geoparquet_version` moves
to `geo_metadata.declared_version`; `CarriedSource`, `auto_version_upgrade`,
`preserved_geoparquet_version`, `edges.restore_edges`
and `column_selection.bound_column` were retired as their policies moved into the
owners. Covering validators and repairs named `*covering*` are now `*cover*`:
that family is reserved for the composer and covering lookup owner.

## Signature and behavior changes

- Version selection uses `resolve_output_geoparquet_version(table=, source_version=,
  file_type=, preserve_input=)`. Unknown explicit versions raise. GeoArrow without a
  `geo` key writes 2.0. `add bbox` upgrades 1.x to 1.1, including pass-through input;
  native geometry with an existing bbox is rewritten at 2.0.
- Compression uses `resolve_codec(name, level, context=)`. Unset COPY ZSTD level is
  15; process ZSTD level is 9. `none`/`uncompressed` mean `UNCOMPRESSED`; `LZ4_RAW`
  is rejected. Byte sizes are decimal for MB/GB, binary for MiB/GiB; bare sizes are MB.
- `WriteOptions` travels whole through write drivers. Aggregate core drivers accept
  `AggregateOptions` through `options=`; public `ops`/`Table` signatures stay stable.
- `repair_arrow_table_geometry` requires `geometry_column`;
  `format_parquet_metadata_enhanced` drops `primary_geom_col`;
  `require_planar_geojson_file` drops `use_primary_geometry`.
  `resolve_geometry_read` receives the source/relation and returns the read expression;
  a missing explicitly named column always errors.
- `crs_of` reads a present block key first, including explicit null, then the GeoArrow
  field only when that key is absent. `projjson_for_label` fails closed.
- Column matching follows DuckDB's ASCII-only fold. Case twins are ambiguous even
  for an exactly spelled request. `detect_geometry_columns` reads root fields only.
- `patch_or_rewrite` takes an injected `rewrite` fallback. Unpatchable STAC metadata
  updates re-encode through the funnel; footer races propagate. The fallback uses
  facade row groups and codec and retains sidecar kv and nullability.
- `file_utils.atomic_write` keeps destination mode (otherwise input mode or umask),
  follows an in-place symlink's target and preserves the link. Backups use `.bak`,
  then `.bak.1`, `.bak.2`, and so on. Scratch directories are private and cleaned up.
- A row group containing only unlocated geometries is merged with a neighbour;
  all-unlocated files are the exception. Every MB resolver measures the same first
  20,000 output rows. This can change group count and byte layout.
- Planar edges are omitted; non-planar values survive Arrow round trips. Index keys
  use the great-circle representative point for non-planar input. Kdtree/admin
  centroids and geometry-metrics areas/lengths remain planar pending maintainer policy.
- `BBox` accepts dateline wrapping only under its geographic rule, and `pieces()`
  splits the query into two lobes. ArcGIS/WFS explicitly refuse unsupported wrapping.
- The composer's geo-block key order is canonical across strategies; streaming writes
  recompute bbox and covering from output data. EMPTY/NULL/NaN do not contribute to
  bounds; infinite coordinates fail before repair or writing invalid JSON.
- Spill directories carry a POSIX ownership lock before the first spill. A sweeper
  cannot remove a live directory just because its pid is absent in another namespace.
  Windows retains pid checks. CARTO metadata and Overture release requests now use
  the shared HTTP retry budget; CARTO retains its per-service timeout.
