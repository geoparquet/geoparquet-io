"""Vulture whitelist (package): the dead-code baseline for guard G4 of #1367.

vulture runs at ``min_confidence = 60`` (``[tool.vulture]`` in pyproject.toml), over the package
alone, so a name only a test calls is still reported. At 80 it reported only
unused imports and arguments, so a function with no caller passed.

Each line is a finding on the tree it was generated from, tagged either
``dynamic: <how it is used>`` (a name used only by a framework, a decorator,
reflection or the public API) or ``dead: <track> deletes``. Reasons for the
baseline as a whole are in ``scripts/owner_exemptions.toml`` (``[G4]``);
``tests/test_owner_guards.py`` pins the entry count and fails on an entry whose
name no longer occurs in its file's AST. Delete a line with the code it names.
Never add a line to make a new finding go away.

This module is never imported; vulture only parses it.
"""

_.__class__  # unused attribute (geoparquet_io/__init__.py:120) -- dynamic: PEP 562 module hook / module class swap
layer_id  # unused variable (geoparquet_io/core/arcgis.py:1660) -- dead: T0e deletes
baseline_time  # unused variable (geoparquet_io/core/benchmark_suite.py:323) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
baseline_rss_memory  # unused variable (geoparquet_io/core/benchmark_suite.py:326) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
issue_severity  # unused variable (geoparquet_io/core/check_bbox_values.py:152) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
checked_rows  # unused variable (geoparquet_io/core/check_bbox_values.py:164) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
undersized_rows  # unused variable (geoparquet_io/core/check_bbox_values.py:167) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
oversized_rows  # unused variable (geoparquet_io/core/check_bbox_values.py:168) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
null_bbox_rows  # unused variable (geoparquet_io/core/check_bbox_values.py:169) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
nan_bbox_rows  # unused variable (geoparquet_io/core/check_bbox_values.py:170) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
unparsable_geometry_rows  # unused variable (geoparquet_io/core/check_bbox_values.py:172) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
absent_geometry_rows  # unused variable (geoparquet_io/core/check_bbox_values.py:174) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
bbox_without_geometry_rows  # unused variable (geoparquet_io/core/check_bbox_values.py:176) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
wrapped_rows  # unused variable (geoparquet_io/core/check_bbox_values.py:178) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
worst_shortfall  # unused variable (geoparquet_io/core/check_bbox_values.py:179) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
worst_excess  # unused variable (geoparquet_io/core/check_bbox_values.py:180) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
undersized_examples  # unused variable (geoparquet_io/core/check_bbox_values.py:181) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
oversized_examples  # unused variable (geoparquet_io/core/check_bbox_values.py:182) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
null_bbox_examples  # unused variable (geoparquet_io/core/check_bbox_values.py:183) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
unparsable_examples  # unused variable (geoparquet_io/core/check_bbox_values.py:184) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
DEFAULT_QUADKEY_PARTITION_RESOLUTION  # unused variable (geoparquet_io/core/constants.py:26) -- dead: T0e deletes
DEFAULT_S2_TARGET_ROWS  # unused variable (geoparquet_io/core/constants.py:38) -- dead: T0e deletes
FIBOA_CORE_SCHEMA  # unused variable (geoparquet_io/core/constants.py:44) -- dead: T0e deletes (no package caller; only tests)
NULL_CRS_NO_FLAG_ERROR  # unused variable (geoparquet_io/core/crs_utils.py:518) -- dead: T0e deletes
ERROR_NO_GEOMETRY  # unused variable (geoparquet_io/core/format_writers.py:58) -- dead: T0e deletes
column_existed  # unused variable (geoparquet_io/core/partition/by_a5.py:214) -- dead: T0e deletes
column_existed  # unused variable (geoparquet_io/core/partition/by_h3.py:214) -- dead: T0e deletes
column_existed  # unused variable (geoparquet_io/core/partition/by_s2.py:274) -- dead: T0e deletes
has_estimate  # unused variable (geoparquet_io/core/preview.py:60) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
stats_source  # unused variable (geoparquet_io/core/preview.py:64) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
est_rows  # unused variable (geoparquet_io/core/preview.py:68) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
est_bytes  # unused variable (geoparquet_io/core/preview.py:70) -- dynamic: field of a dataclass/TypedDict, part of the result's public shape
_.maxstring  # unused attribute (geoparquet_io/core/validate.py:1833) -- dynamic: reprlib.Repr setting, read by reprlib
_.maxdict  # unused attribute (geoparquet_io/core/validate.py:1834) -- dynamic: reprlib.Repr setting, read by reprlib
_.maxlist  # unused attribute (geoparquet_io/core/validate.py:1834) -- dynamic: reprlib.Repr setting, read by reprlib
_.maxother  # unused attribute (geoparquet_io/core/validate.py:1835) -- dynamic: reprlib.Repr setting, read by reprlib
supports_streaming  # unused variable (geoparquet_io/core/write_strategies/arrow_memory.py:42) -- dynamic: write-strategy capability flag of the strategy protocol
supports_remote  # unused variable (geoparquet_io/core/write_strategies/arrow_memory.py:43) -- dynamic: write-strategy capability flag of the strategy protocol
supports_streaming  # unused variable (geoparquet_io/core/write_strategies/arrow_streaming.py:299) -- dynamic: write-strategy capability flag of the strategy protocol
supports_remote  # unused variable (geoparquet_io/core/write_strategies/arrow_streaming.py:300) -- dynamic: write-strategy capability flag of the strategy protocol
supports_streaming  # unused variable (geoparquet_io/core/write_strategies/base.py:399) -- dynamic: write-strategy capability flag of the strategy protocol
supports_remote  # unused variable (geoparquet_io/core/write_strategies/base.py:400) -- dynamic: write-strategy capability flag of the strategy protocol
supports_streaming  # unused variable (geoparquet_io/core/write_strategies/disk_rewrite.py:118) -- dynamic: write-strategy capability flag of the strategy protocol
supports_remote  # unused variable (geoparquet_io/core/write_strategies/disk_rewrite.py:119) -- dynamic: write-strategy capability flag of the strategy protocol
supports_streaming  # unused variable (geoparquet_io/core/write_strategies/duckdb_kv.py:232) -- dynamic: write-strategy capability flag of the strategy protocol
supports_remote  # unused variable (geoparquet_io/core/write_strategies/duckdb_kv.py:233) -- dynamic: write-strategy capability flag of the strategy protocol
__dir__  # unused function (geoparquet_io/__init__.py:90) -- dynamic: PEP 562 module hook / module class swap
reset_crs_disagreement_warnings  # unused function (geoparquet_io/core/crs_utils.py:1128) -- dynamic: test seam that resets module state between tests
reset_malformed_geo_warnings  # unused function (geoparquet_io/core/geo_metadata.py:217) -- dynamic: test seam that resets module state between tests
is_stdout  # unused function (geoparquet_io/core/streaming.py:63) -- dead: T0e deletes (no package caller; only tests)
get_num_row_groups  # unused function (geoparquet_io/core/duckdb_metadata.py:708) -- dead: T0e deletes
_clear_s3_cache  # unused function (geoparquet_io/core/duckdb_utils.py:247) -- dynamic: test seam that resets module state between tests
__getattr__  # unused function (geoparquet_io/core/duckdb_utils.py:1101) -- dynamic: PEP 562 module hook / module class swap
reset_null_crs_warnings  # unused function (geoparquet_io/core/crs_utils.py:604) -- dynamic: test seam that resets module state between tests
_table_geo_column_meta  # unused function (geoparquet_io/core/reproject.py:126) -- dead: T0e deletes (no package caller; only tests)
detect_aggregate_file  # unused function (geoparquet_io/core/process/overview/detect.py:316) -- dead: T0e deletes (no package caller; only tests)
_.to_arrow  # unused method (geoparquet_io/api/table.py:1210) -- dynamic: public Python API (docs/api/python-api.md)
LibraryFormatter  # unused class (geoparquet_io/core/logging_config.py:112) -- dead: T0e deletes (no package caller; only tests)
_.list_strategies  # unused method (geoparquet_io/core/write_strategies/__init__.py:82) -- dead: T0e deletes (no package caller; only tests)
is_azure_url  # unused function (geoparquet_io/core/remote.py:71) -- dead: T0e deletes (no package caller; only tests)
is_gcs_url  # unused function (geoparquet_io/core/remote.py:84) -- dead: T0e deletes (no package caller; only tests)
_.sort_hilbert  # unused method (geoparquet_io/api/table.py:1903) -- dynamic: public Python API (docs/api/python-api.md)
apply_geo_metadata  # unused function (geoparquet_io/core/streaming.py:189) -- dead: T0e deletes (no package caller; only tests)
verbose_logging  # unused function (geoparquet_io/core/logging_config.py:194) -- dead: T0e deletes (no package caller; only tests)
__getattr__  # unused function (geoparquet_io/__init__.py:72) -- dynamic: PEP 562 module hook / module class swap
extract_geo_metadata  # unused function (geoparquet_io/core/streaming.py:171) -- dead: T0e deletes (no package caller; only tests)
sort_hilbert  # unused function (geoparquet_io/api/ops.py:293) -- dynamic: public Python API (docs/api/python-api.md)
sort_column  # unused function (geoparquet_io/api/ops.py:888) -- dynamic: public Python API (docs/api/python-api.md)
extract_columns_info  # unused function (geoparquet_io/core/inspect_utils.py:423) -- dead: T0e deletes (no package caller; only tests)
_.add_h3  # unused method (geoparquet_io/api/table.py:1994) -- dynamic: public Python API (docs/api/python-api.md)
_.add_a5  # unused method (geoparquet_io/api/table.py:2018) -- dynamic: public Python API (docs/api/python-api.md)
_.sort_column  # unused method (geoparquet_io/api/table.py:2362) -- dynamic: public Python API (docs/api/python-api.md)
add_h3  # unused function (geoparquet_io/api/ops.py:389) -- dynamic: public Python API (docs/api/python-api.md)
add_a5  # unused function (geoparquet_io/api/ops.py:415) -- dynamic: public Python API (docs/api/python-api.md)
sort_str  # unused function (geoparquet_io/api/ops.py:313) -- dynamic: public Python API (docs/api/python-api.md)
_.sort_str  # unused method (geoparquet_io/api/table.py:1918) -- dynamic: public Python API (docs/api/python-api.md)
add_quadkey  # unused function (geoparquet_io/api/ops.py:185) -- dynamic: public Python API (docs/api/python-api.md)
_.add_quadkey  # unused method (geoparquet_io/api/table.py:1875) -- dynamic: public Python API (docs/api/python-api.md)
format_comparison_table  # unused function (geoparquet_io/core/benchmark_report.py:33) -- dead: T0e deletes (no package caller; only tests)
get_files_to_check  # unused function (geoparquet_io/core/partition/reader.py:258) -- dead: T0e deletes (no package caller; only tests)
_geo_block_to_carry_on_fast_path  # unused function (geoparquet_io/core/write_funnels.py:944) -- dead: T0e deletes (no package caller; only tests)
sort_quadkey  # unused function (geoparquet_io/api/ops.py:911) -- dynamic: public Python API (docs/api/python-api.md)
_.check_spatial_pushdown  # unused method (geoparquet_io/api/table.py:3277) -- dynamic: public Python API (docs/api/python-api.md)
_.add_s2  # unused method (geoparquet_io/api/table.py:2254) -- dynamic: public Python API (docs/api/python-api.md)
add_bbox_metadata_cmd  # unused function (geoparquet_io/cli/commands/add.py:508) -- dynamic: Click command, registered by its decorator
_.sort_quadkey  # unused method (geoparquet_io/api/table.py:2386) -- dynamic: public Python API (docs/api/python-api.md)
OptionalIntCommand  # unused class (geoparquet_io/cli/main.py:29) -- dead: T0e deletes
add_s2  # unused function (geoparquet_io/api/ops.py:853) -- dynamic: public Python API (docs/api/python-api.md)
convert_to_shapefile  # unused function (geoparquet_io/api/ops.py:2409) -- dynamic: public Python API (docs/api/python-api.md)
check_stac_cmd  # unused function (geoparquet_io/cli/commands/check.py:1561) -- dynamic: Click command, registered by its decorator
convert_to_flatgeobuf  # unused function (geoparquet_io/api/ops.py:2322) -- dynamic: public Python API (docs/api/python-api.md)
_.overview  # unused method (geoparquet_io/api/table.py:2215) -- dynamic: public Python API (docs/api/python-api.md)
geodesic_bbox_of_wkb  # unused function (geoparquet_io/core/geodesic_bbox.py:776) -- dead: T0e deletes (no package caller; only tests)
_.to_geojson  # unused method (geoparquet_io/api/table.py:3069) -- dynamic: public Python API (docs/api/python-api.md)
inspect_head  # unused function (geoparquet_io/cli/commands/inspect.py:243) -- dynamic: Click command, registered by its decorator
inspect_tail  # unused function (geoparquet_io/cli/commands/inspect.py:286) -- dynamic: Click command, registered by its decorator
get_native_geo_statistics  # unused function (geoparquet_io/core/duckdb_metadata.py:1371) -- dead: T0e deletes (no package caller; only tests)
convert_to_geopackage  # unused function (geoparquet_io/api/ops.py:2275) -- dynamic: public Python API (docs/api/python-api.md)
convert_to_csv  # unused function (geoparquet_io/api/ops.py:2362) -- dynamic: public Python API (docs/api/python-api.md)
get_row_group_geo_stats  # unused function (geoparquet_io/api/ops.py:2767) -- dynamic: public Python API (docs/api/python-api.md)
inspect_layers  # unused function (geoparquet_io/cli/commands/inspect.py:405) -- dynamic: Click command, registered by its decorator
calculate_file_bounds  # unused function (geoparquet_io/core/common.py:140) -- dead: T0e deletes (no package caller; only tests)
add_admin_divisions  # unused function (geoparquet_io/api/ops.py:243) -- dynamic: public Python API (docs/api/python-api.md)
add_kdtree  # unused function (geoparquet_io/api/ops.py:803) -- dynamic: public Python API (docs/api/python-api.md)
contour  # unused function (geoparquet_io/api/ops.py:3240) -- dynamic: public Python API (docs/api/python-api.md)
_.polygonize  # unused method (geoparquet_io/api/table.py:811) -- dynamic: public Python API (docs/api/python-api.md)
_.add_kdtree  # unused method (geoparquet_io/api/table.py:2313) -- dynamic: public Python API (docs/api/python-api.md)
polygonize  # unused function (geoparquet_io/api/ops.py:3189) -- dynamic: public Python API (docs/api/python-api.md)
extract_bigquery_table  # unused function (geoparquet_io/core/extract_bigquery.py:363) -- dead: T0e deletes (no package caller; only tests)
publish_stac  # unused function (geoparquet_io/cli/commands/publish.py:202) -- dynamic: Click command, registered by its decorator
add_stac_metadata_cmd  # unused function (geoparquet_io/cli/commands/add.py:540) -- dynamic: Click command, registered by its decorator
_.contour  # unused method (geoparquet_io/api/table.py:860) -- dynamic: public Python API (docs/api/python-api.md)
benchmark_report  # unused function (geoparquet_io/cli/commands/benchmark.py:314) -- dynamic: Click command, registered by its decorator
_.aggregate_a5  # unused method (geoparquet_io/api/table.py:2042) -- dynamic: public Python API (docs/api/python-api.md)
_.aggregate_h3  # unused method (geoparquet_io/api/table.py:2100) -- dynamic: public Python API (docs/api/python-api.md)
_.add_admin_divisions  # unused method (geoparquet_io/api/table.py:3514) -- dynamic: public Python API (docs/api/python-api.md)
format_profile_stats  # unused function (geoparquet_io/benchmarks/profile_report.py:11) -- dead: T0e deletes (no package caller; only tests)
skills  # unused function (geoparquet_io/cli/main.py:133) -- dynamic: Click command, registered by its decorator
partition_by_admin  # unused function (geoparquet_io/api/ops.py:1378) -- dynamic: public Python API (docs/api/python-api.md)
inspect_meta  # unused function (geoparquet_io/cli/commands/inspect.py:343) -- dynamic: Click command, registered by its decorator
aggregate_a5  # unused function (geoparquet_io/api/ops.py:441) -- dynamic: public Python API (docs/api/python-api.md)
aggregate_h3  # unused function (geoparquet_io/api/ops.py:504) -- dynamic: public Python API (docs/api/python-api.md)
compare_results  # unused function (geoparquet_io/core/benchmark_suite.py:332) -- dead: T0e deletes (no package caller; only tests)
convert_flatgeobuf  # unused function (geoparquet_io/cli/commands/convert.py:789) -- dynamic: Click command, registered by its decorator
benchmark_explain  # unused function (geoparquet_io/cli/commands/benchmark.py:246) -- dynamic: Click command, registered by its decorator
convert_shapefile  # unused function (geoparquet_io/cli/commands/convert.py:941) -- dynamic: Click command, registered by its decorator
str_order_command  # unused function (geoparquet_io/cli/commands/sort.py:108) -- dynamic: Click command, registered by its decorator
write_format  # unused function (geoparquet_io/core/format_writers.py:784) -- dead: T0e deletes (no package caller; only tests)
sort_column  # unused function (geoparquet_io/cli/commands/sort.py:177) -- dynamic: Click command, registered by its decorator
_.partition_by_admin  # unused method (geoparquet_io/api/table.py:3817) -- dynamic: public Python API (docs/api/python-api.md)
from_wfs  # unused function (geoparquet_io/api/ops.py:2527) -- dynamic: public Python API (docs/api/python-api.md)
add_h3  # unused function (geoparquet_io/cli/commands/add.py:594) -- dynamic: Click command, registered by its decorator
add_a5  # unused function (geoparquet_io/cli/commands/add.py:669) -- dynamic: Click command, registered by its decorator
add_s2  # unused function (geoparquet_io/cli/commands/add.py:744) -- dynamic: Click command, registered by its decorator
_.from_wfs  # unused method (geoparquet_io/api/table.py:914) -- dynamic: public Python API (docs/api/python-api.md)
from_arcgis  # unused function (geoparquet_io/api/ops.py:2445) -- dynamic: public Python API (docs/api/python-api.md)
from_carto  # unused function (geoparquet_io/api/ops.py:2685) -- dynamic: public Python API (docs/api/python-api.md)
add_quadkey  # unused function (geoparquet_io/cli/commands/add.py:932) -- dynamic: Click command, registered by its decorator
sub_partition_by_s2  # unused function (geoparquet_io/api/ops.py:1884) -- dynamic: public Python API (docs/api/python-api.md)
from_wfs_layers  # unused function (geoparquet_io/api/ops.py:2601) -- dynamic: public Python API (docs/api/python-api.md)
convert_geopackage  # unused function (geoparquet_io/cli/commands/convert.py:705) -- dynamic: Click command, registered by its decorator
check_optimization_cmd  # unused function (geoparquet_io/cli/commands/check.py:1703) -- dynamic: Click command, registered by its decorator
sub_partition_by_a5  # unused function (geoparquet_io/api/ops.py:1697) -- dynamic: public Python API (docs/api/python-api.md)
convert_csv  # unused function (geoparquet_io/cli/commands/convert.py:855) -- dynamic: Click command, registered by its decorator
process_polygonize  # unused function (geoparquet_io/cli/commands/process.py:666) -- dynamic: Click command, registered by its decorator
sub_partition_by_h3  # unused function (geoparquet_io/api/ops.py:1609) -- dynamic: public Python API (docs/api/python-api.md)
add_geometry_metrics_cmd  # unused function (geoparquet_io/cli/commands/add.py:316) -- dynamic: Click command, registered by its decorator
partition_string  # unused function (geoparquet_io/cli/commands/partition.py:190) -- dynamic: Click command, registered by its decorator
process_aggregate_a5  # unused function (geoparquet_io/cli/commands/process.py:234) -- dynamic: Click command, registered by its decorator
process_aggregate_h3  # unused function (geoparquet_io/cli/commands/process.py:324) -- dynamic: Click command, registered by its decorator
sort_quadkey  # unused function (geoparquet_io/cli/commands/sort.py:249) -- dynamic: Click command, registered by its decorator
benchmark_compare  # unused function (geoparquet_io/cli/commands/benchmark.py:30) -- dynamic: Click command, registered by its decorator
convert_reproject  # unused function (geoparquet_io/cli/commands/convert.py:453) -- dynamic: Click command, registered by its decorator
sub_partition_by_quadkey  # unused function (geoparquet_io/api/ops.py:1783) -- dynamic: public Python API (docs/api/python-api.md)
check_compression_cmd  # unused function (geoparquet_io/cli/commands/check.py:1159) -- dynamic: Click command, registered by its decorator
check_spec  # unused function (geoparquet_io/cli/commands/check.py:1598) -- dynamic: Click command, registered by its decorator
process_aggregate_admin  # unused function (geoparquet_io/cli/commands/process.py:414) -- dynamic: Click command, registered by its decorator
create_pmtiles  # unused function (geoparquet_io/api/ops.py:2930) -- dynamic: public Python API (docs/api/python-api.md)
check_row_group_cmd  # unused function (geoparquet_io/cli/commands/check.py:1451) -- dynamic: Click command, registered by its decorator
process_contour  # unused function (geoparquet_io/cli/commands/process.py:752) -- dynamic: Click command, registered by its decorator
add_kdtree  # unused function (geoparquet_io/cli/commands/add.py:819) -- dynamic: Click command, registered by its decorator
publish_upload  # unused function (geoparquet_io/cli/commands/publish.py:255) -- dynamic: Click command, registered by its decorator
benchmark_suite  # unused function (geoparquet_io/cli/commands/benchmark.py:123) -- dynamic: Click command, registered by its decorator
process_simplify  # unused function (geoparquet_io/cli/commands/process.py:522) -- dynamic: Click command, registered by its decorator
partition_kdtree  # unused function (geoparquet_io/cli/commands/partition.py:793) -- dynamic: Click command, registered by its decorator
partition_admin  # unused function (geoparquet_io/cli/commands/partition.py:49) -- dynamic: Click command, registered by its decorator
pmtiles_pyramid  # unused function (geoparquet_io/cli/commands/pmtiles.py:203) -- dynamic: Click command, registered by its decorator
_.from_bigquery  # unused method (geoparquet_io/api/table.py:666) -- dynamic: public Python API (docs/api/python-api.md)
process_overview  # unused function (geoparquet_io/cli/commands/process.py:69) -- dynamic: Click command, registered by its decorator
pmtiles_create  # unused function (geoparquet_io/cli/commands/pmtiles.py:49) -- dynamic: Click command, registered by its decorator
convert_geojson  # unused function (geoparquet_io/cli/commands/convert.py:550) -- dynamic: Click command, registered by its decorator
partition_a5  # unused function (geoparquet_io/cli/commands/partition.py:624) -- dynamic: Click command, registered by its decorator
partition_h3  # unused function (geoparquet_io/cli/commands/partition.py:280) -- dynamic: Click command, registered by its decorator
partition_s2  # unused function (geoparquet_io/cli/commands/partition.py:451) -- dynamic: Click command, registered by its decorator
extract_carto_cmd  # unused function (geoparquet_io/cli/commands/extract.py:1187) -- dynamic: Click command, registered by its decorator
check_bbox_cmd  # unused function (geoparquet_io/cli/commands/check.py:1269) -- dynamic: Click command, registered by its decorator
extract_geoparquet  # unused function (geoparquet_io/cli/commands/extract.py:72) -- dynamic: Click command, registered by its decorator
convert_to_geoparquet_cmd  # unused function (geoparquet_io/cli/commands/convert.py:127) -- dynamic: Click command, registered by its decorator
partition_quadkey  # unused function (geoparquet_io/cli/commands/partition.py:929) -- dynamic: Click command, registered by its decorator
extract_bigquery_cmd  # unused function (geoparquet_io/cli/commands/extract.py:590) -- dynamic: Click command, registered by its decorator
extract_wfs_cmd  # unused function (geoparquet_io/cli/commands/extract.py:851) -- dynamic: Click command, registered by its decorator
a5_from_query_farm  # unused variable (geoparquet_io/core/duckdb_utils.py:415) -- dynamic: field of the frozen DuckDBCapabilities, no reader yet by design (#1360 reads it)
