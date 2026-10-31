"""Vulture whitelist (tests): the dead-code baseline for guard G4 of #1367.

vulture runs at ``min_confidence = 60`` (the ``vulture-tests`` pre-push hook), over the test tree
alone. At 80 it reported only
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

style  # unused variable (tests/docs_examples/collector.py:173) -- dynamic: parameter of a stub that mirrors the real signature
MOCK_FEATURE_COUNT  # unused variable (tests/test_arcgis.py:39) -- dead: T0e deletes
_.access_key  # unused attribute (tests/test_aws_credentials.py:195) -- dynamic: method of a stub, called by the code under test
_.secret_key  # unused attribute (tests/test_aws_credentials.py:196) -- dynamic: method of a stub, called by the code under test
_.is_multi_file  # unused attribute (tests/test_check_cli_guards.py:183) -- dynamic: method of a stub, called by the code under test
ARCGIS_URL  # unused variable (tests/test_core_column_list_guard.py:129) -- dead: T0e deletes
max_y  # unused variable (tests/test_crs_conversion.py:883) -- dynamic: field of a test record or stub
min_y  # unused variable (tests/test_crs_conversion.py:883) -- dynamic: field of a test record or stub
_.complete  # unused attribute (tests/test_duckdb_fetch_one.py:71) -- dynamic: method of a stub, called by the code under test
table_source  # unused variable (tests/test_extract_bigquery.py:1047) -- dynamic: parameter of a stub that mirrors the real signature
table_source  # unused variable (tests/test_extract_bigquery.py:2403) -- dynamic: parameter of a stub that mirrors the real signature
extension_metadata  # unused variable (tests/test_geoarrow_crs_reader.py:126) -- dynamic: field of a test record or stub
_.extension_metadata  # unused attribute (tests/test_geoarrow_crs_reader.py:235) -- dynamic: method of a stub, called by the code under test
repair_geometry  # unused variable (tests/test_geojson_native_writer.py:341) -- dynamic: parameter of a stub that mirrors the real signature
_.wkb_col  # unused attribute (tests/test_linearize.py:821) -- dynamic: method of a stub, called by the code under test
_.linearized  # unused attribute (tests/test_linearize.py:823) -- dynamic: method of a stub, called by the code under test
_._reader  # unused attribute (tests/test_linearize.py:825) -- dynamic: method of a stub, called by the code under test
spatial_index_type  # unused variable (tests/test_process_aggregate_where.py:175) -- dynamic: parameter of a stub that mirrors the real signature
partition_marks  # unused variable (tests/test_spatial_index_family.py:185) -- dynamic: field of a test record or stub
auto_marks  # unused variable (tests/test_spatial_index_family.py:186) -- dynamic: field of a test record or stub
declared_types  # unused variable (tests/test_validate_malformed_wkb.py:199) -- dynamic: parameter of a stub that mirrors the real signature
chunk_size  # unused variable (tests/test_wfs.py:1766) -- dynamic: parameter of a stub that mirrors the real signature
extract_fid  # unused variable (tests/test_wfs.py:2554) -- dynamic: parameter of a stub that mirrors the real signature
extract_fid  # unused variable (tests/test_wfs.py:2637) -- dynamic: parameter of a stub that mirrors the real signature
chunk_size  # unused variable (tests/test_wfs.py:3546) -- dynamic: parameter of a stub that mirrors the real signature
_.log_message  # unused method (tests/test_aws_profile_no_env_leak.py:253) -- dynamic: override called by the base class or the code under test
_.log_message  # unused method (tests/test_carto.py:988) -- dynamic: override called by the base class or the code under test
_.log_message  # unused method (tests/test_file_utils.py:580) -- dynamic: override called by the base class or the code under test
_.log_message  # unused method (tests/test_file_utils.py:957) -- dynamic: override called by the base class or the code under test
_.configure_s3  # unused method (tests/test_process_aggregate_breakdown_metric.py:333) -- dynamic: method of a stub, called by the code under test
_.configure_s3  # unused method (tests/test_process_aggregate_bucket_point.py:737) -- dynamic: method of a stub, called by the code under test
_.configure_s3  # unused method (tests/test_process_aggregate_column_validation.py:172) -- dynamic: method of a stub, called by the code under test
_.configure_s3  # unused method (tests/test_process_aggregate_metric_nodata.py:514) -- dynamic: method of a stub, called by the code under test
_.log_message  # unused method (tests/test_remote_url_encoding.py:140) -- dynamic: override called by the base class or the code under test
_.iter_bytes  # unused method (tests/test_wfs.py:1766) -- dynamic: method of a stub, called by the code under test
_.iter_bytes  # unused method (tests/test_wfs.py:3546) -- dynamic: method of a stub, called by the code under test
_.reportinfo  # unused method (tests/docs_examples/collector.py:178) -- dynamic: pytest hook / collector protocol method
_.no_psutil  # unused method (tests/test_copy_memory_limit.py:393) -- dynamic: pytest fixture, requested by name
spec_order_v1_1  # unused function (tests/test_covering_bbox_field_order.py:110) -- dynamic: pytest fixture, requested by name
_._import_module  # unused method (tests/test_doc_sync.py:251) -- dynamic: pytest fixture, requested by name
_._import_module  # unused method (tests/test_doc_sync.py:320) -- dynamic: pytest fixture, requested by name
_._query  # unused method (tests/test_duckdb_utils.py:678) -- dynamic: method of a stub, called by the code under test
noisy  # unused function (tests/test_cli_error_boundary.py:371) -- dynamic: Click command, registered by its decorator
no_pytz  # unused function (tests/test_convert_geojson_timestamps.py:25) -- dynamic: pytest fixture, requested by name
_fresh_warnings  # unused function (tests/test_covering_bbox_field_order.py:135) -- dynamic: pytest fixture, requested by name
_._import_module  # unused method (tests/test_doc_sync.py:105) -- dynamic: pytest fixture, requested by name
_._import_module  # unused method (tests/test_doc_sync.py:155) -- dynamic: pytest fixture, requested by name
_._import_module  # unused method (tests/test_doc_sync.py:193) -- dynamic: pytest fixture, requested by name
no_pytz  # unused function (tests/test_timestamptz_without_pytz.py:31) -- dynamic: pytest fixture, requested by name
_._no_ambient_credentials  # unused method (tests/test_upload.py:347) -- dynamic: pytest fixture, requested by name
mock_geojson_page_1  # unused function (tests/test_wfs.py:495) -- dynamic: pytest fixture, requested by name
mock_geojson_page_2  # unused function (tests/test_wfs.py:501) -- dynamic: pytest fixture, requested by name
mock_geojson_page_3  # unused function (tests/test_wfs.py:507) -- dynamic: pytest fixture, requested by name
mock_minimal_capabilities_xml  # unused function (tests/test_wfs.py:513) -- dynamic: pytest fixture, requested by name
_isolate_package_logging  # unused function (tests/conftest.py:169) -- dynamic: pytest fixture, requested by name
_isolate_package_caches  # unused function (tests/conftest.py:250) -- dynamic: pytest fixture, requested by name
pytest_runtest_setup  # unused function (tests/conftest.py:857) -- dynamic: pytest hook / collector protocol method
session_tz  # unused function (tests/test_convert_geojson_timestamps.py:31) -- dynamic: pytest fixture, requested by name
native_parquet  # unused function (tests/test_geoarrow_native.py:26) -- dynamic: pytest fixture, requested by name
_fresh_warnings  # unused function (tests/test_parquet_geo_only_secondary_crs.py:41) -- dynamic: pytest fixture, requested by name
_.stub_reader  # unused method (tests/test_process_polygonize_validity.py:241) -- dynamic: pytest fixture, requested by name
_.stub_reader_cli  # unused method (tests/test_process_polygonize_validity.py:292) -- dynamic: pytest fixture, requested by name
_._clean_s3_env  # unused method (tests/test_remote_files.py:359) -- dynamic: pytest fixture, requested by name
_fresh_warnings  # unused function (tests/test_unknown_crs_reads.py:44) -- dynamic: pytest fixture, requested by name
undeclared_bbox_file  # unused function (tests/test_check_fix_inplace_handles.py:167) -- dynamic: pytest fixture, requested by name
_fresh_warning_cache  # unused function (tests/test_crs_source_disagreement.py:104) -- dynamic: pytest fixture, requested by name
_.forbid_native  # unused method (tests/test_geojson_native_writer.py:601) -- dynamic: pytest fixture, requested by name
_clear_overture_cache  # unused function (tests/test_overture_release.py:11) -- dynamic: pytest fixture, requested by name
DocExampleFile  # unused class (tests/docs_examples/collector.py:78) -- dead: T0e deletes
_.geo_table  # unused method (tests/test_streaming.py:121) -- dynamic: pytest fixture, requested by name
tiny_row_group_file  # unused function (tests/test_check_fix_inplace_handles.py:175) -- dynamic: pytest fixture, requested by name
pytest_runtest_call  # unused function (tests/conftest.py:846) -- dynamic: pytest hook / collector protocol method
_.null_write_scope  # unused method (tests/test_geojson_native_writer.py:314) -- dynamic: pytest fixture, requested by name
fake_country_cache  # unused function (tests/test_process_overview.py:79) -- dynamic: pytest fixture, requested by name
v1_input  # unused function (tests/test_v2_primary_column_survives_duckdb.py:78) -- dynamic: pytest fixture, requested by name
_.runtest  # unused method (tests/docs_examples/collector.py:117) -- dynamic: pytest hook / collector protocol method
fake_country_cache  # unused function (tests/test_pmtiles_pyramid.py:218) -- dynamic: pytest fixture, requested by name
MockCapabilities  # unused class (tests/test_wfs.py:724) -- dead: T0e deletes
_rewrite_covering  # unused function (tests/test_covering_write_path_parity.py:496) -- dead: T0e deletes
spatial_unavailable  # unused function (tests/test_inspect_core_geometry.py:72) -- dynamic: pytest fixture, requested by name
_.setup_logging  # unused method (tests/test_partition_auto_resolution.py:239) -- dynamic: pytest fixture, requested by name
_extract_json_from_output  # unused function (tests/conftest.py:721) -- dead: T0e deletes
_.setup_logging  # unused method (tests/test_logging_config.py:181) -- dynamic: pytest fixture, requested by name
_.setup_logging  # unused method (tests/test_partition_auto_resolution.py:307) -- dynamic: pytest fixture, requested by name
_poisoned_worker  # unused function (tests/test_cache_state_isolation.py:154) -- dynamic: pytest fixture, requested by name
fake_admin_dataset  # unused function (tests/test_process_aggregate_column_validation.py:179) -- dynamic: pytest fixture, requested by name
MockLayerInfo  # unused class (tests/test_wfs.py:704) -- dead: T0e deletes
fake_admin_dataset  # unused function (tests/test_process_aggregate_metric_nodata.py:521) -- dynamic: pytest fixture, requested by name
mock_wfs_responses  # unused function (tests/test_wfs.py:395) -- dynamic: pytest fixture, requested by name
restore_logging_state  # unused function (tests/test_library_state_leaks.py:38) -- dynamic: pytest fixture, requested by name
_poison_before_any_test_in_this_module  # unused function (tests/test_logging_state_isolation.py:170) -- dynamic: pytest fixture, requested by name
fake_aws_files  # unused function (tests/test_aws_profile_no_env_leak.py:211) -- dynamic: pytest fixture, requested by name
offline_wfs_probes  # unused function (tests/test_wfs.py:46) -- dynamic: pytest fixture, requested by name
is_v2  # unused attribute (tests/test_duckdb_capabilities.py:146) -- dynamic: assigned to prove the dataclass is frozen
native_writer_on  # unused function (tests/test_geojson_native_writer.py:104) -- dynamic: pytest fixture used via usefixtures
_.do_PUT  # unused method (tests/_fake_s3.py:65) -- dynamic: http.server dispatches do_<COMMAND> by name (HANDOFFS row 43 test)
