"""
Tests to ensure all GeoParquet writing operations produce properly formatted output.
These tests verify that all commands produce files that pass 'gpio check all' standards.
"""

import json
import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.common import (
    check_bbox_structure,
    get_parquet_metadata,
    parse_geo_metadata,
)

# ============================================================================
# CENTRAL FORMAT REQUIREMENTS
# Update these constants to change format requirements across all tests
# ============================================================================

DEFAULT_GEOPARQUET_VERSION = "1.1.0"
DEFAULT_COMPRESSION = "ZSTD"
DEFAULT_COMPRESSION_LEVEL = 15
REQUIRE_BBOX_METADATA_WHEN_COLUMN_EXISTS = True
REQUIRE_PRIMARY_COLUMN_IN_METADATA = True


# ============================================================================
# COMMON VALIDATION FUNCTIONALITY
# ============================================================================


def validate_output_format(
    parquet_file, expected_compression=None, expect_bbox=None, custom_checks=None, verbose=False
):
    """
    Common validation function for all GeoParquet output tests.

    This is the single source of truth for what constitutes properly formatted output.
    Update this function to change format requirements across all tests.

    Args:
        parquet_file: Path to the parquet file to validate
        expected_compression: Override default compression expectation
        expect_bbox: True = must have bbox, False = must not have bbox, None = don't check
        custom_checks: Optional dict of additional checks to perform
        verbose: Print detailed validation info

    Returns:
        Dict with validation results

    Raises:
        AssertionError: If any validation fails
    """
    # Use defaults if not specified
    if expected_compression is None:
        expected_compression = DEFAULT_COMPRESSION

    results = {
        "file": parquet_file,
        "version": None,
        "compression": None,
        "bbox_status": None,
        "passed": True,
        "errors": [],
    }

    # Check file exists
    assert os.path.exists(parquet_file), f"Output file not found: {parquet_file}"

    # Get metadata
    metadata, schema = get_parquet_metadata(parquet_file, verbose)
    geo_meta = parse_geo_metadata(metadata, verbose)

    # =========================
    # 1. GEOPARQUET VERSION CHECK
    # =========================
    if geo_meta:
        actual_version = geo_meta.get("version", "unknown")
        results["version"] = actual_version

        if actual_version != DEFAULT_GEOPARQUET_VERSION:
            error_msg = (
                f"Expected GeoParquet version {DEFAULT_GEOPARQUET_VERSION}, got {actual_version}"
            )
            results["errors"].append(error_msg)
            raise AssertionError(error_msg)
    else:
        error_msg = "No GeoParquet metadata found"
        results["errors"].append(error_msg)
        raise AssertionError(error_msg)

    # =========================
    # 2. COMPRESSION CHECK
    # =========================
    pf = pq.ParquetFile(parquet_file)
    if pf.num_row_groups > 0:
        row_group = pf.metadata.row_group(0)
        if row_group.num_columns > 0:
            # Find geometry column
            geom_col_name = geo_meta.get("primary_column", "geometry")
            geom_col_idx = None
            for i in range(len(schema)):
                if schema.field(i).name == geom_col_name:
                    geom_col_idx = i
                    break

            if geom_col_idx is not None:
                col_meta = row_group.column(geom_col_idx)
                actual_compression = str(col_meta.compression).upper()

                # Normalize compression names
                compression_map = {
                    "ZSTD": "ZSTD",
                    "GZIP": "GZIP",
                    "BROTLI": "BROTLI",
                    "LZ4": "LZ4",
                    "SNAPPY": "SNAPPY",
                    "UNCOMPRESSED": "UNCOMPRESSED",
                }

                for key in compression_map:
                    if key in actual_compression:
                        actual_compression = compression_map[key]
                        break

                results["compression"] = actual_compression

                if actual_compression != expected_compression:
                    error_msg = (
                        f"Expected {expected_compression} compression, got {actual_compression}"
                    )
                    results["errors"].append(error_msg)
                    raise AssertionError(error_msg)

    # =========================
    # 3. BBOX STRUCTURE CHECK
    # =========================
    bbox_info = check_bbox_structure(parquet_file, verbose)
    results["bbox_status"] = bbox_info["status"]

    if expect_bbox is not None:
        if expect_bbox:
            # Must have bbox column and metadata
            if not bbox_info["has_bbox_column"]:
                error_msg = "Expected bbox column but none found"
                results["errors"].append(error_msg)
                raise AssertionError(error_msg)

            if REQUIRE_BBOX_METADATA_WHEN_COLUMN_EXISTS and not bbox_info["has_bbox_metadata"]:
                error_msg = "Expected bbox metadata but none found"
                results["errors"].append(error_msg)
                raise AssertionError(error_msg)

            if bbox_info["status"] != "optimal":
                error_msg = f"Expected optimal bbox status, got {bbox_info['status']}: {bbox_info['message']}"
                results["errors"].append(error_msg)
                raise AssertionError(error_msg)
        else:
            # Must NOT have bbox column
            if bbox_info["has_bbox_column"]:
                error_msg = "Expected no bbox column but found one"
                results["errors"].append(error_msg)
                raise AssertionError(error_msg)

    # =========================
    # 4. METADATA STRUCTURE CHECK
    # =========================
    if REQUIRE_PRIMARY_COLUMN_IN_METADATA and geo_meta and "columns" in geo_meta:
        primary_col = geo_meta.get("primary_column", "geometry")
        if primary_col not in geo_meta["columns"]:
            error_msg = f"Primary column '{primary_col}' not found in geo metadata columns"
            results["errors"].append(error_msg)
            raise AssertionError(error_msg)

    # =========================
    # 5. VERSION-SPECIFIC CHECKS
    # =========================
    if DEFAULT_GEOPARQUET_VERSION == "1.1.0" and geo_meta:
        required_fields = ["version", "primary_column", "columns"]
        for field in required_fields:
            if field not in geo_meta:
                error_msg = f"Missing required field '{field}' in geo metadata for version 1.1.0"
                results["errors"].append(error_msg)
                raise AssertionError(error_msg)

    # =========================
    # 6. CUSTOM CHECKS
    # =========================
    if custom_checks:
        for check_name, check_func in custom_checks.items():
            try:
                check_func(parquet_file, metadata, geo_meta, results)
            except AssertionError as e:
                error_msg = f"Custom check '{check_name}' failed: {str(e)}"
                results["errors"].append(error_msg)
                raise

    if verbose:
        print(f"Validation passed for {parquet_file}")
        print(f"  Version: {results['version']}")
        print(f"  Compression: {results['compression']}")
        print(f"  Bbox status: {results['bbox_status']}")

    return results


def run_command_and_validate(
    cli_args, expected_compression=None, expect_bbox=None, custom_checks=None, skip_on_error=False
):
    """
    Helper to run a CLI command and validate its output.

    Args:
        cli_args: List of CLI arguments
        expected_compression: Expected compression (uses default if None)
        expect_bbox: Whether to expect bbox column/metadata
        custom_checks: Additional validation checks
        skip_on_error: Skip test if command fails (for network-dependent tests)

    Returns:
        Validation results dict
    """
    runner = CliRunner()

    # Find output file in args (assumes it's the last .parquet argument)
    output_file = None
    for arg in reversed(cli_args):
        if arg.endswith(".parquet"):
            output_file = arg
            break

    result = runner.invoke(cli, cli_args)

    if result.exit_code != 0:
        if skip_on_error:
            pytest.skip(f"Command failed (possibly network issue): {result.output}")
        else:
            raise AssertionError(
                f"Command failed with exit code {result.exit_code}: {result.output}"
            )

    # Validate the output
    return validate_output_format(
        output_file,
        expected_compression=expected_compression,
        expect_bbox=expect_bbox,
        custom_checks=custom_checks,
    )


# ============================================================================
# TEST FIXTURES
# ============================================================================


@pytest.fixture
def sample_parquet():
    """Path to sample test parquet file."""
    return "tests/data/buildings_test.parquet"


@pytest.fixture
def temp_output():
    """Create a temporary output file that gets cleaned up."""
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.close()
    yield tmp.name
    if os.path.exists(tmp.name):
        os.unlink(tmp.name)


@pytest.fixture
def temp_dir():
    """Create a temporary directory that gets cleaned up."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


# ============================================================================
# TESTS FOR EACH COMMAND
# ============================================================================


class TestHilbertSort:
    """Test hilbert sort output format."""

    def test_default_format(self, sample_parquet, temp_output):
        """Test default hilbert sort format."""
        run_command_and_validate(
            ["sort", "hilbert", sample_parquet, temp_output], expect_bbox=False
        )

    def test_with_bbox(self, sample_parquet, temp_output):
        """Test hilbert sort with bbox."""
        run_command_and_validate(
            ["sort", "hilbert", sample_parquet, temp_output, "--add-bbox"], expect_bbox=True
        )

    def test_custom_compression(self, sample_parquet, temp_output):
        """Test hilbert sort with custom compression."""
        run_command_and_validate(
            [
                "sort",
                "hilbert",
                sample_parquet,
                temp_output,
                "--compression",
                "zstd",
                "--compression-level",
                "15",
            ],
            expected_compression="ZSTD",
            expect_bbox=False,
        )

    def test_row_groups(self, sample_parquet, temp_output):
        """Test hilbert sort with custom row groups."""

        def check_row_groups(parquet_file, metadata, geo_meta, results):
            pf = pq.ParquetFile(parquet_file)
            # With 42 rows and size 20, expect at least 2 groups
            assert pf.num_row_groups >= 2, f"Expected multiple row groups, got {pf.num_row_groups}"

        run_command_and_validate(
            ["sort", "hilbert", sample_parquet, temp_output, "--row-group-size", "20"],
            custom_checks={"row_groups": check_row_groups},
        )


class TestAddBbox:
    """Test add bbox output format."""

    def test_default_format(self, sample_parquet, temp_output):
        """Test default add bbox format."""
        run_command_and_validate(["add", "bbox", sample_parquet, temp_output], expect_bbox=True)

    def test_custom_compression(self, sample_parquet, temp_output):
        """Test add bbox with custom compression."""
        run_command_and_validate(
            [
                "add",
                "bbox",
                sample_parquet,
                temp_output,
                "--compression",
                "zstd",
                "--compression-level",
                "15",
            ],
            expected_compression="ZSTD",
            expect_bbox=True,
        )

    def test_custom_bbox_name(self, sample_parquet, temp_output):
        """Test add bbox with custom column name."""

        def check_bbox_name(parquet_file, metadata, geo_meta, results):
            bbox_info = check_bbox_structure(parquet_file)
            assert bbox_info["bbox_column_name"] == "bounds", (
                f"Expected bbox column 'bounds', got {bbox_info['bbox_column_name']}"
            )

        run_command_and_validate(
            ["add", "bbox", sample_parquet, temp_output, "--bbox-name", "bounds"],
            expect_bbox=True,
            custom_checks={"bbox_name": check_bbox_name},
        )


class TestAddAdminDivisions:
    """Test add admin-divisions output format."""

    def test_dry_run_format(self, sample_parquet, temp_output):
        """Test that dry-run validates command structure."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["add", "admin-divisions", sample_parquet, temp_output, "--dry-run"]
        )
        assert result.exit_code == 0, f"Dry-run failed: {result.output}"
        assert "DRY RUN MODE" in result.output

    @pytest.mark.slow
    @pytest.mark.network
    def test_default_format(self, sample_parquet, temp_output):
        """Test default admin-divisions format (requires network)."""
        run_command_and_validate(
            ["add", "admin-divisions", sample_parquet, temp_output],
            expect_bbox=False,
            skip_on_error=True,
        )


class TestPartition:
    """Test partition output format."""

    def test_string_partition_format(self, temp_dir):
        """Test that partition produces proper format."""
        # Create test input
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_input:
            tmp_input_name = tmp_input.name

        try:
            # Create simple test data with valid WKB
            # WKB for POINT(0 0): 0101000000 00000000 00000000 00000000 00000000
            wkb_point = bytes.fromhex("0101000000000000000000000000000000000000000000000000000000")
            table = pa.table(
                {
                    "id": ["1", "2", "3", "4"],
                    "category": ["A", "A", "B", "B"],
                    "geometry": [wkb_point] * 4,
                }
            )

            # Add minimal geo metadata
            metadata = {
                b"geo": json.dumps(
                    {
                        "version": "1.0.0",
                        "primary_column": "geometry",
                        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
                    }
                ).encode("utf-8")
            }

            table = table.replace_schema_metadata(metadata)
            pq.write_table(table, tmp_input_name)

            # Run partition command
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "partition",
                    "string",
                    tmp_input_name,
                    temp_dir,
                    "--column",
                    "category",
                    "--skip-analysis",
                ],
            )

            assert result.exit_code == 0, f"Partition failed: {result.output}"

            # Validate each partition file
            for partition_file in ["A.parquet", "B.parquet"]:
                partition_path = os.path.join(temp_dir, partition_file)
                if os.path.exists(partition_path):
                    validate_output_format(partition_path, expect_bbox=False)

        finally:
            if os.path.exists(tmp_input_name):
                os.unlink(tmp_input_name)


class TestAllCommandsConsistency:
    """Test consistency across all commands."""

    def test_all_produce_correct_version(self, sample_parquet):
        """Ensure all commands produce the configured GeoParquet version."""
        test_commands = [
            ["sort", "hilbert"],
            ["add", "bbox"],
        ]

        for cmd_base in test_commands:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_name = tmp.name

            try:
                run_command_and_validate(
                    cmd_base + [sample_parquet, tmp_name],
                    expect_bbox=None,  # Don't check bbox, just version/compression
                )
            finally:
                if os.path.exists(tmp_name):
                    os.unlink(tmp_name)

    def test_compression_options_work(self, sample_parquet):
        """Test that all compression options work correctly."""
        compressions = [
            ("zstd", "ZSTD"),
            ("gzip", "GZIP"),
            ("brotli", "BROTLI"),
            ("lz4", "LZ4"),
            ("snappy", "SNAPPY"),
        ]

        for compression_arg, expected_compression in compressions:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_name = tmp.name

            try:
                run_command_and_validate(
                    [
                        "sort",
                        "hilbert",
                        sample_parquet,
                        tmp_name,
                        "--compression",
                        compression_arg,
                    ],
                    expected_compression=expected_compression,
                )
            finally:
                if os.path.exists(tmp_name):
                    os.unlink(tmp_name)


# ============================================================================
# RUN AS SCRIPT
# ============================================================================

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
