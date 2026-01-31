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
from geoparquet_io.core.common import check_bbox_structure

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

    # Get metadata using PyArrow directly (not DuckDB) to avoid duplicate key issues
    # DuckDB's parquet_kv_metadata can return duplicate keys, with the old value
    # overwriting the new value when converted to a Python dict. PyArrow returns
    # only the most recent value for each key.
    pf = pq.ParquetFile(parquet_file)
    schema = pf.schema_arrow
    metadata = schema.metadata or {}
    geo_meta = None
    if b"geo" in metadata:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))

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
    """Create a temporary output path that gets cleaned up."""
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.close()
    # Remove the file so tests don't trigger overwrite checks
    os.unlink(tmp.name)
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
            ["sort", "hilbert", sample_parquet, temp_output, "--geoparquet-version", "1.1"],
            expect_bbox=False,
        )

    def test_with_bbox(self, sample_parquet, temp_output):
        """Test hilbert sort with bbox."""
        run_command_and_validate(
            [
                "sort",
                "hilbert",
                sample_parquet,
                temp_output,
                "--add-bbox",
                "--geoparquet-version",
                "1.1",
            ],
            expect_bbox=True,
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
                "--geoparquet-version",
                "1.1",
            ],
            expected_compression="ZSTD",
            expect_bbox=False,
        )

    def test_row_groups(self, temp_dir, temp_output):
        """Test hilbert sort with custom row groups.

        Note: DuckDB's ROW_GROUP_SIZE is a hint, not a strict limit.
        DuckDB only starts splitting into multiple row groups once the dataset
        exceeds a certain size threshold (~5000 rows). We create a sufficiently
        large synthetic file to reliably trigger multiple row groups.
        """
        # Create a synthetic file with enough rows to reliably split into multiple groups
        # DuckDB needs at least ~5000+ rows to actually create multiple row groups
        import pyarrow as pa

        # Create 10000 rows to ensure DuckDB creates multiple row groups
        wkb_point = bytes.fromhex("0101000000000000000000000000000000000000000000000000000000")
        n_rows = 10000
        table = pa.table(
            {
                "id": [str(i) for i in range(n_rows)],
                "name": [f"feature_{i}" for i in range(n_rows)],
                "geometry": [wkb_point] * n_rows,
            }
        )

        # Add geo metadata with version 1.1.0
        metadata = {
            b"geo": json.dumps(
                {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
                }
            ).encode("utf-8")
        }

        tmp_input = os.path.join(temp_dir, "test_input.parquet")
        table = table.replace_schema_metadata(metadata)
        pq.write_table(table, tmp_input)

        def check_row_groups(parquet_file, metadata, geo_meta, results):
            pf = pq.ParquetFile(parquet_file)
            # With 10000 rows and row-group-size 100, expect at least 2 groups
            # DuckDB uses ROW_GROUP_SIZE as a hint combined with data size factors
            assert pf.num_row_groups >= 2, f"Expected multiple row groups, got {pf.num_row_groups}"

        run_command_and_validate(
            [
                "sort",
                "hilbert",
                tmp_input,
                temp_output,
                "--row-group-size",
                "100",
                "--geoparquet-version",
                "1.1",
            ],
            custom_checks={"row_groups": check_row_groups},
        )


class TestAddBbox:
    """Test add bbox output format."""

    def test_default_format(self, sample_parquet, temp_output):
        """Test default add bbox format."""
        run_command_and_validate(
            ["add", "bbox", sample_parquet, temp_output, "--geoparquet-version", "1.1"],
            expect_bbox=True,
        )

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
                "--geoparquet-version",
                "1.1",
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
            [
                "add",
                "bbox",
                sample_parquet,
                temp_output,
                "--bbox-name",
                "bounds",
                "--geoparquet-version",
                "1.1",
            ],
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

            # Add geo metadata with version 1.1.0
            metadata = {
                b"geo": json.dumps(
                    {
                        "version": "1.1.0",
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
                    "--geoparquet-version",
                    "1.1",
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


class TestExtractOutput:
    """Test extract command output format compliance."""

    def test_default_format(self, sample_parquet, temp_output):
        """Test default extract format."""
        run_command_and_validate(
            ["extract", sample_parquet, temp_output, "--geoparquet-version", "1.1"],
            expect_bbox=None,  # Don't enforce bbox, just validate format
        )

    def test_with_bbox_filter(self, sample_parquet, temp_output):
        """Test extract with bbox filter produces valid output."""
        run_command_and_validate(
            [
                "extract",
                sample_parquet,
                temp_output,
                "--bbox",
                "-180,-90,180,90",
                "--geoparquet-version",
                "1.1",
            ],
            expect_bbox=None,
        )

    def test_with_where_filter(self, sample_parquet, temp_output):
        """Test extract with WHERE filter produces valid output."""
        run_command_and_validate(
            [
                "extract",
                sample_parquet,
                temp_output,
                "--where",
                "1=1",
                "--geoparquet-version",
                "1.1",
            ],
            expect_bbox=None,
        )

    def test_with_include_cols(self, sample_parquet, temp_output):
        """Test extract with column selection produces valid output."""
        run_command_and_validate(
            [
                "extract",
                sample_parquet,
                temp_output,
                "--include-cols",
                "id",
                "--geoparquet-version",
                "1.1",
            ],
            expect_bbox=None,
        )

    def test_custom_compression(self, sample_parquet, temp_output):
        """Test extract with custom compression."""
        run_command_and_validate(
            [
                "extract",
                sample_parquet,
                temp_output,
                "--compression",
                "gzip",
                "--geoparquet-version",
                "1.1",
            ],
            expected_compression="GZIP",
            expect_bbox=None,
        )


@pytest.mark.slow
class TestConvertOutput:
    """Test convert command output format compliance."""

    @pytest.fixture
    def geojson_input(self):
        """Path to sample GeoJSON test file."""
        return "tests/data/buildings_test.geojson"

    @pytest.fixture
    def shapefile_input(self):
        """Path to sample shapefile test file."""
        return "tests/data/buildings_test.shp"

    def test_from_geojson(self, geojson_input, temp_output):
        """Test convert from GeoJSON produces valid GeoParquet."""
        run_command_and_validate(
            ["convert", geojson_input, temp_output],
            expect_bbox=True,  # Convert adds bbox by default
        )

    def test_from_shapefile(self, shapefile_input, temp_output):
        """Test convert from shapefile produces valid GeoParquet."""
        run_command_and_validate(
            ["convert", shapefile_input, temp_output],
            expect_bbox=True,  # Convert adds bbox by default
        )

    def test_skip_hilbert(self, geojson_input, temp_output):
        """Test convert with --skip-hilbert still produces valid output."""
        run_command_and_validate(
            ["convert", geojson_input, temp_output, "--skip-hilbert"],
            expect_bbox=True,
        )

    def test_custom_compression(self, shapefile_input, temp_output):
        """Test convert with custom compression."""
        run_command_and_validate(
            [
                "convert",
                shapefile_input,
                temp_output,
                "--compression",
                "gzip",
                "--skip-hilbert",
            ],
            expected_compression="GZIP",
            expect_bbox=True,
        )


class TestSortColumnOutput:
    """Test sort column output format compliance."""

    def test_default_format(self, sample_parquet, temp_output):
        """Test default sort column format."""
        # sort column takes: INPUT OUTPUT COLUMNS (positional arg)
        run_command_and_validate(
            [
                "sort",
                "column",
                sample_parquet,
                temp_output,
                "id",
                "--geoparquet-version",
                "1.1",
            ],
            expect_bbox=None,  # Don't enforce bbox
        )

    def test_custom_compression(self, sample_parquet, temp_output):
        """Test sort column with custom compression."""
        run_command_and_validate(
            [
                "sort",
                "column",
                sample_parquet,
                temp_output,
                "id",
                "--compression",
                "gzip",
                "--geoparquet-version",
                "1.1",
            ],
            expected_compression="GZIP",
            expect_bbox=None,
        )


class TestSortQuadkeyOutput:
    """Test sort quadkey output format compliance."""

    def test_default_format(self, sample_parquet, temp_output):
        """Test default sort quadkey format."""
        run_command_and_validate(
            [
                "sort",
                "quadkey",
                sample_parquet,
                temp_output,
                "--geoparquet-version",
                "1.1",
            ],
            expect_bbox=None,  # Don't enforce bbox
        )

    def test_custom_compression(self, sample_parquet, temp_output):
        """Test sort quadkey with custom compression."""
        run_command_and_validate(
            [
                "sort",
                "quadkey",
                sample_parquet,
                temp_output,
                "--compression",
                "gzip",
                "--geoparquet-version",
                "1.1",
            ],
            expected_compression="GZIP",
            expect_bbox=None,
        )


class TestAllCommandsConsistency:
    """Test consistency across all commands."""

    def test_all_produce_correct_version(self, sample_parquet):
        """Ensure all commands produce the configured GeoParquet version."""
        # Commands where the pattern is: cmd INPUT OUTPUT [OPTIONS]
        simple_commands = [
            ["sort", "hilbert"],
            ["sort", "quadkey"],
            ["add", "bbox"],
            ["extract"],
        ]

        for cmd_base in simple_commands:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_name = tmp.name
            # Remove the file so tests don't trigger overwrite checks
            os.unlink(tmp_name)

            try:
                run_command_and_validate(
                    cmd_base + [sample_parquet, tmp_name, "--geoparquet-version", "1.1"],
                    expect_bbox=None,  # Don't check bbox, just version/compression
                )
            finally:
                if os.path.exists(tmp_name):
                    os.unlink(tmp_name)

        # sort column has special syntax: sort column INPUT OUTPUT COLUMNS [OPTIONS]
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name
        # Remove the file so tests don't trigger overwrite checks
        os.unlink(tmp_name)

        try:
            run_command_and_validate(
                ["sort", "column", sample_parquet, tmp_name, "id", "--geoparquet-version", "1.1"],
                expect_bbox=None,
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
            # Remove the file so tests don't trigger overwrite checks
            os.unlink(tmp_name)

            try:
                run_command_and_validate(
                    [
                        "sort",
                        "hilbert",
                        sample_parquet,
                        tmp_name,
                        "--compression",
                        compression_arg,
                        "--geoparquet-version",
                        "1.1",
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
