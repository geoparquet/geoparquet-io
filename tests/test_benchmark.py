"""
Tests for benchmark functionality.
"""

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.benchmark import (
    CONVERTERS,
    calculate_statistics,
    detect_available_converters,
    format_json_output,
    format_table_output,
    get_environment_info,
    get_file_info,
    run_benchmark,
)

# Test data
TEST_DATA_DIR = Path(__file__).parent / "data"
GEOJSON_FILE = TEST_DATA_DIR / "buildings_test.geojson"


class TestConverterDetection:
    """Tests for converter detection."""

    def test_detect_available_converters(self):
        """Test that converter detection returns valid results."""
        available, missing = detect_available_converters()

        # DuckDB should always be available
        assert "duckdb" in available

        # All returned converters should be valid
        for conv in available + missing:
            assert conv in CONVERTERS

        # No overlap between available and missing
        assert not set(available) & set(missing)

    def test_all_converters_have_required_fields(self):
        """Test that all converters have required configuration."""
        for _name, info in CONVERTERS.items():
            assert "name" in info
            assert "check" in info
            assert "install" in info
            assert callable(info["check"])


class TestFileInfo:
    """Tests for file info extraction."""

    def test_get_file_info_geojson(self):
        """Test getting file info from GeoJSON."""
        info = get_file_info(GEOJSON_FILE)

        assert info["name"] == "buildings_test.geojson"
        assert info["format"] == ".geojson"
        assert "size_mb" in info
        assert "feature_count" in info
        assert info["feature_count"] > 0

    def test_get_file_info_nonexistent(self):
        """Test getting file info from nonexistent file."""
        info = get_file_info(Path("/nonexistent/file.geojson"))

        assert "error" in info


class TestEnvironmentInfo:
    """Tests for environment info collection."""

    def test_get_environment_info(self):
        """Test environment info collection."""
        env = get_environment_info()

        assert "os" in env
        assert "python_version" in env
        assert "duckdb_version" in env
        assert "cpu" in env
        assert "ram" in env


class TestStatistics:
    """Tests for statistics calculation."""

    def test_calculate_statistics_multiple_iterations(self):
        """Test statistics calculation with multiple iterations."""
        results = [
            {"converter": "duckdb", "elapsed_seconds": 1.0, "memory_mb": 100, "success": True},
            {"converter": "duckdb", "elapsed_seconds": 1.2, "memory_mb": 110, "success": True},
            {"converter": "duckdb", "elapsed_seconds": 0.9, "memory_mb": 95, "success": True},
        ]

        stats = calculate_statistics(results, ["duckdb"])

        assert "duckdb" in stats
        assert stats["duckdb"]["iterations"] == 3
        assert stats["duckdb"]["mean_time"] > 0
        assert stats["duckdb"]["std_time"] >= 0
        assert stats["duckdb"]["mean_memory"] > 0

    def test_calculate_statistics_single_iteration(self):
        """Test statistics calculation with single iteration."""
        results = [
            {"converter": "duckdb", "elapsed_seconds": 1.5, "memory_mb": 100, "success": True},
        ]

        stats = calculate_statistics(results, ["duckdb"])

        assert stats["duckdb"]["iterations"] == 1
        assert stats["duckdb"]["std_time"] == 0
        assert stats["duckdb"]["std_memory"] == 0

    def test_calculate_statistics_with_failures(self):
        """Test statistics calculation ignores failed results."""
        results = [
            {"converter": "duckdb", "elapsed_seconds": 1.0, "memory_mb": 100, "success": True},
            {"converter": "duckdb", "error": "failed", "success": False},
        ]

        stats = calculate_statistics(results, ["duckdb"])

        assert stats["duckdb"]["iterations"] == 1


class TestOutputFormatting:
    """Tests for output formatting."""

    def test_format_table_output(self):
        """Test table output formatting."""
        stats = {
            "duckdb": {
                "mean_time": 1.5,
                "std_time": 0.1,
                "mean_memory": 100,
                "std_memory": 5,
                "iterations": 3,
            }
        }
        file_info = {
            "name": "test.geojson",
            "format": ".geojson",
            "size_mb": 1.5,
            "feature_count": 1000,
        }

        output = format_table_output(stats, file_info, ["duckdb"])

        assert "BENCHMARK RESULTS" in output
        assert "test.geojson" in output
        assert "DuckDB" in output
        assert "1.500" in output

    def test_format_json_output(self):
        """Test JSON output formatting."""
        stats = {"duckdb": {"mean_time": 1.5}}
        file_info = {"name": "test.geojson"}
        environment = {"os": "Linux"}
        raw_results = []
        config = {"iterations": 3}

        output = format_json_output(stats, file_info, environment, raw_results, config)

        # Should be valid JSON
        parsed = json.loads(output)
        assert "statistics" in parsed
        assert "file_info" in parsed
        assert "environment" in parsed


class TestRunBenchmark:
    """Tests for running benchmarks."""

    def test_run_benchmark_duckdb_only(self, temp_output_dir):
        """Test running benchmark with DuckDB only."""
        results = run_benchmark(
            input_file=str(GEOJSON_FILE),
            iterations=1,
            converters=["duckdb"],
            output_json=None,
            keep_output=None,
            warmup=False,
            output_format="table",
            quiet=True,
        )

        assert "statistics" in results
        assert "duckdb" in results["statistics"]
        assert results["statistics"]["duckdb"]["iterations"] == 1

    def test_run_benchmark_save_json(self, temp_output_dir):
        """Test running benchmark and saving JSON output."""
        json_path = os.path.join(temp_output_dir, "results.json")

        run_benchmark(
            input_file=str(GEOJSON_FILE),
            iterations=1,
            converters=["duckdb"],
            output_json=json_path,
            keep_output=None,
            warmup=False,
            output_format="table",
            quiet=True,
        )

        assert os.path.exists(json_path)

        with open(json_path) as f:
            saved = json.load(f)

        assert "statistics" in saved
        assert "duckdb" in saved["statistics"]

    def test_run_benchmark_keep_output(self, temp_output_dir):
        """Test running benchmark and keeping output files."""
        output_dir = os.path.join(temp_output_dir, "output")

        run_benchmark(
            input_file=str(GEOJSON_FILE),
            iterations=1,
            converters=["duckdb"],
            output_json=None,
            keep_output=output_dir,
            warmup=False,
            output_format="table",
            quiet=True,
        )

        # Output directory should exist with files
        assert os.path.exists(output_dir)
        parquet_files = list(Path(output_dir).glob("*.parquet"))
        assert len(parquet_files) > 0

    def test_run_benchmark_invalid_converter(self):
        """Test running benchmark with invalid converter raises error."""
        with pytest.raises(Exception) as exc_info:
            run_benchmark(
                input_file=str(GEOJSON_FILE),
                iterations=1,
                converters=["invalid_converter"],
                quiet=True,
            )

        assert "Unknown converters" in str(exc_info.value)

    def test_run_benchmark_nonexistent_file(self):
        """Test running benchmark with nonexistent file raises error."""
        with pytest.raises(Exception) as exc_info:
            run_benchmark(
                input_file="/nonexistent/file.geojson",
                iterations=1,
                quiet=True,
            )

        assert "not found" in str(exc_info.value)


class TestCLI:
    """Tests for CLI interface."""

    def test_benchmark_help(self):
        """Test benchmark command help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["benchmark", "--help"])

        assert result.exit_code == 0
        assert "Benchmark GeoParquet conversion" in result.output
        assert "--iterations" in result.output
        assert "--converters" in result.output

    def test_benchmark_cli_basic(self):
        """Test basic benchmark CLI invocation."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "benchmark",
                str(GEOJSON_FILE),
                "--iterations",
                "1",
                "--converters",
                "duckdb",
                "--no-warmup",
            ],
        )

        assert result.exit_code == 0
        assert "BENCHMARK RESULTS" in result.output
        assert "DuckDB" in result.output

    def test_benchmark_cli_json_format(self, temp_output_dir):
        """Test benchmark CLI with JSON output format."""
        json_path = os.path.join(temp_output_dir, "results.json")
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "benchmark",
                str(GEOJSON_FILE),
                "--iterations",
                "1",
                "--converters",
                "duckdb",
                "--no-warmup",
                "--format",
                "json",
                "--output-json",
                json_path,
            ],
        )

        assert result.exit_code == 0

        # Check saved JSON file
        with open(json_path) as f:
            output = json.load(f)
        assert "statistics" in output

    def test_benchmark_cli_quiet(self, temp_output_dir):
        """Test benchmark CLI with quiet mode."""
        json_path = os.path.join(temp_output_dir, "results.json")
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "benchmark",
                str(GEOJSON_FILE),
                "--iterations",
                "1",
                "--converters",
                "duckdb",
                "--no-warmup",
                "--quiet",
                "--output-json",
                json_path,
            ],
        )

        assert result.exit_code == 0
        # Quiet mode suppresses output, but JSON file should still be written
        assert os.path.exists(json_path)

    def test_benchmark_cli_missing_file(self):
        """Test benchmark CLI with missing file."""
        runner = CliRunner()
        result = runner.invoke(cli, ["benchmark", "/nonexistent/file.geojson"])

        assert result.exit_code != 0
