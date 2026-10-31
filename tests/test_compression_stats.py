"""Tests for per-column compression ratio statistics."""

import json

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


@pytest.fixture
def runner():
    """Provide a Click CLI runner."""
    return CliRunner()


class TestGetCompressionStats:
    """Tests for the core get_compression_stats function."""

    def test_returns_list_of_dicts(self, places_test_file):
        """Compression stats should return a list of per-column dicts."""
        from geoparquet_io.core.duckdb_metadata import get_compression_stats

        result = get_compression_stats(places_test_file)

        assert isinstance(result, list)
        assert len(result) > 0

    def test_dict_keys(self, places_test_file):
        """Each entry should have expected keys."""
        from geoparquet_io.core.duckdb_metadata import get_compression_stats

        result = get_compression_stats(places_test_file)
        first = result[0]

        assert "column_name" in first
        assert "compression" in first
        assert "compressed_bytes" in first
        assert "uncompressed_bytes" in first
        assert "ratio" in first

    def test_ratio_calculation(self, places_test_file):
        """Ratio should be uncompressed / compressed."""
        from geoparquet_io.core.duckdb_metadata import get_compression_stats

        result = get_compression_stats(places_test_file)

        for entry in result:
            if entry["compressed_bytes"] > 0:
                expected_ratio = round(entry["uncompressed_bytes"] / entry["compressed_bytes"], 2)
                assert abs(entry["ratio"] - expected_ratio) < 0.01

    def test_ordered_by_compressed_bytes_desc(self, places_test_file):
        """Results should be ordered by compressed_bytes descending."""
        from geoparquet_io.core.duckdb_metadata import get_compression_stats

        result = get_compression_stats(places_test_file)

        compressed_sizes = [entry["compressed_bytes"] for entry in result]
        assert compressed_sizes == sorted(compressed_sizes, reverse=True)

    def test_has_expected_columns(self, places_test_file):
        """Known columns from test file should appear in compression stats."""
        from geoparquet_io.core.duckdb_metadata import get_compression_stats

        stats = get_compression_stats(places_test_file)
        stats_columns = {entry["column_name"] for entry in stats}

        # Test file has these top-level columns
        assert "geometry" in stats_columns
        assert "name" in stats_columns
        assert "fsq_place_id" in stats_columns

    def test_compressed_bytes_positive(self, places_test_file):
        """All columns should have positive compressed bytes."""
        from geoparquet_io.core.duckdb_metadata import get_compression_stats

        result = get_compression_stats(places_test_file)

        for entry in result:
            assert entry["compressed_bytes"] >= 0
            assert entry["uncompressed_bytes"] >= 0

    def test_with_existing_connection(self, places_test_file):
        """Should work with a provided DuckDB connection."""
        from geoparquet_io.core.duckdb_metadata import get_compression_stats
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            result = get_compression_stats(places_test_file, con=con)
            assert isinstance(result, list)
            assert len(result) > 0
        finally:
            con.close()


class TestInspectStatsCompression:
    """Tests for compression stats in inspect stats output."""

    def test_stats_terminal_includes_compression(self, runner, places_test_file):
        """inspect stats should include compression ratios section."""
        result = runner.invoke(cli, ["inspect", "stats", places_test_file])

        assert result.exit_code == 0
        assert "Compression" in result.output
        assert "Ratio" in result.output

    def test_stats_json_includes_compression(self, runner, places_test_file):
        """inspect stats --json should include compression_stats key."""
        result = runner.invoke(cli, ["inspect", "stats", places_test_file, "--json"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "compression_stats" in data
        assert isinstance(data["compression_stats"], list)
        assert len(data["compression_stats"]) > 0

        first = data["compression_stats"][0]
        assert "column_name" in first
        assert "compression" in first
        assert "compressed_bytes" in first
        assert "uncompressed_bytes" in first
        assert "ratio" in first

    def test_stats_markdown_includes_compression(self, runner, places_test_file):
        """inspect stats --markdown should include compression table."""
        result = runner.invoke(cli, ["inspect", "stats", places_test_file, "--markdown"])

        assert result.exit_code == 0
        assert "Compression" in result.output
        assert "Ratio" in result.output


class TestCompressionStatsAPI:
    """Tests for the Python API compression_stats function."""

    def test_compression_stats_function(self, places_test_file):
        """gpio.compression_stats() should return list of per-column dicts."""
        from geoparquet_io.api.ops import compression_stats

        result = compression_stats(places_test_file)

        assert isinstance(result, list)
        assert len(result) > 0
        assert "column_name" in result[0]
        assert "ratio" in result[0]
