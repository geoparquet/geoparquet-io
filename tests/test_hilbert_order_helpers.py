"""Tests for hilbert_order helper functions."""

import logging
from unittest.mock import patch

import pytest

from geoparquet_io.core.hilbert_order import _cleanup_temp_file


class TestCleanupTempFile:
    """Tests for _cleanup_temp_file function."""

    def test_cleanup_nonexistent_file(self, tmp_path):
        """Test cleanup with non-existent file."""
        nonexistent = str(tmp_path / "nonexistent.parquet")
        # Should not raise
        _cleanup_temp_file(nonexistent, verbose=False)

    def test_cleanup_none_file(self):
        """Test cleanup with None file."""
        # Should not raise
        _cleanup_temp_file(None, verbose=False)

    def test_cleanup_existing_file(self, tmp_path):
        """Test cleanup with existing file."""
        temp_file = tmp_path / "temp.parquet"
        temp_file.write_text("test content")
        assert temp_file.exists()

        _cleanup_temp_file(str(temp_file), verbose=False)

        assert not temp_file.exists()

    def test_cleanup_with_verbose(self, tmp_path, capsys):
        """Test cleanup with verbose output."""
        temp_file = tmp_path / "temp.parquet"
        temp_file.write_text("test content")

        _cleanup_temp_file(str(temp_file), verbose=True)

        assert not temp_file.exists()


class TestHilbertV11Warning:
    """Tests for v1.1 Hilbert sorting warning."""

    def test_warns_when_version_is_1_1(self, tmp_path, places_test_file):
        """Hilbert sorting to v1.1 should warn about no filter pushdown benefit."""
        from unittest.mock import patch

        output = str(tmp_path / "out.parquet")
        with patch("geoparquet_io.core.hilbert_order.warn") as mock_warn:
            from geoparquet_io.core.hilbert_order import hilbert_order

            hilbert_order(places_test_file, output, geoparquet_version="1.1")

        mock_warn.assert_any_call(
            "Hilbert sorting to GeoParquet v1.1 provides no spatial filter pushdown benefit. "
            "Consider using --geoparquet-version 2.0 to enable native geo_bbox row group statistics."
        )

    def test_warns_when_version_is_default_none(self, tmp_path, places_test_file):
        """Hilbert sorting with default version (None, resolves to 1.1) should warn."""
        from unittest.mock import patch

        output = str(tmp_path / "out.parquet")
        with patch("geoparquet_io.core.hilbert_order.warn") as mock_warn:
            from geoparquet_io.core.hilbert_order import hilbert_order

            hilbert_order(places_test_file, output, geoparquet_version=None)

        mock_warn.assert_any_call(
            "Hilbert sorting to GeoParquet v1.1 provides no spatial filter pushdown benefit. "
            "Consider using --geoparquet-version 2.0 to enable native geo_bbox row group statistics."
        )

    def test_no_warning_when_version_is_2_0(self, tmp_path, places_test_file):
        """Hilbert sorting to v2.0 should NOT warn."""
        from unittest.mock import patch

        output = str(tmp_path / "out.parquet")
        with patch("geoparquet_io.core.hilbert_order.warn") as mock_warn:
            from geoparquet_io.core.hilbert_order import hilbert_order

            hilbert_order(places_test_file, output, geoparquet_version="2.0")

        # Check that warn was never called with the v1.1 message
        for call in mock_warn.call_args_list:
            assert "no spatial filter pushdown" not in str(call)


class TestHilbertRgSizeGuidance:
    """Tests for row group size guidance when Hilbert sorting with v2.0."""

    @patch("geoparquet_io.core.hilbert_order._hilbert_order_file_based")
    def test_large_rg_with_v20_shows_guidance(self, _mock_file_based, caplog):
        """Large RG size + v2.0 should show spatial guidance."""
        from geoparquet_io.core.hilbert_order import hilbert_order

        with caplog.at_level(logging.INFO):
            hilbert_order(
                "input.parquet",
                "output.parquet",
                row_group_rows=100000,
                geoparquet_version="2.0",
            )
        from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS

        assert "10000" in caplog.text or "10,000" in caplog.text
        # The guidance quotes the sort default, so it must not be a literal here
        # either -- it moved to 49,152 in #961.
        assert f"{DEFAULT_ROW_GROUP_ROWS:,}" in caplog.text

    @patch("geoparquet_io.core.hilbert_order._hilbert_order_file_based")
    def test_large_rg_with_pgo_shows_guidance(self, _mock_file_based, caplog):
        """Large RG size + parquet-geo-only should show spatial guidance."""
        from geoparquet_io.core.hilbert_order import hilbert_order

        with caplog.at_level(logging.INFO):
            hilbert_order(
                "input.parquet",
                "output.parquet",
                row_group_rows=60000,
                geoparquet_version="parquet-geo-only",
            )
        assert "row group" in caplog.text.lower() or "row-group" in caplog.text.lower()

    @patch("geoparquet_io.core.hilbert_order._hilbert_order_file_based")
    def test_small_rg_with_v20_no_guidance(self, _mock_file_based, caplog):
        """Small RG size + v2.0 should NOT show guidance."""
        from geoparquet_io.core.hilbert_order import hilbert_order

        with caplog.at_level(logging.INFO):
            hilbert_order(
                "input.parquet",
                "output.parquet",
                row_group_rows=10000,
                geoparquet_version="2.0",
            )
        assert "spatial filter pushdown" not in caplog.text

    @patch("geoparquet_io.core.hilbert_order._hilbert_order_file_based")
    def test_large_rg_with_v11_no_guidance(self, _mock_file_based, caplog):
        """Large RG size + v1.1 should NOT show RG guidance (v1.1 has no geo stats anyway)."""
        from geoparquet_io.core.hilbert_order import hilbert_order

        with caplog.at_level(logging.INFO):
            hilbert_order(
                "input.parquet",
                "output.parquet",
                row_group_rows=100000,
                geoparquet_version="1.1",
            )
        # Should NOT show the row group size guidance (check for specific phrase)
        # Note: v1.1 warning about "no spatial filter pushdown benefit" IS expected
        assert "Smaller row groups" not in caplog.text

    @patch("geoparquet_io.core.hilbert_order._hilbert_order_file_based")
    def test_no_rg_rows_specified_no_guidance(self, _mock_file_based, caplog):
        """No row_group_rows specified should NOT show guidance."""
        from geoparquet_io.core.hilbert_order import hilbert_order

        with caplog.at_level(logging.INFO):
            hilbert_order(
                "input.parquet",
                "output.parquet",
                geoparquet_version="2.0",
            )
        assert "spatial filter pushdown" not in caplog.text


class TestHilbertOrderTableEmptyGeometries:
    """Tests for hilbert_order_table handling of empty/null geometries.

    Issue #442: ST_Hilbert does not support empty geometries.
    Fix: Empty/null geometries are placed at the end of the sorted output.
    """

    def test_mixed_empty_and_valid_geometries(self):
        """Table with mix of valid and empty geometries should sort correctly."""
        import duckdb

        from geoparquet_io.core.hilbert_order import hilbert_order_table

        # Create table with valid and empty geometries
        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")

        arrow_table = (
            con.execute("""
            SELECT * FROM (VALUES
                (1, ST_AsWKB(ST_GeomFromText('POINT(10 20)'))),
                (2, ST_AsWKB(ST_GeomFromText('POINT EMPTY'))),
                (3, ST_AsWKB(ST_GeomFromText('POINT(30 40)'))),
                (4, ST_AsWKB(ST_GeomFromText('POLYGON EMPTY'))),
                (5, ST_AsWKB(ST_GeomFromText('POINT(5 5)')))
            ) t(id, geometry)
        """)
            .arrow()
            .read_all()
        )
        con.close()

        # Should not raise - empty geometries handled gracefully
        with patch("geoparquet_io.core.hilbert_order.warn") as mock_warn:
            result = hilbert_order_table(arrow_table, geometry_column="geometry")

        # Verify warning was issued about empty geometries
        mock_warn.assert_any_call(
            "Found 2 empty/null geometries. These will be placed at the end of the sorted output."
        )

        # Verify row count preserved
        assert result.num_rows == 5

        # Verify empty geometries are at the end (IDs 2 and 4)
        result_ids = result.column("id").to_pylist()
        # Last two should be the empty geometry rows
        assert set(result_ids[-2:]) == {2, 4}

    def test_all_empty_geometries_returns_unchanged(self):
        """Table with only empty/null geometries should return unchanged."""
        import duckdb

        from geoparquet_io.core.hilbert_order import hilbert_order_table

        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")

        arrow_table = (
            con.execute("""
            SELECT * FROM (VALUES
                (1, ST_AsWKB(ST_GeomFromText('POINT EMPTY'))),
                (2, ST_AsWKB(ST_GeomFromText('LINESTRING EMPTY'))),
                (3, NULL)
            ) t(id, geometry)
        """)
            .arrow()
            .read_all()
        )
        con.close()

        with patch("geoparquet_io.core.hilbert_order.warn") as mock_warn:
            result = hilbert_order_table(arrow_table, geometry_column="geometry")

        # Should warn about all geometries being empty
        mock_warn.assert_any_call(
            "All geometries are empty or null. Returning table without Hilbert ordering."
        )

        # Row count preserved
        assert result.num_rows == 3

    def test_null_geometries_placed_at_end(self):
        """NULL geometries should be placed at the end."""
        import duckdb

        from geoparquet_io.core.hilbert_order import hilbert_order_table

        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")

        arrow_table = (
            con.execute("""
            SELECT * FROM (VALUES
                (1, ST_AsWKB(ST_GeomFromText('POINT(10 20)'))),
                (2, NULL),
                (3, ST_AsWKB(ST_GeomFromText('POINT(30 40)')))
            ) t(id, geometry)
        """)
            .arrow()
            .read_all()
        )
        con.close()

        with patch("geoparquet_io.core.hilbert_order.warn"):
            result = hilbert_order_table(arrow_table, geometry_column="geometry")

        # Verify row count preserved
        assert result.num_rows == 3

        # NULL geometry (id=2) should be last
        result_ids = result.column("id").to_pylist()
        assert result_ids[-1] == 2

    def test_preserves_metadata(self):
        """Schema metadata should be preserved when handling empty geometries."""
        import duckdb

        from geoparquet_io.core.hilbert_order import hilbert_order_table

        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")

        arrow_table = (
            con.execute("""
            SELECT * FROM (VALUES
                (1, ST_AsWKB(ST_GeomFromText('POINT(10 20)'))),
                (2, ST_AsWKB(ST_GeomFromText('POINT EMPTY')))
            ) t(id, geometry)
        """)
            .arrow()
            .read_all()
        )
        con.close()

        # Add metadata
        metadata = {b"geo": b'{"primary_column": "geometry"}'}
        table_with_meta = arrow_table.replace_schema_metadata(metadata)

        with patch("geoparquet_io.core.hilbert_order.warn"):
            result = hilbert_order_table(table_with_meta, geometry_column="geometry")

        # Metadata should be preserved
        assert result.schema.metadata == metadata


class TestHilbertOrderCLIEmptyGeometries:
    """CLI integration tests for empty/null geometry handling.

    Tests that the CLI paths (_hilbert_order_streaming, _hilbert_order_file_based)
    handle empty geometries gracefully, matching the Python API behavior.
    """

    @pytest.fixture
    def mixed_empty_parquet(self, tmp_path):
        """Create a GeoParquet file with mixed valid and empty geometries."""
        import duckdb
        import pyarrow.parquet as pq

        output_path = tmp_path / "mixed_empty.parquet"
        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")

        # Create table with WKB geometries
        arrow_table = (
            con.execute("""
                SELECT * FROM (VALUES
                    (1, ST_AsWKB(ST_GeomFromText('POINT(10 20)'))),
                    (2, ST_AsWKB(ST_GeomFromText('POINT EMPTY'))),
                    (3, ST_AsWKB(ST_GeomFromText('POINT(30 40)')))
                ) t(id, geometry)
            """)
            .arrow()
            .read_all()
        )
        con.close()

        # Add GeoParquet metadata so DuckDB recognizes geometry column
        geo_metadata = b'{"version": "1.0.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}}}'
        table_with_meta = arrow_table.replace_schema_metadata({b"geo": geo_metadata})
        pq.write_table(table_with_meta, output_path)
        return str(output_path)

    @pytest.fixture
    def all_empty_parquet(self, tmp_path):
        """Create a GeoParquet file with only empty/null geometries."""
        import duckdb
        import pyarrow.parquet as pq

        output_path = tmp_path / "all_empty.parquet"
        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")

        # Create table with empty/null WKB geometries
        arrow_table = (
            con.execute("""
                SELECT * FROM (VALUES
                    (1, ST_AsWKB(ST_GeomFromText('POINT EMPTY'))),
                    (2, ST_AsWKB(ST_GeomFromText('LINESTRING EMPTY'))),
                    (3, NULL)
                ) t(id, geometry)
            """)
            .arrow()
            .read_all()
        )
        con.close()

        # Add GeoParquet metadata so DuckDB recognizes geometry column
        geo_metadata = b'{"version": "1.0.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point", "LineString"]}}}'
        table_with_meta = arrow_table.replace_schema_metadata({b"geo": geo_metadata})
        pq.write_table(table_with_meta, output_path)
        return str(output_path)

    def test_cli_mixed_empty_geometries(self, mixed_empty_parquet, tmp_path):
        """CLI should handle mixed valid/empty geometries without crashing."""
        import pyarrow.parquet as pq
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        output_path = str(tmp_path / "output.parquet")
        runner = CliRunner()
        result = runner.invoke(cli, ["sort", "hilbert", mixed_empty_parquet, output_path])

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "empty/null geometries" in result.output

        # Verify output exists and has correct row count
        output_table = pq.read_table(output_path)
        assert output_table.num_rows == 3

        # Empty geometry (id=2) should be at the end
        ids = output_table.column("id").to_pylist()
        assert ids[-1] == 2

    def test_cli_all_empty_geometries_graceful(self, all_empty_parquet, tmp_path):
        """CLI should handle all-empty geometries gracefully (not crash)."""
        import pyarrow.parquet as pq
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        output_path = str(tmp_path / "output.parquet")
        runner = CliRunner()
        result = runner.invoke(cli, ["sort", "hilbert", all_empty_parquet, output_path])

        # Should succeed, not crash
        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert "empty or null" in result.output.lower()

        # Output should exist and preserve all rows
        output_table = pq.read_table(output_path)
        assert output_table.num_rows == 3
