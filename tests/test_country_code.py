"""
Tests for find_country_code_column function.
"""

import os
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add_country_codes import find_country_code_column


class TestFindCountryCodeColumn:
    """Test suite for find_country_code_column function."""

    def create_test_parquet(self, columns, data, filename):
        """Helper to create a test parquet file with specified columns."""
        table_dict = {}
        for col, values in zip(columns, data, strict=True):
            table_dict[col] = values

        table = pa.table(table_dict)
        pq.write_table(table, filename)
        return filename

    def test_find_admin_country_code_column(self):
        """Test finding admin:country_code column."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file with admin:country_code column
            self.create_test_parquet(
                ["id", "admin:country_code", "name"],
                [[1, 2], ["US", "CA"], ["Place1", "Place2"]],
                tmp_name,
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                result = find_country_code_column(con, tmp_name, is_subquery=False)
                assert result == "admin:country_code"
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_find_country_column(self):
        """Test finding country column."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file with country column
            self.create_test_parquet(
                ["id", "country", "name"],
                [[1, 2], ["US", "CA"], ["Place1", "Place2"]],
                tmp_name,
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                result = find_country_code_column(con, tmp_name, is_subquery=False)
                assert result == "country"
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_find_iso_a2_column(self):
        """Test finding ISO_A2 column."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file with ISO_A2 column
            self.create_test_parquet(
                ["id", "ISO_A2", "name"], [[1, 2], ["US", "CA"], ["Place1", "Place2"]], tmp_name
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                result = find_country_code_column(con, tmp_name, is_subquery=False)
                assert result == "ISO_A2"
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_priority_order(self):
        """Test that columns are found in priority order."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file with multiple matching columns
            self.create_test_parquet(
                ["id", "ISO_A2", "country", "admin:country_code"],
                [[1, 2], ["US", "CA"], ["USA", "CAN"], ["US", "CA"]],
                tmp_name,
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                result = find_country_code_column(con, tmp_name, is_subquery=False)
                # Should find admin:country_code first due to priority
                assert result == "admin:country_code"
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_no_country_column_raises_error(self):
        """Test that error is raised when no country column is found."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file without any country column
            self.create_test_parquet(
                ["id", "name", "value"], [[1, 2], ["Place1", "Place2"], [100, 200]], tmp_name
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                import click

                with pytest.raises(click.UsageError) as exc_info:
                    find_country_code_column(con, tmp_name, is_subquery=False)

                assert "Could not find country code column" in str(exc_info.value)
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_with_subquery(self):
        """Test finding column with a subquery source."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file
            self.create_test_parquet(
                ["id", "country", "name"],
                [[1, 2], ["US", "CA"], ["Place1", "Place2"]],
                tmp_name,
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                # Create a subquery
                subquery = f"(SELECT * FROM '{tmp_name}')"

                result = find_country_code_column(con, subquery, is_subquery=True)
                assert result == "country"
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
