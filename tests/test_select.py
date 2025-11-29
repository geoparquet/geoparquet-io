"""
Tests for the select command.

Tests verify field selection/exclusion functionality:
- Basic field selection
- Field exclusion with --exclude
- Handling of missing fields
- Quoted field names with special characters
- Geometry column preservation
- GeoParquet metadata preservation
"""

import os

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.common import get_parquet_metadata, parse_geo_metadata
from geoparquet_io.core.select import parse_fields, select_fields, validate_fields


class TestParseFields:
    """Test the parse_fields function for handling field name parsing."""

    def test_parse_simple_fields(self):
        """Test parsing simple comma-separated fields."""
        result = parse_fields("field1,field2,field3")
        assert result == ["field1", "field2", "field3"]

    def test_parse_single_field(self):
        """Test parsing a single field."""
        result = parse_fields("field1")
        assert result == ["field1"]

    def test_parse_empty_string(self):
        """Test parsing empty string returns empty list."""
        result = parse_fields("")
        assert result == []

    def test_parse_field_with_spaces(self):
        """Test parsing quoted field with spaces."""
        result = parse_fields('"field with space",field2')
        assert result == ["field with space", "field2"]

    def test_parse_field_with_comma(self):
        """Test parsing quoted field containing comma."""
        result = parse_fields('"field, with comma",field2')
        assert result == ["field, with comma", "field2"]

    def test_parse_field_with_escaped_quote(self):
        """Test parsing field with escaped double quote."""
        result = parse_fields('"field with \\" quote",field2')
        assert result == ['field with " quote', "field2"]

    def test_parse_deduplication(self):
        """Test that duplicate fields are removed."""
        result = parse_fields("field1,field2,field1,field3,field2")
        assert result == ["field1", "field2", "field3"]

    def test_parse_preserves_order(self):
        """Test that first occurrence order is preserved."""
        result = parse_fields("z_field,a_field,m_field")
        assert result == ["z_field", "a_field", "m_field"]

    def test_parse_whitespace_trimming(self):
        """Test that whitespace around fields is trimmed."""
        result = parse_fields("  field1  ,  field2  ")
        assert result == ["field1", "field2"]

    def test_parse_multiple_quoted_fields(self):
        """Test parsing multiple quoted fields."""
        result = parse_fields('"first field","second field",regular')
        assert result == ["first field", "second field", "regular"]

    def test_parse_escaped_backslash(self):
        """Test parsing field with escaped backslash."""
        result = parse_fields('"field with \\\\ backslash"')
        assert result == ["field with \\ backslash"]

    def test_parse_complex_combination(self):
        """Test parsing complex combination of quoted and unquoted fields."""
        result = parse_fields('normal,"has space","has, comma","has \\" quote"')
        assert result == ["normal", "has space", "has, comma", 'has " quote']


class TestValidateFields:
    """Test the validate_fields function."""

    def test_validate_all_fields_exist(self):
        """Test validation when all fields exist."""
        result = validate_fields(
            requested_fields=["id", "name"],
            available_fields=["id", "name", "geometry"],
            ignore_missing=False,
        )
        assert result == ["id", "name"]

    def test_validate_missing_field_error(self):
        """Test that missing field raises error by default."""
        with pytest.raises(Exception) as exc_info:
            validate_fields(
                requested_fields=["id", "nonexistent"],
                available_fields=["id", "name", "geometry"],
                ignore_missing=False,
            )
        assert "nonexistent" in str(exc_info.value)
        assert "not found" in str(exc_info.value).lower()

    def test_validate_missing_field_warning(self, capsys):
        """Test that missing field emits warning when ignore_missing=True."""
        result = validate_fields(
            requested_fields=["id", "nonexistent"],
            available_fields=["id", "name", "geometry"],
            ignore_missing=True,
        )
        assert result == ["id"]
        captured = capsys.readouterr()
        assert "nonexistent" in captured.out
        assert "Warning" in captured.out

    def test_validate_preserves_order(self):
        """Test that field order is preserved."""
        result = validate_fields(
            requested_fields=["name", "id"],
            available_fields=["id", "name", "geometry"],
            ignore_missing=False,
        )
        assert result == ["name", "id"]


class TestSelectFieldsCore:
    """Test core select_fields function."""

    def test_select_basic_fields(self, places_test_file, temp_output_file):
        """Test basic field selection."""
        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["fsq_place_id", "name"],
            exclude=False,
            ignore_missing=False,
            verbose=False,
        )

        assert os.path.exists(temp_output_file)

        # Verify columns in output
        con = duckdb.connect()
        result = con.execute(f"DESCRIBE SELECT * FROM '{temp_output_file}'").fetchall()
        column_names = [row[0] for row in result]
        con.close()

        # Should have selected fields plus geometry
        assert "fsq_place_id" in column_names
        assert "name" in column_names
        assert "geometry" in column_names
        # Should NOT have excluded fields
        assert "address" not in column_names
        assert "placemaker_url" not in column_names

    def test_select_preserves_row_count(self, places_test_file, temp_output_file):
        """Test that all rows are preserved during selection."""
        # Get input row count
        con = duckdb.connect()
        input_count = con.execute(f"SELECT COUNT(*) FROM '{places_test_file}'").fetchone()[0]

        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["name"],
            exclude=False,
            ignore_missing=False,
            verbose=False,
        )

        output_count = con.execute(f"SELECT COUNT(*) FROM '{temp_output_file}'").fetchone()[0]
        con.close()

        assert input_count == output_count

    def test_select_exclude_mode(self, places_test_file, temp_output_file):
        """Test exclude mode keeps all fields except specified ones."""
        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["address", "placemaker_url"],
            exclude=True,
            ignore_missing=False,
            verbose=False,
        )

        assert os.path.exists(temp_output_file)

        # Verify columns in output
        con = duckdb.connect()
        result = con.execute(f"DESCRIBE SELECT * FROM '{temp_output_file}'").fetchall()
        column_names = [row[0] for row in result]
        con.close()

        # Should have fields that were NOT excluded
        assert "fsq_place_id" in column_names
        assert "name" in column_names
        assert "geometry" in column_names
        # Should NOT have excluded fields
        assert "address" not in column_names
        assert "placemaker_url" not in column_names

    def test_select_auto_includes_geometry(self, places_test_file, temp_output_file, capsys):
        """Test that geometry column is automatically included."""
        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["name"],
            exclude=False,
            ignore_missing=False,
            verbose=False,
        )

        # Verify geometry is in output
        con = duckdb.connect()
        result = con.execute(f"DESCRIBE SELECT * FROM '{temp_output_file}'").fetchall()
        column_names = [row[0] for row in result]
        con.close()

        assert "geometry" in column_names

        captured = capsys.readouterr()
        assert "Adding geometry column" in captured.out

    def test_select_geometry_explicit(self, places_test_file, temp_output_file, capsys):
        """Test that explicitly including geometry doesn't warn."""
        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["name", "geometry"],
            exclude=False,
            ignore_missing=False,
            verbose=False,
        )

        captured = capsys.readouterr()
        # Should NOT have the "Adding geometry" message since it was explicitly included
        assert "Adding geometry column" not in captured.out

    def test_select_preserves_metadata(self, places_test_file, temp_output_file):
        """Test that GeoParquet metadata is preserved."""
        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["name"],
            exclude=False,
            ignore_missing=False,
            verbose=False,
        )

        metadata, _ = get_parquet_metadata(temp_output_file, verbose=False)
        geo_meta = parse_geo_metadata(metadata, verbose=False)

        assert geo_meta is not None
        assert "version" in geo_meta
        assert "primary_column" in geo_meta
        assert geo_meta["primary_column"] == "geometry"

    def test_select_missing_field_error(self, places_test_file, temp_output_file):
        """Test error when field doesn't exist."""
        with pytest.raises(Exception) as exc_info:
            select_fields(
                input_parquet=places_test_file,
                output_parquet=temp_output_file,
                fields=["nonexistent_field"],
                exclude=False,
                ignore_missing=False,
                verbose=False,
            )
        assert "nonexistent_field" in str(exc_info.value)
        assert "not found" in str(exc_info.value).lower()

    def test_select_missing_field_ignore(self, places_test_file, temp_output_file, capsys):
        """Test warning when field doesn't exist with ignore_missing=True."""
        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["name", "nonexistent_field"],
            exclude=False,
            ignore_missing=True,
            verbose=False,
        )

        assert os.path.exists(temp_output_file)
        captured = capsys.readouterr()
        assert "nonexistent_field" in captured.out
        assert "Warning" in captured.out

    def test_select_verbose_output(self, places_test_file, temp_output_file, capsys):
        """Test verbose mode output."""
        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["name"],
            exclude=False,
            ignore_missing=False,
            verbose=True,
        )

        captured = capsys.readouterr()
        assert "Available fields" in captured.out
        assert "Selecting fields" in captured.out
        assert "Final output fields" in captured.out


class TestSelectCLI:
    """Test CLI interface for select command."""

    def test_cli_basic_select(self, places_test_file, temp_output_file):
        """Test CLI basic field selection."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["select", places_test_file, temp_output_file, "--fields", "name,fsq_place_id"]
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)
        assert "Created output with" in result.output

    def test_cli_exclude_flag(self, places_test_file, temp_output_file):
        """Test CLI --exclude flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "select",
                places_test_file,
                temp_output_file,
                "--fields",
                "address,placemaker_url",
                "--exclude",
            ],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(temp_output_file)

        # Verify excluded fields are not present
        con = duckdb.connect()
        columns = con.execute(f"DESCRIBE SELECT * FROM '{temp_output_file}'").fetchall()
        column_names = [row[0] for row in columns]
        con.close()

        assert "address" not in column_names
        assert "placemaker_url" not in column_names

    def test_cli_ignore_missing_fields(self, places_test_file, temp_output_file):
        """Test CLI --ignore-missing-fields flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "select",
                places_test_file,
                temp_output_file,
                "--fields",
                "name,nonexistent",
                "--ignore-missing-fields",
            ],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert "Warning" in result.output
        assert "nonexistent" in result.output

    def test_cli_missing_field_error(self, places_test_file, temp_output_file):
        """Test CLI error on missing field without ignore flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["select", places_test_file, temp_output_file, "--fields", "nonexistent_field"],
        )

        assert result.exit_code != 0
        assert "not found" in result.output.lower()

    def test_cli_verbose_flag(self, places_test_file, temp_output_file):
        """Test CLI --verbose flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["select", places_test_file, temp_output_file, "--fields", "name", "--verbose"]
        )

        assert result.exit_code == 0
        assert "Available fields" in result.output
        assert "Parsed fields" in result.output

    def test_cli_compression_options(self, places_test_file, temp_output_file):
        """Test CLI compression options."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "select",
                places_test_file,
                temp_output_file,
                "--fields",
                "name",
                "--compression",
                "GZIP",
                "--compression-level",
                "6",
            ],
        )

        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_cli_quoted_field_with_space(self, temp_output_dir):
        """Test CLI with quoted field containing space."""
        # First create a test file with a space in the column name
        input_file = os.path.join(temp_output_dir, "input_with_spaces.parquet")
        output_file = os.path.join(temp_output_dir, "output.parquet")

        # Create test data with space in column name
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        con.execute(
            f"""
            COPY (
                SELECT
                    1 as id,
                    'test' as "field with space",
                    ST_Point(0, 0) as geometry
            ) TO '{input_file}' (FORMAT PARQUET);
        """
        )
        con.close()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["select", input_file, output_file, "--fields", '"field with space"'],
        )

        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert os.path.exists(output_file)

    def test_cli_help(self):
        """Test CLI help output."""
        runner = CliRunner()
        result = runner.invoke(cli, ["select", "--help"])

        assert result.exit_code == 0
        assert "--fields" in result.output
        assert "--exclude" in result.output
        assert "--ignore-missing-fields" in result.output

    def test_cli_missing_required_fields_option(self, places_test_file, temp_output_file):
        """Test CLI error when --fields is not provided."""
        runner = CliRunner()
        result = runner.invoke(cli, ["select", places_test_file, temp_output_file])

        assert result.exit_code != 0
        assert "Missing option" in result.output or "--fields" in result.output


class TestSelectEdgeCases:
    """Test edge cases and error handling."""

    def test_select_all_fields_except_geometry(self, places_test_file, temp_output_file, capsys):
        """Test excluding geometry column with explicit warning."""
        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["geometry"],
            exclude=True,
            ignore_missing=False,
            verbose=False,
        )

        captured = capsys.readouterr()
        assert "Geometry column 'geometry' was excluded" in captured.out
        assert "not be a valid GeoParquet" in captured.out

    def test_select_single_field(self, buildings_test_file, temp_output_file):
        """Test selecting just one field (plus auto-added geometry)."""
        select_fields(
            input_parquet=buildings_test_file,
            output_parquet=temp_output_file,
            fields=["id"],
            exclude=False,
            ignore_missing=False,
            verbose=False,
        )

        con = duckdb.connect()
        columns = con.execute(f"DESCRIBE SELECT * FROM '{temp_output_file}'").fetchall()
        column_names = [row[0] for row in columns]
        con.close()

        assert len(column_names) == 2  # id + geometry

    def test_select_with_bbox_column(self, places_test_file, temp_output_file):
        """Test selecting with bbox column included."""
        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["name", "bbox"],
            exclude=False,
            ignore_missing=False,
            verbose=False,
        )

        con = duckdb.connect()
        columns = con.execute(f"DESCRIBE SELECT * FROM '{temp_output_file}'").fetchall()
        column_names = [row[0] for row in columns]
        con.close()

        assert "bbox" in column_names

    def test_select_empty_after_exclusion_error(self, buildings_test_file, temp_output_file):
        """Test error when all fields are excluded."""
        with pytest.raises(Exception) as exc_info:
            select_fields(
                input_parquet=buildings_test_file,
                output_parquet=temp_output_file,
                fields=["id", "geometry"],
                exclude=True,
                ignore_missing=False,
                verbose=False,
            )
        assert "No fields remaining" in str(exc_info.value)

    def test_select_all_missing_fields_error(self, places_test_file, temp_output_file):
        """Test error when all specified fields are missing (even with ignore)."""
        with pytest.raises(Exception) as exc_info:
            select_fields(
                input_parquet=places_test_file,
                output_parquet=temp_output_file,
                fields=["nonexistent1", "nonexistent2"],
                exclude=False,
                ignore_missing=True,
                verbose=False,
            )
        assert "No valid fields" in str(exc_info.value)

    def test_select_preserves_data_values(self, places_test_file, temp_output_file):
        """Test that data values are preserved correctly."""
        # Get original value
        con = duckdb.connect()
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        original = con.execute(
            f"SELECT name FROM '{places_test_file}' ORDER BY fsq_place_id LIMIT 1"
        ).fetchone()[0]

        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["name", "fsq_place_id"],
            exclude=False,
            ignore_missing=False,
            verbose=False,
        )

        # Get value from output
        output = con.execute(
            f"SELECT name FROM '{temp_output_file}' ORDER BY fsq_place_id LIMIT 1"
        ).fetchone()[0]
        con.close()

        assert original == output


class TestSelectWithDifferentFormats:
    """Test select with different compression and format options."""

    def test_select_with_different_compressions(self, places_test_file, temp_output_dir):
        """Test select with various compression types."""
        compressions = ["ZSTD", "GZIP", "SNAPPY", "LZ4"]

        for comp in compressions:
            output_file = os.path.join(temp_output_dir, f"output_{comp}.parquet")
            select_fields(
                input_parquet=places_test_file,
                output_parquet=output_file,
                fields=["name"],
                exclude=False,
                ignore_missing=False,
                verbose=False,
                compression=comp,
            )
            assert os.path.exists(output_file), f"Failed for compression {comp}"

    def test_select_with_row_group_options(self, places_test_file, temp_output_file):
        """Test select with row group size options."""
        select_fields(
            input_parquet=places_test_file,
            output_parquet=temp_output_file,
            fields=["name"],
            exclude=False,
            ignore_missing=False,
            verbose=False,
            row_group_rows=100,
        )
        assert os.path.exists(temp_output_file)
