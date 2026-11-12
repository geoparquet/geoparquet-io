"""Tests for core/partition_by_string.py module."""

from unittest.mock import patch

import pytest

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.partition.by_string import (
    partition_by_string,
    validate_column_exists,
)


class TestValidateColumnExists:
    """Tests for validate_column_exists function."""

    def test_valid_column(self, places_test_file):
        """Test validation passes for existing column."""
        # Should not raise
        validate_column_exists(places_test_file, "name", verbose=False)

    def test_invalid_column(self, places_test_file):
        """Test validation fails for non-existent column."""
        with pytest.raises(InvalidParameterError) as exc_info:
            validate_column_exists(places_test_file, "nonexistent_column", verbose=False)
        assert "not found" in str(exc_info.value)
        assert "nonexistent_column" in str(exc_info.value)

    def test_with_verbose(self, places_test_file):
        """Test validation with verbose output."""
        # Should not raise
        validate_column_exists(places_test_file, "name", verbose=True)


class TestPartitionByString:
    """Tests for partition_by_string function."""

    def test_chars_zero_raises_error(self, places_test_file, tmp_path):
        """Test that chars=0 raises InvalidParameterError (line 86)."""
        with pytest.raises(InvalidParameterError) as exc_info:
            partition_by_string(
                input_parquet=places_test_file,
                output_folder=str(tmp_path),
                column="name",
                chars=0,
                verbose=False,
            )
        assert "must be a positive integer" in str(exc_info.value)

    def test_chars_negative_raises_error(self, places_test_file, tmp_path):
        """Test that negative chars raises InvalidParameterError (line 86)."""
        with pytest.raises(InvalidParameterError) as exc_info:
            partition_by_string(
                input_parquet=places_test_file,
                output_folder=str(tmp_path),
                column="name",
                chars=-5,
                verbose=False,
            )
        assert "must be a positive integer" in str(exc_info.value)

    def test_preview_with_partition_analysis_error(self, places_test_file, tmp_path):
        """Test preview mode when PartitionAnalysisError is raised (lines 103-105)."""
        # Import the actual exception class
        from geoparquet_io.core.partition.common import PartitionAnalysisError

        # Patch at the source module where it's defined
        with patch(
            "geoparquet_io.core.partition.common.analyze_partition_strategy"
        ) as mock_analyze:
            mock_analyze.side_effect = PartitionAnalysisError("Test analysis error")

            # Should not raise - exception is caught and preview continues
            partition_by_string(
                input_parquet=places_test_file,
                output_folder=str(tmp_path),
                column="name",
                preview=True,
                verbose=False,
            )

    def test_preview_with_generic_exception(self, places_test_file, tmp_path):
        """Test preview mode when generic Exception is raised (lines 106-108)."""
        # Patch at the source module where it's defined
        with patch(
            "geoparquet_io.core.partition.common.analyze_partition_strategy"
        ) as mock_analyze:
            mock_analyze.side_effect = Exception("Unexpected error")

            # Should not raise - exception is caught and preview continues
            partition_by_string(
                input_parquet=places_test_file,
                output_folder=str(tmp_path),
                column="name",
                preview=True,
                verbose=False,
            )

    def test_preview_mode_basic(self, places_test_file, tmp_path, capsys):
        """Test basic preview mode execution."""
        partition_by_string(
            input_parquet=places_test_file,
            output_folder=str(tmp_path),
            column="name",
            preview=True,
            verbose=False,
        )
        # Preview should not create any files
        output_files = list(tmp_path.glob("*.parquet"))
        assert len(output_files) == 0

    def test_invalid_column_raises_error(self, places_test_file, tmp_path):
        """Test that invalid column raises UsageError."""
        with pytest.raises(InvalidParameterError) as exc_info:
            partition_by_string(
                input_parquet=places_test_file,
                output_folder=str(tmp_path),
                column="nonexistent_column",
                verbose=False,
            )
        assert "not found" in str(exc_info.value)


def _strip_geometry_types(src, dst):
    """Copy ``src`` to ``dst`` with ``geometry_types`` removed from its geo metadata.

    That makes the file unreadable by DuckDB, which is how a non-conformant
    GeoParquet from any producer reaches a partition command (#722).
    """
    import json

    import pyarrow.parquet as pq

    table = pq.read_table(src)
    metadata = dict(table.schema.metadata or {})
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    for col_meta in geo["columns"].values():
        col_meta.pop("geometry_types", None)
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    pq.write_table(table.replace_schema_metadata(metadata), dst)
    return dst


class TestNonConformantGeoMetadata:
    """A file DuckDB cannot read must fail with an explanation, not a traceback.

    gpio's own writers always emit ``geometry_types``, so this is the residual
    case: an input produced elsewhere whose ``geo`` metadata omits the key the
    GeoParquet spec requires. DuckDB refuses to open such a file at all, so
    there is nothing to fall back to -- but the user gets told what is wrong
    with which column instead of a raw ``_duckdb.InvalidInputException``.
    """

    def test_unrelated_errors_pass_through_untouched(self):
        """The translation is narrow: only DuckDB's missing-geometry-types text."""
        from geoparquet_io.core.partition.common import readable_geoparquet

        with pytest.raises(ValueError, match="something else entirely"):
            with readable_geoparquet("/tmp/whatever.parquet"):
                raise ValueError("something else entirely")

    def test_partition_string_reports_the_missing_key(self, places_test_file, tmp_path):
        from geoparquet_io.core.exceptions import PartitionError

        bad = _strip_geometry_types(places_test_file, str(tmp_path / "bad.parquet"))

        with pytest.raises(PartitionError) as exc_info:
            partition_by_string(
                input_parquet=bad,
                output_folder=str(tmp_path / "out"),
                column="name",
                chars=1,
                verbose=False,
            )

        message = str(exc_info.value)
        assert "geometry_types" in message
        assert "geometry" in message  # names the offending column
        assert "gpio check spec" in message  # tells the user what to run next

    def test_analysis_error_is_not_swallowed_by_force(self, places_test_file, tmp_path):
        """``--force`` waives *analysis* findings, not an unreadable input.

        Also: the refusal must not litter. ``--skip-analysis`` pushes the first
        DuckDB read past the pre-flight into ``partition_by_column``, which used
        to create the output directory first -- so a raise left an empty
        directory behind that a later ``--overwrite``-less run would trip over.
        """
        from geoparquet_io.core.exceptions import PartitionError

        bad = _strip_geometry_types(places_test_file, str(tmp_path / "bad.parquet"))
        out = tmp_path / "out"

        with pytest.raises(PartitionError, match="geometry_types"):
            partition_by_string(
                input_parquet=bad,
                output_folder=str(out),
                column="name",
                chars=1,
                force=True,
                skip_analysis=True,
                verbose=False,
            )

        assert not out.exists(), (
            f"an unreadable input left an output directory: {list(out.iterdir())}"
        )


def test_partition_accepts_glob_without_a_parquet_suffix(places_test_file, tmp_path):
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    source = tmp_path / "source"
    source.mkdir()
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(places_test_file).append_column(
        "grp", pa.array(["one"] * pq.read_metadata(places_test_file).num_rows)
    )
    pq.write_table(table, source / "data.parquet")
    output = tmp_path / "parts"
    result = CliRunner().invoke(
        cli,
        [
            "partition",
            "string",
            str(source / "data.par*"),
            str(output),
            "--column",
            "grp",
            "--skip-analysis",
        ],
    )
    assert result.exit_code == 0, (result.output, result.exception)
    assert list(output.rglob("*.parquet"))
