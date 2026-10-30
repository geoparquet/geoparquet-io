"""
Tests for sort commands.
"""

import io
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path
from unittest import mock

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import sort
from geoparquet_io.core import sort_by_column as sort_by_column_module
from geoparquet_io.core import sort_quadkey as sort_quadkey_module
from geoparquet_io.core.duckdb_utils import fetch_one
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.sort_by_column import sort_by_column, sort_by_column_table
from geoparquet_io.core.sort_quadkey import sort_by_quadkey
from tests.conftest import safe_unlink


class TestSortCommands:
    """Test suite for sort commands."""

    def test_hilbert_sort_places(self, places_test_file, temp_output_file):
        """Test Hilbert sort on places file."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", places_test_file, temp_output_file])
        assert result.exit_code == 0
        # Verify output file was created
        assert os.path.exists(temp_output_file)

        # Verify row count matches
        conn = duckdb.connect()
        input_count = fetch_one(conn.execute(f'SELECT COUNT(*) FROM "{places_test_file}"'))[0]
        output_count = fetch_one(conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"'))[0]
        assert input_count == output_count

    def test_hilbert_sort_buildings(self, buildings_test_file, temp_output_file):
        """Test Hilbert sort on buildings file."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", buildings_test_file, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify row count matches
        conn = duckdb.connect()
        input_count = fetch_one(conn.execute(f'SELECT COUNT(*) FROM "{buildings_test_file}"'))[0]
        output_count = fetch_one(conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"'))[0]
        assert input_count == output_count

    def test_hilbert_sort_with_verbose(self, places_test_file, temp_output_file):
        """Test Hilbert sort with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", places_test_file, temp_output_file, "--verbose"])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_hilbert_sort_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        """--write-memory must configure the DuckDB write connection, not be dropped."""
        runner = CliRunner()
        result = runner.invoke(
            sort,
            [
                "hilbert",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "512MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0
        assert "DuckDB memory limit: 512MB" in result.output

    def test_hilbert_sort_with_custom_geometry_column(self, places_test_file, temp_output_file):
        """Test Hilbert sort with custom geometry column name."""
        runner = CliRunner()
        result = runner.invoke(
            sort, ["hilbert", places_test_file, temp_output_file, "--geometry-column", "geometry"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_hilbert_sort_with_add_bbox(self, buildings_test_file, temp_output_file):
        """Test Hilbert sort with add-bbox flag."""
        runner = CliRunner()
        result = runner.invoke(
            sort, ["hilbert", buildings_test_file, temp_output_file, "--add-bbox"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify bbox column was added
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bbox" in column_names

    def test_hilbert_sort_preserves_columns_places(self, places_test_file, temp_output_file):
        """Test that Hilbert sort preserves all columns from places file."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", places_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        # All input columns should be in output
        assert input_col_names.issubset(output_col_names)

    def test_hilbert_sort_preserves_columns_buildings(self, buildings_test_file, temp_output_file):
        """Test that Hilbert sort preserves all columns from buildings file."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{buildings_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        # All input columns should be in output
        assert input_col_names.issubset(output_col_names)

    def test_hilbert_sort_nonexistent_file(self, temp_output_file):
        """Test Hilbert sort on nonexistent file."""
        runner = CliRunner()
        result = runner.invoke(sort, ["hilbert", "nonexistent.parquet", temp_output_file])
        # Should fail with non-zero exit code
        assert result.exit_code != 0

    def test_str_sort_places(self, places_test_file, temp_output_file):
        """STR is exposed as a file-producing sort alternative."""
        runner = CliRunner()
        result = runner.invoke(
            sort,
            [
                "str",
                places_test_file,
                temp_output_file,
                "--row-group-size",
                "100",
            ],
        )

        assert result.exit_code == 0, result.output
        assert os.path.exists(temp_output_file)
        assert pq.read_table(temp_output_file).num_rows == 766


class TestSortColumnCommands:
    """Test suite for column sort commands."""

    def test_column_sort_single(self, places_test_file, temp_output_file):
        """Test sorting by a single column."""
        runner = CliRunner()
        # Get a column name from the file first
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        # Find a non-geometry column
        test_column = None
        for col in columns:
            if col[0] != "geometry":
                test_column = col[0]
                break
        conn.close()

        assert test_column is not None, "No non-geometry columns found"

        result = runner.invoke(sort, ["column", places_test_file, temp_output_file, test_column])
        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert os.path.exists(temp_output_file)

        # Verify row count matches
        conn = duckdb.connect()
        input_count = fetch_one(conn.execute(f'SELECT COUNT(*) FROM "{places_test_file}"'))[0]
        output_count = fetch_one(conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"'))[0]
        assert input_count == output_count
        conn.close()

    def test_column_sort_descending(self, places_test_file, temp_output_file):
        """Test sorting in descending order."""
        runner = CliRunner()
        # Get a column name from the file first
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        # Find a non-geometry column
        test_column = None
        for col in columns:
            if col[0] != "geometry":
                test_column = col[0]
                break
        conn.close()

        assert test_column is not None, "No non-geometry columns found"

        result = runner.invoke(
            sort, ["column", places_test_file, temp_output_file, test_column, "--descending"]
        )
        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert os.path.exists(temp_output_file)

    def test_column_sort_invalid_column(self, places_test_file, temp_output_file):
        """Test sorting by a column that doesn't exist."""
        runner = CliRunner()
        result = runner.invoke(
            sort, ["column", places_test_file, temp_output_file, "nonexistent_column"]
        )
        # Should fail because column doesn't exist
        assert result.exit_code != 0
        assert "nonexistent_column" in result.output

    def test_column_sort_preserves_columns(self, places_test_file, temp_output_file):
        """Test that column sort preserves all columns."""
        runner = CliRunner()
        # Get a column name from the file first
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        # Find a non-geometry column
        test_column = None
        for col in columns:
            if col[0] != "geometry":
                test_column = col[0]
                break
        conn.close()

        assert test_column is not None, "No non-geometry columns found"

        result = runner.invoke(sort, ["column", places_test_file, temp_output_file, test_column])
        assert result.exit_code == 0, f"Failed with: {result.output}"

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        assert input_col_names == output_col_names
        conn.close()

    def test_column_sort_with_verbose(self, places_test_file, temp_output_file):
        """Test column sort with verbose flag."""
        runner = CliRunner()
        # Get a column name from the file first
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{places_test_file}"').fetchall()
        # Find a non-geometry column
        test_column = None
        for col in columns:
            if col[0] != "geometry":
                test_column = col[0]
                break
        conn.close()

        assert test_column is not None, "No non-geometry columns found"

        result = runner.invoke(
            sort, ["column", places_test_file, temp_output_file, test_column, "--verbose"]
        )
        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert os.path.exists(temp_output_file)


class TestSortByColumnTable:
    """Tests for sort_by_column_table function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def sample_table(self, places_file):
        """Create a sample table from places test data."""
        return pq.read_table(places_file)

    def test_sort_single_column(self, sample_table):
        """Test sorting by single column."""
        result = sort_by_column_table(sample_table, columns="name")
        assert result.num_rows == sample_table.num_rows

    def test_sort_multiple_columns(self, sample_table):
        """Test sorting by multiple columns."""
        result = sort_by_column_table(sample_table, columns=["name", "address"])
        assert result.num_rows == sample_table.num_rows

    def test_sort_descending(self, sample_table):
        """Test sorting in descending order."""
        result = sort_by_column_table(sample_table, columns="name", descending=True)
        assert result.num_rows == sample_table.num_rows

    def test_sort_invalid_column(self, sample_table):
        """Test error with invalid column name."""
        with pytest.raises(ValueError, match="not found in table"):
            sort_by_column_table(sample_table, columns="nonexistent_column")

    def test_sort_empty_columns(self, sample_table):
        """Test error with empty columns."""
        with pytest.raises(ValueError, match="not found in table"):
            sort_by_column_table(sample_table, columns="")

    def test_sort_metadata_preserved(self, sample_table):
        """Test that GeoParquet metadata is preserved."""
        result = sort_by_column_table(sample_table, columns="name")
        if sample_table.schema.metadata and b"geo" in sample_table.schema.metadata:
            assert b"geo" in result.schema.metadata


class TestSortByColumnStreaming:
    """Tests for streaming sort_by_column."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def sample_geo_table(self, places_file):
        """Create a geo table from test data."""
        return pq.read_table(places_file)

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_sort_column_stream_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_stdin_to_file(self, sample_geo_table, output_file, monkeypatch):
        """Test reading from mocked stdin."""
        # Create IPC buffer
        ipc_buffer = io.BytesIO()
        writer = ipc.RecordBatchStreamWriter(ipc_buffer, sample_geo_table.schema)
        writer.write_table(sample_geo_table)
        writer.close()
        ipc_buffer.seek(0)

        # Create a mock stdin with buffer attribute
        mock_stdin = mock.MagicMock()
        mock_stdin.isatty.return_value = False
        mock_stdin.buffer = ipc_buffer

        monkeypatch.setattr(sys, "stdin", mock_stdin)

        # Call function with "-" input
        sort_by_column("-", output_file, columns="name")

        # Verify output
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert result.num_rows == sample_geo_table.num_rows

    def test_file_to_stdout(self, places_file, monkeypatch):
        """Test writing to mocked stdout."""
        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        mock_stdout.isatty.return_value = False
        monkeypatch.setattr(sys, "stdout", mock_stdout)

        # Call function with "-" output
        sort_by_column(places_file, "-", columns="name")

        # Verify stream
        output_buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(output_buffer)
        result = reader.read_all()
        assert result.num_rows > 0


def _geo_bbox(parquet_path) -> list[float] | None:
    """The primary column's declared ``bbox`` from a file's geo metadata."""
    geo = json.loads(pq.ParquetFile(parquet_path).metadata.metadata[b"geo"])
    return geo["columns"][geo["primary_column"]].get("bbox")


@pytest.fixture
def places_parts_dir(places_test_file, tmp_path):
    """A directory of three GeoParquet files split out of places.parquet.

    Each part declares its OWN extent in geo metadata, so a first-file ``bbox``
    carried onto the merged output is distinguishable from one recomputed over
    everything written. Returns ``(directory, union_bbox)``.
    """
    table = pq.read_table(places_test_file)
    table = table.take(pc.sort_indices(pc.struct_field(table["bbox"], "xmin")))

    def extent(bbox_col):
        return [pc.min(pc.struct_field(bbox_col, k)).as_py() for k in ("xmin", "ymin")] + [
            pc.max(pc.struct_field(bbox_col, k)).as_py() for k in ("xmax", "ymax")
        ]

    parts_dir = tmp_path / "places_parts"
    parts_dir.mkdir()
    n = table.num_rows
    slices = [table.slice(0, n // 3), table.slice(n // 3, n // 3), table.slice(2 * (n // 3))]
    for i, part in enumerate(slices):
        meta = dict(part.schema.metadata)
        geo = json.loads(meta[b"geo"])
        geo["columns"]["geometry"]["bbox"] = extent(part["bbox"])
        meta[b"geo"] = json.dumps(geo).encode()
        pq.write_table(part.replace_schema_metadata(meta), parts_dir / f"part_{i}.parquet")

    union = extent(table["bbox"])
    # Fixture sanity: the first file's declared extent must NOT be the union,
    # or the carry assertions below could not tell the two apart.
    assert _geo_bbox(parts_dir / "part_0.parquet") != pytest.approx(union)
    return parts_dir, union


class TestSortPartitionInput:
    """``sort column`` and ``sort quadkey`` accept a directory of files (#817).

    Both already read a quoted glob, but a bare directory reached DuckDB as
    ``FROM '<dir>'`` and died with a Catalog Error. ``sort hilbert`` and
    ``sort str`` reject every multi-file input with a consolidation hint, and
    keep doing so.
    """

    @pytest.mark.parametrize("version_args", [[], ["--geoparquet-version=2.0"]])
    def test_sort_column_accepts_bare_directory(
        self, places_parts_dir, temp_output_file, version_args
    ):
        parts_dir, union_bbox = places_parts_dir
        runner = CliRunner()
        result = runner.invoke(
            sort, ["column", f"{parts_dir}{os.sep}", temp_output_file, "name", *version_args]
        )
        assert result.exit_code == 0, result.output

        out = pq.read_table(temp_output_file)
        assert out.num_rows == 766
        names = out.column("name").to_pylist()
        assert names == sorted(names)
        # The merged output's extent, not the first part's (#793's carry guard).
        assert _geo_bbox(temp_output_file) == pytest.approx(union_bbox)

    @pytest.mark.parametrize("version_args", [[], ["--geoparquet-version=2.0"]])
    def test_sort_column_glob_describes_the_merged_output(
        self, places_parts_dir, temp_output_file, version_args
    ):
        parts_dir, union_bbox = places_parts_dir
        runner = CliRunner()
        result = runner.invoke(
            sort,
            ["column", str(parts_dir / "*.parquet"), temp_output_file, "name", *version_args],
        )
        assert result.exit_code == 0, result.output
        assert pq.read_metadata(temp_output_file).num_rows == 766
        assert _geo_bbox(temp_output_file) == pytest.approx(union_bbox)

    def test_sort_quadkey_accepts_bare_directory(self, places_parts_dir, temp_output_file):
        parts_dir, union_bbox = places_parts_dir
        runner = CliRunner()
        result = runner.invoke(sort, ["quadkey", str(parts_dir), temp_output_file])
        assert result.exit_code == 0, result.output

        out = pq.read_table(temp_output_file)
        assert out.num_rows == 766
        quadkeys = out.column("quadkey").to_pylist()
        assert quadkeys == sorted(quadkeys)
        assert _geo_bbox(temp_output_file) == pytest.approx(union_bbox)

    def test_sort_quadkey_directory_temp_name_has_no_glob_character(
        self, places_parts_dir, temp_output_file
    ):
        """The auto-add scratch file must be a name every OS can create.

        It used to end in ``os.path.basename(input_parquet)``; for a glob
        input that is a literal ``*.parquet``, and ``*`` is not a legal
        filename character on Windows.
        """
        parts_dir, _ = places_parts_dir
        from geoparquet_io.core.index_ops import ensure_index_column, get_index_spec

        working, column, temp_file = ensure_index_column(
            get_index_spec("quadkey"), str(parts_dir / "*.parquet"), "quadkey", 13
        )
        try:
            assert temp_file is not None
            assert not any(c in os.path.basename(temp_file) for c in '*?"<>|'), temp_file
            assert column in pq.read_schema(working).names
            result = CliRunner().invoke(sort, ["quadkey", working, temp_output_file])
            assert result.exit_code == 0, result.output
            quadkeys = pq.read_table(temp_output_file)[column].to_pylist()
            assert quadkeys == sorted(quadkeys)
            assert len(quadkeys) == 766
        finally:
            if temp_file:
                os.unlink(temp_file)

    @pytest.mark.parametrize("subcommand", ["hilbert", "str"])
    def test_spatial_sorts_still_reject_a_directory_with_guidance(
        self, subcommand, places_parts_dir, temp_output_file
    ):
        parts_dir, _ = places_parts_dir
        result = CliRunner().invoke(sort, [subcommand, str(parts_dir), temp_output_file])
        assert result.exit_code != 0
        assert "requires a single parquet file" in result.output
        assert "gpio extract" in result.output


@pytest.fixture
def mismatched_schema_dir(places_test_file, tmp_path):
    """A directory of two files whose geometry columns are named differently.

    ``a.parquet`` keeps the column as ``geometry``; ``b.parquet`` renames it to
    ``geom`` (in the schema and in its geo metadata). DuckDB refuses to read
    the pair as one glob, and the sort commands must turn that refusal into a
    user-facing error, not a raw ``InvalidInputException`` traceback.
    """
    table = pq.read_table(places_test_file)
    half = table.num_rows // 2
    parts_dir = tmp_path / "mismatched_parts"
    parts_dir.mkdir()
    pq.write_table(table.slice(0, half), parts_dir / "a.parquet")

    part = table.slice(half)
    meta = dict(part.schema.metadata)
    geo = json.loads(meta[b"geo"])
    geo["columns"]["geom"] = geo["columns"].pop("geometry")
    geo["primary_column"] = "geom"
    meta[b"geo"] = json.dumps(geo).encode()
    renamed = part.rename_columns(
        ["geom" if name == "geometry" else name for name in part.column_names]
    ).replace_schema_metadata(meta)
    pq.write_table(renamed, parts_dir / "b.parquet")
    return parts_dir


class TestSortPartitionSchemaMismatch:
    """Mismatched schemas across a directory's files fail cleanly (#817 follow-up).

    Reachable only through the multi-file input this branch added: a single
    file can never disagree with itself. The message points at the explicit
    reconciliation (``gpio extract ... --allow-schema-diff``) instead of
    quietly union-by-naming, which would NULL-fill a renamed geometry column.
    """

    def test_sort_column_cli_reports_mismatch_cleanly(
        self, mismatched_schema_dir, temp_output_file
    ):
        result = CliRunner().invoke(
            sort, ["column", str(mismatched_schema_dir), temp_output_file, "name"]
        )
        assert result.exit_code == 1
        assert "do not share one schema" in result.output
        assert "--allow-schema-diff" in result.output
        assert "InvalidInputException" not in result.output
        assert "Traceback" not in result.output
        assert not os.path.exists(temp_output_file)

    def test_sort_column_core_raises_geoparquet_error(
        self, mismatched_schema_dir, temp_output_file
    ):
        with pytest.raises(GeoParquetError, match="allow-schema-diff"):
            sort_by_column(str(mismatched_schema_dir), temp_output_file, "name")
        assert not os.path.exists(temp_output_file)

    def test_sort_quadkey_cli_reports_mismatch_cleanly(
        self, mismatched_schema_dir, temp_output_file
    ):
        """The auto-add step reads the whole glob, so it hits the mismatch."""
        result = CliRunner().invoke(sort, ["quadkey", str(mismatched_schema_dir), temp_output_file])
        assert result.exit_code == 1
        assert "do not share one schema" in result.output
        assert "--allow-schema-diff" in result.output
        assert "InvalidInputException" not in result.output
        assert not os.path.exists(temp_output_file)

    def test_sort_quadkey_existing_column_raises_geoparquet_error(
        self, mismatched_schema_dir, temp_output_file
    ):
        """With quadkey already present the sort itself reads the glob."""
        for name in ("a.parquet", "b.parquet"):
            path = mismatched_schema_dir / name
            part = pq.read_table(path)
            part = part.append_column("quadkey", pa.array(["0"] * part.num_rows))
            pq.write_table(part, path)

        with pytest.raises(GeoParquetError, match="allow-schema-diff"):
            sort_by_quadkey(str(mismatched_schema_dir), temp_output_file)
        assert not os.path.exists(temp_output_file)

    def test_sort_column_unrelated_invalid_input_propagates(
        self, places_parts_dir, temp_output_file
    ):
        """Only the schema-mismatch complaint is rewrapped; anything else
        DuckDB calls invalid input must keep its original type and text."""
        parts_dir, _ = places_parts_dir
        boom = duckdb.InvalidInputException("something else entirely")
        with mock.patch.object(
            sort_by_column_module, "write_parquet_with_metadata", side_effect=boom
        ):
            with pytest.raises(duckdb.InvalidInputException, match="something else entirely"):
                sort_by_column(str(parts_dir), temp_output_file, "name")

    def test_sort_quadkey_unrelated_invalid_input_propagates(
        self, places_parts_dir, temp_output_file
    ):
        parts_dir, _ = places_parts_dir
        boom = duckdb.InvalidInputException("something else entirely")
        with mock.patch.object(
            sort_quadkey_module, "write_parquet_with_metadata", side_effect=boom
        ):
            with pytest.raises(duckdb.InvalidInputException, match="something else entirely"):
                sort_by_quadkey(str(parts_dir), temp_output_file)


@pytest.fixture
def schema_diff_parts_dir(places_test_file, tmp_path):
    """Two parquet files where the second carries a column the first lacks.

    DuckDB's multi-file reader defaults to the first file's schema, so ``note``
    is dropped without a word unless the read asks for ``union_by_name``.
    Returns ``(directory, total_rows)``.
    """
    table = pq.read_table(places_test_file).slice(0, 200)
    parts_dir = tmp_path / "schema_diff_parts"
    parts_dir.mkdir()
    first = table.slice(0, 100)
    pq.write_table(first, parts_dir / "a.parquet")

    second = table.slice(100, 100)
    second = second.append_column("note", pa.array(["later"] * second.num_rows))
    pq.write_table(second.replace_schema_metadata(first.schema.metadata), parts_dir / "b.parquet")
    return parts_dir, 200


@pytest.fixture
def schema_diff_quadkey_parts_dir(places_test_file, tmp_path):
    """Like ``schema_diff_parts_dir``, but both files already carry a
    ``quadkey`` column, so ``sort quadkey`` skips the auto-add step and reads
    the directory glob directly. Returns ``(directory, total_rows)``."""
    table = pq.read_table(places_test_file).slice(0, 200)
    table = table.append_column("quadkey", pa.array([f"{i:03d}" for i in range(200)]))
    parts_dir = tmp_path / "schema_diff_quadkey_parts"
    parts_dir.mkdir()
    first = table.slice(0, 100)
    pq.write_table(first, parts_dir / "a.parquet")

    second = table.slice(100, 100)
    second = second.append_column("note", pa.array(["later"] * second.num_rows))
    pq.write_table(second.replace_schema_metadata(first.schema.metadata), parts_dir / "b.parquet")
    return parts_dir, 200


class TestSortAllowSchemaDiff:
    """``sort column``/``sort quadkey`` take ``--allow-schema-diff`` (#867).

    Without it a column only some files carry vanishes from the merged output,
    exactly as DuckDB's glob reader defaults. ``extract`` has spelled the
    opt-in ``--allow-schema-diff`` since it learned to read directories, so the
    sorts reuse that flag rather than inventing a second name for it.
    """

    def test_sort_column_drops_the_extra_column_by_default(
        self, schema_diff_parts_dir, temp_output_file
    ):
        parts_dir, total = schema_diff_parts_dir
        result = CliRunner().invoke(sort, ["column", str(parts_dir), temp_output_file, "name"])
        assert result.exit_code == 0, result.output
        out = pq.read_table(temp_output_file)
        assert out.num_rows == total
        assert "note" not in out.column_names

    def test_sort_column_allow_schema_diff_keeps_the_extra_column(
        self, schema_diff_parts_dir, temp_output_file
    ):
        parts_dir, total = schema_diff_parts_dir
        result = CliRunner().invoke(
            sort, ["column", str(parts_dir), temp_output_file, "name", "--allow-schema-diff"]
        )
        assert result.exit_code == 0, result.output
        out = pq.read_table(temp_output_file)
        assert out.num_rows == total
        assert "note" in out.column_names
        assert out.column("note").to_pylist().count("later") == 100

    def test_sort_column_allow_schema_diff_can_sort_by_the_extra_column(
        self, schema_diff_parts_dir, temp_output_file
    ):
        """The column check must see the union too, or the flag is unusable
        for the very column it was turned on to keep."""
        parts_dir, _ = schema_diff_parts_dir
        result = CliRunner().invoke(
            sort, ["column", str(parts_dir), temp_output_file, "note", "--allow-schema-diff"]
        )
        assert result.exit_code == 0, result.output
        assert "note" in pq.read_schema(temp_output_file).names

    def test_sort_column_without_the_flag_still_rejects_an_unknown_column(
        self, schema_diff_parts_dir, temp_output_file
    ):
        parts_dir, _ = schema_diff_parts_dir
        result = CliRunner().invoke(sort, ["column", str(parts_dir), temp_output_file, "note"])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_sort_quadkey_allow_schema_diff_keeps_the_extra_column(
        self, schema_diff_parts_dir, temp_output_file
    ):
        """The auto-add step reads the glob first, so the flag has to reach it."""
        parts_dir, total = schema_diff_parts_dir
        result = CliRunner().invoke(
            sort, ["quadkey", str(parts_dir), temp_output_file, "--allow-schema-diff"]
        )
        assert result.exit_code == 0, result.output
        out = pq.read_table(temp_output_file)
        assert out.num_rows == total
        assert "note" in out.column_names

    def test_sort_quadkey_drops_the_extra_column_by_default(
        self, schema_diff_parts_dir, temp_output_file
    ):
        parts_dir, total = schema_diff_parts_dir
        result = CliRunner().invoke(sort, ["quadkey", str(parts_dir), temp_output_file])
        assert result.exit_code == 0, result.output
        out = pq.read_table(temp_output_file)
        assert out.num_rows == total
        assert "note" not in out.column_names

    def test_core_sort_by_column_takes_the_same_keyword(
        self, schema_diff_parts_dir, temp_output_file
    ):
        parts_dir, _ = schema_diff_parts_dir
        sort_by_column(str(parts_dir), temp_output_file, "name", allow_schema_diff=True)
        assert "note" in pq.read_schema(temp_output_file).names

    def test_core_sort_by_quadkey_takes_the_same_keyword(
        self, schema_diff_parts_dir, temp_output_file
    ):
        parts_dir, _ = schema_diff_parts_dir
        sort_by_quadkey(str(parts_dir), temp_output_file, allow_schema_diff=True)
        assert "note" in pq.read_schema(temp_output_file).names

    def test_sort_quadkey_remove_column_allow_schema_diff_keeps_the_extra_column(
        self, schema_diff_quadkey_parts_dir, temp_output_file
    ):
        """``--remove-quadkey-column`` builds an explicit SELECT list, which
        must describe the union the read returns, not the first file's schema
        -- or the very column the flag protects is silently dropped."""
        parts_dir, total = schema_diff_quadkey_parts_dir
        result = CliRunner().invoke(
            sort,
            [
                "quadkey",
                str(parts_dir),
                temp_output_file,
                "--remove-quadkey-column",
                "--allow-schema-diff",
            ],
        )
        assert result.exit_code == 0, result.output
        out = pq.read_table(temp_output_file)
        assert out.num_rows == total
        assert "note" in out.column_names
        assert out.column("note").to_pylist().count("later") == 100
        assert "quadkey" not in out.column_names

    def test_sort_quadkey_remove_column_without_the_flag_keeps_first_file_schema(
        self, schema_diff_quadkey_parts_dir, temp_output_file
    ):
        """Without the flag the strict read returns the first file's columns,
        so the SELECT list built from that first file stays correct."""
        parts_dir, total = schema_diff_quadkey_parts_dir
        result = CliRunner().invoke(
            sort, ["quadkey", str(parts_dir), temp_output_file, "--remove-quadkey-column"]
        )
        assert result.exit_code == 0, result.output
        out = pq.read_table(temp_output_file)
        assert out.num_rows == total
        assert "note" not in out.column_names
        assert "quadkey" not in out.column_names


class TestSortSelfReadGuard:
    """An output written inside the input dataset is refused (#867).

    The directory input #852 added is re-globbed on every run, so an output
    left inside it becomes part of the input: the next run reads its own
    previous output back and the row count grows. Both sorts refuse before
    writing anything.
    """

    def test_sort_column_refuses_an_output_inside_the_input_directory(self, places_parts_dir):
        parts_dir, _ = places_parts_dir
        output = parts_dir / "sorted.parquet"
        result = CliRunner().invoke(sort, ["column", str(parts_dir), str(output), "name"])
        assert result.exit_code != 0
        assert str(parts_dir) in result.output
        assert "sorted.parquet" in result.output
        assert "somewhere else" in result.output
        assert not output.exists()

    def test_sort_column_refuses_an_output_matching_the_input_glob(self, places_parts_dir):
        parts_dir, _ = places_parts_dir
        output = parts_dir / "sorted.parquet"
        result = CliRunner().invoke(
            sort, ["column", str(parts_dir / "*.parquet"), str(output), "name"]
        )
        assert result.exit_code != 0
        assert "somewhere else" in result.output
        assert not output.exists()

    def test_sort_column_overwrite_does_not_bypass_the_guard(self, places_parts_dir):
        parts_dir, _ = places_parts_dir
        output = parts_dir / "sorted.parquet"
        output.write_bytes(b"stale")
        result = CliRunner().invoke(
            sort, ["column", str(parts_dir), str(output), "name", "--overwrite"]
        )
        assert result.exit_code != 0
        assert "somewhere else" in result.output
        assert output.read_bytes() == b"stale"

    def test_sort_quadkey_refuses_an_output_inside_the_input_directory(self, places_parts_dir):
        parts_dir, _ = places_parts_dir
        output = parts_dir / "sorted.parquet"
        result = CliRunner().invoke(sort, ["quadkey", str(parts_dir), str(output)])
        assert result.exit_code != 0
        assert "somewhere else" in result.output
        assert not output.exists()

    def test_a_sibling_directory_is_still_accepted(self, places_parts_dir, tmp_path):
        parts_dir, _ = places_parts_dir
        output = tmp_path / "elsewhere.parquet"
        result = CliRunner().invoke(sort, ["column", str(parts_dir), str(output), "name"])
        assert result.exit_code == 0, result.output
        assert pq.read_metadata(output).num_rows == 766


class TestSortEmptyDirectory:
    """An empty directory says so, instead of blaming the reader (#867)."""

    def test_sort_column_reports_no_parquet_files(self, tmp_path, temp_output_file):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = CliRunner().invoke(sort, ["column", str(empty), temp_output_file, "name"])
        assert result.exit_code != 0
        assert "No .parquet files found" in result.output
        assert str(empty) in result.output

    def test_sort_quadkey_reports_no_parquet_files(self, tmp_path, temp_output_file):
        empty = tmp_path / "empty"
        empty.mkdir()
        result = CliRunner().invoke(sort, ["quadkey", str(empty), temp_output_file])
        assert result.exit_code != 0
        assert "No .parquet files found" in result.output


class TestSortQuadkeyHiveInput:
    """``sort quadkey`` threads the resolved read options, not just the path."""

    def test_hive_partition_keys_survive_the_sort(self, tmp_path, places_test_file):
        table = pq.read_table(places_test_file).slice(0, 60)
        root = tmp_path / "hive_root"
        for i, country in enumerate(("US", "CA")):
            part_dir = root / f"country={country}"
            part_dir.mkdir(parents=True)
            pq.write_table(table.slice(i * 30, 30), part_dir / "part.parquet")

        output = tmp_path / "sorted.parquet"
        result = CliRunner().invoke(sort, ["quadkey", str(root), str(output)])
        assert result.exit_code == 0, result.output
        out = pq.read_table(output)
        assert out.num_rows == 60
        assert "country" in out.column_names
        assert set(out.column("country").to_pylist()) == {"US", "CA"}

    def test_a_failing_connection_does_not_mask_itself(self, places_parts_dir, temp_output_file):
        """``con.close()`` in the ``finally`` used to raise UnboundLocalError
        when the constructor itself threw, hiding the real failure."""
        parts_dir, _ = places_parts_dir
        boom = RuntimeError("no connection for you")
        with mock.patch.object(sort_quadkey_module, "get_duckdb_connection", side_effect=boom):
            with pytest.raises(RuntimeError, match="no connection for you"):
                sort_by_quadkey(str(parts_dir), temp_output_file)
