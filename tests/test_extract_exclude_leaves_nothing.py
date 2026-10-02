"""A column selection that leaves nothing is refused, not sent to DuckDB (#1176).

``gpio extract arcgis`` got this guard first; its siblings did not.
``build_column_selection`` happily returned ``[]``, which reached three SQL
builders and came back as a raw ``ParserException: SELECT clause without
selection list`` -- on the file-based CLI path, the streaming path and
``extract_table`` alike. Carto's tabular ``_apply_column_exclusions`` was worse:
it wrote a 0-column file without a word.

One guard in ``build_column_selection`` covers all three extract paths; carto's
own exclusion does the same for the table it fetched.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.carto import _apply_column_exclusions
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.extract import build_column_selection, extract_table


@pytest.fixture
def two_column_file(test_data_dir):
    """``buildings_test.parquet`` is exactly ``id`` and ``geometry``."""
    path = str(test_data_dir / "buildings_test.parquet")
    assert pq.read_schema(path).names == ["id", "geometry"]
    return path


class TestBuildColumnSelection:
    def test_excluding_every_column_is_refused(self):
        with pytest.raises(InvalidParameterError) as excinfo:
            build_column_selection(["id", "geometry"], None, ["id", "geometry"], "geometry", None)

        message = str(excinfo.value)
        assert "no columns" in message
        assert "id" in message and "geometry" in message

    def test_the_message_names_whichever_option_emptied_it(self):
        """An include list naming nothing the source has is the other way in."""
        with pytest.raises(InvalidParameterError) as excinfo:
            build_column_selection(["id"], ["not_a_column"], None, None, None)

        assert "'columns'" in str(excinfo.value)

    def test_an_exclude_all_names_the_exclude_option(self):
        with pytest.raises(InvalidParameterError) as excinfo:
            build_column_selection(["id", "geometry"], None, ["id", "geometry"], "geometry", None)

        assert "'exclude_cols'" in str(excinfo.value)

    def test_a_partial_exclusion_still_works(self):
        assert build_column_selection(
            ["id", "name", "geometry"], None, ["id", "name"], "geometry", None
        ) == ["geometry"]

    def test_a_source_with_no_columns_at_all_is_not_this_error(self):
        """Nothing was asked for and nothing is there: not a bad selection."""
        assert build_column_selection([], None, None, None, None) == []


class TestFileBasedCli:
    def test_excluding_every_column_fails_cleanly(self, two_column_file, tmp_path):
        output = tmp_path / "out.parquet"
        result = CliRunner().invoke(
            cli,
            [
                "extract",
                "geoparquet",
                two_column_file,
                str(output),
                "--exclude-cols",
                "id,geometry",
            ],
        )

        assert result.exit_code != 0
        assert "SELECT clause without selection list" not in result.output, (
            f"the empty selection reached DuckDB: {result.output}"
        )
        assert "no columns" in result.output
        assert not output.exists()

    def test_excluding_all_but_one_still_works(self, two_column_file, tmp_path):
        output = tmp_path / "out.parquet"
        result = CliRunner().invoke(
            cli, ["extract", "geoparquet", two_column_file, str(output), "--exclude-cols", "id"]
        )

        assert result.exit_code == 0, result.output
        assert pq.read_schema(str(output)).names == ["geometry"]

    def test_the_streaming_path_refuses_too(self, two_column_file):
        """Output to stdout takes the streaming path, which shares the guard."""
        result = CliRunner().invoke(
            cli, ["extract", "geoparquet", two_column_file, "-", "--exclude-cols", "id,geometry"]
        )

        assert result.exit_code != 0
        assert "no columns" in result.output


class TestExtractTable:
    def test_excluding_every_column_is_refused(self, two_column_file):
        table = pq.read_table(two_column_file)

        with pytest.raises(InvalidParameterError) as excinfo:
            extract_table(table, exclude_columns=["id", "geometry"])

        assert "no columns" in str(excinfo.value)

    def test_excluding_all_but_one_still_works(self, two_column_file):
        table = pq.read_table(two_column_file)

        result = extract_table(table, exclude_columns=["id"])

        assert result.column_names == ["geometry"]


class TestCartoExclusion:
    def test_excluding_every_column_is_refused(self):
        table = pa.table({"a": [1, 2], "b": [3, 4]})

        with pytest.raises(InvalidParameterError) as excinfo:
            _apply_column_exclusions(table, ["a", "b"], protect_geometry=False)

        message = str(excinfo.value)
        assert "no columns" in message
        assert "a" in message and "b" in message

    def test_a_partial_exclusion_still_works(self):
        table = pa.table({"a": [1, 2], "b": [3, 4]})

        result = _apply_column_exclusions(table, ["a"], protect_geometry=False)

        assert result.column_names == ["b"]
