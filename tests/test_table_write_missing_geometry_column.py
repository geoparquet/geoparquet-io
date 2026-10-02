"""``Table.write`` with a geometry column the table does not have (#1176).

Four strategies, four outcomes: duckdb-kv and disk-rewrite raised a bare
``KeyError``, streaming invented a ``geo`` block naming a column the file does
not contain, and in-memory wrote plain Parquet with no geometry metadata at all.
#1163 fixed the one producer that handed ``Table`` a stale name; the write
itself still has to say what is wrong.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.api.table import Table
from geoparquet_io.core.exceptions import InvalidParameterError

STRATEGIES = ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"]


class TestMissingGeometryColumnIsRefused:
    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_every_strategy_raises(self, strategy, buildings_test_file, tmp_path):
        table = Table(pq.read_table(buildings_test_file), geometry_column="geom")

        with pytest.raises(InvalidParameterError) as excinfo:
            table.write(tmp_path / f"{strategy}.parquet", write_strategy=strategy)

        message = str(excinfo.value)
        assert "geom" in message
        # The error names what the table does have, so the typo is fixable.
        assert "geometry" in message

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_nothing_is_written(self, strategy, buildings_test_file, tmp_path):
        output = tmp_path / f"{strategy}.parquet"
        table = Table(pq.read_table(buildings_test_file), geometry_column="geom")

        with pytest.raises(InvalidParameterError):
            table.write(output, write_strategy=strategy)

        assert not output.exists()

    def test_case_variant_is_not_the_same_column(self, buildings_test_file, tmp_path):
        """Parquet field names are case-sensitive, so ``GEOMETRY`` is not ``geometry``."""
        table = Table(pq.read_table(buildings_test_file), geometry_column="GEOMETRY")

        with pytest.raises(InvalidParameterError):
            table.write(tmp_path / "out.parquet")


class TestValidNamesStillWrite:
    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_a_present_column_writes(self, strategy, buildings_test_file, tmp_path):
        output = tmp_path / f"{strategy}.parquet"
        table = Table(pq.read_table(buildings_test_file), geometry_column="geometry")

        table.write(output, write_strategy=strategy)

        assert pq.read_table(str(output)).num_rows > 0

    def test_a_table_with_no_geometry_at_all_still_writes(self, tmp_path):
        """No geometry column is not a wrong geometry column: plain Parquet is fine."""
        output = tmp_path / "plain.parquet"
        table = Table(pa.table({"id": [1, 2], "name": ["a", "b"]}))
        assert table.geometry_column is None

        table.write(output)

        assert pq.read_table(str(output)).num_rows == 2
