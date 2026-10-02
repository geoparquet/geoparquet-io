"""The bounds pass must read the layer the conversion reads (#1176).

``_calculate_bounds`` rebuilt the source expression from the input path alone,
dropping ``layer``, so ``gpio convert --layer roads multilayer.gpkg`` measured
the *first* layer: a binder error when the layers' geometry columns are named
differently, and a silently wrong Hilbert extent when they are not.
``tests/test_convert_layer.py`` passes ``--skip-hilbert`` throughout, which is
why nothing caught it.
"""

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.convert import (
    _calculate_bounds,
    _detect_spatial_geometry,
    convert_to_geoparquet,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection


@pytest.fixture
def multilayer_gpkg(test_data_dir):
    """Two layers whose geometry columns differ: buildings/geometry, roads/geom."""
    return str(test_data_dir / "multilayer_test.gpkg")


@pytest.fixture
def con():
    connection = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        yield connection
    finally:
        connection.close()


class TestCalculateBoundsHonoursLayer:
    def test_bounds_of_a_non_first_layer(self, con, multilayer_gpkg):
        """The second layer's geometry column only exists in that layer."""
        geom_column, _aliases = _detect_spatial_geometry(
            con, multilayer_gpkg, False, "roads", None
        )
        assert geom_column == "geom"

        bounds = _calculate_bounds(con, multilayer_gpkg, geom_column, False, layer="roads")

        assert bounds is not None
        xmin, ymin, xmax, ymax = bounds
        assert xmin < xmax and ymin < ymax

    def test_bounds_match_the_layer_read(self, con, multilayer_gpkg):
        """Measured through ST_Read for that layer, not the file's first one."""
        expected = con.execute(
            "SELECT MIN(ST_XMin(geom)), MIN(ST_YMin(geom)), "
            "MAX(ST_XMax(geom)), MAX(ST_YMax(geom)) "
            f"FROM ST_Read('{multilayer_gpkg}', layer := 'roads')"
        ).fetchone()

        bounds = _calculate_bounds(con, multilayer_gpkg, "geom", False, layer="roads")

        assert bounds == expected


class TestConvertLayerWithHilbert:
    def test_non_first_layer_converts_with_hilbert_ordering(self, multilayer_gpkg, tmp_path):
        output = tmp_path / "roads.parquet"

        convert_to_geoparquet(multilayer_gpkg, str(output), layer="roads")

        table = pq.read_table(str(output))
        assert table.num_rows == 42
        assert "id" in table.column_names

    def test_first_layer_still_converts_with_hilbert_ordering(self, multilayer_gpkg, tmp_path):
        output = tmp_path / "buildings.parquet"

        convert_to_geoparquet(multilayer_gpkg, str(output), layer="buildings")

        assert pq.read_table(str(output)).num_rows == 42
