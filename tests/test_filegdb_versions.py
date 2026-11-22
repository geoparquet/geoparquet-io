"""A layer guard must preserve old vector datasets and reject raster internals."""

from zipfile import ZipFile

import pytest

from geoparquet_io.core.convert import read_spatial_to_arrow
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.layers import _spatial_layer_metadata, list_layers


@pytest.fixture
def unpack_filegdb(test_data_dir, tmp_path):
    def unpack(name):
        with ZipFile(test_data_dir / "filegdb-fixtures" / f"{name}.zip") as archive:
            archive.extractall(tmp_path)
        return str(tmp_path / name)

    return unpack


@pytest.mark.parametrize("version", [92, 93])
def test_old_filegdb_catalog_keeps_vector_layers(unpack_filegdb, version):
    source = unpack_filegdb(f"testopenfilegdb{version}.gdb")
    names = list_layers(source)
    assert {"point", "polygon", "none"} <= set(names)
    assert all(not name.endswith(".gdbtable") for name in names)
    table, _crs, geometry_column = read_spatial_to_arrow(source, layer="point")
    assert table.num_rows > 0
    assert geometry_column == "geometry"
    assert table[geometry_column].null_count == 0


def test_raster_backing_tables_are_not_vector_layers(unpack_filegdb, monkeypatch):
    source = unpack_filegdb("int8.gdb")
    con = get_duckdb_connection(load_spatial=True)
    try:
        assert _spatial_layer_metadata(con, source) == ("OpenFileGDB", [])
    finally:
        con.close()
    assert list_layers(source) is None

    def unexpected_bind(*_args, **_kwargs):
        pytest.fail("Raster internals reached the vector ST_Read expression builder")

    monkeypatch.setattr("geoparquet_io.core.convert._build_st_read_expr", unexpected_bind)
    for name in ("int8", "fras_ras_int8", "fras_blk_int8"):
        with pytest.raises(InvalidParameterError, match="Available layers: \\(none\\)"):
            read_spatial_to_arrow(source, layer=name)
