"""Physical files beside a FileGDB are not necessarily selectable dataset layers.

Never bind ST_Read with an adversarial name here: the original missing-layer
bug can terminate the process. Discover those names with ST_Read_Meta only.
"""

import shutil

import pytest

from geoparquet_io.core.convert import read_spatial_to_arrow
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.layers import _spatial_layer_metadata, list_layers


@pytest.fixture
def membership_connection():
    """Reuse the spatial database, including in the ordinary fast test lane."""
    con = get_duckdb_connection(load_spatial=True)
    try:
        yield con
    finally:
        con.close()


@pytest.fixture
def copied_filegdb(test_data_dir, tmp_path):
    """Mutate only a scratch copy of the existing GDAL test fixture."""
    return shutil.copytree(test_data_dir / "testopenfilegdb.gdb", tmp_path / "members.gdb")


@pytest.mark.parametrize("extra_file", ["orphan", "foreign-driver"])
def test_unregistered_physical_files_do_not_become_layers(
    copied_filegdb, membership_connection, extra_file, monkeypatch
):
    """Real OpenFileGDB and foreign-driver strays must not change membership."""
    con = membership_connection
    original = _spatial_layer_metadata(con, str(copied_filegdb))
    assert original is not None
    expected = {entry["name"] for entry in original[1]}
    assert {"point", "polygon"} <= expected
    assert list_layers(copied_filegdb) == sorted(expected)

    stray = copied_filegdb / "a00000fff.gdbtable"
    assert not stray.exists()
    if extra_file == "orphan":
        for suffix in (".gdbtable", ".gdbtablx"):
            shutil.copyfile(
                copied_filegdb / f"a0000000a{suffix}",
                stray.with_suffix(suffix),
            )
        expected_driver = "OpenFileGDB"
    else:
        stray.write_text(
            '{"type":"FeatureCollection","name":"foreign_payload","features":['
            '{"type":"Feature","properties":{"marker":999},'
            '"geometry":{"type":"Point","coordinates":[1,2]}}]}',
            encoding="utf-8",
        )
        expected_driver = "GeoJSON"

    # Prove the payload really is discoverable; a malformed/unreadable extra
    # file would let even the unsafe glob implementation pass this test.
    physical_rows = con.execute(
        f"SELECT driver_short_name, layers FROM ST_Read_Meta({sql_path(str(stray))})"
    ).fetchall()
    assert physical_rows
    assert {driver for driver, _entries in physical_rows} == {expected_driver}
    stray_names = {entry["name"] for _driver, entries in physical_rows for entry in entries}
    assert stray_names
    assert stray_names.isdisjoint(expected)

    metadata = _spatial_layer_metadata(con, str(copied_filegdb))
    assert metadata is not None
    assert {entry["name"] for entry in metadata[1]} == expected
    assert list_layers(copied_filegdb) == sorted(expected)

    # Keep these calls after the membership assertions: on the old glob
    # implementation the assertions fail before an unsafe name can bind.
    def unexpected_bind(*_args, **_kwargs):
        pytest.fail("Unregistered layer reached the native ST_Read expression builder")

    monkeypatch.setattr("geoparquet_io.core.convert._build_st_read_expr", unexpected_bind)
    for name in sorted(stray_names):
        with pytest.raises(InvalidParameterError) as error:
            read_spatial_to_arrow(str(copied_filegdb), layer=name)
        assert name in str(error.value)
        assert "Available layers:" in str(error.value)


def test_foreign_driver_cannot_impersonate_a_registered_layer(
    test_data_dir, tmp_path, membership_connection
):
    """A matching catalog name does not make GeoJSON a valid FileGDB table."""
    source = shutil.copytree(test_data_dir / "curved_geometry_test.gdb", tmp_path / "spoof.gdb")
    metadata = _spatial_layer_metadata(membership_connection, str(source))
    assert metadata is not None
    assert [entry["name"] for entry in metadata[1]] == ["curve"]

    table = source / "a00000009.gdbtable"
    assert table.is_file()
    table.write_text(
        '{"type":"FeatureCollection","name":"curve","features":['
        '{"type":"Feature","properties":{},'
        '"geometry":{"type":"Point","coordinates":[1,2]}}]}',
        encoding="utf-8",
    )
    physical_rows = membership_connection.execute(
        f"SELECT driver_short_name, layers FROM ST_Read_Meta({sql_path(str(table))})"
    ).fetchall()
    assert [
        (driver, [entry["name"] for entry in entries]) for driver, entries in physical_rows
    ] == [("GeoJSON", ["curve"])]

    with pytest.raises(RuntimeError):
        list_layers(source)


@pytest.mark.parametrize("damage", ["empty-directory", "missing-catalog", "corrupt-catalog"])
def test_unusable_filegdb_is_an_error_not_a_single_layer(copied_filegdb, damage):
    """The public None result must not disguise a dataset that cannot enumerate."""
    if damage == "empty-directory":
        # This directory is owned by this test, not the source fixture.
        source = copied_filegdb.parent / "empty.gdb"
        source.mkdir()
    else:
        source = copied_filegdb
        catalog = source / "a00000001.gdbtable"
        if damage == "missing-catalog":
            catalog.unlink()
        else:
            catalog.write_bytes(b"not a FileGDB table\x00")

    with pytest.raises(RuntimeError):
        list_layers(source)


@pytest.mark.parametrize("feature_dataset", [None, "nested_dataset"], ids=["single", "nested"])
def test_registered_unicode_layer_reads_from_quoted_path(
    tmp_path, membership_connection, feature_dataset
):
    """Single/nested registered layers remain readable despite list_layers=None."""
    geopandas = pytest.importorskip("geopandas")
    pyogrio = pytest.importorskip("pyogrio")
    from shapely.geometry import Point

    source = tmp_path / "O'Brien_東京.gdb"
    layer = "registered_東京"
    frame = geopandas.GeoDataFrame({"marker": [731]}, geometry=[Point(1, 2)], crs="EPSG:4326")
    frame["marker"] = frame["marker"].astype("int32")
    options = {"FEATURE_DATASET": feature_dataset} if feature_dataset else {}
    pyogrio.write_dataframe(frame, source, layer=layer, driver="OpenFileGDB", layer_options=options)
    assert pyogrio.list_layers(source)[:, 0].tolist() == [layer]

    metadata = _spatial_layer_metadata(membership_connection, str(source))
    assert metadata is not None
    assert [entry["name"] for entry in metadata[1]] == [layer]
    assert list_layers(source) is None

    # The independent GDAL enumeration above establishes that this name is
    # valid before either the old or new implementation can bind ST_Read.
    table, _crs, geometry_column = read_spatial_to_arrow(str(source), layer=layer)
    assert table.num_rows == 1
    assert table["marker"].to_pylist() == [731]
    assert geometry_column == "geometry"
    assert table[geometry_column].null_count == 0
