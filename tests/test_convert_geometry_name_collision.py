"""A tabular source that already has a column named ``geometry`` (#1176).

The CSV/Parquet-tabular conversion query aliases the parsed geometry
``AS geometry`` next to ``SELECT * EXCLUDE (<source cols>)``. A *non-geometry*
column called ``geometry`` -- a label, say -- beside ``--wkt-column`` or
``--lat-column/--lon-column`` therefore collided with it: DuckDB renamed the
computed struct and the Hilbert key, the repair pass and the geo block all bound
to the VARCHAR column instead ("Invalid geometry data: ... ST_IsEmpty(VARCHAR)").

The fix is the one #1079/#1168 gave the computed bbox: pick a free name up front
and use that one name everywhere.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.convert import convert_to_geoparquet


def _geo_block(path):
    metadata = pq.read_table(path).schema.metadata
    assert metadata and b"geo" in metadata, "output carries no 'geo' key"
    return json.loads(metadata[b"geo"])


def _assert_wkb_geometry(path):
    """The declared primary column exists and really holds WKB."""
    table = pq.read_table(path)
    geo = _geo_block(path)
    primary = geo["primary_column"]
    assert primary in table.column_names, (
        f"geo.primary_column '{primary}' is not a column of the output: {table.column_names}"
    )
    field_type = table.schema.field(primary).type
    assert pa.types.is_binary(field_type) or pa.types.is_large_binary(field_type), (
        f"primary column '{primary}' is {field_type}, not WKB"
    )
    return table, primary


class TestCsvGeometryColumnCollision:
    """A CSV whose own ``geometry`` column is not the geometry."""

    def test_wkt_column_beside_a_label_column_named_geometry(self, tmp_path):
        source = tmp_path / "labels.csv"
        source.write_text(
            "name,geometry,wkt\n"
            "a,label-a,POINT (1 2)\n"
            "b,label-b,POINT (3 4)\n"
            "c,label-c,POINT (5 6)\n"
        )
        output = tmp_path / "out.parquet"

        convert_to_geoparquet(str(source), str(output), wkt_column="wkt")

        table, primary = _assert_wkb_geometry(output)
        assert table.num_rows == 3
        # The user's own column keeps its name and its values.
        assert table.column("geometry").to_pylist() == ["label-a", "label-b", "label-c"]
        assert primary != "geometry"

    def test_latlon_columns_beside_a_label_column_named_geometry(self, tmp_path):
        source = tmp_path / "points.csv"
        source.write_text("geometry,lat,lon\nfirst,1.0,2.0\nsecond,3.0,4.0\n")
        output = tmp_path / "out.parquet"

        convert_to_geoparquet(
            str(source), str(output), lat_column="lat", lon_column="lon"
        )

        table, _ = _assert_wkb_geometry(output)
        assert table.num_rows == 2
        assert table.column("geometry").to_pylist() == ["first", "second"]

    def test_wkt_column_named_geometry_still_becomes_the_geometry(self, tmp_path):
        """No collision: the WKT column is consumed, so ``geometry`` is free (#1164)."""
        source = tmp_path / "wkt.csv"
        source.write_text("name,geometry\na,POINT (1 2)\nb,POINT (3 4)\n")
        output = tmp_path / "out.parquet"

        convert_to_geoparquet(str(source), str(output), wkt_column="geometry")

        table, primary = _assert_wkb_geometry(output)
        assert primary == "geometry"
        assert table.num_rows == 2

    def test_collision_is_announced(self, tmp_path, caplog):
        import logging

        source = tmp_path / "labels.csv"
        source.write_text("geometry,wkt\nlabel-a,POINT (1 2)\n")
        output = tmp_path / "out.parquet"

        with caplog.at_level(logging.WARNING):
            convert_to_geoparquet(str(source), str(output), wkt_column="wkt")

        assert "column named 'geometry'" in caplog.text
        assert "geometry_1" in caplog.text

    def test_output_is_valid_geoparquet(self, tmp_path):
        from geoparquet_io.core.validate import validate_geoparquet

        source = tmp_path / "labels.csv"
        source.write_text("geometry,wkt\nlabel-a,POINT (1 2)\nlabel-b,POINT (3 4)\n")
        output = tmp_path / "out.parquet"

        convert_to_geoparquet(str(source), str(output), wkt_column="wkt")

        result = validate_geoparquet(str(output))
        failed = [check.message for check in result.checks if check.status.value == "failed"]
        assert result.is_valid, f"output failed spec validation: {failed}"

    def test_skip_invalid_with_a_label_column_named_geometry(self, tmp_path):
        source = tmp_path / "labels.csv"
        source.write_text("geometry,wkt\nlabel-a,POINT (1 2)\nlabel-b,NOT WKT\n")
        output = tmp_path / "out.parquet"

        convert_to_geoparquet(str(source), str(output), wkt_column="wkt", skip_invalid=True)

        table, _ = _assert_wkb_geometry(output)
        assert table.num_rows == 1
        assert table.column("geometry").to_pylist() == ["label-a"]


@pytest.mark.parametrize("taken", ["geometry", "GEOMETRY"])
def test_case_variant_collision(tmp_path, taken):
    """Parquet names are case-sensitive; DuckDB binds identifiers case-insensitively."""
    source = tmp_path / "labels.csv"
    source.write_text(f"{taken},wkt\nlabel-a,POINT (1 2)\n")
    output = tmp_path / "out.parquet"

    convert_to_geoparquet(str(source), str(output), wkt_column="wkt")

    table, primary = _assert_wkb_geometry(output)
    assert table.column(taken).to_pylist() == ["label-a"]
    assert primary.lower() != taken.lower()


class TestArrowReadGeometryColumnCollision:
    """``gpio.convert()`` builds the same geometry through Arrow, not SQL COPY."""

    def test_no_duplicate_column_names(self, tmp_path):
        from geoparquet_io.api.table import convert

        source = tmp_path / "labels.csv"
        source.write_text("geometry,wkt\nlabel-a,POINT (1 2)\nlabel-b,POINT (3 4)\n")

        result = convert(str(source), wkt_column="wkt")

        names = result.table.column_names
        assert len(names) == len(set(names)), f"duplicate column names in the table: {names}"
        assert result.geometry_column != "geometry"
        geom_type = result.table.schema.field(result.geometry_column).type
        assert pa.types.is_binary(geom_type) or pa.types.is_large_binary(geom_type)
        assert result.table.column("geometry").to_pylist() == ["label-a", "label-b"]

    def test_the_written_file_declares_the_column_it_wrote(self, tmp_path):
        from geoparquet_io.api.table import convert

        source = tmp_path / "labels.csv"
        source.write_text("geometry,lat,lon\nfirst,1.0,2.0\n")
        output = tmp_path / "out.parquet"

        convert(str(source), lat_column="lat", lon_column="lon").write(str(output))

        table, primary = _assert_wkb_geometry(output)
        assert table.column("geometry").to_pylist() == ["first"]
        assert primary != "geometry"
