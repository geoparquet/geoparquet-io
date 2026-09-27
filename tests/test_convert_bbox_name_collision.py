"""Convert must not collide its computed bbox column with an input column (#1079).

A 1.x input can carry a *non-struct* column named ``bbox`` (say a string tile
id). ``check_bbox_structure`` rightly reports no bbox column, so convert
computes one — but aliasing it ``AS bbox`` next to ``SELECT *`` makes DuckDB
rename the computed struct to ``bbox_1`` while the covering still pointed at
the string column. Convert must pick a free name up front and declare the
covering over the column it actually wrote.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.bbox_structure import check_bbox_structure
from geoparquet_io.core.convert import _free_bbox_name, convert_to_geoparquet
from geoparquet_io.core.validate import validate_geoparquet

BBOX_STRUCT_FIELDS = {"xmin", "ymin", "xmax", "ymax"}


def _read_covering_bbox_column(path):
    """The column the output's covering.bbox points at (None when absent)."""
    geo = json.loads(pq.read_schema(str(path)).metadata[b"geo"].decode("utf-8"))
    covering = geo["columns"][geo["primary_column"]].get("covering")
    if not covering:
        return None
    return covering["bbox"]["xmin"][0]


def _assert_covering_matches_schema(output):
    """The covering names a real struct column; nothing lands undeclared."""
    schema = pq.read_schema(str(output))
    covering_col = _read_covering_bbox_column(output)
    assert covering_col is not None, "expected a bbox covering to be declared"
    assert covering_col in schema.names, (
        f"covering points at '{covering_col}' which is not in the schema {schema.names}"
    )
    field = schema.field(covering_col)
    assert pa.types.is_struct(field.type), (
        f"covering points at '{covering_col}' which is not a struct: {field.type}"
    )
    assert {f.name for f in field.type} >= BBOX_STRUCT_FIELDS

    # No stray computed struct outside the declared covering: every bbox-shaped
    # struct column in the output must be the declared one.
    stray = [
        name
        for name in schema.names
        if name != covering_col
        and pa.types.is_struct(schema.field(name).type)
        and {f.name for f in schema.field(name).type} >= BBOX_STRUCT_FIELDS
    ]
    assert not stray, f"undeclared bbox struct columns in output: {stray}"

    bbox_info = check_bbox_structure(str(output), verbose=False)
    assert bbox_info["has_bbox_column"]
    assert bbox_info["bbox_column_name"] == covering_col
    assert bbox_info["has_bbox_metadata"]
    assert bbox_info["covering_problem"] is None

    result = validate_geoparquet(str(output))
    failed = [c.message for c in result.checks if c.status.value == "failed"]
    assert result.is_valid, f"output failed spec validation: {failed}"
    return covering_col


@pytest.fixture
def parquet_with_string_bbox(test_data_dir, tmp_path):
    """A 1.x GeoParquet input whose ``bbox`` column is a plain string."""
    table = pq.read_table(str(test_data_dir / "buildings_test.parquet"))
    tile_ids = pa.array([f"tile-{i}" for i in range(table.num_rows)], type=pa.string())
    table = table.append_column("bbox", tile_ids)
    path = tmp_path / "string_bbox_input.parquet"
    pq.write_table(table, str(path))
    return path


class TestFreeBboxName:
    def test_no_collision_keeps_bbox(self):
        assert _free_bbox_name(["id", "geometry"]) == "bbox"

    def test_collision_picks_bbox_1(self):
        assert _free_bbox_name(["id", "bbox", "geometry"]) == "bbox_1"

    def test_chained_collision_picks_next_free(self):
        assert _free_bbox_name(["id", "bbox", "bbox_1", "geometry"]) == "bbox_2"

    def test_collision_is_case_insensitive(self):
        # Parquet allows both spellings; DuckDB binds identifiers
        # case-insensitively, so either spelling collides.
        assert _free_bbox_name(["id", "BBOX", "geometry"]) == "bbox_1"


class TestParquetStringBboxCollision:
    def test_covering_declared_over_computed_column(self, parquet_with_string_bbox, tmp_path):
        output = tmp_path / "out.parquet"
        convert_to_geoparquet(
            str(parquet_with_string_bbox),
            str(output),
            geoparquet_version="1.1",
        )

        covering_col = _assert_covering_matches_schema(output)
        assert covering_col == "bbox_1"

        # The user's string column survives untouched, next to the computed one.
        out_table = pq.read_table(str(output))
        assert pa.types.is_string(out_table.schema.field("bbox").type)
        assert set(out_table.column("bbox").to_pylist()) == {
            f"tile-{i}" for i in range(out_table.num_rows)
        }

    def test_input_with_bbox_and_bbox_1_taken(self, parquet_with_string_bbox, tmp_path):
        table = pq.read_table(str(parquet_with_string_bbox))
        table = table.append_column("bbox_1", pa.array(["x"] * table.num_rows, type=pa.string()))
        stacked = tmp_path / "stacked.parquet"
        pq.write_table(table, str(stacked))

        output = tmp_path / "out.parquet"
        convert_to_geoparquet(str(stacked), str(output), geoparquet_version="1.1")

        covering_col = _assert_covering_matches_schema(output)
        assert covering_col == "bbox_2"


class TestCsvStringBboxCollision:
    def test_csv_covering_declared_over_computed_column(self, tmp_path):
        csv_path = tmp_path / "points.csv"
        csv_path.write_text(
            "id,bbox,wkt\n1,tile-a,POINT(-74.006 40.7128)\n2,tile-b,POINT(-0.1276 51.5074)\n",
            encoding="utf-8",
        )

        output = tmp_path / "out.parquet"
        convert_to_geoparquet(str(csv_path), str(output), geoparquet_version="1.1")

        covering_col = _assert_covering_matches_schema(output)
        assert covering_col == "bbox_1"

        out_table = pq.read_table(str(output))
        assert out_table.column("bbox").to_pylist() == ["tile-a", "tile-b"]


class TestOnlyOutputColumnsCollide:
    """The free name is picked from what the query EMITS, not from the raw source."""

    def test_a_wkt_column_named_bbox_is_not_a_collision(self, tmp_path, caplog):
        """The WKT source column is excluded from the output, so `bbox` is free."""
        import logging

        csv_path = tmp_path / "tiles.csv"
        csv_path.write_text(
            "id,bbox\n"
            '1,"POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"\n'
            '2,"POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))"\n',
            encoding="utf-8",
        )
        output = tmp_path / "out.parquet"

        with caplog.at_level(logging.WARNING):
            convert_to_geoparquet(
                str(csv_path), str(output), wkt_column="bbox", geoparquet_version="1.1"
            )

        assert "already has a column named" not in caplog.text
        assert _assert_covering_matches_schema(output) == "bbox"
        assert "bbox_1" not in pq.read_schema(str(output)).names

    def test_a_geojson_property_named_bbox_collides(self, tmp_path):
        """The ST_Read path: a source attribute called `bbox` gets `bbox_1` beside it."""
        geojson = tmp_path / "tiles.geojson"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"id": i, "bbox": f"tile-{i}"},
                            "geometry": {"type": "Point", "coordinates": [i, i]},
                        }
                        for i in range(3)
                    ],
                }
            ),
            encoding="utf-8",
        )
        output = tmp_path / "out.parquet"

        convert_to_geoparquet(str(geojson), str(output), geoparquet_version="1.1")

        assert _assert_covering_matches_schema(output) == "bbox_1"
        assert pq.read_table(str(output)).column("bbox").to_pylist() == [
            "tile-0",
            "tile-1",
            "tile-2",
        ]


class TestCollisionWarning:
    def test_names_the_colliding_column_as_spelled(
        self, parquet_with_string_bbox, tmp_path, caplog
    ):
        import logging

        source = pq.read_table(str(parquet_with_string_bbox))
        table = source.rename_columns(
            ["BBOX" if name == "bbox" else name for name in source.column_names]
        ).replace_schema_metadata(source.schema.metadata)
        upper = tmp_path / "upper.parquet"
        pq.write_table(table, str(upper))
        output = tmp_path / "out.parquet"

        with caplog.at_level(logging.WARNING):
            convert_to_geoparquet(str(upper), str(output), geoparquet_version="1.1")

        assert "column named 'BBOX'" in caplog.text
        assert "declaring the covering over it" in caplog.text

    def test_promises_no_covering_at_1_0(self, parquet_with_string_bbox, tmp_path, caplog):
        """1.0 has no covering key, so the warning must not claim one."""
        import logging

        output = tmp_path / "out.parquet"
        with caplog.at_level(logging.WARNING):
            convert_to_geoparquet(
                str(parquet_with_string_bbox), str(output), geoparquet_version="1.0"
            )

        assert "bbox_1" in caplog.text
        assert "declaring the covering" not in caplog.text
        assert "no covering metadata" in caplog.text
        geo = json.loads(pq.read_schema(str(output)).metadata[b"geo"])
        assert "covering" not in geo["columns"][geo["primary_column"]]

    def test_no_computed_bbox_and_no_warning_at_2_0(
        self, parquet_with_string_bbox, tmp_path, caplog
    ):
        """2.0 computes no bbox, so nothing can collide with the user's column."""
        import logging

        output = tmp_path / "out.parquet"
        with caplog.at_level(logging.WARNING):
            convert_to_geoparquet(
                str(parquet_with_string_bbox), str(output), geoparquet_version="2.0"
            )

        assert "already has a column named" not in caplog.text
        names = pq.read_schema(str(output)).names
        assert "bbox_1" not in names
        assert pa.types.is_string(pq.read_schema(str(output)).field("bbox").type)
