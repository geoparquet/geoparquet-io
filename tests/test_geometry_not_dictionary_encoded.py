"""A WKB geometry column is never dictionary-encoded on the Arrow write path (#1122).

A WKB blob is unique per row, so a dictionary is a full copy of the column plus
an index. The overviews spec (4.5) says a writer must not dictionary-encode the
geometry. Every other column keeps pyarrow's dictionary default.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1122
"""

from __future__ import annotations

import json
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.parquet_writer import ParquetWriteSettings, dictionary_columns
from geoparquet_io.core.write_funnels import write_geoparquet_table

DICT = "RLE_DICTIONARY"


def _point(x: float, y: float) -> bytes:
    return struct.pack("<BI2d", 1, 1, x, y)


def _encodings(path, column: str) -> tuple[str, ...]:
    meta = pq.ParquetFile(str(path)).metadata.row_group(0)
    for i in range(meta.num_columns):
        if meta.column(i).path_in_schema == column:
            return meta.column(i).encodings
    raise AssertionError(f"no column {column}")


def _table(geo: dict | None = None) -> pa.Table:
    n = 50
    table = pa.table(
        {
            "name": ["a", "b"] * (n // 2),
            "geometry": [_point(float(i), float(i)) for i in range(n)],
            "bbox": pa.array(
                [{"xmin": float(i), "ymin": 0.0, "xmax": float(i), "ymax": 0.0} for i in range(n)]
            ),
        }
    )
    if geo is not None:
        table = table.replace_schema_metadata({"geo": json.dumps(geo)})
    return table


class TestDictionaryColumns:
    def test_leaves_out_every_geometry_column(self):
        schema = pa.schema(
            [
                ("name", pa.string()),
                ("geometry", pa.binary()),
                ("geog", pa.binary()),
                ("other", pa.binary()),
            ],
            metadata={
                "geo": json.dumps(
                    {"primary_column": "geometry", "columns": {"geometry": {}, "geog": {}}}
                )
            },
        )
        assert dictionary_columns(schema) == ["name", "other"]

    def test_named_geometry_without_geo_block(self):
        schema = pa.schema([("name", pa.string()), ("wkb", pa.binary())])
        assert dictionary_columns(schema, ["wkb"]) == ["name"]

    def test_geoarrow_extension_field_is_geometry(self):
        field = pa.field("shape", pa.binary(), metadata={"ARROW:extension:name": "geoarrow.wkb"})
        schema = pa.schema([("name", pa.string()), field])
        assert dictionary_columns(schema) == ["name"]

    def test_nested_columns_are_named_by_leaf_path(self, tmp_path):
        """The names must match the written leaf paths, or pyarrow ignores them."""
        table = pa.table(
            {
                "s": pa.array([{"a": 1, "b": "x"}]),
                "l": pa.array([[1, 2]]),
                "m": pa.array([[("k", 1)]], type=pa.map_(pa.string(), pa.int64())),
                "d": pa.array(["x"]).dictionary_encode(),
                "geometry": pa.array([_point(0, 0)]),
            }
        )
        path = tmp_path / "nested.parquet"
        pq.write_table(table, path)
        meta = pq.ParquetFile(path).metadata.row_group(0)
        written = [meta.column(i).path_in_schema for i in range(meta.num_columns)]
        expected = [p for p in written if p != "geometry"]
        assert dictionary_columns(table.schema, ["geometry"]) == expected

    def test_non_geo_extension_column_is_named_by_its_storage(self):
        schema = pa.schema([("u", pa.uuid()), ("geometry", pa.binary())])
        assert dictionary_columns(schema, ["geometry"]) == ["u"]

    def test_settings_pass_the_list_to_pyarrow(self):
        schema = pa.schema([("name", pa.string()), ("geometry", pa.binary())])
        kwargs = ParquetWriteSettings().get_pyarrow_kwargs(
            schema=schema, geometry_columns=["geometry"]
        )
        assert kwargs["use_dictionary"] == ["name"]

    def test_settings_without_a_schema_keep_the_default(self):
        assert ParquetWriteSettings().get_pyarrow_kwargs()["use_dictionary"] is True


class TestArrowFunnel:
    @pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
    def test_geometry_has_no_dictionary(self, tmp_path, version):
        out = tmp_path / "out.parquet"
        write_geoparquet_table(
            _table(), str(out), geometry_column="geometry", geoparquet_version=version
        )
        assert DICT not in _encodings(out, "geometry")
        assert DICT in _encodings(out, "name")

    def test_bbox_struct_stays_dictionary_eligible(self, tmp_path):
        out = tmp_path / "out.parquet"
        write_geoparquet_table(
            _table(), str(out), geometry_column="geometry", geoparquet_version="1.1"
        )
        assert DICT in _encodings(out, "bbox.ymin")

    def test_secondary_geometry_has_no_dictionary(self, tmp_path):
        table = _table().append_column("geom2", pa.array([_point(1, 1)] * 50))
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB", "geometry_types": []},
                "geom2": {"encoding": "WKB", "geometry_types": []},
            },
        }
        out = tmp_path / "out.parquet"
        write_geoparquet_table(
            table.replace_schema_metadata({"geo": json.dumps(geo)}),
            str(out),
            geometry_column="geometry",
            geoparquet_version="1.1",
        )
        assert DICT not in _encodings(out, "geom2")


class TestOtherArrowWriters:
    def test_streaming_simplify(self, tmp_path):
        from geoparquet_io.core.process.simplify import simplify_file

        src = tmp_path / "in.parquet"
        write_geoparquet_table(
            _table(), str(src), geometry_column="geometry", geoparquet_version="1.1"
        )
        out = tmp_path / "out.parquet"
        simplify_file(str(src), str(out), 0.1, geoparquet_version="1.1")
        assert DICT not in _encodings(out, "geometry")
        assert DICT in _encodings(out, "name")

    def test_geo_metadata_rewrite(self, tmp_path):
        from geoparquet_io.core.derive_geo_from_file import _rewrite_file_with_geo_metadata

        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": []}},
        }
        out = tmp_path / "out.parquet"
        pq.write_table(_table(), out)
        assert DICT in _encodings(out, "geometry")  # pyarrow's own default
        _rewrite_file_with_geo_metadata(str(out), geo)
        assert DICT not in _encodings(out, "geometry")
        assert DICT in _encodings(out, "name")


def _dictionary(encodings: tuple[str, ...]) -> bool:
    """DuckDB writes PLAIN_DICTIONARY where pyarrow writes RLE_DICTIONARY."""
    return any("DICTIONARY" in e for e in encodings)


class TestTableWriteStrategies:
    """Every ``Table.write`` strategy makes the same choice as the funnel."""

    @pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
    @pytest.mark.parametrize("version", ["1.1", "2.0", "parquet-geo-only"])
    def test_geometry_has_no_dictionary(self, tmp_path, strategy, version):
        import geoparquet_io as gpio

        src = tmp_path / "in.parquet"
        write_geoparquet_table(
            _table(), str(src), geometry_column="geometry", geoparquet_version="1.1"
        )
        out = tmp_path / "out.parquet"
        gpio.read(str(src)).write(str(out), geoparquet_version=version, write_strategy=strategy)
        assert not _dictionary(_encodings(out, "geometry"))
        assert _dictionary(_encodings(out, "name"))

    @pytest.mark.parametrize("where", ["", " WHERE false"])
    def test_streaming_from_query(self, tmp_path, monkeypatch, where):
        from geoparquet_io.core.write_strategies import arrow_streaming

        requested = []
        real_writer = arrow_streaming.pq.ParquetWriter

        def spy(*args, **kwargs):
            requested.append(kwargs.get("use_dictionary"))
            return real_writer(*args, **kwargs)

        monkeypatch.setattr(arrow_streaming.pq, "ParquetWriter", spy)
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
        from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory

        src = tmp_path / "in.parquet"
        write_geoparquet_table(
            _table(), str(src), geometry_column="geometry", geoparquet_version="1.1"
        )
        out = tmp_path / "out.parquet"
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING).write_from_query(
                con=con,
                query=f"SELECT * FROM read_parquet({sql_path(str(src))}){where}",
                output_path=str(out),
                geometry_column="geometry",
                original_metadata=None,
                geoparquet_version="1.1",
                compression="ZSTD",
                compression_level=15,
                row_group_size_mb=None,
                row_group_rows=None,
                input_crs=None,
                verbose=False,
            )
        finally:
            con.close()
        # An empty result writes no row group, so the request is what is checked.
        assert requested == [["name", "bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax"]]
        if where:
            return
        assert not _dictionary(_encodings(out, "geometry"))
        assert _dictionary(_encodings(out, "name"))
