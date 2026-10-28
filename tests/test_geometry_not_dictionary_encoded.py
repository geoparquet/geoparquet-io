"""A WKB geometry column is never dictionary-encoded on the Arrow write path (#1122).

A WKB blob is unique per row, so a dictionary is a full copy of the column plus
an index. The overviews spec (4.5) says a writer must not dictionary-encode the
geometry. Every other column keeps pyarrow's dictionary default.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1122
"""

from __future__ import annotations

import ast
import json
import struct
from pathlib import Path

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


def _secondary_table() -> pa.Table:
    """Two WKB geometry columns, both declared by the table's own ``geo`` block."""
    # Unique per row, as real geometry is. DuckDB picks dictionary encoding by
    # how much a column repeats, so a constant column would test DuckDB, not gpio.
    table = _table().append_column("geom2", pa.array([_point(i, -i) for i in range(50)]))
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Point"]},
            "geom2": {"encoding": "WKB", "geometry_types": ["Point"]},
        },
    }
    return table.replace_schema_metadata({"geo": json.dumps(geo)})


def _logical_type(path, column: str) -> str:
    schema = pq.ParquetFile(str(path)).schema
    for i in range(len(schema)):
        if schema.column(i).name == column:
            return str(schema.column(i).logical_type)
    raise AssertionError(f"no column {column}")


class TestParquetGeoOnlySecondary:
    """parquet-geo-only drops the ``geo`` key, but not before it names the secondaries."""

    @pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
    @pytest.mark.parametrize("version", ["1.1", "2.0", "parquet-geo-only"])
    def test_secondary_is_not_dictionary(self, tmp_path, strategy, version):
        import geoparquet_io as gpio

        src = tmp_path / "in.parquet"
        pq.write_table(_secondary_table(), src, use_dictionary=False)
        out = tmp_path / "out.parquet"
        gpio.read(str(src)).write(str(out), geoparquet_version=version, write_strategy=strategy)
        assert not _dictionary(_encodings(out, "geom2"))
        assert _dictionary(_encodings(out, "name"))
        if version != "1.1":
            # With no geo block, the native type is the column's only identity.
            assert _logical_type(out, "geom2").startswith("Geometry")


class TestVecorelRewrite:
    """The vecorel schema fix rewrites the output, so it asks the facade too."""

    def test_fix_vecorel_schema_keeps_geometry_out(self, tmp_path):
        from geoparquet_io.core.constants import _fix_vecorel_schema

        path = tmp_path / "out.parquet"
        write_geoparquet_table(
            _table(), str(path), geometry_column="geometry", geoparquet_version="1.1"
        )
        _fix_vecorel_schema(str(path), ["geometry"])
        assert not pq.read_schema(path).field("geometry").nullable  # the rewrite ran
        assert DICT not in _encodings(path, "geometry")
        assert DICT in _encodings(path, "name")

    def test_query_funnel_with_collection_key(self, tmp_path):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        src = tmp_path / "in.parquet"
        write_geoparquet_table(
            _table(), str(src), geometry_column="geometry", geoparquet_version="1.1"
        )
        out = tmp_path / "out.parquet"
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            write_parquet_with_metadata(
                con,
                f"SELECT * FROM read_parquet({sql_path(str(src))})",
                str(out),
                geoparquet_version="1.1",
                extra_kv_metadata={"collection": json.dumps({"id": "c"})},
                write_strategy="in-memory",
            )
        finally:
            con.close()
        assert not pq.read_schema(out).field("geometry").nullable
        assert DICT not in _encodings(out, "geometry")


class TestWriterKwargsHelper:
    def test_lives_in_the_write_facade(self):
        from geoparquet_io.core.parquet_writer import without_geometry_dictionary

        schema = pa.schema([("name", pa.string()), ("geometry", pa.binary())])
        kwargs = without_geometry_dictionary({"compression": "zstd"}, schema, ["geometry"])
        assert kwargs == {"compression": "zstd", "use_dictionary": ["name"]}

    def test_geometry_columns_default_to_the_schema(self):
        from geoparquet_io.core.parquet_writer import without_geometry_dictionary

        field = pa.field("shape", pa.binary(), metadata={"ARROW:extension:name": "geoarrow.wkb"})
        schema = pa.schema([("name", pa.string()), field])
        assert without_geometry_dictionary({}, schema) == {"use_dictionary": ["name"]}


PACKAGE = Path(__file__).resolve().parents[1] / "geoparquet_io"

#: The functions that ask the facade for their writer kwargs.
_FACADE_CALLS = {"without_geometry_dictionary", "get_pyarrow_kwargs"}

#: pyarrow writes that may keep pyarrow's own dictionary default, and why.
#: Keyed by (path under geoparquet_io/, enclosing function).
_EXEMPT = {
    ("core/add/kdtree.py", "add_kdtree_table"): "scratch copy, read back by DuckDB",
    ("core/streaming.py", "read_stdin_to_temp_file"): "scratch copy of stdin, read by DuckDB",
    ("core/reproject.py", "_sanitize_null_crs_file"): "scratch copy, read back by DuckDB",
    ("core/arcgis.py", "_stream_features_to_parquet"): "scratch file, rewritten by a second pass",
    ("api/ops.py", "convert_to_geojson"): "scratch copy of the input table",
    ("api/ops.py", "_file_round_trip"): "scratch copy of the input table",
    ("api/ops.py", "_table_to_temp_parquet_and_convert"): "scratch copy of the input table",
    ("benchmarks/operations.py", "_run_write"): "measures a plain pyarrow write",
    ("core/extract_bigquery.py", "_execute_bigquery_extraction"): (
        "the no-geography branch, BigQuery said there is no geometry"
    ),
    ("core/process/aggregate/by_admin.py", "aggregate_by_admin"): "out_geometry none",
    ("core/process/aggregate/grid_common.py", "aggregate_grid_file"): "out_geometry none",
    ("core/process/overview/run.py", "_write_overview"): "out_geometry none",
    ("core/write_strategies/arrow_streaming.py", "_write_all_batches"): (
        "takes writer_kwargs from write_from_query, which asks the facade"
    ),
    (
        "core/write_strategies/arrow_streaming.py",
        "_write_plain_parquet_from_table",
    ): "no geometry column",
    (
        "core/write_strategies/arrow_streaming.py",
        "_write_plain_parquet_from_query",
    ): "no geometry column",
    ("core/write_strategies/disk_rewrite.py", "_write_plain_parquet_from_table"): (
        "no geometry column"
    ),
}


def _pyarrow_writes() -> set[tuple[str, str]]:
    """Every ``pq.ParquetWriter(...)`` and ``pq.write_table(...)`` whose function never asks the facade."""
    found: set[tuple[str, str]] = set()
    for path in PACKAGE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for func in ast.walk(tree):
            if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            calls = [n for n in ast.walk(func) if isinstance(n, ast.Call)]
            names = {getattr(c.func, "attr", None) or getattr(c.func, "id", None) for c in calls}
            if names & _FACADE_CALLS:
                continue
            for call in calls:
                target = call.func
                if (
                    isinstance(target, ast.Attribute)
                    and target.attr in {"ParquetWriter", "write_table"}
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "pq"
                ):
                    found.add((path.relative_to(PACKAGE).as_posix(), func.name))
    return found


class TestEveryWriterAsksTheFacade:
    """A new pyarrow write must take its ``use_dictionary`` from the facade (#1122).

    Every site that forgot it re-encoded the geometry, one at a time (the
    streaming strategy, disk-rewrite, the vecorel fix). This names the sites
    instead of finding them by review.
    """

    def test_no_new_bare_pyarrow_write(self):
        bare = _pyarrow_writes() - set(_EXEMPT)
        assert not bare, (
            f"{sorted(bare)} write with pyarrow's default dictionary encoding. Build the "
            "writer kwargs with parquet_writer.without_geometry_dictionary, or add the "
            "site to _EXEMPT with the reason it writes no geometry."
        )

    def test_exemptions_are_not_stale(self):
        stale = set(_EXEMPT) - _pyarrow_writes()
        assert not stale, f"{sorted(stale)} no longer write bare; drop them from _EXEMPT"
