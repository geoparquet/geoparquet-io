"""
Regression tests for stale geo-metadata after transforms/filters.

Covers empirically reproduced correctness bugs:

1. ``gpio convert reproject`` carried the input's degree-space ``bbox`` into the
   output geo metadata even though coordinates were reprojected to meters.
2. ``gpio extract`` with a row filter (bbox/geometry/WHERE/LIMIT) kept the full
   pre-filter ``bbox`` in the output geo metadata.
3. A multi-file/glob/directory extract stamped the FIRST file's ``bbox`` on the
   merged output — an UNDER-covering bbox that makes conformant readers skip
   data.
4. ``geometry_types`` (required in GeoParquet 1.1, the exact sibling of
   ``bbox``) was carried unchanged by every path.
5. ``--exclude-cols`` left dangling ``covering`` / ``columns`` references to
   columns that are not in the output schema.

Also verifies:
- Zero-row extracts omit ``bbox`` entirely (a non-empty bbox would be a lie).
- An unfiltered, untransformed single-file copy still PRESERVES its bbox.
- A failure while reading input metadata for preservation warns (not silence).
"""

import json
import logging
import struct

import duckdb
import pytest

from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.extract import extract
from geoparquet_io.core.geo_metadata import strip_derived_stats
from geoparquet_io.core.reproject import reproject_impl
from tests.conftest import get_geo_metadata

# Every write strategy extract can be pointed at.
WRITE_STRATEGIES = ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"]


class TestStripDerivedStats:
    """Unit tests for the canonical strip_derived_stats helper."""

    def _geo(self):
        return {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "bbox": [1, 1, 3, 3],
                    "geometry_types": ["Point", "Polygon"],
                },
            },
        }

    def test_none_and_empty(self):
        assert strip_derived_stats(None) is None
        assert strip_derived_stats({}) == {}

    def test_no_geo_key_unchanged(self):
        meta = {"pandas": "{}"}
        assert strip_derived_stats(meta) == meta

    def test_bytes_form(self):
        meta = {b"geo": json.dumps(self._geo()).encode("utf-8")}
        out = strip_derived_stats(meta)
        col = json.loads(out[b"geo"].decode("utf-8"))["columns"]["geometry"]
        assert "bbox" not in col
        assert "geometry_types" not in col
        assert col["encoding"] == "WKB"
        # original untouched
        original_col = json.loads(meta[b"geo"].decode("utf-8"))["columns"]["geometry"]
        assert "bbox" in original_col
        assert "geometry_types" in original_col

    def test_str_form(self):
        meta = {"geo": json.dumps(self._geo())}
        col = json.loads(strip_derived_stats(meta)["geo"])["columns"]["geometry"]
        assert "bbox" not in col
        assert "geometry_types" not in col

    def test_dict_form(self):
        meta = {"geo": self._geo()}
        out = strip_derived_stats(meta)
        assert "bbox" not in out["geo"]["columns"]["geometry"]
        assert "geometry_types" not in out["geo"]["columns"]["geometry"]
        # original dict not mutated
        assert "bbox" in meta["geo"]["columns"]["geometry"]

    def test_both_geo_keys_are_stripped(self):
        """A dict carrying both "geo" and b"geo" must have BOTH stripped."""
        meta = {
            "geo": json.dumps(self._geo()),
            b"geo": json.dumps(self._geo()).encode("utf-8"),
        }
        out = strip_derived_stats(meta)
        assert "bbox" not in json.loads(out["geo"])["columns"]["geometry"]
        assert "bbox" not in json.loads(out[b"geo"].decode("utf-8"))["columns"]["geometry"]

    def test_no_stats_returns_equal_copy(self):
        """Docstring promises a copy: assert equality, never identity."""
        geo = self._geo()
        del geo["columns"]["geometry"]["bbox"]
        del geo["columns"]["geometry"]["geometry_types"]
        meta = {"geo": geo}
        out = strip_derived_stats(meta)
        assert out == meta
        assert out is not meta

    def test_malformed_columns_returns_equal_copy(self):
        meta = {"geo": {"version": "1.1.0"}}  # no "columns"
        assert strip_derived_stats(meta) == meta

    def test_unparseable_geo_left_alone(self):
        meta = {"geo": "not json at all"}
        assert strip_derived_stats(meta) == meta

    def test_other_keys_preserved(self):
        meta = {"geo": json.dumps(self._geo()), "vecorel": '{"a": 1}'}
        assert strip_derived_stats(meta)["vecorel"] == '{"a": 1}'


class TestStripDerivedStatsColumns:
    """``columns=`` scopes the strip to the columns a caller actually touched (#890)."""

    def _geo(self):
        return {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "bbox": [1, 1, 3, 3],
                    "geometry_types": ["Point"],
                },
                "other_geom": {
                    "encoding": "WKB",
                    "bbox": [0, 0, 9, 9],
                    "geometry_types": ["Polygon"],
                },
            },
        }

    def test_scoped_strip_leaves_untouched_column_intact(self):
        out = strip_derived_stats({"geo": self._geo()}, columns={"geometry"})
        cols = out["geo"]["columns"]
        assert "bbox" not in cols["geometry"]
        assert "geometry_types" not in cols["geometry"]
        assert cols["other_geom"]["bbox"] == [0, 0, 9, 9]
        assert cols["other_geom"]["geometry_types"] == ["Polygon"]

    def test_none_still_strips_every_column(self):
        out = strip_derived_stats({"geo": self._geo()})
        for col in out["geo"]["columns"].values():
            assert "bbox" not in col
            assert "geometry_types" not in col

    def test_unknown_column_name_strips_nothing(self):
        meta = {"geo": self._geo()}
        assert strip_derived_stats(meta, columns={"nope"}) == meta


def _write_points(path, points, version="1.1"):
    """Write a CRS84 GeoParquet file from ``[(id, wkt), ...]``."""
    from geoparquet_io.core.common import write_parquet_with_metadata

    values = ", ".join(f"({pid}, ST_GeomFromText('{wkt}'))" for pid, wkt in points)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    query = f"SELECT * FROM (VALUES {values}) AS t(id, geometry)"
    try:
        write_parquet_with_metadata(con, query, str(path), geoparquet_version=version)
    finally:
        con.close()


def _make_crs84_points(path):
    """Write points at (1,1), (2,2), (3,3): bbox [1, 1, 3, 3] in degrees."""
    _write_points(path, [(1, "POINT (1 1)"), (2, "POINT (2 2)"), (3, "POINT (3 3)")])


def _make_mixed_types(path):
    """Two points (id 1, 2) plus one polygon (id 3) far to the north-east."""
    _write_points(
        path,
        [
            (1, "POINT (1 1)"),
            (2, "POINT (2 2)"),
            (3, "POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))"),
        ],
    )


def _make_bbox_column_file(path):
    """Write a file with a struct ``bbox`` column and the covering describing it.

    The covering is declared explicitly, the way production callers do: the
    query below computes the bbox from the geometry, so this fixture can vouch
    for it. Writers no longer infer a covering from a column's name (#738).
    """
    from geoparquet_io.core.common import write_parquet_with_metadata
    from geoparquet_io.core.geo_metadata import build_bbox_covering

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    query = """
        SELECT
            id,
            'name-' || id AS name,
            geometry,
            {
                'xmin': ST_XMin(geometry)::FLOAT,
                'ymin': ST_YMin(geometry)::FLOAT,
                'xmax': ST_XMax(geometry)::FLOAT,
                'ymax': ST_YMax(geometry)::FLOAT
            } AS bbox
        FROM (VALUES
            (1, ST_Point(1.0, 1.0)),
            (2, ST_Point(2.0, 2.0))
        ) AS t(id, geometry)
    """
    try:
        write_parquet_with_metadata(
            con,
            query,
            str(path),
            geoparquet_version="1.1",
            custom_metadata={"covering": {"bbox": build_bbox_covering("bbox")}},
        )
    finally:
        con.close()


def _make_two_geometry_columns(path):
    """A file with a primary ``geometry`` and a secondary ``centroid`` column.

    GeoParquet allows more than one geometry column, and a projection that keeps
    the primary but drops the secondary leaves the ``geo`` metadata describing a
    column that is no longer in the schema.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    col_meta = {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "bbox": [1.0, 1.0, 2.0, 2.0],
    }
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": dict(col_meta), "centroid": dict(col_meta)},
    }
    table = pa.table(
        {
            "id": [1, 2],
            "name": ["a", "b"],
            "geometry": [_wkb_point(1, 1), _wkb_point(2, 2)],
            "centroid": [_wkb_point(1, 1), _wkb_point(2, 2)],
        }
    )
    pq.write_table(
        table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")}), str(path)
    )


def _primary_col_meta(path):
    geo = get_geo_metadata(str(path))
    return geo["columns"][geo["primary_column"]]


def _parquet_columns(path):
    import pyarrow.parquet as pq

    return pq.ParquetFile(str(path)).schema_arrow.names


def _row_count(path):
    con = duckdb.connect()
    try:
        return con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    finally:
        con.close()


def _read_x_range(path):
    """Return (min_x, max_x) of the geometry column via DuckDB."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        row = con.execute(
            f"SELECT MIN(ST_X(geometry)), MAX(ST_X(geometry)) FROM read_parquet('{path}')"
        ).fetchone()
    finally:
        con.close()
    return row[0], row[1]


class TestReprojectBboxRefreshed:
    """Reproject must not advertise the old degree-space bbox."""

    @pytest.mark.parametrize("version", ["1.1", "2.0"])
    def test_bbox_in_meters_and_encloses_data(self, tmp_path, version):
        src = tmp_path / "src.parquet"
        out = tmp_path / f"out_{version}.parquet"
        _make_crs84_points(src)

        reproject_impl(
            str(src),
            str(out),
            target_crs="EPSG:3857",
            source_crs="OGC:CRS84",
            geoparquet_version=version,
        )

        bbox = _primary_col_meta(out).get("bbox")
        assert bbox is not None, "reprojected output should still carry a bbox"

        # Web Mercator meters for lon 1..3 deg are ~111k..334k, never <= 3.
        assert bbox[0] > 1000, f"bbox xmin still in degrees: {bbox}"
        assert bbox[2] > 1000, f"bbox xmax still in degrees: {bbox}"

        # The advertised bbox must enclose the actual reprojected coordinates.
        min_x, max_x = _read_x_range(out)
        assert bbox[0] <= min_x + 1e-6
        assert bbox[2] >= max_x - 1e-6


class TestExtractBboxReflectsFilter:
    """Filtered extract must reflect only surviving rows."""

    def test_bbox_filter_shrinks_bbox(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        # bbox 0.5,0.5,1.5,1.5 keeps only the point at (1, 1).
        extract(str(src), str(out), bbox="0.5,0.5,1.5,1.5")

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 1.0, 1.0]

    def test_where_filter_shrinks_bbox(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        extract(str(src), str(out), where="id < 3")

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 2.0, 2.0]

    def test_limit_shrinks_bbox(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        extract(str(src), str(out), limit=1)

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 1.0, 1.0]

    def test_geometry_filter_shrinks_bbox(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        extract(
            str(src),
            str(out),
            geometry="POLYGON ((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))",
        )

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 1.0, 1.0]

    def test_zero_row_extract_omits_bbox(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        # A bbox far from the data yields zero rows.
        extract(str(src), str(out), bbox="100,100,101,101")

        assert _row_count(out) == 0
        col = _primary_col_meta(out)
        assert "bbox" not in col, f"zero-row output advertises a bbox: {col.get('bbox')}"

    def test_unfiltered_copy_preserves_bbox(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        # Pure column pass-through: no row filter, no transform, single file.
        extract(str(src), str(out))

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 3.0, 3.0]

    @pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
    def test_every_write_strategy_recomputes_bbox(self, tmp_path, strategy):
        """Regression: the streaming strategy emitted no bbox at all for 1.x."""
        src, out = tmp_path / "src.parquet", tmp_path / f"out_{strategy}.parquet"
        _make_crs84_points(src)

        extract(str(src), str(out), where="id < 3", write_strategy=strategy)

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 2.0, 2.0]


class TestExtractGeometryTypesReflectFilter:
    """geometry_types is required in 1.1 and must not survive a filter."""

    def test_filtered_out_polygon_drops_from_geometry_types(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_mixed_types(src)
        assert set(_primary_col_meta(src)["geometry_types"]) == {"Point", "Polygon"}

        extract(str(src), str(out), where="id < 3")

        assert _primary_col_meta(out)["geometry_types"] == ["Point"]

    @pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
    def test_every_write_strategy_recomputes_geometry_types(self, tmp_path, strategy):
        src, out = tmp_path / "src.parquet", tmp_path / f"out_{strategy}.parquet"
        _make_mixed_types(src)

        extract(str(src), str(out), where="id < 3", write_strategy=strategy)

        assert _primary_col_meta(out)["geometry_types"] == ["Point"]


class TestMultiFileExtract:
    """A glob/directory merge must not stamp the first file's stats."""

    def _make_two_file_dataset(self, tmp_path):
        multi = tmp_path / "multi"
        multi.mkdir()
        _write_points(multi / "a.parquet", [(1, "POINT (1 1)")])
        _write_points(
            multi / "b.parquet",
            [(2, "POLYGON ((50 50, 51 50, 51 51, 50 51, 50 50))")],
        )
        return multi

    def test_glob_merge_bbox_covers_all_files(self, tmp_path):
        multi = self._make_two_file_dataset(tmp_path)
        out = tmp_path / "merged.parquet"

        extract(str(multi / "*.parquet"), str(out))

        assert _row_count(out) == 2
        # First-file bbox was [1, 1, 1, 1]: an UNDER-covering lie.
        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 51.0, 51.0]

    def test_glob_merge_geometry_types_cover_all_files(self, tmp_path):
        multi = self._make_two_file_dataset(tmp_path)
        out = tmp_path / "merged.parquet"

        extract(str(multi / "*.parquet"), str(out))

        assert _primary_col_meta(out)["geometry_types"] == ["Point", "Polygon"]

    def test_directory_input_bbox_covers_all_files(self, tmp_path):
        multi = self._make_two_file_dataset(tmp_path)
        out = tmp_path / "merged_dir.parquet"

        extract(str(multi), str(out))

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 51.0, 51.0]


class TestExcludeColsPrunesMetadata:
    """--exclude-cols must not leave references to columns that are gone."""

    def test_excluding_bbox_column_drops_covering(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_bbox_column_file(src)
        assert "covering" in _primary_col_meta(src)

        extract(str(src), str(out), exclude_cols="bbox")

        assert "bbox" not in _parquet_columns(out)
        col = _primary_col_meta(out)
        assert "covering" not in col, f"dangling covering: {col.get('covering')}"

    def test_excluding_geometry_drops_geo_metadata(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_bbox_column_file(src)

        extract(str(src), str(out), exclude_cols="geometry")

        assert "geometry" not in _parquet_columns(out)
        assert get_geo_metadata(str(out)) is None, "no-geometry output still claims GeoParquet"

    def test_keeping_bbox_column_keeps_covering(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_bbox_column_file(src)

        extract(str(src), str(out), exclude_cols="name")

        covering = _primary_col_meta(out).get("covering")
        assert covering is not None
        assert covering["bbox"]["xmin"] == ["bbox", "xmin"]


class TestStreamingPathsInvalidateStats:
    """The stream_io write paths must also drop the stale carried stats."""

    def _stream_extract(self, src, out, **overrides):
        from geoparquet_io.core.extract import _extract_streaming

        kwargs = {
            "input_path": str(src),
            "output_path": str(out) if out is not None else "-",
            "include_cols": None,
            "exclude_cols": None,
            "bbox_tuple": None,
            "geometry_wkt": None,
            "where": None,
            "limit": None,
            "verbose": False,
            "compression": "ZSTD",
            "compression_level": None,
            "row_group_size_mb": None,
            "row_group_rows": None,
            "profile": None,
            "geoparquet_version": "1.1",
        }
        kwargs.update(overrides)
        _extract_streaming(**kwargs)

    def test_streaming_extract_bbox_filter(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        self._stream_extract(src, out, bbox_tuple=(0.5, 0.5, 1.5, 1.5))

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 1.0, 1.0]

    def test_streaming_extract_where_filter(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        self._stream_extract(src, out, where="id < 3")

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 2.0, 2.0]

    def test_streaming_extract_limit(self, tmp_path):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        self._stream_extract(src, out, limit=1)

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 1.0, 1.0]

    def test_streaming_extract_glob_input(self, tmp_path):
        multi = tmp_path / "multi"
        multi.mkdir()
        _write_points(multi / "a.parquet", [(1, "POINT (1 1)")])
        _write_points(multi / "b.parquet", [(2, "POINT (50 50)")])
        out = tmp_path / "out.parquet"

        self._stream_extract(multi / "*.parquet", out)

        assert _primary_col_meta(out).get("bbox") == [1.0, 1.0, 50.0, 50.0]

    def _stream_to_stdout(self, monkeypatch, src, **overrides):
        """Run a streaming extract to stdout and return the emitted Arrow table."""
        import geoparquet_io.core.stream_io as stream_io

        captured = {}

        def _capture(table):
            captured["table"] = table

        monkeypatch.setattr(stream_io, "write_arrow_stream", _capture)
        self._stream_extract(src, None, **overrides)
        return captured["table"]

    def test_stdout_stream_recomputes_derived_stats(self, tmp_path, monkeypatch):
        """Output to stdout carries schema metadata too — it must not lie.

        The stats are recomputed from the rows actually streamed, not carried
        from the unfiltered input (bbox [1,1,3,3] here) and not left absent:
        ``geometry_types`` is required by GeoParquet 1.1, and DuckDB refuses to
        read a Parquet file whose ``geo`` metadata omits it, which is what broke
        ``extract | add | partition`` (#722).
        """
        src = tmp_path / "src.parquet"
        _make_crs84_points(src)

        table = self._stream_to_stdout(monkeypatch, src, where="id < 3")

        geo = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
        col = geo["columns"][geo["primary_column"]]
        assert col["bbox"] == [1.0, 1.0, 2.0, 2.0], "stdout stream advertises a stale bbox"
        assert col["geometry_types"] == ["Point"]

    def test_stdout_stream_prunes_dropped_geometry_column(self, tmp_path, monkeypatch):
        """A projection that drops a secondary geometry column must drop its entry.

        The file path prunes the geo metadata to the columns it actually writes;
        the stream path did not, so an ``--include-cols`` that left ``centroid``
        behind emitted a ``geo`` block naming a column no longer in the schema --
        and, once the derived stats were stripped, one with no ``geometry_types``
        either, which is exactly what DuckDB refuses to read (#722).
        """
        src = tmp_path / "two_geom.parquet"
        _make_two_geometry_columns(src)

        table = self._stream_to_stdout(
            monkeypatch, src, include_cols=["id", "name", "geometry"], where="id < 3"
        )

        assert "centroid" not in table.column_names
        geo = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
        assert set(geo["columns"]) <= set(table.column_names), (
            f"geo metadata names absent columns: {sorted(set(geo['columns']))}"
        )
        assert geo["columns"]["geometry"]["geometry_types"] == ["Point"]

    def test_streaming_reproject(self, tmp_path):
        from geoparquet_io.core.reproject import _reproject_streaming

        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        _reproject_streaming(
            str(src),
            str(out),
            target_crs="EPSG:3857",
            source_crs="OGC:CRS84",
            compression="ZSTD",
            compression_level=None,
            verbose=False,
            profile=None,
            geoparquet_version="1.1",
        )

        bbox = _primary_col_meta(out).get("bbox")
        assert bbox is not None, "reprojected stream output should still carry a bbox"
        # Web Mercator meters for lon/lat 1..3 deg, rounded to the nearest metre.
        assert [round(v) for v in bbox] == [111319, 111325, 333958, 334111]


class TestMetadataPreservationWarns:
    """A metadata-read failure must warn, not silently drop all metadata."""

    @pytest.mark.parametrize(
        "exc",
        [
            OSError("simulated metadata read failure"),
            GeoParquetError("simulated corrupt footer"),
            ValueError("simulated bad value"),
        ],
        ids=["oserror", "geoparqueterror", "valueerror"],
    )
    def test_warns_when_preservation_fails(self, tmp_path, monkeypatch, caplog, exc):
        src, out = tmp_path / "src.parquet", tmp_path / "out.parquet"
        _make_crs84_points(src)

        import geoparquet_io.core.extract as extract_mod

        def _boom(*args, **kwargs):
            raise exc

        monkeypatch.setattr(extract_mod, "get_parquet_metadata", _boom)

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            extract(str(src), str(out))

        # The write still succeeds despite the metadata read failing.
        assert _row_count(out) == 3

        assert any(
            "metadata" in rec.message.lower() and rec.levelno == logging.WARNING
            for rec in caplog.records
        ), f"expected a warning about skipped metadata; got {[r.message for r in caplog.records]}"


def _wkb_point(x: float, y: float) -> bytes:
    """Little-endian WKB for POINT (x y) -- no shapely dependency in tests."""
    return struct.pack("<BIdd", 1, 1, x, y)


class TestBackfillDerivedStats:
    """Unit tests for the canonical backfill_derived_stats helper (#722).

    ``strip_derived_stats`` drops the stats a filter or transform invalidated;
    the write path is then responsible for recomputing them from the rows
    actually written. The file writer already did. The Arrow IPC stream writer
    did not, which left ``geometry_types`` -- REQUIRED by GeoParquet 1.1 --
    absent from every piped stage after a filtered ``extract``.
    """

    def _geo_without_stats(self):
        return {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB"}},
        }

    def _points_table(self):
        """Points at (1,1) and (2,2), WKB-encoded, in a ``geometry`` column."""
        import pyarrow as pa

        return pa.table({"id": [1, 2], "geometry": [_wkb_point(1, 1), _wkb_point(2, 2)]})

    def test_none_and_empty(self):
        from geoparquet_io.core.geo_metadata import backfill_derived_stats

        table = self._points_table()
        assert backfill_derived_stats(None, table) is None
        assert backfill_derived_stats({}, table) == {}

    def test_computes_both_stats_from_the_data(self):
        from geoparquet_io.core.geo_metadata import backfill_derived_stats

        meta = {b"geo": json.dumps(self._geo_without_stats()).encode("utf-8")}
        out = backfill_derived_stats(meta, self._points_table())

        col = json.loads(out[b"geo"].decode("utf-8"))["columns"]["geometry"]
        assert col["geometry_types"] == ["Point"]
        assert col["bbox"] == [1.0, 1.0, 2.0, 2.0]
        assert col["encoding"] == "WKB"

    def test_does_not_mutate_the_input(self):
        from geoparquet_io.core.geo_metadata import backfill_derived_stats

        meta = {"geo": self._geo_without_stats()}
        backfill_derived_stats(meta, self._points_table())
        assert "geometry_types" not in meta["geo"]["columns"]["geometry"]

    def test_existing_stats_are_left_alone(self):
        """Backfill only FILLS gaps -- it never second-guesses a present value."""
        from geoparquet_io.core.geo_metadata import backfill_derived_stats

        geo = self._geo_without_stats()
        geo["columns"]["geometry"]["geometry_types"] = ["Polygon"]
        geo["columns"]["geometry"]["bbox"] = [0.0, 0.0, 9.0, 9.0]
        out = backfill_derived_stats({"geo": geo}, self._points_table())

        col = out["geo"]["columns"]["geometry"]
        assert col["geometry_types"] == ["Polygon"]
        assert col["bbox"] == [0.0, 0.0, 9.0, 9.0]

    def test_empty_table_gets_an_empty_type_list_and_no_bbox(self):
        """Zero rows: [] means "any type" per spec; a bbox would be a lie."""
        import pyarrow as pa

        from geoparquet_io.core.geo_metadata import backfill_derived_stats

        empty = pa.table({"id": pa.array([], pa.int64()), "geometry": pa.array([], pa.binary())})
        out = backfill_derived_stats({"geo": self._geo_without_stats()}, empty)

        col = out["geo"]["columns"]["geometry"]
        assert col["geometry_types"] == []
        assert "bbox" not in col

    def test_column_absent_from_the_table_is_skipped(self):
        from geoparquet_io.core.geo_metadata import backfill_derived_stats

        geo = self._geo_without_stats()
        geo["columns"]["other_geom"] = {"encoding": "WKB"}
        out = backfill_derived_stats({"geo": geo}, self._points_table())

        assert "geometry_types" not in out["geo"]["columns"]["other_geom"]
        assert out["geo"]["columns"]["geometry"]["geometry_types"] == ["Point"]

    def test_unparseable_geo_left_alone(self):
        from geoparquet_io.core.geo_metadata import backfill_derived_stats

        meta = {"geo": "not json at all"}
        assert backfill_derived_stats(meta, self._points_table()) == meta

    def test_malformed_columns_returns_equal_copy(self):
        from geoparquet_io.core.geo_metadata import backfill_derived_stats

        meta = {"geo": {"version": "1.1.0"}}  # no "columns"
        assert backfill_derived_stats(meta, self._points_table()) == meta
