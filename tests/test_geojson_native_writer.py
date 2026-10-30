"""
Tests for the DuckDB 2.0 native GeoJSON fast path (issue #1349).

DuckDB 2.0's spatial extension can write a GeoJSON FeatureCollection itself
(``COPY ... TO 'x.geojson' (FORMAT GEOJSON)``). gpio uses it for the file-output
path only when the result is semantically equivalent to the hand-built
``ST_AsGeoJSON`` writer. These tests ARE the proof: every eligible shape is
converted through both writers and the parsed outputs compared feature by
feature. Options the native writer cannot reproduce (``--pretty``,
``--description``, ``--id-field``, ``--write-bbox``, a second geometry-typed
column) must fall back to the hand-built writer, on every DuckDB version.
"""

import json
from pathlib import Path

import duckdb
import pytest

from geoparquet_io.core import geojson_stream
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.geojson_stream import convert_to_geojson

TEST_DATA_DIR = Path(__file__).parent / "data"
REPROJECT_PARQUET = TEST_DATA_DIR / "fields_pgo_5070_snappy.parquet"

NATIVE_AVAILABLE = int(duckdb.__version__.split(".", 1)[0]) >= 2

requires_native = pytest.mark.skipif(
    not NATIVE_AVAILABLE, reason="needs DuckDB 2.0's native GeoJSON writer"
)


def _write_parquet(path: Path, select_sql: str) -> None:
    """Write a GeoParquet fixture from a SELECT with a GEOMETRY column."""
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        con.execute(f"COPY ({select_sql}) TO {sql_path(str(path))} (FORMAT PARQUET)")
    finally:
        con.close()


# Fixture shapes chosen to exercise what the issue asked to compare: point and
# polygon geometries, NULL geometries, nested list/struct columns, a column
# named ``id`` (which the native writer promotes by default), and non-ASCII
# strings. Each is a SELECT whose result is written to GeoParquet.
EQUIVALENCE_CASES = {
    "points_mixed_props": """
        SELECT * FROM (VALUES
            (1, 'café ☕', 10.5, true, ST_GeomFromText('POINT(1.23456789123 2.1)')),
            (2, NULL, NULL, NULL, ST_GeomFromText('POINT(-122.419415999 37.774928999)')),
            (3, 'null-geom', 3.0, false, NULL)
        ) AS t(id, name, val, flag, geometry)
    """,
    "polygons_and_lines": """
        SELECT * FROM (VALUES
            ('cw-ring', ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')),
            ('multi', ST_GeomFromText(
                'MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))')),
            ('line', ST_GeomFromText('LINESTRING(0 0, 1.123456789 2.987654321)'))
        ) AS t(name, geometry)
    """,
    "nested_list_struct": """
        SELECT * FROM (VALUES
            ([1, 2, 3], {'a': 1, 'b': 'x'}, ST_GeomFromText('POINT(1 2)')),
            (NULL, NULL, ST_GeomFromText('POINT(3 4)')),
            ([], {'a': 7, 'b': '日本'}, ST_GeomFromText('POINT(5 6)'))
        ) AS t(lst, st, geometry)
    """,
    "quoted_column_names": """
        SELECT * FROM (VALUES
            (1, 'x', ST_GeomFromText('POINT(1 2)'))
        ) AS t("qu""ote", "日本", geometry)
    """,
    "invalid_bowtie": """
        SELECT 'bowtie' AS name,
               ST_GeomFromText('POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))') AS geometry
    """,
    "geometry_only": """
        SELECT ST_GeomFromText('POINT(9.99999999 -9.99999999)') AS geometry
    """,
}


def _convert_both(
    parquet: Path, tmp_path: Path, monkeypatch, **kwargs
) -> tuple[dict, dict, int, int]:
    """Convert through the native fast path and the hand-built writer."""
    native_out = tmp_path / "native.geojson"
    legacy_out = tmp_path / "legacy.geojson"

    native_count = convert_to_geojson(str(parquet), str(native_out), **kwargs)

    with monkeypatch.context() as m:
        m.setattr(geojson_stream, "_native_geojson_writer_available", lambda: False)
        legacy_count = convert_to_geojson(str(parquet), str(legacy_out), **kwargs)

    native_fc = json.loads(native_out.read_text(encoding="utf-8"))
    legacy_fc = json.loads(legacy_out.read_text(encoding="utf-8"))
    return native_fc, legacy_fc, native_count, legacy_count


class TestVersionGate:
    """The fast path exists only where the COPY format does."""

    @pytest.mark.parametrize(
        ("version", "expected"),
        [
            ("1.5.6", False),
            ("2.0.0", True),
            ("2.1.0-dev123", True),
            ("10.0.0", True),
            ("unknown", False),
        ],
        ids=["v1", "v2", "v2dev", "v10", "unparsable"],
    )
    def test_gate_parses_major_version(self, monkeypatch, version, expected):
        monkeypatch.setattr(duckdb, "__version__", version)
        assert geojson_stream._native_geojson_writer_available() is expected

    def test_gate_matches_installed_duckdb(self):
        assert geojson_stream._native_geojson_writer_available() is NATIVE_AVAILABLE

    def test_never_invoked_on_duckdb_1x(self, tmp_path, monkeypatch):
        """On DuckDB 1.x the conversion must run without touching the fast path."""
        if NATIVE_AVAILABLE:
            pytest.skip("gate-off behavior is exercised on the DuckDB 1.5 leg")
        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["points_mixed_props"])

        def _boom(*args, **kwargs):  # pragma: no cover - failure path
            raise AssertionError("native fast path must not run on DuckDB 1.x")

        monkeypatch.setattr(geojson_stream, "_native_feature_collection_to_file", _boom)
        out = tmp_path / "out.geojson"
        count = convert_to_geojson(str(parquet), str(out))
        assert count == 2
        assert json.loads(out.read_text(encoding="utf-8"))["type"] == "FeatureCollection"


@requires_native
class TestNativeEquivalence:
    """Both writers must produce the same parsed FeatureCollection."""

    @pytest.mark.parametrize("case", sorted(EQUIVALENCE_CASES), ids=sorted(EQUIVALENCE_CASES))
    def test_parsed_output_identical(self, tmp_path, monkeypatch, case):
        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES[case])
        native_fc, legacy_fc, native_count, legacy_count = _convert_both(
            parquet, tmp_path, monkeypatch
        )
        assert native_count == legacy_count
        assert native_fc == legacy_fc
        assert native_fc["type"] == "FeatureCollection"
        assert len(native_fc["features"]) == native_count

    def test_null_geometry_rows_are_dropped(self, tmp_path, monkeypatch):
        """The fast path keeps the hand-built writer's NULL-geometry filter."""
        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["points_mixed_props"])
        native_fc, _, native_count, _ = _convert_both(parquet, tmp_path, monkeypatch)
        assert native_count == 2
        assert all(f["geometry"] is not None for f in native_fc["features"])

    def test_id_column_stays_in_properties(self, tmp_path, monkeypatch):
        """A column named ``id`` is a property, never promoted to feature id."""
        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["points_mixed_props"])
        native_fc, legacy_fc, _, _ = _convert_both(parquet, tmp_path, monkeypatch)
        for feature in native_fc["features"]:
            assert "id" not in feature
            assert feature["properties"]["id"] in (1, 2)
        assert native_fc == legacy_fc

    @pytest.mark.parametrize("precision", [3, 7], ids=["p3", "p7"])
    def test_precision_equivalence(self, tmp_path, monkeypatch, precision):
        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["geometry_only"])
        native_fc, legacy_fc, _, _ = _convert_both(
            parquet, tmp_path, monkeypatch, precision=precision
        )
        assert native_fc == legacy_fc
        coords = native_fc["features"][0]["geometry"]["coordinates"]
        assert coords == [round(9.99999999, precision), round(-9.99999999, precision)]

    def test_repair_disabled_equivalence(self, tmp_path, monkeypatch):
        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["polygons_and_lines"])
        native_fc, legacy_fc, _, _ = _convert_both(
            parquet, tmp_path, monkeypatch, repair_geometry=False
        )
        assert native_fc == legacy_fc

    def test_repair_disabled_invalid_geometry_errors_identically(self, tmp_path, monkeypatch):
        """Both writers run the same SQL chain, so they fail the same way too.

        ST_ReducePrecision raises TopologyException on an unrepaired bowtie;
        the fast path must not turn that error into silent different output.
        """
        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["invalid_bowtie"])
        with pytest.raises(duckdb.Error):
            convert_to_geojson(
                str(parquet), str(tmp_path / "native.geojson"), repair_geometry=False
            )
        with monkeypatch.context() as m:
            m.setattr(geojson_stream, "_native_geojson_writer_available", lambda: False)
            with pytest.raises(duckdb.Error):
                convert_to_geojson(
                    str(parquet), str(tmp_path / "legacy.geojson"), repair_geometry=False
                )

    def test_reprojection_equivalence(self, tmp_path, monkeypatch):
        """A projected input reprojects to WGS84 identically through both writers."""
        native_fc, legacy_fc, native_count, legacy_count = _convert_both(
            REPROJECT_PARQUET, tmp_path, monkeypatch
        )
        assert native_count == legacy_count > 0
        assert native_fc == legacy_fc
        lon, lat = _first_coordinate(native_fc["features"][0]["geometry"])
        assert -180 <= lon <= 180
        assert -90 <= lat <= 90

    def test_fast_path_is_used_when_eligible(self, tmp_path, monkeypatch):
        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["points_mixed_props"])
        calls = []
        real = geojson_stream._native_feature_collection_to_file

        def _recording(*args, **kwargs):
            calls.append(1)
            return real(*args, **kwargs)

        monkeypatch.setattr(geojson_stream, "_native_feature_collection_to_file", _recording)
        out = tmp_path / "out.geojson"
        convert_to_geojson(str(parquet), str(out))
        assert calls == [1]

    def test_stream_input_equivalence(self, tmp_path, monkeypatch):
        """The Arrow-IPC stdin path takes the same fast path for file output."""
        import pyarrow.parquet as pq

        from geoparquet_io.core import streaming

        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["points_mixed_props"])
        table = pq.read_table(parquet)
        monkeypatch.setattr(streaming, "read_arrow_stream", lambda: table)

        native_fc, legacy_fc, native_count, legacy_count = _convert_both(
            Path("-"), tmp_path, monkeypatch
        )
        assert native_count == legacy_count == 2
        assert native_fc == legacy_fc

    def test_zero_feature_collection_equivalence(self, tmp_path, monkeypatch):
        """Every row filtered out still yields the same empty FeatureCollection."""
        import pyarrow.parquet as pq

        from geoparquet_io.core import streaming

        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["points_mixed_props"])
        # Keep only the NULL-geometry row; both writers must drop it.
        table = pq.read_table(parquet).slice(2, 1)
        monkeypatch.setattr(streaming, "read_arrow_stream", lambda: table)

        native_fc, legacy_fc, native_count, legacy_count = _convert_both(
            Path("-"), tmp_path, monkeypatch
        )
        assert native_count == legacy_count == 0
        assert native_fc == legacy_fc == {"type": "FeatureCollection", "features": []}

    def test_cli_file_output_uses_fast_path(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["points_mixed_props"])
        calls = []
        real = geojson_stream._native_feature_collection_to_file

        def _recording(*args, **kwargs):
            calls.append(1)
            return real(*args, **kwargs)

        monkeypatch.setattr(geojson_stream, "_native_feature_collection_to_file", _recording)
        out = tmp_path / "out.geojson"
        result = CliRunner().invoke(cli, ["convert", "geojson", str(parquet), str(out)])
        assert result.exit_code == 0, result.output
        assert calls == [1]
        assert len(json.loads(out.read_text(encoding="utf-8"))["features"]) == 2


class TestFastPathPlumbing:
    """Version-independent pieces, exercised on every DuckDB version.

    The CI coverage leg runs the locked DuckDB 1.5, where the equivalence
    tests skip; these cover the fast path's plumbing there.
    """

    @pytest.fixture
    def con(self):
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        con.execute(
            "CREATE TABLE t AS SELECT 1 AS id, 'x' AS name, "
            "ST_GeomFromText('POINT(1 2)') AS geometry, "
            "ST_GeomFromText('POINT(3 4)') AS geom2"
        )
        yield con
        con.close()

    @pytest.fixture
    def null_write_scope(self, monkeypatch):
        """Neutralize the write-memory scope so fake connections see only the COPY."""
        from contextlib import contextmanager

        @contextmanager
        def _null(con, memory_limit, verbose, **kwargs):
            yield

        monkeypatch.setattr(geojson_stream, "scoped_write_memory_limit", _null)

    def test_geometry_typed_property_columns(self, con):
        found = geojson_stream._geometry_typed_property_columns(con, "t", ["id", "name", "geom2"])
        assert found == ["geom2"]

    def test_maybe_runs_native_writer_when_eligible(self, con, tmp_path, monkeypatch):
        monkeypatch.setattr(geojson_stream, "_native_geojson_writer_available", lambda: True)
        recorded = {}

        def _fake_native(
            _con,
            source_ref,
            geometry_column,
            property_columns,
            output_path,
            precision,
            source_crs,
            repair_geometry,
            verbose=False,
        ):
            recorded["args"] = (source_ref, geometry_column, property_columns, precision, verbose)
            return 42

        monkeypatch.setattr(geojson_stream, "_native_feature_collection_to_file", _fake_native)
        count = geojson_stream._maybe_native_feature_collection(
            con,
            "t",
            "geometry",
            ["id", "name"],
            output_path=str(tmp_path / "o.geojson"),
            precision="7",
            write_bbox=False,
            id_field=None,
            description=None,
            pretty=False,
            source_crs=None,
            repair_geometry=True,
            verbose=True,
        )
        assert count == 42
        # The free-form Python API precision is coerced before interpolation,
        # and the caller's verbose flag reaches the write scope.
        assert recorded["args"] == ("t", "geometry", ["id", "name"], 7, True)

    def test_maybe_declines_geometry_typed_property(self, con, tmp_path, monkeypatch):
        monkeypatch.setattr(geojson_stream, "_native_geojson_writer_available", lambda: True)
        count = geojson_stream._maybe_native_feature_collection(
            con,
            "t",
            "geometry",
            ["id", "name", "geom2"],
            output_path=str(tmp_path / "o.geojson"),
            precision=7,
            write_bbox=False,
            id_field=None,
            description=None,
            pretty=False,
            source_crs=None,
            repair_geometry=True,
        )
        assert count is None

    @pytest.mark.usefixtures("null_write_scope")
    def test_native_copy_statement_shape(self, tmp_path):
        """The COPY statement pins the semantics the equivalence tests prove."""
        executed = []

        class _Result:
            def fetchone(self):
                return (7,)

        class _Con:
            def execute(self, query):
                executed.append(query)
                return _Result()

        count = geojson_stream._native_feature_collection_to_file(
            _Con(),
            "read_parquet('in.parquet')",
            "geometry",
            ["id", "name"],
            str(tmp_path / "o'brien.geojson"),
            7,
            None,
            True,
        )
        assert count == 7
        (query,) = executed
        assert query.startswith("COPY (SELECT ")
        assert "FORMAT GEOJSON" in query
        # A column named `id` must stay a property, never the feature id.
        assert "ID_COLUMN ''" in query
        assert "GEOMETRY_COLUMN 'geometry'" in query
        # NULL-geometry rows are dropped, like the hand-built writer.
        assert 'WHERE "geometry" IS NOT NULL' in query
        # The shared repair/precision chain feeds the writer.
        assert "ST_ReducePrecision" in query
        # The output path is escaped exactly once, by sql_path.
        assert "o''brien.geojson" in query

    @pytest.mark.usefixtures("null_write_scope")
    def test_native_copy_statement_reprojects(self, tmp_path):
        executed = []

        class _Result:
            def fetchone(self):
                return None

        class _Con:
            def execute(self, query):
                executed.append(query)
                return _Result()

        count = geojson_stream._native_feature_collection_to_file(
            _Con(),
            "read_parquet('in.parquet')",
            "geometry",
            [],
            str(tmp_path / "o.geojson"),
            7,
            "EPSG:5070",
            True,
        )
        assert count == 0
        (query,) = executed
        assert "ST_Transform" in query
        assert "EPSG:5070" in query

    def test_native_copy_runs_inside_write_memory_scope(self, tmp_path, monkeypatch):
        """The COPY is a DuckDB write, so it runs inside scoped_write_memory_limit.

        Without the scope the COPY runs at the connection's default ceiling but
        full host threads -- the under-250MB-per-thread regime where DuckDB
        raises instead of spilling on cgroup-limited hosts (#1174, like the
        GDAL and CSV export COPY sites in format_writers).
        """
        from contextlib import contextmanager

        scopes = []
        state = {"active": False}

        @contextmanager
        def _recording(con, memory_limit, verbose, **kwargs):
            state["active"] = True
            try:
                yield
            finally:
                state["active"] = False
                scopes.append((memory_limit, verbose))

        monkeypatch.setattr(geojson_stream, "scoped_write_memory_limit", _recording)
        copies = []

        class _Result:
            def fetchone(self):
                return (1,)

        class _Con:
            def execute(self, query):
                assert state["active"], (
                    "the native GeoJSON COPY ran outside scoped_write_memory_limit"
                )
                copies.append(query)
                return _Result()

        count = geojson_stream._native_feature_collection_to_file(
            _Con(),
            "t",
            "geometry",
            [],
            str(tmp_path / "o.geojson"),
            7,
            None,
            True,
            verbose=True,
        )
        assert count == 1
        assert len(copies) == 1
        assert copies[0].startswith("COPY (")
        # No --write-memory reaches this path, so the ceiling-based default
        # applies, under the caller's verbose flag.
        assert scopes == [(None, True)]

    @pytest.mark.parametrize("exc_name", ["CatalogException", "BinderException"])
    @pytest.mark.usefixtures("null_write_scope")
    def test_unknown_format_refusal_returns_none(self, tmp_path, exc_name):
        """A bind-time refusal of FORMAT GEOJSON signals "not taken", not failure.

        The version gate proves the library is 2.x, not that this build ships
        the copy format; an engine without it must leave the conversion on the
        hand-built writer.
        """

        class _Con:
            def execute(self, query):
                raise getattr(duckdb, exc_name)("Copy Function with name geojson does not exist!")

        count = geojson_stream._native_feature_collection_to_file(
            _Con(),
            "t",
            "geometry",
            [],
            str(tmp_path / "o.geojson"),
            7,
            None,
            True,
        )
        assert count is None

    @pytest.mark.parametrize("exc_name", ["IOException", "InterruptException"])
    @pytest.mark.usefixtures("null_write_scope")
    def test_mid_write_errors_propagate(self, tmp_path, exc_name):
        """Only the unknown-format refusal falls back; a mid-write IO error or
        an interrupt must surface, never be masked by a silent rewrite."""
        # The two caught types must not swallow an interrupt by inheritance.
        assert not issubclass(
            duckdb.InterruptException, (duckdb.CatalogException, duckdb.BinderException)
        )
        exc_type = getattr(duckdb, exc_name)

        class _Con:
            def execute(self, query):
                raise exc_type("boom")

        with pytest.raises(exc_type):
            geojson_stream._native_feature_collection_to_file(
                _Con(),
                "t",
                "geometry",
                [],
                str(tmp_path / "o.geojson"),
                7,
                None,
                True,
            )

    def test_file_call_site_returns_native_count(self, tmp_path, monkeypatch):
        """convert_to_geojson returns the fast path's count from both entry points."""
        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["points_mixed_props"])
        out = tmp_path / "out.geojson"

        def _fake_maybe(*args, **kwargs):
            Path(kwargs["output_path"]).write_text('{"type":"FeatureCollection","features":[]}')
            return 42

        monkeypatch.setattr(geojson_stream, "_maybe_native_feature_collection", _fake_maybe)
        assert convert_to_geojson(str(parquet), str(out)) == 42
        assert out.exists()

    def test_stream_call_site_returns_native_count(self, tmp_path, monkeypatch):
        import pyarrow.parquet as pq

        from geoparquet_io.core import streaming

        parquet = tmp_path / "in.parquet"
        _write_parquet(parquet, EQUIVALENCE_CASES["points_mixed_props"])
        monkeypatch.setattr(streaming, "read_arrow_stream", lambda: pq.read_table(parquet))

        def _fake_maybe(*args, **kwargs):
            Path(kwargs["output_path"]).write_text('{"type":"FeatureCollection","features":[]}')
            return 42

        monkeypatch.setattr(geojson_stream, "_maybe_native_feature_collection", _fake_maybe)
        out = tmp_path / "out.geojson"
        assert convert_to_geojson("-", str(out)) == 42


class TestFallbacks:
    """Options the native writer cannot match stay on the hand-built writer."""

    @pytest.fixture
    def parquet(self, tmp_path):
        path = tmp_path / "in.parquet"
        _write_parquet(path, EQUIVALENCE_CASES["points_mixed_props"])
        return path

    @pytest.fixture
    def forbid_native(self, monkeypatch):
        def _boom(*args, **kwargs):  # pragma: no cover - failure path
            raise AssertionError("ineligible options must not take the native fast path")

        monkeypatch.setattr(geojson_stream, "_native_feature_collection_to_file", _boom)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"pretty": True},
            {"description": "a note"},
            {"id_field": "id"},
            {"write_bbox": True},
        ],
        ids=["pretty", "description", "id_field", "write_bbox"],
    )
    @pytest.mark.usefixtures("forbid_native")
    def test_option_falls_back(self, parquet, tmp_path, kwargs):
        out = tmp_path / "out.geojson"
        count = convert_to_geojson(str(parquet), str(out), **kwargs)
        assert count == 2
        fc = json.loads(out.read_text(encoding="utf-8"))
        assert len(fc["features"]) == 2

    @pytest.mark.usefixtures("forbid_native")
    def test_id_field_keeps_hand_built_semantics(self, parquet, tmp_path):
        """``--id-field`` keeps the column in properties and omits NULL ids."""
        out = tmp_path / "out.geojson"
        convert_to_geojson(str(parquet), str(out), id_field="name")
        features = json.loads(out.read_text(encoding="utf-8"))["features"]
        assert features[0]["id"] == "café ☕"
        assert features[0]["properties"]["name"] == "café ☕"
        assert "id" not in features[1]

    @pytest.mark.usefixtures("forbid_native")
    def test_second_geometry_column_falls_back(self, tmp_path):
        """The writers disagree on geometry-typed properties (WKT vs nested JSON)."""
        parquet = tmp_path / "two_geoms.parquet"
        _write_parquet(
            parquet,
            "SELECT 1 AS k, ST_GeomFromText('POINT(1 2)') AS geometry, "
            "ST_GeomFromText('POINT(3 4)') AS geom2",
        )
        out = tmp_path / "out.geojson"
        count = convert_to_geojson(str(parquet), str(out))
        assert count == 1
        feature = json.loads(out.read_text(encoding="utf-8"))["features"][0]
        assert isinstance(feature["properties"]["geom2"], str)
        assert feature["properties"]["geom2"].startswith("POINT")

    def test_missing_copy_format_falls_back_to_hand_built(self, parquet, tmp_path, monkeypatch):
        """A 2.x library whose engine lacks FORMAT GEOJSON converts via fallback.

        The gate trusts the major version; if the COPY itself comes back as an
        unknown copy function, the conversion must land on the hand-built
        writer instead of hard-failing where DuckDB 1.5 worked.
        """
        monkeypatch.setattr(geojson_stream, "_native_geojson_writer_available", lambda: True)
        real = geojson_stream._native_feature_collection_to_file

        class _CopyRefusingCon:
            """Delegate everything, but refuse the COPY the way a formatless engine does."""

            def __init__(self, con):
                self._con = con

            def execute(self, query, *args, **kwargs):
                if query.lstrip().upper().startswith("COPY"):
                    raise duckdb.CatalogException("Copy Function with name geojson does not exist!")
                return self._con.execute(query, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._con, name)

        def _refusing(con, *args, **kwargs):
            return real(_CopyRefusingCon(con), *args, **kwargs)

        monkeypatch.setattr(geojson_stream, "_native_feature_collection_to_file", _refusing)
        out = tmp_path / "out.geojson"
        count = convert_to_geojson(str(parquet), str(out))
        assert count == 2
        fc = json.loads(out.read_text(encoding="utf-8"))
        assert fc["type"] == "FeatureCollection"
        assert len(fc["features"]) == 2

    @pytest.mark.usefixtures("forbid_native")
    def test_stdout_streaming_never_uses_native_writer(self, parquet, capsys):
        """The tippecanoe RS stream stays on the hand-built writer."""
        count = convert_to_geojson(str(parquet), None, rs=True)
        assert count == 2
        captured = capsys.readouterr()
        assert captured.out.count(geojson_stream.RS) == 2


def _first_coordinate(geometry: dict) -> tuple[float, float]:
    coords = geometry["coordinates"]
    while isinstance(coords[0], list):
        coords = coords[0]
    return coords[0], coords[1]
