"""Secondary geometry columns are described and their emptied stats recomputed.

Two halves of the same blind spot (#1000, #952) — the write path only really
knew about the PRIMARY geometry column:

* **#1000**: `add *` (and every caller that passes no ``geometry_info``) never
  told ``build_geo_metadata`` a secondary existed, so a secondary NATIVE
  geometry column was left out of ``geo.columns`` entirely — which GeoParquet
  2.0 requires for every geometry column in the file. The funnel now derives
  ``geometry_info`` from the ``input_file`` witness itself
  (:func:`geoparquet_io.core.derive_geo_from_file.derive_secondary_geometry_info`),
  each secondary with its OWN CRS read from its logical type — never the
  primary's/file-level CRS (pinned by
  ``test_add_attaches_the_witness_crs_to_the_primary_column_only``).

* **#952**: a merge/partition write that could not carry a secondary's stats
  writes the spec's "not known" sentinel ``geometry_types: []`` — and
  ``duckdb_kv._compute_missing_metadata`` gated its recompute on the key being
  absent, so the sentinel was sticky: no file → file command ever replaced it.
  The gate now mirrors ``backfill_derived_stats``: an empty list is a gap,
  computed once, for every declared geometry column; a genuinely empty result
  writes ``[]`` again without a warning and without another pass.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.derive_geo_from_file import derive_secondary_geometry_info


def _geo(path) -> dict:
    kv = pq.ParquetFile(str(path)).metadata.metadata or {}
    assert b"geo" in kv, f"{path} has no geo key"
    return json.loads(kv[b"geo"].decode("utf-8"))


def _run_cli(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


def _write_two_geometry_file(path, rows: bool = True, geometry_types=None) -> None:
    """Two WKB geometry columns; ``geometry_types`` values land verbatim.

    ``geometry_types=None`` writes the real lists; ``[]`` writes the #952
    sentinel on both columns. ``rows=False`` writes a zero-row table, built
    with ``from_pydict`` (never ``from_batches([])``: geoarrow aborts on
    zero-chunk arrays).
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    points, polys = [], []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        points.append(con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y}))").fetchone()[0])
        wkt = f"POLYGON(({x - 0.5} {y - 0.5}, {x + 0.5} {y - 0.5}, {x + 0.5} {y + 0.5}, {x - 0.5} {y + 0.5}, {x - 0.5} {y - 0.5}))"
        polys.append(con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()[0])
    con.close()

    if not rows:
        points, polys = [], []
    table = pa.Table.from_pydict(
        {
            "id": pa.array(range(len(points)), type=pa.int32()),
            "geometry": pa.array(points, type=pa.binary()),
            "boundary": pa.array(polys, type=pa.binary()),
        }
    )

    point_types = ["Point"] if geometry_types is None else geometry_types
    polygon_types = ["Polygon"] if geometry_types is None else geometry_types
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": point_types},
            "boundary": {"encoding": "WKB", "geometry_types": polygon_types},
        },
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(geo_meta).encode("utf-8")})
    pq.write_table(table, str(path))


# ---------------------------------------------------------------------------
# #952: the emptied sentinel is a gap, not a value
# ---------------------------------------------------------------------------


class TestEmptiedGeometryTypesAreRecomputed:
    def test_convert_geoparquet_restores_the_real_list_on_both_columns(self, tmp_path):
        """The #952 repro: before, geometry came back ['Point'] but boundary stayed []."""
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        _write_two_geometry_file(input_file, geometry_types=[])

        _run_cli("convert", "geoparquet", input_file, output_file)

        columns = _geo(output_file)["columns"]
        assert columns["geometry"]["geometry_types"] == ["Point"]
        assert columns["boundary"]["geometry_types"] == ["Polygon"]

    def test_the_recompute_also_fills_the_secondarys_missing_bbox(self, tmp_path):
        """bbox and geometry_types come out of the same single scan."""
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        _write_two_geometry_file(input_file, geometry_types=[])

        _run_cli("convert", "geoparquet", input_file, output_file)

        boundary = _geo(output_file)["columns"]["boundary"]
        assert boundary["bbox"] == [-0.5, -0.5, 2.5, 2.5]

    def test_a_genuinely_empty_file_keeps_the_sentinel_quietly(self, tmp_path, capsys):
        """[] is also the honest answer for zero rows: written once, no warning."""
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        _write_two_geometry_file(input_file, rows=False, geometry_types=[])

        result = CliRunner().invoke(
            cli, ["convert", "geoparquet", str(input_file), str(output_file)]
        )
        assert result.exit_code == 0, result.output

        columns = _geo(output_file)["columns"]
        assert columns["geometry"]["geometry_types"] == []
        assert columns["boundary"]["geometry_types"] == []
        assert "geometry_types" not in result.output.lower() or "warn" not in result.output.lower()

    def test_known_values_are_left_alone(self, tmp_path):
        """This fills gaps, it does not audit: a carried real list is not rescanned away."""
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        _write_two_geometry_file(input_file)

        _run_cli("convert", "geoparquet", input_file, output_file)

        columns = _geo(output_file)["columns"]
        assert columns["boundary"]["geometry_types"] == ["Polygon"]


# ---------------------------------------------------------------------------
# #1000: geometry_info derived from the input-file witness
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def two_native_columns(tmp_path_factory):
    """``geometry`` EPSG:5070 + ``centroid`` EPSG:3857, native types, no geo key."""
    import geoarrow.pyarrow as ga

    from tests.native_geo_probes import conus_wkb, projjson, write_native_geo_only

    rows = conus_wkb(
        "ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)",
        "ST_Transform(ST_Centroid(cell), 'EPSG:4326', 'EPSG:3857', always_xy := true)",
    )
    return write_native_geo_only(
        tmp_path_factory.mktemp("two_native") / "pgo.parquet",
        rows,
        {
            "geometry": (2, ga.wkb().with_crs(projjson(5070))),
            "centroid": (3, ga.wkb().with_crs(projjson(3857))),
        },
    )


class TestDeriveSecondaryGeometryInfo:
    def test_native_secondary_is_derived_with_its_own_crs(self, two_native_columns):
        info = derive_secondary_geometry_info(str(two_native_columns), "geometry")

        assert info is not None
        assert info["primary"] == "geometry"
        assert info["secondary"] == ["centroid"]
        crs = info["metadata"]["centroid"]["crs"]
        assert crs["id"] == {"authority": "EPSG", "code": 3857}, (
            "the secondary's CRS must come from ITS logical type, never the primary's"
        )

    def test_single_geometry_file_derives_nothing(self, tmp_path):
        import duckdb

        path = tmp_path / "single.parquet"
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"COPY (SELECT 1 AS id, ST_Point(0, 0) AS geometry) TO '{path}'")
        con.close()

        assert derive_secondary_geometry_info(str(path), "geometry") is None

    def test_a_projected_away_secondary_is_not_derived(self, two_native_columns):
        """extract --exclude-cols must not resurrect a column the output drops."""
        info = derive_secondary_geometry_info(
            str(two_native_columns), "geometry", output_columns=["cell", "geometry"]
        )
        assert info is None

    def test_declared_secondaries_are_named_without_stale_stats(self, tmp_path):
        """A geo-block secondary is listed, but its derived metadata carries no
        bbox/geometry_types: those flow through original_metadata, where the
        caller's invalidation (#934) has already had its say."""
        input_file = tmp_path / "declared.parquet"
        _write_two_geometry_file(input_file)

        info = derive_secondary_geometry_info(str(input_file), "geometry")

        assert info is not None
        assert info["secondary"] == ["boundary"]
        assert "geometry_types" not in info["metadata"]["boundary"]
        assert "bbox" not in info["metadata"]["boundary"]

    def test_an_unreadable_input_derives_nothing(self, tmp_path):
        missing = tmp_path / "nope.parquet"
        assert derive_secondary_geometry_info(str(missing), "geometry") is None
