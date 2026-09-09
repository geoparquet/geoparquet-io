"""Reprojecting to a projected CRS must not carry `edges: spherical` (#601).

Reprojection transforms *vertices*. gpio does not densify along great circles
first, so the edges of the output are straight lines in the destination CRS.
``planar`` (the spec default) is the truthful description of that output, so a
non-planar ``edges`` declaration is dropped — with one warning per column —
whenever the destination CRS is projected. A geographic destination (a datum
shift, e.g. EPSG:4326 -> EPSG:4269) keeps great-circle semantics, so the
declaration survives there unchanged.
"""

import json
import logging

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.reproject import reproject, reproject_table
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet
from tests.conftest import get_geo_metadata

DROP_WARNING = "the reprojected edges are straight lines"


def _write_crs84_points(path):
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
              SELECT * FROM (VALUES
                (1, ST_Point(1, 1)),
                (2, ST_Point(3, 3))
              ) t(id, geometry)
            ) TO '{path.as_posix()}' (FORMAT PARQUET)
        """)
    finally:
        con.close()


def _set_edges(path, edges):
    """Stamp an ``edges`` value onto the file's geo metadata, in place."""
    table = pq.read_table(str(path))
    kv = dict(table.schema.metadata or {})
    geo = json.loads(kv[b"geo"].decode("utf-8"))
    for col_meta in geo["columns"].values():
        col_meta["edges"] = edges
    kv[b"geo"] = json.dumps(geo).encode("utf-8")
    pq.write_table(table.replace_schema_metadata(kv), str(path))


@pytest.fixture
def spherical_crs84(tmp_path):
    """A CRS84 file declaring ``edges: spherical``."""
    path = tmp_path / "spherical.parquet"
    _write_crs84_points(path)
    _set_edges(path, "spherical")
    return path


@pytest.fixture
def planar_crs84(tmp_path):
    """The same file with no ``edges`` declaration at all."""
    path = tmp_path / "planar.parquet"
    _write_crs84_points(path)
    return path


def _edges(path):
    geo = get_geo_metadata(str(path))
    return geo["columns"][geo["primary_column"]].get("edges")


class TestReprojectToProjectedCrs:
    """A projected destination drops the declaration and says so once."""

    def test_edges_dropped_and_warned(self, spherical_crs84, tmp_path, caplog):
        out = tmp_path / "out.parquet"
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            reproject(str(spherical_crs84), str(out), target_crs="EPSG:3857")

        assert _edges(out) is None, get_geo_metadata(str(out))

        warnings = [r for r in caplog.records if DROP_WARNING in r.message]
        assert len(warnings) == 1, caplog.text
        assert "column 'geometry'" in warnings[0].message
        assert "spherical" in warnings[0].message
        assert "EPSG:3857" in warnings[0].message

    def test_cli_drops_edges(self, spherical_crs84, tmp_path):
        out = tmp_path / "out.parquet"
        result = CliRunner().invoke(
            cli,
            [
                "convert",
                "reproject",
                str(spherical_crs84),
                str(out),
                "--dst-crs",
                "EPSG:3857",
            ],
        )
        assert result.exit_code == 0, result.output
        assert _edges(out) is None, get_geo_metadata(str(out))

    def test_planar_input_says_nothing(self, planar_crs84, tmp_path, caplog):
        out = tmp_path / "out.parquet"
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            reproject(str(planar_crs84), str(out), target_crs="EPSG:3857")

        assert _edges(out) is None
        assert DROP_WARNING not in caplog.text

    def test_python_api_table_drops_edges(self, spherical_crs84):
        table = pq.read_table(str(spherical_crs84))
        result = reproject_table(table, target_crs="EPSG:3857")
        geo = json.loads(result.schema.metadata[b"geo"].decode("utf-8"))
        assert geo["columns"]["geometry"].get("edges") is None, geo["columns"]["geometry"]


def _write_crs84_two_geometry_columns(path):
    """A file with a primary ``geometry`` and a secondary ``centerline`` column."""
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
              SELECT * FROM (VALUES
                (1, ST_Point(1, 1), ST_Point(10, 10)),
                (2, ST_Point(3, 3), ST_Point(30, 30))
              ) t(id, geometry, centerline)
            ) TO '{path.as_posix()}' (FORMAT PARQUET)
        """)
    finally:
        con.close()


@pytest.fixture
def spherical_two_columns(tmp_path):
    """Two geometry columns, both declaring ``edges: spherical``."""
    path = tmp_path / "two_spherical.parquet"
    _write_crs84_two_geometry_columns(path)
    _set_edges(path, "spherical")
    return path


class TestMultiGeometryColumns:
    """Only the column actually transformed loses its declaration.

    Reproject transforms the primary geometry column only; a secondary column
    keeps its geographic CRS and untransformed coordinates, so its great-circle
    ``edges`` declaration still holds and must survive — without a warning
    falsely claiming it was reprojected.
    """

    def test_only_transformed_column_loses_edges(self, spherical_two_columns, tmp_path, caplog):
        out = tmp_path / "out.parquet"
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            reproject(str(spherical_two_columns), str(out), target_crs="EPSG:3857")

        geo = get_geo_metadata(str(out))
        assert geo["columns"]["geometry"].get("edges") is None, geo["columns"]["geometry"]
        assert geo["columns"]["centerline"].get("edges") == "spherical", geo["columns"][
            "centerline"
        ]

        warnings = [r for r in caplog.records if DROP_WARNING in r.message]
        assert len(warnings) == 1, caplog.text
        assert "column 'geometry'" in warnings[0].message
        assert "centerline" not in caplog.text

    def test_python_api_table_scopes_drop_to_transformed_column(
        self, spherical_two_columns, caplog
    ):
        table = pq.read_table(str(spherical_two_columns))
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            result = reproject_table(table, target_crs="EPSG:3857")

        geo = json.loads(result.schema.metadata[b"geo"].decode("utf-8"))
        assert geo["columns"]["geometry"].get("edges") is None, geo["columns"]["geometry"]
        assert geo["columns"]["centerline"].get("edges") == "spherical", geo["columns"][
            "centerline"
        ]
        assert "centerline" not in caplog.text

    def test_streaming_path_scopes_drop_to_transformed_column(
        self, spherical_two_columns, tmp_path, caplog
    ):
        from geoparquet_io.core.reproject import _reproject_streaming

        out = tmp_path / "streamed.parquet"
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            _reproject_streaming(
                str(spherical_two_columns),
                str(out),
                "EPSG:3857",
                None,  # source_crs
                "ZSTD",  # compression
                None,  # compression_level
                False,  # verbose
                None,  # profile
                None,  # geoparquet_version
            )

        geo = get_geo_metadata(str(out))
        assert geo["columns"]["geometry"].get("edges") is None, geo["columns"]["geometry"]
        assert geo["columns"]["centerline"].get("edges") == "spherical", geo["columns"][
            "centerline"
        ]
        assert "centerline" not in caplog.text


class TestReprojectToGeographicCrs:
    """A datum shift between geographic CRSs keeps great-circle semantics."""

    def test_edges_preserved_without_warning(self, spherical_crs84, tmp_path, caplog):
        out = tmp_path / "out.parquet"
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            reproject(str(spherical_crs84), str(out), target_crs="EPSG:4269")

        assert _edges(out) == "spherical", get_geo_metadata(str(out))
        assert DROP_WARNING not in caplog.text

    def test_python_api_table_preserves_edges(self, spherical_crs84):
        table = pq.read_table(str(spherical_crs84))
        result = reproject_table(table, target_crs="EPSG:4269")
        geo = json.loads(result.schema.metadata[b"geo"].decode("utf-8"))
        assert geo["columns"]["geometry"].get("edges") == "spherical"


class TestNonReprojectingRewrites:
    """The drop is scoped to CRS-changing writes; other rewrites still carry."""

    def test_sort_hilbert_keeps_spherical_edges(self, spherical_crs84, tmp_path):
        from geoparquet_io.core.hilbert_order import hilbert_order

        out = tmp_path / "sorted.parquet"
        hilbert_order(str(spherical_crs84), str(out))
        assert _edges(out) == "spherical", get_geo_metadata(str(out))


def _new_check(result):
    return [c for c in result.checks if c.name.startswith("edges_spherical_on_projected_crs")]


class TestValidatorWarnsOnSphericalPlusProjected:
    """`gpio check spec` flags the combination as a warning, never a failure."""

    def test_spherical_on_projected_crs_warns_and_still_valid(self, spherical_crs84, tmp_path):
        out = tmp_path / "out.parquet"
        reproject(str(spherical_crs84), str(out), target_crs="EPSG:3857")
        # Put the declaration back the way a third-party writer would leave it.
        _set_edges(out, "spherical")

        result = validate_geoparquet(str(out), validate_data=False)
        checks = _new_check(result)
        assert len(checks) == 1, [c.name for c in result.checks]
        assert checks[0].status == CheckStatus.WARNING
        assert "spherical" in checks[0].message
        assert "geometry" in checks[0].message
        assert result.is_valid, [c.message for c in result.checks if c.status == CheckStatus.FAILED]

    def test_cli_check_spec_does_not_fail_the_file(self, spherical_crs84, tmp_path):
        """Exit 2 is `check spec`'s warnings-only code; a failure would be exit 1."""
        out = tmp_path / "out.parquet"
        reproject(str(spherical_crs84), str(out), target_crs="EPSG:3857")
        _set_edges(out, "spherical")

        result = CliRunner().invoke(cli, ["check", "spec", str(out)])
        assert result.exit_code == 2, result.output
        assert "0 failed" in result.output
        assert "projected CRS" in result.output

    def test_spherical_on_geographic_crs_is_silent(self, spherical_crs84):
        result = validate_geoparquet(str(spherical_crs84), validate_data=False)
        assert _new_check(result) == []

    def test_planar_on_projected_crs_is_silent(self, spherical_crs84, tmp_path):
        out = tmp_path / "out.parquet"
        reproject(str(spherical_crs84), str(out), target_crs="EPSG:3857")

        result = validate_geoparquet(str(out), validate_data=False)
        assert _new_check(result) == []
