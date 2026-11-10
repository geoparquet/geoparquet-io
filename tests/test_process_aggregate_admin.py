import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import sql_path
from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin


def _write_points_geoparquet(path, rows):
    # Writes a real GEOMETRY-typed column (as DuckDB 1.5 produces/reads for
    # GeoParquet), not a plain WKB blob, so the test matches real input files.
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, '{cls}')" for lon, lat, cls in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, cls
            FROM (VALUES {values}) AS t(lon, lat, cls)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_admin_country_with_unassigned(tmp_path):
    src = tmp_path / "pts.parquet"
    out = tmp_path / "by_country.parquet"
    # Two points in France, one in the middle of the ocean (unassigned).
    _write_points_geoparquet(
        src,
        [(2.35, 48.85, "a"), (4.85, 45.75, "b"), (-30.0, 0.0, "c")],
    )
    aggregate_by_admin(str(src), str(out), level="country", out_geometry="polygon")
    table = pq.read_table(out)
    cols = table.column_names
    assert "admin_code" in cols and "admin_name" in cols and "count" in cols
    codes = set(table.column("admin_code").to_pylist())
    assert "unassigned" in codes
    df = table.to_pandas()
    assert int(df.loc[df["admin_code"] == "unassigned", "count"].iloc[0]) == 1


@pytest.mark.slow
@pytest.mark.network
def test_cli_process_aggregate_admin_runs(tmp_path):
    src = tmp_path / "pts.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(2.35, 48.85, "a"), (4.85, 45.75, "b")])
    runner = CliRunner()
    result = runner.invoke(
        cli, ["process", "aggregate", "admin", str(src), str(out), "--level", "country"]
    )
    assert result.exit_code == 0, result.output
    assert out.exists()


@pytest.mark.slow
@pytest.mark.network
def test_table_aggregate_admin_api(tmp_path):
    import duckdb

    from geoparquet_io.api.table import Table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.sql("SELECT ST_AsWKB(ST_Point(2.35, 48.85)) AS geometry, 'a' AS cls").arrow().read_all()
    )
    result = Table(tbl).aggregate_admin(level="country")
    assert "admin_code" in result.column_names
    assert "count" in result.column_names


@pytest.fixture
def local_admin(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate import by_admin

    src = tmp_path / "points.parquet"
    admin = tmp_path / "admin.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(
        f"COPY (SELECT ST_Point(0, 0) geometry, 'a' cls FROM range(1000)) "
        f"TO {sql_path(str(src))} (FORMAT PARQUET)"
    )
    dataset = SimpleNamespace(
        get_level_column_mapping=lambda: {"country": "code"},
        get_geometry_column=lambda: "geometry",
        get_bbox_column=lambda: None,
        get_s3_config=lambda: {"profile": None},
        configure_s3=lambda con: None,
    )
    monkeypatch.setattr(by_admin, "resolve_admin_dataset", lambda *a, **k: (dataset, None, None))
    monkeypatch.setattr(
        by_admin, "_get_admin_ref", lambda *a: f"read_parquet({sql_path(str(admin))})"
    )
    yield con, src, admin
    con.close()


def test_admin_breakdown_join_size_is_independent_of_polygon_size(
    tmp_path, monkeypatch, local_admin
):
    """The per-feature table must not grow with country boundary complexity."""
    from geoparquet_io.core.process.aggregate import by_admin

    con, src, admin = local_admin
    sizes = []
    pivot = by_admin.build_breakdown_pivot

    def measure(connection, *args, **kwargs):
        sizes.append(connection.sql("SELECT * FROM __agg_joined").arrow().read_all().nbytes)
        return pivot(connection, *args, **kwargs)

    monkeypatch.setattr(by_admin, "build_breakdown_pivot", measure)
    for vertices in (8, 256):
        con.execute(
            f"COPY (SELECT 'AA' code, ST_Buffer(ST_Point(0, 0), 1, {vertices}) geometry) "
            f"TO {sql_path(str(admin))} (FORMAT PARQUET)"
        )
        out = tmp_path / f"out-{vertices}.parquet"
        aggregate_by_admin(str(src), str(out), breakdown="cls")
        result = pq.read_table(out)
        assert result.column("count").to_pylist() == [1000]
        assert result.column("count_a").to_pylist() == [1000]
    assert sizes[1] <= sizes[0] * 1.1


@pytest.mark.parametrize("breakdown", [None, "cls"])
@pytest.mark.parametrize("out_geometry", ["polygon", "centroid", "both", "none"])
def test_admin_geometry_uses_matched_row_with_duplicate_codes(
    tmp_path, local_admin, breakdown, out_geometry
):
    con, src, admin = local_admin
    con.execute(
        f"COPY (SELECT ST_Point(x, 0) geometry, 'a' cls FROM (VALUES (10), (30)) t(x)) "
        f"TO {sql_path(str(src))} (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT 'AA' code, ST_Buffer(ST_Point(x, 0), 1) geometry "
        f"FROM (VALUES (0), (10)) t(x)) TO {sql_path(str(admin))} (FORMAT PARQUET)"
    )
    out = tmp_path / "out.parquet"
    aggregate_by_admin(str(src), str(out), breakdown=breakdown, out_geometry=out_geometry)
    table = pq.read_table(out)
    rows = {row["admin_code"]: row for row in table.to_pylist()}
    assert set(rows) == {"AA", "unassigned"}
    assert rows["AA"]["count"] == rows["unassigned"]["count"] == 1
    assert rows["unassigned"]["admin_name"] is None
    assert all(not name.startswith("__") for name in table.column_names)
    if out_geometry == "none":
        assert "geometry" not in table.column_names
    else:
        geometry = (
            "ST_Point(10, 0)" if out_geometry == "centroid" else "ST_Buffer(ST_Point(10, 0), 1)"
        )
        expected = con.sql(f"SELECT ST_AsWKB({geometry}) geometry").arrow().read_all()[0][0].as_py()
        if out_geometry == "centroid":
            # Floating-point centroid coordinates need a geometric comparison.
            import shapely

            assert shapely.from_wkb(rows["AA"]["geometry"]).equals_exact(
                shapely.from_wkb(expected), tolerance=1e-12
            )
        else:
            assert rows["AA"]["geometry"] == expected
        assert rows["unassigned"]["geometry"] is None
    if out_geometry == "both":
        assert rows["AA"]["centroid"] is not None
        assert rows["unassigned"]["centroid"] is None


def test_admin_geometry_attachment_preserves_group_order(local_admin):
    from geoparquet_io.core.process.aggregate.by_admin import _wrap_admin_geometry

    con, _, _ = local_admin
    admin = "(SELECT i AS __bnd, ST_Point(i, 0) geometry FROM range(10) t(i))"
    groups = (
        "SELECT * FROM (VALUES (2, 'BB'), (1, 'AA'), (NULL, 'unassigned')) t(__bnd, admin_code)"
    )
    result = con.sql(_wrap_admin_geometry(groups, "polygon", admin, "geometry")).arrow().read_all()
    assert result.column("admin_code").to_pylist() == ["BB", "AA", "unassigned"]
    assert result.column("geometry")[2].as_py() is None
