import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
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
