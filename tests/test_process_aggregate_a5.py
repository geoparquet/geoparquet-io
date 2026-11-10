import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5
from geoparquet_io.core.process.aggregate.options import AggregateOptions


def _write_points_geoparquet(path, rows):
    """rows: list of (lon, lat, crop, area). Writes a tiny GeoParquet of points.

    Writes a real GEOMETRY-typed column (as DuckDB 1.5 produces and reads back for
    GeoParquet), NOT a plain WKB blob -- so these tests exercise the same
    geometry typing the tool sees on real input files.
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, '{crop}', {area})" for lon, lat, crop, area in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, crop, area
            FROM (VALUES {values}) AS t(lon, lat, crop, area)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_native_geometry_column(tmp_path):
    """Regression: DuckDB 1.5 reads GeoParquet geometry as GEOMETRY (not WKB BLOB).

    The tool must not unconditionally wrap the column in ST_GeomFromWKB, which only
    accepts BLOB and raised a Binder Error on real GeoParquet input.
    """
    src = tmp_path / "fields.parquet"
    out = tmp_path / "agg.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0), (10.001, 50.001, "corn", 2.0)])
    # Confirm the fixture is a GEOMETRY column, matching real GeoParquet input.
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    desc = con.execute(f"DESCRIBE SELECT geometry FROM read_parquet('{src}')").fetchall()
    con.close()
    assert "GEOMETRY" in desc[0][1].upper()

    aggregate_by_a5(str(src), str(out), resolution=5)
    table = pq.read_table(out)
    assert "a5_cell" in table.column_names
    assert "count" in table.column_names
    assert "geometry" in table.column_names


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_null_geometry_feature(tmp_path):
    """Regression: features with NULL/empty geometry have no assignable cell.

    Their NULL cell id must yield a row with NULL geometry (like admin's
    'unassigned' bucket), not crash ST_MakePolygon on a degenerate boundary.
    """
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT * FROM (VALUES
                (ST_Point(10.0, 50.0), 'a'),
                (ST_Point(10.001, 50.001), 'b'),
                (CAST(NULL AS GEOMETRY), 'c')
            ) AS t(geometry, cls)
        ) TO '{src}' (FORMAT PARQUET)
        """
    )
    con.close()

    aggregate_by_a5(str(src), str(out), resolution=4)  # must not raise
    table = pq.read_table(out)
    df = table.to_pandas()
    # All 3 input features accounted for: 2 valid (one cell) + 1 null-cell row.
    assert int(df["count"].sum()) == 3
    null_rows = df[df["a5_cell"].isna()]
    assert len(null_rows) == 1
    assert int(null_rows["count"].iloc[0]) == 1
    assert null_rows["geometry"].iloc[0] is None


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_count_metric_breakdown(tmp_path):
    src = tmp_path / "fields.parquet"
    out = tmp_path / "agg.parquet"
    # Two clusters far apart so they land in different res-5 cells.
    _write_points_geoparquet(
        src,
        [
            (10.00, 50.00, "wheat", 4.0),
            (10.001, 50.001, "wheat", 6.0),
            (10.002, 50.002, "corn", 2.0),
            (-120.0, 40.0, "wheat", 1.0),
        ],
    )
    aggregate_by_a5(
        str(src),
        str(out),
        resolution=5,
        options=AggregateOptions(metric="sum:area", breakdown="crop", out_geometry="polygon"),
    )
    table = pq.read_table(out)
    cols = table.column_names
    assert "a5_cell" in cols
    assert "count" in cols
    assert "sum_area" in cols
    assert "count_wheat" in cols and "count_corn" in cols
    assert "geometry" in cols
    # Two output cells (two clusters)
    assert table.num_rows == 2
    # The 3-feature cell totals area 12 and has 2 wheat + 1 corn
    df = table.to_pandas().sort_values("count", ascending=False).reset_index(drop=True)
    assert int(df.loc[0, "count"]) == 3
    assert float(df.loc[0, "sum_area"]) == 12.0
    assert int(df.loc[0, "count_wheat"]) == 2
    assert int(df.loc[0, "count_corn"]) == 1


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_out_geometry_none_is_plain_table(tmp_path):
    src = tmp_path / "fields.parquet"
    out = tmp_path / "agg_none.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0)])
    aggregate_by_a5(str(src), str(out), resolution=5, options=AggregateOptions(out_geometry="none"))
    table = pq.read_table(out)
    assert "a5_cell" in table.column_names
    assert "count" in table.column_names
    assert "geometry" not in table.column_names
    assert b"geo" not in (table.schema.metadata or {})


def test_aggregate_a5_requires_resolution_or_auto(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0)])
    with pytest.raises(InvalidParameterError):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"))  # neither given


def test_aggregate_a5_rejects_resolution_and_auto(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0)])
    with pytest.raises(InvalidParameterError):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, auto=True)


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_auto_runs(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0), (10.001, 50.001, "corn", 2.0)])
    aggregate_by_a5(str(src), str(out), auto=True, target_per_cell=1)
    assert out.exists()


@pytest.mark.slow
@pytest.mark.network
def test_cli_process_aggregate_a5(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0), (10.001, 50.001, "corn", 2.0)])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "aggregate",
            "a5",
            str(src),
            str(out),
            "--resolution",
            "5",
            "--metric",
            "sum:area",
            "--breakdown",
            "crop",
        ],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()


def _points_table():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    return (
        con.sql(
            """
        SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry, crop, area FROM (VALUES
            (10.0, 50.0, 'wheat', 4.0),
            (10.001, 50.001, 'corn', 2.0)
        ) AS t(lon, lat, crop, area)
        """
        )
        .arrow()
        .read_all()
    )


@pytest.mark.slow
@pytest.mark.network
def test_table_aggregate_a5_api():
    from geoparquet_io.api.table import Table

    result = Table(_points_table()).aggregate_a5(resolution=5, metric="sum:area")
    assert "a5_cell" in result.column_names
    assert "count" in result.column_names
    assert "sum_area" in result.column_names
    assert "geometry" in result.column_names


def test_cli_process_aggregate_a5_bad_metric(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0)])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "aggregate",
            "a5",
            str(src),
            str(tmp_path / "o.parquet"),
            "--resolution",
            "5",
            "--metric",
            "median:area",
        ],
    )
    assert result.exit_code != 0
    assert "median" in result.output.lower() or "metric" in result.output.lower()


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_input_with_reserved_column_names(tmp_path):
    """Input columns named like internal aliases (__geom/__key) must not shadow
    the generated grid columns."""
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, area, 'junk' AS __geom, 99 AS __key
            FROM (VALUES (10.0, 50.0, 4.0), (10.001, 50.001, 2.0)) AS t(lon, lat, area)
        ) TO '{src}' (FORMAT PARQUET)
        """
    )
    con.close()
    aggregate_by_a5(str(src), str(out), resolution=5, options=AggregateOptions(metric="sum:area"))
    df = pq.read_table(out).to_pandas()
    # The real geometry is produced (not shadowed by the 'junk' __geom user column).
    assert int(df["count"].sum()) == 2
    assert df["geometry"].iloc[0] is not None
    assert float(df["sum_area"].sum()) == 6.0


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_table_tolerates_bad_wkb():
    """A malformed WKB value becomes an unassigned (NULL-cell) row, not a crash."""
    from geoparquet_io.api.table import Table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.sql(
            """
            SELECT * FROM (VALUES
                (ST_AsWKB(ST_Point(10.0, 50.0)), 'a'),
                (CAST('notwkb' AS BLOB), 'b')
            ) AS t(geometry, cls)
            """
        )
        .arrow()
        .read_all()
    )
    result = Table(tbl).aggregate_a5(resolution=5)  # must not raise
    df = result.table.to_pandas()
    assert int(df["count"].sum()) == 2
    assert df["a5_cell"].isna().any()  # the bad-WKB row -> unassigned
