import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3
from geoparquet_io.core.process.aggregate.options import AggregateOptions


def _write_points_geoparquet(path, rows):
    """rows: list of (lon, lat, crop, area). Writes a tiny GeoParquet of points.

    Writes a real GEOMETRY-typed column (as DuckDB 1.5 produces and reads back for
    GeoParquet), matching what the tool sees on real input files.
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
def test_aggregate_h3_native_geometry_column(tmp_path):
    """Real GeoParquet geometry is read as GEOMETRY (not WKB BLOB); must work."""
    src = tmp_path / "fields.parquet"
    out = tmp_path / "agg.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0), (10.001, 50.001, "corn", 2.0)])
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    desc = con.execute(f"DESCRIBE SELECT geometry FROM read_parquet('{src}')").fetchall()
    con.close()
    assert "GEOMETRY" in desc[0][1].upper()

    aggregate_by_h3(str(src), str(out), resolution=6)
    table = pq.read_table(out)
    assert "h3_cell" in table.column_names
    assert "count" in table.column_names
    assert "geometry" in table.column_names


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_h3_count_metric_breakdown(tmp_path):
    src = tmp_path / "fields.parquet"
    out = tmp_path / "agg.parquet"
    # Three points close together (same res-6 cell) plus one far away.
    _write_points_geoparquet(
        src,
        [
            (10.00, 50.00, "wheat", 4.0),
            (10.001, 50.001, "wheat", 6.0),
            (10.002, 50.002, "corn", 2.0),
            (-120.0, 40.0, "wheat", 1.0),
        ],
    )
    aggregate_by_h3(
        str(src),
        str(out),
        resolution=6,
        options=AggregateOptions(metric="sum:area", breakdown="crop"),
    )
    table = pq.read_table(out)
    cols = table.column_names
    assert {"h3_cell", "count", "sum_area", "count_wheat", "count_corn", "geometry"} <= set(cols)
    # h3 cell ids are strings.
    assert table.schema.field("h3_cell").type == "string" or "string" in str(
        table.schema.field("h3_cell").type
    )
    df = table.to_pandas().sort_values("count", ascending=False).reset_index(drop=True)
    assert int(df.loc[0, "count"]) == 3
    assert float(df.loc[0, "sum_area"]) == 12.0
    assert int(df.loc[0, "count_wheat"]) == 2
    assert int(df.loc[0, "count_corn"]) == 1


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_h3_out_geometry_none_is_plain_table(tmp_path):
    src = tmp_path / "fields.parquet"
    out = tmp_path / "agg_none.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0)])
    aggregate_by_h3(str(src), str(out), resolution=6, options=AggregateOptions(out_geometry="none"))
    table = pq.read_table(out)
    assert "h3_cell" in table.column_names
    assert "count" in table.column_names
    assert "geometry" not in table.column_names
    assert b"geo" not in (table.schema.metadata or {})


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_h3_null_geometry_feature(tmp_path):
    """Features with NULL geometry -> NULL-cell row, not a crash."""
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
    aggregate_by_h3(str(src), str(out), resolution=6)  # must not raise
    df = pq.read_table(out).to_pandas()
    assert int(df["count"].sum()) == 3
    null_rows = df[df["h3_cell"].isna()]
    assert len(null_rows) == 1
    assert int(null_rows["count"].iloc[0]) == 1
    assert null_rows["geometry"].iloc[0] is None


def test_aggregate_h3_requires_resolution_or_auto(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0)])
    with pytest.raises(InvalidParameterError):
        aggregate_by_h3(str(src), str(tmp_path / "o.parquet"))


def test_aggregate_h3_rejects_resolution_and_auto(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0)])
    with pytest.raises(InvalidParameterError):
        aggregate_by_h3(str(src), str(tmp_path / "o.parquet"), resolution=6, auto=True)


def test_aggregate_h3_rejects_out_of_range_resolution(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0)])
    # h3 max resolution is 15.
    with pytest.raises(InvalidParameterError):
        aggregate_by_h3(str(src), str(tmp_path / "o.parquet"), resolution=16)


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_h3_auto_runs(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0), (10.001, 50.001, "corn", 2.0)])
    aggregate_by_h3(str(src), str(out), auto=True, target_per_cell=1)
    assert out.exists()


@pytest.mark.slow
@pytest.mark.network
def test_cli_process_aggregate_h3(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0), (10.001, 50.001, "corn", 2.0)])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "aggregate",
            "h3",
            str(src),
            str(out),
            "--resolution",
            "6",
            "--metric",
            "sum:area",
            "--breakdown",
            "crop",
        ],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()


def test_cli_process_aggregate_h3_bad_resolution(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0)])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["process", "aggregate", "h3", str(src), str(tmp_path / "o.parquet"), "--resolution", "20"],
    )
    assert result.exit_code != 0


@pytest.mark.slow
@pytest.mark.network
def test_table_aggregate_h3_api():
    from geoparquet_io.api.table import Table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
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
    result = Table(tbl).aggregate_h3(resolution=6, metric="sum:area")
    assert "h3_cell" in result.column_names
    assert "count" in result.column_names
    assert "sum_area" in result.column_names
    assert "geometry" in result.column_names
