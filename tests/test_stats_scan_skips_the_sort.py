"""The derived-stats scan must not re-sort the input (gpio #1177, item 1).

``convert`` applies its Hilbert ``ORDER BY`` to the query it hands the write
funnel, and the funnel's stats scan wraps that same query. DuckDB keeps the
``ORDER_BY`` operator in the plan, so every write whose carried ``bbox`` or
``geometry_types`` has to be recomputed sorted the input twice: once to measure
it and once to write it. ``bbox`` and ``geometry_types`` are aggregates over
every row, so the order they are read in cannot change either answer.

The one order that *is* load-bearing is a ``LIMIT``'s: there the ORDER BY
decides *which* rows exist, so it has to stay. And the COPY must keep its
ordering whatever happens here -- a Hilbert-sorted output that came out
unsorted would be a far worse bug than the one being fixed.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import compute_geometry_types_via_sql
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.geo_metadata import compute_geo_stats_via_sql

_HILBERT_ORDER = "ORDER BY ST_Hilbert(geometry, ST_Extent(ST_MakeEnvelope(-180, -85, 180, 85)))"


class _RecordingConnection:
    """A DuckDB connection that remembers every statement executed through it."""

    def __init__(self, con):
        self._con = con
        self.statements: list[str] = []

    def execute(self, sql, *args, **kwargs):
        self.statements.append(sql)
        return self._con.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._con, name)

    def sorting_statements(self) -> list[str]:
        return [s for s in self.statements if "ORDER BY" in s.upper()]

    def scanning_statements(self) -> list[str]:
        """Statements that actually read every row, not binder-only probes.

        ``DESCRIBE (...)`` and ``... LIMIT 0`` quote the query back verbatim but
        bind it without running it, so they are not what sorts anything.
        """
        return [
            s
            for s in self.statements
            if not s.lstrip().upper().startswith("DESCRIBE")
            and not s.rstrip().upper().endswith("LIMIT 0")
        ]


@pytest.fixture
def scrambled_points(tmp_path):
    """Six points whose natural file order is not their Hilbert order."""
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_Point(10, 10)), (2, ST_Point(-50, -20)), (3, ST_Point(170, 80)),
            (4, ST_Point(-170, -80)), (5, ST_Point(0, 0)), (6, ST_Point(100, -45))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V1')
    """)
    con.close()
    return path


@pytest.fixture
def spatial_con():
    con = get_duckdb_connection(load_spatial=True)
    yield con
    con.close()


def test_stats_scan_does_not_sort_the_input(spatial_con, scrambled_points):
    """The stats scan must not carry the caller's trailing ORDER BY (#1177)."""
    recorder = _RecordingConnection(spatial_con)
    query = f"SELECT * FROM '{scrambled_points.as_posix()}' {_HILBERT_ORDER}"

    compute_geo_stats_via_sql(recorder, query, "geometry")

    assert recorder.statements, "the stats scan executed nothing"
    assert not recorder.sorting_statements(), "the stats scan still sorts the input: " + "\n".join(
        recorder.sorting_statements()
    )


def test_stats_scan_answers_the_same_with_and_without_the_order(spatial_con, scrambled_points):
    """Dropping the order must not change either aggregate."""
    base = f"SELECT * FROM '{scrambled_points.as_posix()}'"

    assert compute_geo_stats_via_sql(spatial_con, base, "geometry") == compute_geo_stats_via_sql(
        spatial_con, f"{base} {_HILBERT_ORDER}", "geometry"
    )


def test_stats_scan_keeps_an_order_that_a_limit_depends_on(spatial_con, scrambled_points):
    """``ORDER BY ... LIMIT n`` decides *which* rows exist, so it must survive."""
    limited = f"SELECT * FROM '{scrambled_points.as_posix()}' ORDER BY id LIMIT 2"

    bbox, _ = compute_geo_stats_via_sql(spatial_con, limited, "geometry")

    # ids 1 and 2 only: (10, 10) and (-50, -20). Keeping the whole file's rows
    # would give xmax 170 / ymin -80.
    assert bbox == [-50.0, -20.0, 10.0, 10.0], bbox


def test_the_funnels_stats_scan_drops_the_order_but_the_copy_keeps_it(
    spatial_con, scrambled_points, tmp_path
):
    """End to end through the funnel: scan unsorted, COPY sorted (#1177)."""
    from geoparquet_io.core.write_funnels import write_parquet_with_metadata

    # The "not known" geometry_types sentinel a merge/partition write leaves
    # behind (#934) is what makes the funnel run the stats scan at all.
    original = {
        "geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": []}},
            }
        )
    }
    recorder = _RecordingConnection(spatial_con)
    out = tmp_path / "out.parquet"
    query = f"SELECT * FROM '{scrambled_points.as_posix()}' {_HILBERT_ORDER}"

    write_parquet_with_metadata(
        recorder,
        query,
        str(out),
        original_metadata=original,
        geoparquet_version="1.1",
    )

    sorting = [s for s in recorder.scanning_statements() if "ORDER BY" in s.upper()]
    assert sorting, "the COPY lost the Hilbert ORDER BY"
    assert all(s.lstrip().upper().startswith("COPY") for s in sorting), (
        "something other than the COPY still sorts the input: " + "\n".join(sorting)
    )
    geo = json.loads(pq.ParquetFile(str(out)).metadata.metadata[b"geo"])
    assert geo["columns"]["geometry"]["geometry_types"] == ["Point"]
    assert geo["columns"]["geometry"]["bbox"] == [-170.0, -80.0, 170.0, 80.0]


def test_the_disk_rewrite_stats_scan_drops_the_order_but_the_copy_keeps_it(
    spatial_con, scrambled_points, tmp_path
):
    """``disk-rewrite`` calls the type scan directly, so it needs the strip too.

    ``compute_geo_stats_via_sql`` strips the order for the funnel's own scan, but
    this strategy reaches past it: it calls ``compute_bbox_via_sql`` and
    ``compute_geometry_types_via_sql`` itself with the caller's ordered query. The
    type scan is a ``SELECT DISTINCT`` -- an answer no row order can change --
    yet DuckDB still sorted the whole input to produce it (#1177).
    """
    from geoparquet_io.core.write_funnels import write_parquet_with_metadata

    recorder = _RecordingConnection(spatial_con)
    out = tmp_path / "out.parquet"
    query = f"SELECT * FROM '{scrambled_points.as_posix()}' {_HILBERT_ORDER}"

    write_parquet_with_metadata(
        recorder,
        query,
        str(out),
        geoparquet_version="1.1",
        write_strategy="disk-rewrite",
    )

    sorting = [s for s in recorder.scanning_statements() if "ORDER BY" in s.upper()]
    assert sorting, "the COPY lost the Hilbert ORDER BY"
    assert all(s.lstrip().upper().startswith("COPY") for s in sorting), (
        "something other than the COPY still sorts the input: " + "\n".join(sorting)
    )
    geo = json.loads(pq.ParquetFile(str(out)).metadata.metadata[b"geo"])
    assert geo["columns"]["geometry"]["geometry_types"] == ["Point"]
    assert geo["columns"]["geometry"]["bbox"] == [-170.0, -80.0, 170.0, 80.0]


def test_geometry_types_scan_answers_the_same_with_and_without_the_order(
    spatial_con, scrambled_points
):
    """The canonical type scan: same answer ordered or not, and no sort either way."""
    base = f"SELECT * FROM '{scrambled_points.as_posix()}'"
    recorder = _RecordingConnection(spatial_con)

    unordered = compute_geometry_types_via_sql(spatial_con, base, "geometry")
    ordered = compute_geometry_types_via_sql(recorder, f"{base} {_HILBERT_ORDER}", "geometry")

    assert ordered == unordered == ["Point"]
    assert recorder.statements, "the type scan executed nothing"
    assert not recorder.sorting_statements(), "the type scan still sorts the input: " + "\n".join(
        recorder.sorting_statements()
    )


def test_geometry_types_scan_keeps_an_order_that_a_limit_depends_on(spatial_con, tmp_path):
    """``ORDER BY ... LIMIT n`` decides *which* rows exist, so it must survive."""
    path = tmp_path / "mixed.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
              SELECT * FROM (VALUES
                (1, ST_Point(10, 10)), (2, ST_Point(0, 0)),
                (3, ST_GeomFromText('LINESTRING(0 0, 1 1)'))
              ) t(id, geometry)
            ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V1')
        """)
    finally:
        con.close()

    limited = f"SELECT * FROM '{path.as_posix()}' ORDER BY id LIMIT 2"

    # ids 1 and 2 only: both points. Keeping the whole file's rows would also
    # report the LineString.
    assert compute_geometry_types_via_sql(spatial_con, limited, "geometry") == ["Point"]


def test_hilbert_output_row_order_is_unchanged(scrambled_points, tmp_path):
    """The pin: a Hilbert convert's output rows stay in Hilbert order (#1177).

    Losing the ordering would be a far worse regression than the double sort
    this issue removes, so the order is asserted against DuckDB's own answer
    for the same key rather than against a recorded constant.
    """
    from geoparquet_io.core.convert import convert_to_geoparquet

    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(scrambled_points), str(out), geoparquet_version="1.1")

    con = get_duckdb_connection(load_spatial=True)
    try:
        # convert keys the curve to the data's own extent, so the expectation
        # has to as well -- a different envelope is a different curve.
        expected = [
            row[0]
            for row in con.execute(f"""
                SELECT id FROM '{scrambled_points.as_posix()}'
                ORDER BY ST_Hilbert(
                    geometry,
                    (SELECT ST_Extent(ST_MakeEnvelope(
                        MIN(ST_XMin(geometry)), MIN(ST_YMin(geometry)),
                        MAX(ST_XMax(geometry)), MAX(ST_YMax(geometry))
                    )) FROM '{scrambled_points.as_posix()}')
                )
            """).fetchall()
        ]
    finally:
        con.close()

    written = pq.read_table(str(out)).column("id").to_pylist()
    assert written == expected, f"row order changed: {written} != {expected}"
    assert sorted(written) != written, "fixture is already in id order; it cannot detect a loss"
