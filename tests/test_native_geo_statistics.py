"""Native Parquet geospatial statistics must describe the data they ship with.

Issue #721 asked which side zeroed the geospatial statistics of a native
GEOMETRY column on Windows: pyarrow writing them, or DuckDB's
``parquet_metadata()`` reading them. These tests answered it — the reader, and
only on DuckDB <= 1.5.1, which reported [0, 0, 0, 0] on Windows for a
pyarrow-written file whose statistics pyarrow read back correctly from those
same bytes. gpio's floor is now ``duckdb>=1.5.5`` (#737, taken for an unrelated
segfault), and on that floor Windows reads them correctly.

The watch is what caught it. These assertions carried a *strict* xfail on
win32, so when the floor moved they turned CI red with XPASS rather than
quietly staying green — the only signal that would have reported the bug fixed.
The marker is retired; they now hold on every platform, and a platform that
breaks them again fails outright.

Note that the bug never reproduced off Windows: on macOS, DuckDB 1.5.1 and
1.5.5 both read these statistics correctly, so a local run cannot tell the two
apart. Windows CI is the only thing that can, which is why the xfail was worth
carrying rather than a skip.

``test_duckdb_reads_a_file_it_did_not_write`` closes the one branch the pair
above leaves open: a writer and reader that are wrong *symmetrically* would also
agree. It reads a committed corpus fixture, written by neither gpio nor the
runner, so a failure there is the reader's alone.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

from geoparquet_io.core import duckdb_metadata
from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.duckdb_metadata import (
    get_aggregated_native_geo_stats,
    get_native_geo_statistics,
    get_per_row_group_native_geo_stats,
)
from geoparquet_io.core.write_funnels import write_geoparquet_table

# Far from the origin in every direction, so all-zero statistics cannot contain
# the data by accident, and a zeroed *single* bound is still caught. The
# assertions below assume plain min/max bounds; the Parquet geospatial spec also
# permits xmin > xmax for antimeridian-wrapping bounds, which these points would
# qualify for if a writer ever chose to emit them.
POINTS = [(-122.4, 37.8), (151.2, -33.9), (2.35, 48.85)]
EXPECTED = {"xmin": -122.4, "ymin": -33.9, "xmax": 151.2, "ymax": 48.85}

# A native-geo file written by the geoparquet-testing corpus, not by gpio and not
# on the runner reading it. Bounds are the file's own, verified from the fixture.
CORPUS_NATIVE_GEO = Path(__file__).parent / "data" / "geoparquet-testing" / "data" / "encodings"
CORPUS_EXPECTED = {"xmin": 30.0, "ymin": 10.0, "xmax": 40.0, "ymax": 40.0}


def _points_table():
    return pa.table(
        {
            "id": pa.array(range(len(POINTS))),
            "geometry": pa.array(
                [shapely.to_wkb(shapely.Point(x, y)) for x, y in POINTS], pa.binary()
            ),
        }
    )


def _write_via_table_writer(tmp_path):
    """`write_geoparquet_table` -> pyarrow `write_table` (the #702 path)."""
    out = tmp_path / "table_writer.parquet"
    write_geoparquet_table(
        _points_table(), str(out), geometry_column="geometry", geoparquet_version="2.0"
    )
    return str(out)


def _write_via_streaming_strategy(tmp_path):
    """The arrow-streaming write strategy -> pyarrow `ParquetWriter` (the #707 path)."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    src = tmp_path / "src.parquet"
    write_geoparquet_table(
        _points_table(), str(src), geometry_column="geometry", geoparquet_version="1.1"
    )
    out = tmp_path / "streamed.parquet"
    result = CliRunner().invoke(
        cli,
        [
            "extract",
            "geoparquet",
            str(src),
            str(out),
            "--geoparquet-version",
            "2.0",
            "--write-strategy",
            "streaming",
        ],
    )
    assert result.exit_code == 0, result.output
    return str(out)


@pytest.fixture(
    params=[_write_via_table_writer, _write_via_streaming_strategy],
    ids=["table-writer", "streaming-strategy"],
)
def native_geo_file(request, tmp_path):
    """A native-geo file at known coordinates, from each implicated pyarrow path."""
    return request.param(tmp_path)


def _contains_data(xmin, ymin, xmax, ymax) -> bool:
    return (
        xmin <= EXPECTED["xmin"]
        and ymin <= EXPECTED["ymin"]
        and xmax >= EXPECTED["xmax"]
        and ymax >= EXPECTED["ymax"]
    )


def test_all_zero_statistics_are_recognised_as_not_containing_the_data():
    """Pin the guard's own logic: [0,0,0,0] is exactly the reported failure."""
    assert not _contains_data(0.0, 0.0, 0.0, 0.0)
    assert _contains_data(EXPECTED["xmin"], EXPECTED["ymin"], EXPECTED["xmax"], EXPECTED["ymax"])
    # A single zeroed bound is caught too.
    assert not _contains_data(0.0, EXPECTED["ymin"], EXPECTED["xmax"], EXPECTED["ymax"])


def test_native_geo_file_uses_the_geometry_logical_type(native_geo_file):
    """Guard the premise: without the native type there are no statistics to check."""
    schema = str(pq.ParquetFile(native_geo_file).metadata.schema)
    assert "Geometry" in schema or "Geography" in schema


def test_native_geo_statistics_contain_the_data_via_pyarrow(native_geo_file):
    """The writer's own reader: what pyarrow believes it wrote."""
    pf = pq.ParquetFile(native_geo_file)
    index = pf.schema_arrow.names.index("geometry")

    for rg in range(pf.metadata.num_row_groups):
        column = pf.metadata.row_group(rg).column(index)
        assert column.is_geo_stats_set, f"row group {rg} has no geospatial statistics"
        stats = column.geo_statistics
        assert _contains_data(stats.xmin, stats.ymin, stats.xmax, stats.ymax), (
            f"row group {rg} statistics "
            f"[{stats.xmin}, {stats.ymin}, {stats.xmax}, {stats.ymax}] "
            f"do not contain the data {EXPECTED}"
        )


def test_native_geo_statistics_contain_the_data_via_duckdb(native_geo_file):
    """The reader gpio itself uses when it reports and validates these statistics."""
    con = get_duckdb_connection()
    try:
        rows = con.execute(
            "SELECT geo_bbox FROM parquet_metadata(?) WHERE path_in_schema = 'geometry'",
            [native_geo_file],
        ).fetchall()
    finally:
        con.close()

    assert rows, "DuckDB reported no geometry column chunk"
    for (bbox,) in rows:
        assert bbox is not None, "DuckDB reported no geospatial statistics"
        assert _contains_data(bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"]), (
            f"statistics {bbox} do not contain the data {EXPECTED}"
        )


def test_both_readers_agree_on_the_statistics(native_geo_file):
    """Whichever side is wrong, the two readers disagreeing is itself the signal."""
    pf = pq.ParquetFile(native_geo_file)
    index = pf.schema_arrow.names.index("geometry")

    con = get_duckdb_connection()
    try:
        # One row per column chunk, so order it: the comparison below is
        # positional, and parquet_metadata() promises no ordering of its own.
        rows = con.execute(
            "SELECT geo_bbox FROM parquet_metadata(?) "
            "WHERE path_in_schema = 'geometry' ORDER BY row_group_id",
            [native_geo_file],
        ).fetchall()
    finally:
        con.close()

    assert len(rows) == pf.metadata.num_row_groups, (
        f"DuckDB reported {len(rows)} geometry chunks for {pf.metadata.num_row_groups} row groups"
    )

    for rg, (bbox,) in enumerate(rows):
        stats = pf.metadata.row_group(rg).column(index).geo_statistics
        # Exact equality is the property under test, not an approximation of it:
        # both readers decode the same IEEE-754 doubles out of the same thrift
        # footer, with no arithmetic in between. Do not relax this to approx().
        assert (stats.xmin, stats.ymin, stats.xmax, stats.ymax) == (
            bbox["xmin"],
            bbox["ymin"],
            bbox["xmax"],
            bbox["ymax"],
        ), f"row group {rg}: pyarrow and DuckDB disagree"


@pytest.mark.corpus
def test_duckdb_reads_a_file_it_did_not_write():
    """Isolate the reader from the writer, on a file neither gpio nor CI wrote.

    The tests above compare pyarrow against DuckDB on files pyarrow just wrote
    on the same machine. A writer and reader wrong *symmetrically* would agree,
    which is the one reading under which the zeros would still be a write-side
    bug. This fixture is committed to the geoparquet-testing submodule and was
    produced elsewhere, so whatever DuckDB reports about it is the reader's
    doing alone.
    """
    fixture = CORPUS_NATIVE_GEO / "point-native-geometry.parquet"
    if not fixture.exists():
        pytest.skip("run: git submodule update --init")

    con = get_duckdb_connection()
    try:
        bbox = con.execute(
            "SELECT geo_bbox FROM parquet_metadata(?) WHERE path_in_schema = 'geometry'",
            [str(fixture)],
        ).fetchone()
    finally:
        con.close()

    assert bbox is not None, "DuckDB reported no geometry column chunk"
    bbox = bbox[0]
    assert bbox is not None, "DuckDB reported no geospatial statistics"
    assert (bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"]) == (
        CORPUS_EXPECTED["xmin"],
        CORPUS_EXPECTED["ymin"],
        CORPUS_EXPECTED["xmax"],
        CORPUS_EXPECTED["ymax"],
    ), f"DuckDB reports {bbox} for a fixture whose bounds are {CORPUS_EXPECTED}"


# ---------------------------------------------------------------------------
# The whole file's statistics, not whichever row group came first (#770).
# ---------------------------------------------------------------------------

MULTI_RG_EXPECTED = {"xmin": -122.4, "ymin": -33.9, "xmax": 151.2, "ymax": 48.85}


@pytest.fixture
def multi_row_group_file(tmp_path):
    """One native-geo row group per point, so no single one holds the extent."""
    out = tmp_path / "multi_rg.parquet"
    write_geoparquet_table(
        _points_table(),
        str(out),
        geometry_column="geometry",
        geoparquet_version="2.0",
        row_group_rows=1,
    )
    assert pq.ParquetFile(str(out)).metadata.num_row_groups == len(POINTS)
    return str(out)


def test_aggregated_statistics_span_every_row_group(multi_row_group_file):
    """The aggregate is the union of the chunks, not whichever one comes first."""
    per_rg = get_per_row_group_native_geo_stats(multi_row_group_file, "geometry")
    assert len(per_rg) == len(POINTS)
    assert [row["row_group_id"] for row in per_rg] == list(range(len(POINTS)))

    agg = get_aggregated_native_geo_stats(multi_row_group_file, "geometry")
    assert agg["bbox"][:4] == pytest.approx(
        [
            MULTI_RG_EXPECTED["xmin"],
            MULTI_RG_EXPECTED["ymin"],
            MULTI_RG_EXPECTED["xmax"],
            MULTI_RG_EXPECTED["ymax"],
        ]
    )


def _native_geo_checks(path, **kwargs):
    from geoparquet_io.core.validate import validate_geoparquet

    result = validate_geoparquet(path, validate_data=True, sample_size=0, **kwargs)
    return {c.name: c for c in result.checks if c.name.startswith("native_geo_stats")}


def test_validator_judges_data_against_the_whole_files_statistics(multi_row_group_file):
    """Every geometry is inside the file's statistics; only the first chunk's
    bbox made them look outside (#770)."""
    from geoparquet_io.core.validate import CheckStatus

    checks = _native_geo_checks(multi_row_group_file)
    contains = checks["native_geo_stats_contains_data_geometry"]
    assert contains.status == CheckStatus.PASSED, contains.message

    present = checks["native_geo_stats_geometry"]
    assert present.status == CheckStatus.PASSED, present.message
    assert "151.20" in present.message, (
        f"the reported bbox describes one row group, not the file: {present.message}"
    )


def test_validator_reads_its_bbox_from_the_shared_reader(native_geo_file, monkeypatch):
    """Pin the validator to the shared reader, so it cannot grow its own read."""
    from geoparquet_io.core.validate import CheckStatus

    monkeypatch.setattr(
        duckdb_metadata,
        "get_aggregated_native_geo_stats",
        lambda *args, **kwargs: {"bbox": [0.0, 0.0, 0.0, 0.0], "geometry_types": ["Point"]},
    )

    checks = _native_geo_checks(native_geo_file)
    contains = checks["native_geo_stats_contains_data_geometry"]
    assert contains.status == CheckStatus.FAILED, (
        f"the validator ignored the reader it is supposed to use: {contains.message}"
    )


# --- A read failure is not a missing column (#770) ---------------------------


def _explode(*args, **kwargs):
    raise OSError("403 Forbidden: s3://example-bucket/native-geo.parquet")


def test_an_unreadable_file_raises_instead_of_reporting_no_column(native_geo_file, monkeypatch):
    """``None`` must mean "no such column" and nothing else.

    Folding a read failure into ``None`` made every unreachable, truncated or
    permission-denied file look like a file whose geometry column was missing.
    """
    monkeypatch.setattr(duckdb_metadata, "_get_connection_for_file", _explode)

    with pytest.raises(OSError, match="403 Forbidden"):
        duckdb_metadata.get_native_geo_stats_by_row_group(native_geo_file, "geometry")

    # A column that genuinely is not there still reports None, not an exception.
    monkeypatch.undo()
    assert duckdb_metadata.get_native_geo_stats_by_row_group(native_geo_file, "nope") is None


def test_validator_reports_the_read_failure_not_a_missing_column(native_geo_file, monkeypatch):
    from geoparquet_io.core.validate import CheckStatus, _check_native_geo_statistics

    monkeypatch.setattr(duckdb_metadata, "_get_connection_for_file", _explode)

    check = _check_native_geo_statistics(native_geo_file, "geometry")

    assert "403 Forbidden" in check.message, (
        f"the real read failure never reached the user: {check.message}"
    )
    assert "not found" not in check.message, (
        f"an unreadable file was diagnosed as a missing column: {check.message}"
    )
    assert check.status == CheckStatus.SKIPPED


def test_the_getters_still_absorb_a_read_failure(native_geo_file, monkeypatch):
    """Their callers treat empty as "nothing to report"; that contract is unchanged."""
    monkeypatch.setattr(duckdb_metadata, "_get_connection_for_file", _explode)

    assert get_native_geo_statistics(native_geo_file, "geometry") is None
    assert get_aggregated_native_geo_stats(native_geo_file, "geometry") == {}
    assert get_per_row_group_native_geo_stats(native_geo_file, "geometry") == []


# --- One dict describes one row group (#770) ---------------------------------


def test_single_statistics_take_bbox_and_types_from_the_same_row_group(monkeypatch):
    """A bbox from one chunk beside another chunk's types matches no row group."""
    chunks = [
        # Row group 0: types recorded, bounds not.
        {
            "row_group_id": 0,
            "xmin": None,
            "ymin": None,
            "xmax": None,
            "ymax": None,
            "zmin": None,
            "zmax": None,
            "geometry_types": [1],  # Point
        },
        {
            "row_group_id": 1,
            "xmin": -1.0,
            "ymin": -2.0,
            "xmax": 3.0,
            "ymax": 4.0,
            "zmin": None,
            "zmax": None,
            "geometry_types": [3],  # Polygon
        },
    ]
    monkeypatch.setattr(
        duckdb_metadata, "get_native_geo_stats_by_row_group", lambda *a, **k: chunks
    )

    stats = get_native_geo_statistics("ignored.parquet", "geometry")

    assert stats["bbox"] == pytest.approx([-1.0, -2.0, 3.0, 4.0])
    assert stats["geometry_types"] == ["Polygon"], (
        "the types describe row group 0 while the bbox describes row group 1"
    )
