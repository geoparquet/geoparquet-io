"""G9: one strict expected failure per open defect the #1367 consolidation closes.

Each test is the repro from the issue (and from the appendix of #1367), turned
into an assertion of the *expected* behaviour. While the defect is open the test
fails, and ``xfail(strict=True)`` keyed to the issue number keeps the suite
green; the commit that fixes the defect at its owner flips the test to passing,
strict mode turns that into a failure, and that same commit removes the marker.

A repro that already passes on the base (3185269f) is a plain regression test
here, and its docstring says which pull request fixed it.

Fixtures are built in-test with pyarrow (see ``tests/_defrag_fixtures.py``).
Every test drives the real CLI or the public Python API; only the network is
mocked (#1034).
"""

from __future__ import annotations

import json
import logging
import shutil
import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests._defrag_fixtures import (
    SIDECAR_KEYS,
    footer_kv,
    geo_block,
    point_wkb,
    points_table,
    write_with_geo,
)

DATA = Path(__file__).parent / "data"
PLACES = DATA / "places_test.parquet"


def _run(*args: str):
    return CliRunner().invoke(cli, [str(a) for a in args], catch_exceptions=True)


def _explain(result) -> str:
    exc = result.exception
    return f"exit {result.exit_code}; exception {exc!r}; output:\n{result.output[-2000:]}"


# ---------------------------------------------------------------------------
# #913 -- reproject reads `geo` from the Arrow schema, not the footer
# ---------------------------------------------------------------------------


def test_913_reproject_assume_crs84_reads_geo_appended_after_writer_open(tmp_path):
    """Failed with ``KeyError: b'geo'`` in ``_sanitize_null_crs_file`` (fixed in T3a).

    The ``geo`` key is appended to the footer after ``ParquetWriter`` opened, so
    pyarrow's ``schema_arrow.metadata`` (rebuilt from ``ARROW:schema``) does not
    carry it; only the raw footer does.
    """
    src = tmp_path / "nullcrs.parquet"
    geo = geo_block(crs=None)
    table = pa.table(
        {"id": [1, 2], "geometry": pa.array([point_wkb(10.0, 20.0)] * 2, type=pa.binary())}
    )
    writer = pq.ParquetWriter(str(src), table.schema)
    writer.write_table(table)
    writer.add_key_value_metadata({"geo": json.dumps(geo)})
    writer.close()
    assert b"geo" in footer_kv(src)
    assert b"geo" not in (pq.ParquetFile(str(src)).schema_arrow.metadata or {})

    out = tmp_path / "out.parquet"
    result = _run("convert", "reproject", src, out, "--dst-crs", "EPSG:3857", "--assume-crs84")
    assert result.exit_code == 0, _explain(result)
    assert out.exists()


# ---------------------------------------------------------------------------
# #982 -- `add quadkey` reads the carried `geo` block raw
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "malformed",
    [
        pytest.param(["not", "an", "object"], id="geo-is-array"),
        pytest.param(
            {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": "WKB"}},
            id="column-entry-is-string",
        ),
    ],
)
def test_982_add_quadkey_survives_a_malformed_geo_block(tmp_path, malformed):
    """Failed with ``AttributeError: 'list'/'str' object has no attribute 'get'`` (fixed in T3a).

    ``core/add/quadkey.py`` read ``geo_meta.get("columns")`` and
    ``columns_meta[geom_col].get("crs")`` without a shape check. Elsewhere a
    malformed block warns and is treated as absent (approved change 6), and
    the quadkey CRS check now reads it through the same sanitizer.
    """
    src = write_with_geo(tmp_path / "bad_geo.parquet", points_table(), malformed)
    out = tmp_path / "out.parquet"
    result = _run("add", "quadkey", src, out)
    # The block now reaches the CRS check through the sanitizer: it warns and
    # reads as absent instead of crashing in `_validate_crs_from_geo_metadata`.
    assert "Ignoring malformed 'geo' metadata" in result.output, _explain(result)
    # What happens next is DuckDB's: its Parquet reader refuses these blocks
    # outright, as it does for every other DuckDB-read command (`sort hilbert`,
    # `add bbox` on the base), and the CLI boundary turns that into an error
    # line (#983). A raw Python exception is the defect.
    assert result.exception is None or isinstance(result.exception, SystemExit), _explain(result)
    if result.exit_code == 0:
        assert "quadkey" in pq.read_schema(str(out)).names
    else:
        assert result.output.rstrip().splitlines()[-1].startswith("Error:"), _explain(result)


# ---------------------------------------------------------------------------
# #1015 -- `check bbox --fix` drops sidecar KV keys and `epoch`
# ---------------------------------------------------------------------------


def test_1015_check_bbox_fix_keeps_sidecar_keys(tmp_path):
    """Failed with ``[geo]`` for ``[collection, fiboa, geo]`` (fixed in T2d).

    ``check_fixes.fix_bbox_removal`` passed ``original_metadata=None``, so the
    bbox-removal rewrite of a 2.0 file dropped every file-level key but ``geo``.
    It now hands the funnel the input's keys like every other rewrite; the
    covering over the dropped column goes with the column, and ``epoch`` stays.
    """
    geo = geo_block(version="2.0.0", epoch=2020.5)
    src = write_with_geo(
        tmp_path / "f1015.parquet", points_table(with_bbox=True), geo, extra_kv=SIDECAR_KEYS
    )
    out = tmp_path / "out.parquet"
    result = _run("check", "bbox", src, "--fix", "--fix-output", out)
    assert result.exit_code == 0, _explain(result)
    assert "bbox" not in pq.read_schema(str(out)).names, "the fix did not run"
    kv = footer_kv(out)
    for key, value in SIDECAR_KEYS.items():
        assert kv.get(key) == value, f"{key!r} dropped: {sorted(kv)}"
    column = json.loads(kv[b"geo"])["columns"]["geometry"]
    assert column.get("epoch") == 2020.5, column
    assert "covering" not in column, column


# ---------------------------------------------------------------------------
# #1335 -- unset --compression-level on COPY paths writes DuckDB's 3, not 15
# ---------------------------------------------------------------------------


def test_1335_unset_compression_level_is_level_15(tmp_path):
    """Unset is the documented 15, not DuckDB's 3.

    Appendix (base): unset 61,554 B; level 3 61,554 B; level 15 57,963 B, and
    unset was byte-identical to level 3.
    """
    outputs = {}
    for label, extra in (
        ("unset", ()),
        ("l3", ("--compression-level", "3")),
        ("l15", ("--compression-level", "15")),
    ):
        out = tmp_path / f"{label}.parquet"
        result = _run("convert", "geoparquet", PLACES, out, *extra)
        assert result.exit_code == 0, _explain(result)
        outputs[label] = out.read_bytes()
    assert outputs["l3"] != outputs["l15"], "levels 3 and 15 should differ on this fixture"
    assert outputs["unset"] == outputs["l15"]
    assert outputs["unset"] != outputs["l3"]


# ---------------------------------------------------------------------------
# #1243 -- a regular file whose name holds `[` is refused as partitioned input
# ---------------------------------------------------------------------------


def test_1243_sort_hilbert_accepts_a_regular_file_named_with_a_bracket(tmp_path):
    """Failed on the base with ``Partitioned input detected: .../l[c.parquet``."""
    src = tmp_path / "l[c.parquet"
    shutil.copy(PLACES, src)
    out = tmp_path / "out.parquet"
    result = _run("sort", "hilbert", src, out)
    assert result.exit_code == 0, _explain(result)
    assert pq.read_metadata(str(out)).num_rows == pq.read_metadata(str(PLACES)).num_rows


# ---------------------------------------------------------------------------
# #1075 / #1332 -- a remote glob is a dataset, not one file
# ---------------------------------------------------------------------------


def test_1332_preview_of_a_remote_glob_describes_its_first_file(remote_parts):
    """Failed on the base: the glob's totals summed both footers (``total_bytes``
    7310 against the first file's 3655)."""
    from geoparquet_io.api.ops import preview_bbox_filter

    whole_world = (-180.0, -90.0, 180.0, 90.0)
    first = preview_bbox_filter("s3://bkt/parts/a.parquet", whole_world)
    glob = preview_bbox_filter("s3://bkt/parts/*.parquet", whole_world)
    assert glob == first


def test_1075_check_fix_refuses_a_remote_glob_before_checking(remote_parts):
    """Failed on the base: the glob was checked, then handed to ``add bbox`` as one
    file, which stopped with ``Partitioned input detected: s3://bkt/parts/*.parquet``."""
    endpoint, requests = remote_parts
    result = _run(
        "--s3-endpoint", endpoint, "--s3-no-ssl",
        "check", "bbox", "s3://bkt/parts/*.parquet", "--fix", "--overwrite", "--no-backup",
    )  # fmt: skip
    assert result.exit_code == 1, _explain(result)
    assert "cannot --fix a remote glob" in result.output, _explain(result)
    assert "Partitioned input detected" not in result.output, _explain(result)
    assert "GeoParquet Metadata" not in result.output, "the glob was checked before the refusal"
    assert all(method in ("GET", "HEAD") for method, _ in requests)


# ---------------------------------------------------------------------------
# #1359 -- convert geojson on a zero-row geometry column
# ---------------------------------------------------------------------------


def test_1359_convert_geojson_writes_an_empty_collection_for_zero_rows(tmp_path):
    """Failed with ``Binder Error: No function matches ... 'ST_IsValid(BLOB)'`` (fixed in T3c:
    ``geojson_stream`` decodes a WKB column through ``geometry_detection.create_geometry_view``).

    The file is written the way the issue found it, by DuckDB itself: with no
    geometry observed DuckDB writes no ``geo`` key, so the column scans back as
    BLOB. (A pyarrow-written zero-row file *with* a ``geo`` block scans as
    GEOMETRY and already converts.)
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    src = tmp_path / "zero.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            "COPY (SELECT 1 AS id, ST_Point(0, 0) AS geometry WHERE false) "
            f"TO {sql_path(str(src))} (FORMAT parquet)"
        )
    finally:
        con.close()
    assert pq.read_metadata(str(src)).num_rows == 0
    out = tmp_path / "out.geojson"
    result = _run("convert", "geojson", src, out)
    assert result.exit_code == 0, _explain(result)
    collection = json.loads(out.read_text())
    assert collection.get("type") == "FeatureCollection"
    assert collection.get("features") == []


# ---------------------------------------------------------------------------
# #1292 -- Table.bounds is None for a GeoArrow WKB column with no geo key
# ---------------------------------------------------------------------------


def test_1292_table_bounds_of_a_geoarrow_wkb_column():
    """Was ``bounds`` is ``None``; the bounds owner reads the GeoArrow WKB column (T3b)."""
    import geoarrow.pyarrow as ga

    import geoparquet_io as gpio

    geometry = pa.ExtensionArray.from_storage(
        ga.wkb(), pa.array([point_wkb(1.0, 2.0)], pa.binary())
    )
    table = gpio.Table(pa.table({"geometry": geometry}), geometry_column="geometry")
    assert table.bounds == (1.0, 2.0, 1.0, 2.0)


# ---------------------------------------------------------------------------
# #906 -- check spec rejects NAD83 points because EPSG:4269's area of use wraps
# ---------------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason="#906")
def test_906_check_spec_accepts_nad83_points_in_its_area_of_use(tmp_path):
    """Expected to fail: ``✗ coordinates outside valid range for CRS  min_x=-70.5000 < 167.65``."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from tests.native_geo_probes import projjson

    con = get_duckdb_connection(load_spatial=True)
    try:
        rows = con.execute(
            "SELECT i, ST_AsWKB(ST_Point(-70.5 + i * 0.01, 42.0))::BLOB "
            "FROM range(10) r(i) ORDER BY i"
        ).fetchall()
    finally:
        con.close()
    table = pa.table(
        {
            "i": pa.array([r[0] for r in rows], pa.int64()),
            "geometry": pa.array([bytes(r[1]) for r in rows], pa.binary()),
        }
    )
    geo = geo_block(crs=json.loads(projjson(4269)))
    src = write_with_geo(tmp_path / "nad83_v1.parquet", table, geo)
    result = _run("check", "spec", src)
    assert "outside valid range" not in result.output, _explain(result)
    assert result.exit_code == 0, _explain(result)


# ---------------------------------------------------------------------------
# #1165 -- a non-finite coordinate kills the stats pass with a raw DuckDB error
# ---------------------------------------------------------------------------


def test_1165_convert_geoparquet_handles_an_infinite_coordinate(tmp_path):
    """Was ``Error: Conversion failed: Invalid Input Error: UnsupportedOperationException:``; now one clear refusal (T3b).

    The issue accepts either contract -- write with honest (possibly absent)
    stats, or reject up front with a clear error naming the non-finite value --
    so the test accepts either and rejects only the raw DuckDB exception.
    """
    table = pa.table(
        {
            "id": pa.array([0, 1], pa.int64()),
            "geometry": pa.array(
                [struct.pack("<BI2d", 1, 1, float("inf"), 0.0), point_wkb(1.0, 1.0)], pa.binary()
            ),
        }
    )
    src = write_with_geo(tmp_path / "inf.parquet", table, geo_block())
    out = tmp_path / "out.parquet"
    result = _run("convert", "geoparquet", src, out)
    assert "UnsupportedOperationException" not in result.output, _explain(result)
    if result.exit_code != 0:
        lowered = result.output.lower()
        assert "infinite" in lowered or "non-finite" in lowered, _explain(result)
        assert isinstance(result.exception, SystemExit), _explain(result)


# ---------------------------------------------------------------------------
# #236 -- a bbox that is not four numbers: crashing on one, and writing one
# ---------------------------------------------------------------------------


def test_236_inspect_reads_a_bbox_of_nulls_without_crashing(tmp_path):
    """Failed on the base with ``Error: unsupported format string passed to NoneType.__format__``.

    The issue's geoarrow-rs file declares ``"bbox": [null, null, null, null]``
    for a column holding an EMPTY geometry. Fixed on the integration branch
    by T3a's sanitized decoder, which warns and drops the malformed ``bbox``.
    """
    geo = geo_block(bbox=[None, None, None, None])
    src = write_with_geo(tmp_path / "nullbbox.parquet", points_table(3), geo)
    for command in (("inspect", "summary"), ("inspect", "meta")):
        result = _run(*command, src)
        assert result.exit_code == 0, _explain(result)
        assert "NoneType" not in result.output, _explain(result)


def _infinite_point_file(tmp_path) -> Path:
    table = pa.table(
        {
            "id": pa.array([0, 1], pa.int64()),
            "geometry": pa.array(
                [struct.pack("<BI2d", 1, 1, float("inf"), 0.0), point_wkb(1.0, 1.0)], pa.binary()
            ),
        }
    )
    return write_with_geo(tmp_path / "inf.parquet", table, geo_block())


def _assert_finite_bbox_or_clean_refusal(result, out: Path) -> None:
    """Either the write is refused naming the value, or its bbox is four finite numbers."""
    if result.exit_code != 0:
        assert "non-finite" in result.output.lower(), _explain(result)
        assert not out.exists(), "a refused write left an output behind"
        return
    raw = footer_kv(out)[b"geo"].decode("utf-8")
    assert "Infinity" not in raw and "NaN" not in raw, raw
    bbox = json.loads(raw)["columns"]["geometry"].get("bbox")
    assert bbox is None or all(isinstance(v, float) and abs(v) < float("inf") for v in bbox), raw


def test_236_no_write_path_declares_a_non_finite_bbox(tmp_path):
    """Was ``"bbox": [1.0, 0.0, Infinity, 1.0]`` -- not JSON; the owner refuses the geometry (T3b).

    The gpio side of #236: a degenerate value measured into the ``geo`` block
    is what made the issue's file unreadable. ``--no-repair-geometry`` keeps
    the DuckDB stats pass from dying on ``ST_MakeValid`` first (#1165); the
    Python API measures the same table on the Arrow path.
    """
    import geoparquet_io as gpio

    src = _infinite_point_file(tmp_path)
    out = tmp_path / "duckdb.parquet"
    result = _run("convert", "geoparquet", src, out, "--no-repair-geometry", "--skip-hilbert")
    _assert_finite_bbox_or_clean_refusal(result, out)

    out = tmp_path / "arrow.parquet"
    try:
        gpio.read(str(src)).write(str(out))
    except Exception as exc:  # the API's refusal is the exception itself
        assert "non-finite" in str(exc).lower(), repr(exc)
        assert not out.exists()
    else:
        raw = footer_kv(out)[b"geo"].decode("utf-8")
        assert "Infinity" not in raw, raw


# ---------------------------------------------------------------------------
# #1146 -- a trailing row group holding only NULL geometry has no bbox stats
# ---------------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason="#1146")
def test_1146_every_row_group_holds_a_located_geometry(tmp_path):
    """Expected to fail: the last row group is the 3 NULL rows alone, with no covering stats.

    ``convert`` orders located rows first and NULL/EMPTY rows last, so when
    the located count is an exact multiple of the row-group size the writer
    flushes the degenerate rows as a group of their own.
    """
    n = 4096
    geoms = [point_wkb(-10.0 + 20.0 * ((i * 7919) % n) / n, 40.0) for i in range(n)]
    table = pa.table(
        {
            "id": pa.array(range(n + 3), pa.int64()),
            "geometry": pa.array([*geoms, None, None, None], pa.binary()),
        }
    )
    src = write_with_geo(tmp_path / "points.parquet", table, geo_block())
    out = tmp_path / "out.parquet"
    result = _run("convert", "geoparquet", src, out, "--row-group-size", "2048")
    assert result.exit_code == 0, _explain(result)
    md = pq.read_metadata(str(out))
    geom_index = md.schema.names.index("geometry")
    for rg in range(md.num_row_groups):
        group = md.row_group(rg)
        stats = group.column(geom_index).statistics
        assert stats is None or stats.null_count < group.num_rows, (
            f"row group {rg} of {md.num_row_groups} holds {group.num_rows} rows, all NULL"
        )


# ---------------------------------------------------------------------------
# #1014 -- an explicit EPSG:4326 geo block silently loses to the logical type
# ---------------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason="#1014")
def test_1014_explicit_4326_block_against_a_5070_logical_type_is_not_silent(tmp_path, caplog):
    """Expected to fail: the output says EPSG:5070 and no warning names the disagreement.

    Row 3 of the issue's table: the ``geo`` block states an explicit EPSG:4326
    PROJJSON, the Parquet logical type states EPSG:5070. ``_crs_from_geo_block``
    treats the explicit 4326 as "nothing stated" and the logical type wins with
    no signal. Either the block wins (the documented preference) or the
    disagreement is warned about naming both; today neither happens.
    """
    import geoarrow.pyarrow as ga

    from geoparquet_io.core.crs_utils import reset_crs_disagreement_warnings
    from tests.native_geo_probes import (
        conus_wkb,
        geo_block_crs_id,
        logical_crs_id,
        projjson,
        write_native_geo_only,
    )

    rows = conus_wkb("ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)")
    src = write_native_geo_only(
        tmp_path / "disagree.parquet", rows, {"geometry": (2, ga.wkb().with_crs(projjson(5070)))}
    )
    table = pq.read_table(str(src))
    column = {"encoding": "WKB", "geometry_types": ["Polygon"], "crs": json.loads(projjson(4326))}
    block = {"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": column}}
    write_with_geo(src, table, block, compression="zstd")
    assert logical_crs_id(src) == {"authority": "EPSG", "code": 5070}

    reset_crs_disagreement_warnings()
    out = tmp_path / "out.parquet"
    with caplog.at_level(logging.WARNING):
        result = _run("convert", "geoparquet", src, out)
    assert result.exit_code == 0, _explain(result)
    reset_crs_disagreement_warnings()

    text = result.output + "\n".join(r.getMessage() for r in caplog.records)
    warned = "5070" in text and ("4326" in text or "CRS84" in text)
    block_won = geo_block_crs_id(out) != {"authority": "EPSG", "code": 5070}
    assert warned or block_won, _explain(result)


# ---------------------------------------------------------------------------
# #1034 -- a WFS error that echoes the URL leaks the api key into `Error:`
# ---------------------------------------------------------------------------


SECRET = "SUPERSECRETKEY123"


def test_1034_wfs_error_output_does_not_carry_the_api_key(tmp_path, monkeypatch):
    """The ``Error:`` line does not repeat ``apikey=SUPERSECRETKEY123`` (#1034).

    The capabilities request fails with a message that echoes the request URL,
    query string and all (as OWSLib and GDAL do). Every gpio error sanitizes its
    message at construction, so the key never reaches the ``Error:`` line.
    """
    service = f"https://wfs.example.com/geoserver/wfs?apikey={SECRET}"

    def _refuse(url, version="1.0.0", **_kwargs):
        raise ConnectionError(
            f"Connection refused while fetching {url}&service=WFS&request=GetCapabilities"
        )

    monkeypatch.setattr("owslib.wfs.WebFeatureService", _refuse)
    result = _run("extract", "wfs", service, "test:layer", tmp_path / "out.parquet")
    assert result.exit_code != 0, _explain(result)
    assert "Connection refused" in result.output or "connect" in result.output.lower(), _explain(
        result
    )
    assert SECRET not in result.output, _explain(result)


# ---------------------------------------------------------------------------
# #1362 -- a VARIANT column through the write path's schema probe
# ---------------------------------------------------------------------------


def test_1362_output_query_schema_survives_a_variant_column():
    """Already fixed on main by #1362 (3185269f): a plain regression test.

    On DuckDB 1.5 the ``LIMIT 0`` Arrow probe raised ``OSError: Not implemented
    Error: Unsupported Arrow type VARIANT`` and killed every write path.
    """
    # T1c moved the probe to its owner: geo_metadata.output_query_schema -> duckdb_utils.query_schema.
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, query_schema

    con = get_duckdb_connection()
    try:
        schema = query_schema(con, "SELECT 1 AS id, {'a': 1}::VARIANT AS v")
    finally:
        con.close()
    assert "id" in schema.names


# ---------------------------------------------------------------------------
# #1030 -- geometry-free aggregate and overview outputs skip the facade's
# row-group count (one row group for the whole file)
# ---------------------------------------------------------------------------

#: More rows than one default row group, each in its own H3 cell at r14 and r15,
#: so a geometry-free output has as many rows as its input.
ROWS_1030 = 60_000


def _row_group_sizes(path) -> list[int]:
    md = pq.ParquetFile(str(path)).metadata
    return [md.row_group(i).num_rows for i in range(md.num_row_groups)]


def _facade_groups(num_rows: int) -> int:
    from geoparquet_io.core.parquet_writer import DEFAULT_ROW_GROUP_ROWS

    return -(-num_rows // DEFAULT_ROW_GROUP_ROWS)


def test_1030_geometry_free_aggregate_and_overview_get_the_facade_row_groups(tmp_path):
    """Base (3185269f) wrote ``[60000]`` for both; the geometry branch ``[49152, 10848]``.

    ``grid_common`` and ``overview/run`` called ``pq.write_table`` with no
    ``row_group_size`` on the ``--out-geometry none`` branch.
    """
    from tests._defrag_fixtures import grid_points_table

    src = tmp_path / "pts.parquet"
    pq.write_table(
        grid_points_table(ROWS_1030, with_bbox=False).replace_schema_metadata(
            {b"geo": json.dumps(geo_block()).encode()}
        ),
        str(src),
    )
    agg = tmp_path / "cells.parquet"
    result = _run(
        "process", "aggregate", "h3", src, agg, "--resolution", "15", "--out-geometry", "none"
    )
    assert result.exit_code == 0, _explain(result)
    overview_dir = tmp_path / "levels"
    overview_dir.mkdir()
    result = _run("process", "overview", agg, "--levels", "14", "--output-dir", overview_dir)
    assert result.exit_code == 0, _explain(result)
    (level,) = sorted(overview_dir.glob("*.parquet"))

    for out in (agg, level):
        sizes = _row_group_sizes(out)
        assert sum(sizes) == ROWS_1030, f"{out.name}: the fixture lost its one-row-per-cell shape"
        assert len(sizes) == _facade_groups(ROWS_1030), f"{out.name}: {sizes}"


# ---------------------------------------------------------------------------
# #1098 -- `check spatial --fix` collapses a file below the input's row groups
# ---------------------------------------------------------------------------


def test_1098_check_spatial_fix_keeps_the_input_row_group_count(tmp_path):
    """Base: 12 groups of 5,000 shuffled points came back as ``[49152, 10848]``.

    ``fix_spatial_ordering`` wrote ``DEFAULT_ROW_GROUP_ROWS`` whatever the input
    had, so a file the spatial check could judge (12 groups) left ``--fix`` with
    two, below the eight it needs to judge at all.
    """
    import random

    from tests._defrag_fixtures import grid_points_table

    table = grid_points_table(60_000, with_bbox=False)
    order = list(range(table.num_rows))
    random.Random(1098).shuffle(order)
    src = tmp_path / "unsorted.parquet"
    pq.write_table(
        table.take(order).replace_schema_metadata({b"geo": json.dumps(geo_block()).encode()}),
        str(src),
        row_group_size=5_000,
    )
    out = tmp_path / "fixed.parquet"
    result = _run("check", "spatial", src, "--fix", "--fix-output", out)
    assert result.exit_code == 0, _explain(result)
    assert pq.read_table(str(out)).num_rows == 60_000
    assert len(_row_group_sizes(out)) >= 12, _row_group_sizes(out)


# ---------------------------------------------------------------------------
# #1215 -- `--row-group-size-mb` is ignored on the plain COPY path and by the
# streaming strategy
# ---------------------------------------------------------------------------


def _mb_fixture(tmp_path) -> Path:
    """60,000 rows of about 350 bytes each in one row group, as GeoParquet 2.0."""
    from tests._defrag_fixtures import grid_points_table

    table = grid_points_table(60_000, with_bbox=False)
    table = table.append_column(
        "pad", pa.array([f"{i:08d}" * 40 for i in range(table.num_rows)])
    ).replace_schema_metadata({b"geo": json.dumps(geo_block()).encode()})
    v1 = tmp_path / "v1.parquet"
    pq.write_table(table, str(v1), row_group_size=table.num_rows)
    v2 = tmp_path / "v2.parquet"
    result = _run("convert", "geoparquet", v1, v2, "--geoparquet-version", "2.0")
    assert result.exit_code == 0, _explain(result)
    return v2


def _mb_expected_groups(path, mb: float) -> int:
    from geoparquet_io.core.parquet_writer import resolve_row_group_rows_for_table

    table = pq.read_table(str(path))
    return -(-table.num_rows // resolve_row_group_rows_for_table(table, None, mb))


@pytest.mark.parametrize("path", ["convert", "sort_hilbert", "extract", "table_write_streaming"])
def test_1215_row_group_size_mb_is_applied_on_every_path(tmp_path, path):
    """Base: a 2.0 input's plain COPY wrote ``[60000]``, ``streaming`` ``[49152, 10848]``.

    ``resolve_row_group_rows`` answers ``None`` for an MB target (a byte target
    needs data to measure), and only the strategies measured it: the plain-COPY
    fast path a 2.0 input takes wrote DuckDB's own 122,880-row default, and the
    streaming strategy its own 100,000.
    """
    import geoparquet_io as gpio

    src = _mb_fixture(tmp_path)
    out = tmp_path / "out.parquet"
    if path == "table_write_streaming":
        gpio.read(str(src)).write(str(out), write_strategy="streaming", row_group_size_mb=4)
    else:
        command = {"convert": ("convert", "geoparquet"), "sort_hilbert": ("sort", "hilbert")}
        args = command.get(path, (path,))
        result = _run(*args, src, out, "--row-group-size-mb", "4")
        assert result.exit_code == 0, _explain(result)
    sizes = _row_group_sizes(out)
    assert sum(sizes) == 60_000
    assert len(sizes) == _mb_expected_groups(src, 4) > 1, sizes


# ---------------------------------------------------------------------------
# #957 -- the sanitizer's malformed-`geo` warning names no file
# ---------------------------------------------------------------------------


def test_957_malformed_geo_warning_names_each_file(tmp_path, caplog):
    """Failed with one un-sourced warning for the first file and silence for the second.

    The issue's repro, verbatim: ``extract_crs_from_parquet`` over two files
    with the same malformed block in one process. ``sanitize_geo_metadata`` took
    no ``source``, so its warn-once cache keyed on the detail alone. Every CLI
    command already names its input through ``geometry_detection`` (#945); the
    readers that sanitize rather than guard are what this pins.
    """
    from geoparquet_io.core.crs_utils import extract_crs_from_parquet
    from geoparquet_io.core.geo_metadata import reset_malformed_geo_warnings

    reset_malformed_geo_warnings()
    block = geo_block()
    block["primary_column"] = 5
    paths = [
        write_with_geo(tmp_path / f"{name}_malformed.parquet", points_table(), block)
        for name in ("a", "b")
    ]
    for path in paths:
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            extract_crs_from_parquet(str(path))
        named = [r.getMessage() for r in caplog.records if str(path) in r.getMessage()]
        assert any("'primary_column' is a number" in m for m in named), caplog.text


# ---------------------------------------------------------------------------
# #1080 -- one malformed `geometry_types` is two FAILED verdicts
# ---------------------------------------------------------------------------


def test_1080_check_spec_reports_a_string_geometry_types_once(tmp_path):
    """Failed with ``geometry_types_list`` *and* ``geometry_types_match_data``.

    ``"geometry_types": "Point"`` is one defect. The data check compared the
    string to the data as a set of its letters and FAILed ``{'Point'}`` as
    undeclared on top of the metadata verdict.
    """
    block = geo_block()
    block["columns"]["geometry"]["geometry_types"] = "Point"
    src = write_with_geo(tmp_path / "gt_string.parquet", points_table(), block)
    result = _run("check", "spec", src)
    failed = [line for line in result.output.splitlines() if "✗" in line]
    assert len(failed) == 1, _explain(result)
    assert '"geometry_types" list' in failed[0]


# ---------------------------------------------------------------------------
# #543 / #942 -- the admin spatial join (fixed by the T1d+T1e PR, #1386)
# ---------------------------------------------------------------------------


def _local_overture(tmp_path, monkeypatch, bbox_name="bbox"):
    """Overture-shaped admin boundaries on disk: AA [0,2]x[0,2] and BB [1,3]x[0,2]
    overlap on x in [1, 2]. Only the network source is replaced."""
    import duckdb

    from geoparquet_io.core import admin_datasets
    from geoparquet_io.core.duckdb_utils import quote_identifier, sql_path

    path = tmp_path / "admin.parquet"
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute(
            "COPY (SELECT 'country' AS subtype, code AS country, NULL::VARCHAR AS region, "
            "ST_GeomFromText(wkt) AS geometry, struct_pack(xmin := ST_XMin(ST_GeomFromText(wkt)), "
            "xmax := ST_XMax(ST_GeomFromText(wkt)), ymin := ST_YMin(ST_GeomFromText(wkt)), "
            f"ymax := ST_YMax(ST_GeomFromText(wkt))) AS {quote_identifier(bbox_name)} "
            "FROM (VALUES ('AA', 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), "
            "('BB', 'POLYGON((1 0, 3 0, 3 2, 1 2, 1 0))')) t(code, wkt)) "
            f"TO {sql_path(path)} (FORMAT PARQUET)"
        )
    finally:
        con.close()

    class LocalOverture(admin_datasets.OvertureAdminDataset):
        def get_bbox_column(self):
            return bbox_name

    monkeypatch.setattr(
        admin_datasets.AdminDatasetFactory,
        "create",
        classmethod(
            lambda cls, dataset_name, source_path=None, verbose=False: LocalOverture(
                source_path=str(path), verbose=verbose
            )
        ),
    )


def _three_points(tmp_path):
    """id 1 in AA only, id 2 in the AA/BB overlap, id 3 in BB only."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "geometry": pa.array([point_wkb(x, 1.0) for x in (0.5, 1.5, 2.5)], pa.binary()),
        }
    )
    return write_with_geo(tmp_path / "points.parquet", table, geo_block())


def test_543_a_feature_in_two_admin_polygons_is_counted_once(tmp_path, monkeypatch):
    """Fixed by the T1d+T1e PR (#1386): a plain regression test.

    On the base the LEFT JOIN fanned out: ``add admin-divisions`` wrote 4 rows
    for 3 inputs, and ``process aggregate admin`` counted AA 2 + BB 2.
    """
    _local_overture(tmp_path, monkeypatch)
    src = _three_points(tmp_path)
    added = tmp_path / "added.parquet"
    result = _run(
        "add", "admin-divisions", src, added, "--dataset", "overture", "--levels", "country"
    )
    assert result.exit_code == 0, _explain(result)
    assert pq.read_table(added).column("overture_country").to_pylist() == ["AA", "AA", "BB"]

    agg = tmp_path / "agg.parquet"
    result = _run(
        "process", "aggregate", "admin", src, agg, "--level", "country", "--out-geometry", "none"
    )
    assert result.exit_code == 0, _explain(result)
    counts = {r["admin_code"]: r["count"] for r in pq.read_table(agg).to_pylist()}
    assert counts == {"AA": 2, "BB": 1}


def test_942_partition_admin_joins_through_the_owner(tmp_path, monkeypatch):
    """Fixed by the T1d+T1e PR (#1386): a plain regression test.

    ``partition admin`` hand-rolled its ON clause and its admin subquery, so
    what the owner's tests cover (a bbox name that needs quoting, the
    one-match rule) did not reach it. Through the owner it quotes ``b "box"``
    and assigns exactly as ``add admin-divisions`` does. On the base the
    overlap point went to both partitions: ``country=AA [1, 2]``,
    ``country=BB [2, 3]``.
    """
    _local_overture(tmp_path, monkeypatch, bbox_name='b "box"')
    src = _three_points(tmp_path)
    out = tmp_path / "parts"
    result = _run(
        "partition", "admin", src, out, "--dataset", "overture", "--levels", "country", "--hive"
    )
    assert result.exit_code == 0, _explain(result)
    parts = {
        f.parent.name: sorted(pq.read_table(f, columns=["id"]).column("id").to_pylist())
        for f in out.rglob("*.parquet")
    }
    assert parts == {"country=AA": [1, 2], "country=BB": [3]}
