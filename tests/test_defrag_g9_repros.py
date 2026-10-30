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


@pytest.mark.xfail(strict=True, reason="#913")
def test_913_reproject_assume_crs84_reads_geo_appended_after_writer_open(tmp_path):
    """Expected to fail with ``KeyError: b'geo'`` in ``_sanitize_null_crs_file``.

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


@pytest.mark.xfail(strict=True, reason="#982")
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
    """Expected to fail with ``AttributeError: 'list'/'str' object has no attribute 'get'``.

    ``core/add/quadkey.py`` reads ``geo_meta.get("columns")`` and
    ``columns_meta[geom_col].get("crs")`` without a shape check. Elsewhere a
    malformed block warns and is treated as absent (approved change 6).
    """
    src = write_with_geo(tmp_path / "bad_geo.parquet", points_table(), malformed)
    out = tmp_path / "out.parquet"
    result = _run("add", "quadkey", src, out)
    assert result.exit_code == 0, _explain(result)
    assert "quadkey" in pq.read_schema(str(out)).names


# ---------------------------------------------------------------------------
# #1015 -- `check bbox --fix` drops sidecar KV keys and `epoch`
# ---------------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason="#1015")
def test_1015_check_bbox_fix_keeps_sidecar_keys(tmp_path):
    """Expected to fail: before ``[collection, fiboa, geo]``, after ``[geo]``.

    ``check_fixes.fix_bbox_removal`` passes ``original_metadata=None``, so the
    bbox-removal rewrite of a 2.0 file drops every file-level key but ``geo``.
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


@pytest.fixture
def remote_parts(monkeypatch):
    """Two copies of a bbox-less 1.0 file behind a loopback S3 endpoint, as
    ``s3://bkt/parts/``, with that endpoint as the ambient S3 config."""
    from geoparquet_io.core.duckdb_utils import s3_config_scope
    from tests._fake_s3 import fake_s3

    data = (DATA / "buildings_test.parquet").read_bytes()
    for name, value in {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with (
        fake_s3({"bkt/parts/a.parquet": data, "bkt/parts/b.parquet": data}) as served,
        s3_config_scope({"s3_endpoint": served[0], "s3_use_ssl": False}),
    ):
        yield served


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


@pytest.mark.xfail(strict=True, reason="#1359")
def test_1359_convert_geojson_writes_an_empty_collection_for_zero_rows(tmp_path):
    """Expected to fail with ``Binder Error: No function matches ... 'ST_IsValid(BLOB)'``.

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


@pytest.mark.xfail(strict=True, reason="#1292")
def test_1292_table_bounds_of_a_geoarrow_wkb_column():
    """Expected to fail: ``bounds`` is ``None``, expected ``(1.0, 2.0, 1.0, 2.0)``."""
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


@pytest.mark.xfail(strict=True, reason="#1165")
def test_1165_convert_geoparquet_handles_an_infinite_coordinate(tmp_path):
    """Expected to fail: ``Error: Conversion failed: Invalid Input Error: UnsupportedOperationException:``.

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
