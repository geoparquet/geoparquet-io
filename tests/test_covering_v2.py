"""GeoParquet 2.0 outputs must keep their spatial access metadata.

GeoParquet 2.0 gets row-group bounds for free from the native Parquet
GEOMETRY/GEOGRAPHY geospatial statistics. ``covering`` is not in the 2.0 spec
text (introduced in 1.1, dropped in 2.0), but unknown fields are tolerated and
opengeospatial/geoparquet#302 proposes reinstating it, so a carried covering
stays meaningful for page-level pruning. Writes that
took the 2.0 "no metadata rewrite needed" fast path let DuckDB regenerate the
``geo`` key from scratch, which silently dropped the covering — a bbox column
that costs bytes and tells readers nothing (issue #738).

These tests pin both halves of the contract:

- a bbox column in a 2.0 output is always described by a ``covering`` entry
- a 2.0 output always carries native geospatial statistics
"""

from __future__ import annotations

import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.conftest import get_geo_metadata

BBOX_COVERING = {
    "xmin": ["bbox", "xmin"],
    "ymin": ["bbox", "ymin"],
    "xmax": ["bbox", "xmax"],
    "ymax": ["bbox", "ymax"],
}


def _points_table(count: int = 400) -> pa.Table:
    side = int(count**0.5)
    pts = [shapely.Point(x / 10.0, y / 10.0) for x in range(side) for y in range(side)]
    return pa.table(
        {
            "id": pa.array(range(len(pts))),
            "geometry": pa.array([shapely.to_wkb(p) for p in pts], pa.binary()),
        }
    )


def _bbox_struct(table: pa.Table) -> pa.Array:
    geoms = [shapely.from_wkb(v.as_py()) for v in table.column("geometry")]
    bounds = [g.bounds for g in geoms]
    return pa.StructArray.from_arrays(
        [
            pa.array([b[0] for b in bounds], pa.float64()),
            pa.array([b[1] for b in bounds], pa.float64()),
            pa.array([b[2] for b in bounds], pa.float64()),
            pa.array([b[3] for b in bounds], pa.float64()),
        ],
        names=["xmin", "ymin", "xmax", "ymax"],
    )


def _write_v2_wkb(
    path,
    with_bbox_column: bool = False,
    with_covering: bool = False,
    bbox_name: str = "bbox",
) -> str:
    """Write a WKB-encoded GeoParquet 2.0.0 file, optionally with a bbox column."""
    table = _points_table()
    col_meta: dict = {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "crs": {"id": {"authority": "OGC", "code": "CRS84"}},
    }
    if with_bbox_column:
        table = table.append_column(bbox_name, _bbox_struct(table))
    if with_covering:
        col_meta["covering"] = {"bbox": BBOX_COVERING}
    geo = {"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": col_meta}}
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, str(path))
    return str(path)


def _geometry_column_meta(path) -> dict:
    geo = get_geo_metadata(str(path))
    assert geo is not None, f"{path} has no geo metadata"
    return geo["columns"][geo["primary_column"]]


def _has_native_geo_statistics(path) -> bool:
    """True when the geometry column chunk carries Parquet geospatial statistics."""
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT geo_bbox FROM parquet_metadata(?) WHERE path_in_schema = 'geometry'",
            [str(path)],
        ).fetchall()
    finally:
        con.close()
    return bool(rows) and all(row[0] is not None for row in rows)


def _run(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


# ---------------------------------------------------------------------------
# covering is written for a bbox column the command itself adds
# ---------------------------------------------------------------------------


def test_sort_hilbert_add_bbox_writes_covering_at_v2(tmp_path):
    """#738: --add-bbox on a 2.0 input must describe the bbox column it adds."""
    src = _write_v2_wkb(tmp_path / "in.parquet")
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out, "--add-bbox")

    assert "bbox" in pq.ParquetFile(str(out)).schema_arrow.names
    assert get_geo_metadata(str(out))["version"] == "2.0.0"
    assert _geometry_column_meta(out)["covering"] == {"bbox": BBOX_COVERING}


@pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
def test_every_write_strategy_declares_the_bbox_column_at_v2(tmp_path, strategy):
    """The covering must survive whichever strategy actually writes the file."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=True)
    out = tmp_path / "out.parquet"

    _run("extract", "geoparquet", src, out, "--write-strategy", strategy)

    assert _geometry_column_meta(out)["covering"] == {"bbox": BBOX_COVERING}


# ---------------------------------------------------------------------------
# covering carried on the input survives a 2.0 -> 2.0 rewrite
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command",
    [
        ["sort", "hilbert"],
        ["sort", "quadkey"],
        ["extract", "geoparquet"],
    ],
)
def test_existing_covering_survives_v2_roundtrip(tmp_path, command):
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=True)
    out = tmp_path / "out.parquet"

    _run(*command, src, out)

    assert "bbox" in pq.ParquetFile(str(out)).schema_arrow.names
    # The bbox entry survives unchanged. Asserting the whole dict would pin a
    # command's *other* coverings out of existence: `sort quadkey` legitimately
    # adds a `quadkey` entry alongside this one.
    assert _geometry_column_meta(out)["covering"]["bbox"] == BBOX_COVERING


def test_a_conventional_bbox_column_is_declared_at_v2(tmp_path):
    """The one covering gpio derives: a struct column literally named ``bbox``.

    ``bbox``, as a struct of xmin/ymin/xmax/ymax, is the universal GeoParquet
    convention and is what every 1.0-era writer emitted before ``covering``
    existed, so a writer may vouch for it; broader names may not (#738, and
    ``test_unrelated_bbox_shaped_column_never_becomes_a_covering`` below). The
    rewrite path has always declared it — a 2.0 output used to miss out purely
    because it took the no-rewrite fast path, which is the path difference #772
    closed.
    """
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=False)
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out)

    assert _geometry_column_meta(out)["covering"]["bbox"] == BBOX_COVERING


def test_unrelated_bbox_shaped_column_never_becomes_a_covering(tmp_path):
    """A struct column that merely looks like a bbox is not declared.

    Regression test for the review finding: a ``tile_bounds`` column of zeros
    was being declared as the geometry's covering, so a reader pruning on it
    returned no rows at all for a query over the data's own extent.
    """
    src = _write_v2_wkb(
        tmp_path / "in.parquet", with_bbox_column=True, with_covering=False, bbox_name="tile_bounds"
    )
    out = tmp_path / "out.parquet"

    _run("extract", "geoparquet", src, out)

    assert "covering" not in _geometry_column_meta(out)


# ---------------------------------------------------------------------------
# spatial statistics on every 2.0 output
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("add_bbox", [True, False])
def test_v2_output_has_native_geo_statistics(tmp_path, add_bbox):
    src = _write_v2_wkb(tmp_path / "in.parquet")
    out = tmp_path / "out.parquet"

    args = ["sort", "hilbert", src, out]
    if add_bbox:
        args.append("--add-bbox")
    _run(*args)

    assert _has_native_geo_statistics(out)


def test_explicit_v2_output_has_native_geo_statistics(tmp_path):
    """A 1.1 input asked for 2.0 output gets the native types (and their stats)."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=True)
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out, "--geoparquet-version", "2.0")

    assert _has_native_geo_statistics(out)
    assert _geometry_column_meta(out)["covering"] == {"bbox": BBOX_COVERING}


# ---------------------------------------------------------------------------
# parquet-geo-only must stay metadata-free
# ---------------------------------------------------------------------------


def test_parquet_geo_only_output_gains_no_geo_metadata(tmp_path):
    """A bbox column must not talk gpio into writing a 'geo' key for pgo output."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=True)
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out, "--geoparquet-version", "parquet-geo-only")

    metadata = pq.ParquetFile(str(out)).metadata.metadata or {}
    assert b"geo" not in metadata


# ---------------------------------------------------------------------------
# auto mode must not advise a version the output already has
# ---------------------------------------------------------------------------


def test_auto_mode_on_v2_input_does_not_advise_upgrading_to_v2(tmp_path):
    """#738: the v1.1 pushdown warning fired on 2.0 output, hiding the real gap."""
    from unittest.mock import patch

    src = _write_v2_wkb(tmp_path / "in.parquet")
    out = tmp_path / "out.parquet"

    with patch("geoparquet_io.core.hilbert_order.warn") as mock_warn:
        from geoparquet_io.core.hilbert_order import hilbert_order

        hilbert_order(src, str(out), geoparquet_version=None)

    assert get_geo_metadata(str(out))["version"] == "2.0.0"
    for call in mock_warn.call_args_list:
        assert "no spatial filter pushdown" not in str(call)


# ---------------------------------------------------------------------------
# `gpio check` must not fight the covering it now writes
# ---------------------------------------------------------------------------


def _check_bbox(path) -> dict:
    from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

    return check_metadata_and_bbox(str(path), verbose=False, return_results=True)


def test_check_accepts_a_declared_bbox_covering_at_v2(tmp_path):
    """A declared covering is not a defect at 2.0, even though 2.0 does not specify it."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=True)

    result = _check_bbox(src)

    assert result["file_type"] == "geoparquet_v2"
    assert result["has_bbox_column"] is True
    assert result["passed"] is True
    assert result["needs_bbox_removal"] is False
    assert result["fix_available"] is False
    assert result["issues"] == []


def test_check_flags_an_undeclared_bbox_column_at_v2(tmp_path):
    """The #738 shape: bytes on disk that no covering points at."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=False)

    result = _check_bbox(src)

    assert result["passed"] is False
    assert result["needs_bbox_removal"] is True
    assert result["fix_available"] is True
    assert "covering" in result["issues"][0]


def test_sort_hilbert_add_bbox_output_passes_check_at_v2(tmp_path):
    """End to end: what gpio writes, gpio accepts."""
    src = _write_v2_wkb(tmp_path / "in.parquet")
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out, "--add-bbox")

    assert _check_bbox(out)["passed"] is True


# ---------------------------------------------------------------------------
# a covering must name a column that is really there
# ---------------------------------------------------------------------------


def _write_v2_with_covering_named(path, referenced_column: str, bbox_name: str = "bbox") -> str:
    """A 2.0 file with a real bbox column whose covering names ``referenced_column``."""
    table = _points_table()
    table = table.append_column(bbox_name, _bbox_struct(table))
    covering = {axis: [referenced_column, axis] for axis in ("xmin", "ymin", "xmax", "ymax")}
    geo = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": {"id": {"authority": "OGC", "code": "CRS84"}},
                "covering": {"bbox": covering},
            }
        },
    }
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), str(path))
    return str(path)


def test_check_flags_a_covering_that_names_a_missing_column(tmp_path):
    """A dangling covering is not a declaration.

    ``has_bbox_metadata`` used to mean "some structurally valid covering exists
    somewhere", so a covering pointing at a column that is not in the file made
    `check` pass and print a success line naming a *different* column (#738).
    """
    src = _write_v2_with_covering_named(tmp_path / "dangling.parquet", "ghost")

    result = CliRunner().invoke(cli, ["check", "bbox", src])

    assert "not declared" in result.output
    assert "ghost" not in result.output


def test_check_accepts_a_covering_that_names_the_real_column(tmp_path):
    src = _write_v2_with_covering_named(tmp_path / "good.parquet", "bbox")

    result = CliRunner().invoke(cli, ["check", "bbox", src])

    assert "not declared" not in result.output


def test_check_spec_validates_coverings_at_v2(tmp_path):
    """The covering checks are gated at 1.1+, not "1.1 only".

    They were skipped entirely for 2.0, so gpio validated a dangling covering
    at 1.1 and waved the identical file through at 2.0 (#738).
    """
    src = _write_v2_with_covering_named(tmp_path / "dangling.parquet", "ghost")

    result = CliRunner().invoke(cli, ["check", "spec", src])

    assert 'bbox column "ghost" is not at the schema root' in result.output


# ---------------------------------------------------------------------------
# a covering is never inferred from a column name alone
# ---------------------------------------------------------------------------


def test_non_struct_column_named_bbox_never_becomes_a_covering(tmp_path):
    """The struct-type guard is load-bearing: a covering must point at a struct.

    Exercised at 1.1, where a write still derives the covering from the output
    schema. Without the guard gpio emits a covering whose four paths resolve to
    a string column, i.e. to nothing a reader can prune on.
    """
    table = _points_table()
    table = table.append_column("bbox", pa.array(["not-a-struct"] * table.num_rows, pa.string()))
    geo = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": {"id": {"authority": "OGC", "code": "CRS84"}},
            }
        },
    }
    src = tmp_path / "in.parquet"
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), str(src))
    out = tmp_path / "out.parquet"

    _run("extract", "geoparquet", src, out, "--geoparquet-version", "1.1")

    assert "covering" not in _geometry_column_meta(out)


@pytest.mark.parametrize("bbox_name", ["tile_bounds", "parcel_extent", "mybbox"])
def test_bbox_shaped_columns_gpio_did_not_compute_stay_undeclared(tmp_path, bbox_name):
    """Names that merely end in bbox/bounds/extent are not evidence of anything.

    ``tile_bounds`` holding tile extents was being declared as the geometry's
    covering, so a reader pruning on it dropped rows that genuinely matched.
    """
    src = _write_v2_wkb(
        tmp_path / "in.parquet",
        with_bbox_column=True,
        with_covering=False,
        bbox_name=bbox_name,
    )
    out = tmp_path / "out.parquet"

    _run("extract", "geoparquet", src, out)

    assert "covering" not in _geometry_column_meta(out)


# ---------------------------------------------------------------------------
# resolution helpers: stay silent rather than guess
# ---------------------------------------------------------------------------


def test_resolve_output_version_is_silent_for_stdin():
    """A stdin stream's version is not knowable here, so nothing is claimed.

    Returning the default instead made `sort hilbert -` advise "consider
    --geoparquet-version 2.0" while it was already writing 2.0 (#738).
    """
    from geoparquet_io.core.hilbert_order import _resolve_output_version

    assert _resolve_output_version("-", None, verbose=False) is None


def test_resolve_output_version_is_silent_for_an_unreadable_input(tmp_path):
    """A failed read is not evidence of a 1.1 input."""
    from geoparquet_io.core.hilbert_order import _resolve_output_version

    assert _resolve_output_version(str(tmp_path / "nope.parquet"), None, verbose=False) is None


def test_resolve_output_version_honours_an_explicit_version():
    from geoparquet_io.core.hilbert_order import _resolve_output_version

    assert _resolve_output_version("-", "2.0", verbose=False) == "2.0"


def test_fast_path_carry_declines_a_geo_block_too_thin_to_stand_in(tmp_path):
    """Carrying a block that lacks the fields DuckDB would have written is worse
    than letting DuckDB write it, so the carry declines and the write falls back."""
    from geoparquet_io.core.write_funnels import _geo_block_to_carry_on_fast_path

    thin = {
        "geo": json.dumps(
            {
                "version": "2.0.0",
                "primary_column": "geometry",
                # covering present, but no encoding/geometry_types
                "columns": {"geometry": {"covering": {"bbox": BBOX_COVERING}}},
            }
        )
    }
    assert _geo_block_to_carry_on_fast_path(thin, "geometry", "2.0") is None


def test_fast_path_carry_declines_a_block_duckdb_would_write_itself():
    """No covering, no epoch, no orientation: DuckDB's own generated block stands."""
    from geoparquet_io.core.write_funnels import _geo_block_to_carry_on_fast_path

    plain = {
        "geo": json.dumps(
            {
                "version": "2.0.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
            }
        )
    }
    assert _geo_block_to_carry_on_fast_path(plain, "geometry", "2.0") is None


def test_a_non_bbox_covering_survives_a_write_that_adds_a_bbox_covering(tmp_path):
    """`covering` is a dict of entries, not just `bbox`.

    Assigning a fresh dict destroyed an `h3` covering the input declared, on a
    write that was only meant to add the `bbox` entry beside it.
    """
    table = _points_table()
    table = table.append_column("bbox", _bbox_struct(table))
    table = table.append_column("h3", pa.array(["8928308280fffff"] * table.num_rows, pa.string()))
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": {"id": {"authority": "OGC", "code": "CRS84"}},
                "covering": {"h3": {"column": "h3", "resolution": 9}},
            }
        },
    }
    src = tmp_path / "in.parquet"
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), str(src))
    out = tmp_path / "out.parquet"

    _run("extract", "geoparquet", src, out, "--geoparquet-version", "1.1")

    covering = _geometry_column_meta(out)["covering"]
    assert covering.get("h3") == {"column": "h3", "resolution": 9}


def test_convert_to_1_1_declares_the_bbox_column_it_computes(tmp_path):
    """Convert computes the bbox column itself, so it can vouch for the covering."""
    src = _write_v2_wkb(tmp_path / "in.parquet")
    out = tmp_path / "out.parquet"

    _run("convert", "geoparquet", src, out, "--geoparquet-version", "1.1")

    assert "bbox" in pq.ParquetFile(str(out)).schema_arrow.names
    assert _geometry_column_meta(out)["covering"]["bbox"] == BBOX_COVERING


def test_convert_to_1_1_leaves_a_carried_undeclared_bbox_column_undeclared(tmp_path):
    """A carried bbox column gpio did not compute gains no covering.

    Convert passes it through untouched; nothing established that its values
    bound the geometry, so gpio does not assert that they do. It is also not
    taken for *the* bbox column, so convert computes its own `bbox` beside it
    and declares that one -- the column it can vouch for (#1171). Before that,
    the name alone made `bounds` "the" bbox column, which both suppressed the
    computed one and left a 1.1 output with no covering at all.
    """
    src = _write_v2_wkb(
        tmp_path / "in.parquet", with_bbox_column=True, with_covering=False, bbox_name="bounds"
    )
    out = tmp_path / "out.parquet"

    _run("convert", "geoparquet", src, out, "--geoparquet-version", "1.1")

    names = pq.ParquetFile(str(out)).schema_arrow.names
    assert "bounds" in names, names
    assert _geometry_column_meta(out)["covering"]["bbox"] == BBOX_COVERING


@pytest.mark.parametrize("bbox_name", ["tile_bounds", "parcel_extent", "mybbox", "geom_bbox"])
def test_only_a_column_named_bbox_is_self_evident_at_1_1(tmp_path, bbox_name):
    """The 1.0 -> 1.1 upgrade declares a carried `bbox`, and nothing else.

    `bbox` as a struct of xmin/ymin/xmax/ymax is the universal GeoParquet
    convention and is what every 1.0-era writer emitted before `covering`
    existed, so upgrading declares it. Any other name -- including the
    `tile_bounds` that made readers prune away matching rows -- needs a caller
    that can vouch for it (#738).
    """
    src = _write_v2_wkb(
        tmp_path / "in.parquet",
        with_bbox_column=True,
        with_covering=False,
        bbox_name=bbox_name,
    )
    out = tmp_path / "out.parquet"

    _run("extract", "geoparquet", src, out, "--geoparquet-version", "1.1")

    assert "covering" not in _geometry_column_meta(out)


def test_a_carried_conventional_bbox_column_is_declared_at_1_1(tmp_path):
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=False)
    out = tmp_path / "out.parquet"

    _run("extract", "geoparquet", src, out, "--geoparquet-version", "1.1")

    assert _geometry_column_meta(out)["covering"]["bbox"] == BBOX_COVERING


# ---------------------------------------------------------------------------
# exactly one `geo` key: a carried block must not sit beside DuckDB's own
# ---------------------------------------------------------------------------
#
# DuckDB's COPY with GEOPARQUET_VERSION 'V2' and a `geo` key in KV_METADATA
# writes the supplied entry and then appends its own generated one. Only
# 'NONE' writes the supplied entry alone, and on a native GEOMETRY column it
# keeps the logical type and the geospatial statistics (#1225).


def _geo_kv_entries(path) -> list[dict]:
    """Every `geo` entry in the footer, in order. pyarrow shows only one."""
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT decode(value) FROM parquet_kv_metadata(?) WHERE decode(key) = 'geo'",
            [str(path)],
        ).fetchall()
    finally:
        con.close()
    return [json.loads(row[0]) for row in rows]


def _geometry_logical_type(path, column: str = "geometry") -> str | None:
    con = duckdb.connect()
    try:
        row = con.execute(
            "SELECT logical_type FROM parquet_schema(?) WHERE name = ?", [str(path), column]
        ).fetchone()
    finally:
        con.close()
    return row[0] if row else None


def _write_native_v2(path, srid: str) -> str:
    """A native 2.0 file, written the way DuckDB writes one, in ``srid``."""
    con = duckdb.connect()
    try:
        con.execute("LOAD spatial")
        con.execute(
            f"COPY (SELECT i AS id, ST_SetCRS(ST_Point(13.4 + i / 100.0, 52.5 + i / 100.0), "
            f"'{srid}') AS geometry FROM range(20) t(i)) "
            f"TO '{path}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
        )
    finally:
        con.close()
    return str(path)


@pytest.mark.parametrize("command", ["str", "hilbert"])
@pytest.mark.parametrize("version_args", [["--geoparquet-version", "2.0"], []], ids=["2.0", "auto"])
@pytest.mark.parametrize(
    "srid, expected_crs_id",
    [
        # The default CRS is signalled by omitting `crs` (#815).
        ("EPSG:4326", None),
        ("EPSG:3857", {"authority": "EPSG", "code": 3857}),
    ],
)
def test_sort_add_bbox_at_v2_writes_one_geo_entry(
    tmp_path, command, version_args, srid, expected_crs_id
):
    """#1225: the carried block replaces DuckDB's, it does not sit beside it."""
    src = _write_native_v2(tmp_path / "in.parquet", srid)
    out = tmp_path / "out.parquet"

    _run("sort", command, src, out, "--add-bbox", *version_args)

    entries = _geo_kv_entries(out)
    assert len(entries) == 1, f"expected one geo entry, got {len(entries)}: {entries}"
    col = entries[0]["columns"]["geometry"]
    assert entries[0]["version"] == "2.0.0"
    assert col["covering"] == {"bbox": BBOX_COVERING}
    assert (col.get("crs") or {}).get("id") == expected_crs_id
    logical = _geometry_logical_type(out)
    assert logical and logical.startswith("GeometryType"), logical
    native_crs_id = json.loads(logical[len("GeometryType(crs=") : -1])["id"]
    # The single entry agrees with the native column: the same CRS, or the
    # default spelled by omission.
    assert native_crs_id == (expected_crs_id or {"authority": "EPSG", "code": 4326})
    assert _has_native_geo_statistics(out)


def test_sort_add_bbox_at_1_1_keeps_one_geo_entry_and_wkb(tmp_path):
    src = _write_native_v2(tmp_path / "in.parquet", "EPSG:3857")
    out = tmp_path / "out.parquet"

    _run("sort", "str", src, out, "--add-bbox", "--geoparquet-version", "1.1")

    entries = _geo_kv_entries(out)
    assert len(entries) == 1, entries
    assert entries[0]["version"] == "1.1.0"
    assert entries[0]["columns"]["geometry"]["covering"] == {"bbox": BBOX_COVERING}
    assert entries[0]["columns"]["geometry"]["crs"]["id"] == {"authority": "EPSG", "code": 3857}
    assert _geometry_logical_type(out) is None, "a 1.1 output must keep a plain WKB column"


def test_plain_copy_to_writes_a_carried_block_as_the_only_geo_entry(tmp_path):
    from geoparquet_io.core.write_funnels import _plain_copy_to

    carried = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "bbox": [1.0, 2.0, 1.0, 2.0],
                "epoch": 2020.5,
            }
        },
    }
    out = tmp_path / "out.parquet"
    con = duckdb.connect()
    try:
        con.execute("LOAD spatial")
        _plain_copy_to(
            con,
            "SELECT ST_Point(1, 2) AS geometry",
            str(out),
            geoparquet_version="2.0",
            geometry_column="geometry",
            carry_geo_metadata=carried,
            extra_kv_metadata={"fiboa": '{"x": 1}'},
        )
    finally:
        con.close()

    assert _geo_kv_entries(out) == [carried]
    assert pq.read_metadata(str(out)).metadata[b"fiboa"] == b'{"x": 1}'
    assert _geometry_logical_type(out).startswith("GeometryType")
    assert _has_native_geo_statistics(out)


def test_geoparquet_copy_options_tells_duckdb_none_when_a_geo_block_is_supplied():
    from geoparquet_io.core.parquet_writer import geoparquet_copy_options

    with_geo = geoparquet_copy_options("V2", {"geo": "{}", "fiboa": "{}"})
    assert with_geo[0] == "GEOPARQUET_VERSION 'NONE'"
    assert with_geo[1].startswith("KV_METADATA")

    assert geoparquet_copy_options("V2", {"fiboa": "{}"})[0] == "GEOPARQUET_VERSION 'V2'"
    assert geoparquet_copy_options("V1", None) == ["GEOPARQUET_VERSION 'V1'"]
    assert geoparquet_copy_options("NONE", {"geo": "{}"})[0] == "GEOPARQUET_VERSION 'NONE'"


@pytest.mark.parametrize("duckdb_version", ["V1", "BOTH"])
def test_geoparquet_copy_options_refuses_a_geo_block_beside_a_v1_writer(duckdb_version):
    """'NONE' would turn a 1.x WKB column native, so there is no safe answer here."""
    from geoparquet_io.core.parquet_writer import geoparquet_copy_options

    with pytest.raises(ValueError, match="geo"):
        geoparquet_copy_options(duckdb_version, {"geo": "{}"})


@pytest.mark.parametrize(
    "command",
    [["extract", "geoparquet"], ["sort", "column"]],
    ids=["extract", "sort-column"],
)
def test_a_carried_covering_on_a_v2_input_stays_one_geo_entry(tmp_path, command):
    """#1225 reached every fast-path write that carries the input's block, not only --add-bbox."""
    src = _write_native_v2(tmp_path / "raw.parquet", "EPSG:3857")
    staged = tmp_path / "in.parquet"
    _run("sort", "str", src, staged, "--add-bbox")
    out = tmp_path / "out.parquet"

    tail = ["id"] if command == ["sort", "column"] else []
    _run(*command, staged, out, *tail)

    entries = _geo_kv_entries(out)
    assert len(entries) == 1, f"expected one geo entry, got {len(entries)}: {entries}"
    col = entries[0]["columns"]["geometry"]
    assert col["covering"] == {"bbox": BBOX_COVERING}
    assert col["crs"]["id"] == {"authority": "EPSG", "code": 3857}


def test_sort_add_bbox_at_parquet_geo_only_writes_no_geo_entry(tmp_path):
    src = _write_native_v2(tmp_path / "in.parquet", "EPSG:3857")
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out, "--add-bbox", "--geoparquet-version", "parquet-geo-only")

    assert _geo_kv_entries(out) == []
