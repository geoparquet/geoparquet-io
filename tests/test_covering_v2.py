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
    from geoparquet_io.core.common import _geo_block_to_carry_on_fast_path

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
    from geoparquet_io.core.common import _geo_block_to_carry_on_fast_path

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


def test_convert_to_1_1_leaves_a_preserved_undeclared_bbox_column_undeclared(tmp_path):
    """A preserved bbox column gpio did not compute gains no covering.

    Convert passes it through untouched; nothing established that its values
    bound the geometry, so gpio does not assert that they do.
    """
    src = _write_v2_wkb(
        tmp_path / "in.parquet", with_bbox_column=True, with_covering=False, bbox_name="bounds"
    )
    out = tmp_path / "out.parquet"

    _run("convert", "geoparquet", src, out, "--geoparquet-version", "1.1")

    assert "covering" not in _geometry_column_meta(out)


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
