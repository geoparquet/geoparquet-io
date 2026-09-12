"""A file that states two different CRSs must not be resolved silently.

A GeoParquet 2.0 file says what its coordinates are in twice -- the ``geo``
block's ``columns.<name>.crs`` and the Parquet ``GEOMETRY``/``GEOGRAPHY``
logical type. ``crs_utils.extract_crs_from_parquet`` prefers the first and falls
back to the second, and since #993 every rewrite answers the output's CRS
through it (``parquet_writer.resolve_input_crs``).

When the two sources *disagree*, that preference is load-bearing in a way it was
never asked to be. The input is self-contradictory and ``gpio check spec`` says
so; the output is written with the winner's answer in **both** places and comes
back clean. The coordinates never moved, so a loud, detectable inconsistency
becomes a silent, clean-looking assertion -- gpio launders the contradiction
rather than reporting it. Picking a winner is defensible; doing it with no
signal is not, and the repo's rule (#883) is that an overridden metadata key
gets named in a warning.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1004
Refs: https://github.com/geoparquet/geoparquet-io/issues/993
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.crs_utils import extract_crs_from_parquet, reset_crs_disagreement_warnings
from tests.native_geo_probes import (
    conus_wkb,
    geo_block_crs_id,
    logical_crs_id,
    projjson,
    spec_problems,
    write_native_geo_only,
)

EPSG_5070 = {"authority": "EPSG", "code": 5070}
EPSG_3857 = {"authority": "EPSG", "code": 3857}


def _with_geo_block(path: Path, crs_epsg: int | None) -> Path:
    """Add a ``geo`` block to a native-geo file, naming ``crs_epsg`` (or no CRS)."""
    table = pq.read_table(str(path))
    column: dict = {"encoding": "WKB", "geometry_types": ["Polygon"]}
    if crs_epsg is not None:
        column["crs"] = json.loads(projjson(crs_epsg))
    block = {"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": column}}
    metadata = dict(table.schema.metadata or {})
    metadata[b"geo"] = json.dumps(block).encode("utf-8")
    pq.write_table(table.replace_schema_metadata(metadata), str(path), compression="zstd")
    return path


@pytest.fixture(scope="module")
def _native_rows():
    return conus_wkb("ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)")


def _native_file(tmp_path: Path, name: str, logical_epsg: int, rows) -> Path:
    import geoarrow.pyarrow as ga

    return write_native_geo_only(
        tmp_path / name, rows, {"geometry": (2, ga.wkb().with_crs(projjson(logical_epsg)))}
    )


@pytest.fixture
def disagreeing(tmp_path, _native_rows) -> Path:
    """``geo`` block says EPSG:3857; the Parquet logical type says EPSG:5070."""
    return _with_geo_block(_native_file(tmp_path, "disagree.parquet", 5070, _native_rows), 3857)


@pytest.fixture
def agreeing(tmp_path, _native_rows) -> Path:
    """Both sources say EPSG:5070."""
    return _with_geo_block(_native_file(tmp_path, "agree.parquet", 5070, _native_rows), 5070)


@pytest.fixture
def logical_type_only(tmp_path, _native_rows) -> Path:
    """Only the Parquet logical type answers -- there is no ``geo`` block."""
    return _native_file(tmp_path, "pgo.parquet", 5070, _native_rows)


@pytest.fixture
def geo_block_only(tmp_path, _native_rows) -> Path:
    """Only the ``geo`` block answers; the logical type declares no CRS."""
    import geoarrow.pyarrow as ga

    path = write_native_geo_only(
        tmp_path / "block_only.parquet", _native_rows, {"geometry": (2, ga.wkb())}
    )
    return _with_geo_block(path, 5070)


def _disagreement_warnings(records) -> list[str]:
    return [r.message for r in records if "EPSG:3857" in r.message and "EPSG:5070" in r.message]


@pytest.fixture(autouse=True)
def _fresh_warning_cache():
    """The warning is deduped per file; every test starts from an empty cache."""
    reset_crs_disagreement_warnings()
    yield
    reset_crs_disagreement_warnings()


# ---------------------------------------------------------------------------
# Non-vacuity: the fixture really does contradict itself, and gpio can see it
# ---------------------------------------------------------------------------


def test_the_fixture_really_states_two_different_crss(disagreeing):
    assert geo_block_crs_id(disagreeing) == EPSG_3857
    assert logical_crs_id(disagreeing) == EPSG_5070
    assert any("CRS in geo metadata must match" in p for p in spec_problems(disagreeing)), (
        "`check spec` does not report the fixture as inconsistent; "
        "the warning under test would have nothing to be louder than"
    )


# ---------------------------------------------------------------------------
# The warning
# ---------------------------------------------------------------------------


def test_disagreeing_sources_warn_naming_both_and_the_winner(disagreeing, caplog):
    with caplog.at_level(logging.WARNING):
        resolved = extract_crs_from_parquet(str(disagreeing))

    assert (resolved or {}).get("id") == EPSG_3857, "the documented preference must not change"

    warnings = _disagreement_warnings(caplog.records)
    assert len(warnings) == 1, caplog.records
    message = warnings[0]
    assert "EPSG:3857" in message and "EPSG:5070" in message
    assert str(disagreeing) in message
    # Which one won has to be readable off the line, not inferred from the order
    # the two CRSs happen to appear in.
    assert "Using EPSG:3857" in message


def test_the_warning_is_emitted_once_per_file(disagreeing, caplog):
    """Several commands read the same input through this function in one run."""
    with caplog.at_level(logging.WARNING):
        extract_crs_from_parquet(str(disagreeing))
        extract_crs_from_parquet(str(disagreeing))

    assert len(_disagreement_warnings(caplog.records)) == 1


def test_two_distinct_disagreeing_files_each_warn(disagreeing, tmp_path, _native_rows, caplog):
    """The dedup key must not be so broad that a second bad file goes unreported."""
    second = _with_geo_block(_native_file(tmp_path, "second.parquet", 5070, _native_rows), 3857)

    with caplog.at_level(logging.WARNING):
        extract_crs_from_parquet(str(disagreeing))
        extract_crs_from_parquet(str(second))

    assert len(_disagreement_warnings(caplog.records)) == 2


# ---------------------------------------------------------------------------
# Silence everywhere else
# ---------------------------------------------------------------------------


def test_agreeing_sources_do_not_warn(agreeing, caplog):
    with caplog.at_level(logging.WARNING):
        resolved = extract_crs_from_parquet(str(agreeing))

    assert (resolved or {}).get("id") == EPSG_5070
    assert caplog.records == []


def test_a_logical_type_only_file_does_not_warn(logical_type_only, caplog):
    with caplog.at_level(logging.WARNING):
        resolved = extract_crs_from_parquet(str(logical_type_only))

    assert (resolved or {}).get("id") == EPSG_5070
    assert caplog.records == []


def test_a_geo_block_only_file_does_not_warn(geo_block_only, caplog):
    with caplog.at_level(logging.WARNING):
        resolved = extract_crs_from_parquet(str(geo_block_only))

    assert (resolved or {}).get("id") == EPSG_5070
    assert caplog.records == []


def test_a_crs_less_file_does_not_warn(tmp_path, _native_rows, caplog):
    """Neither source answers: nothing to disagree about, and nothing to say."""
    import geoarrow.pyarrow as ga

    path = write_native_geo_only(
        tmp_path / "none.parquet", _native_rows, {"geometry": (2, ga.wkb())}
    )

    with caplog.at_level(logging.WARNING):
        assert extract_crs_from_parquet(str(path)) is None
    assert caplog.records == []


# ---------------------------------------------------------------------------
# Two geometry columns: the comparison is about the primary one
# ---------------------------------------------------------------------------


def _two_column_file(tmp_path: Path, *, block_primary_epsg: int) -> Path:
    """``aux`` (EPSG:5070) ahead of the primary ``geometry`` (EPSG:3857) in schema order.

    Both logical types carry their CRS; the ``geo`` block describes ``aux``
    truthfully and says ``block_primary_epsg`` for ``geometry``. Passing 3857
    makes a valid file; anything else makes the *primary* column disagree while
    the non-primary one still agrees.
    """
    import geoarrow.pyarrow as ga

    rows = conus_wkb(
        "ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)",
        "ST_Transform(cell, 'EPSG:4326', 'EPSG:3857', always_xy := true)",
    )
    path = write_native_geo_only(
        tmp_path / "two_columns.parquet",
        rows,
        {
            "aux": (2, ga.wkb().with_crs(projjson(5070))),
            "geometry": (3, ga.wkb().with_crs(projjson(3857))),
        },
    )
    table = pq.read_table(str(path))
    block = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "aux": {
                "encoding": "WKB",
                "geometry_types": ["Polygon"],
                "crs": json.loads(projjson(5070)),
            },
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Polygon"],
                "crs": json.loads(projjson(block_primary_epsg)),
            },
        },
    }
    metadata = dict(table.schema.metadata or {})
    metadata[b"geo"] = json.dumps(block).encode("utf-8")
    pq.write_table(table.replace_schema_metadata(metadata), str(path), compression="zstd")
    return path


def test_a_valid_second_geometry_column_ahead_of_the_primary_does_not_warn(tmp_path, caplog):
    """Both columns agree with themselves; only the schema order is unusual.

    Comparing the block's *primary* CRS against the *first* logical type in the
    schema accuses a valid file of contradicting itself and sends the user to
    ``gpio check spec``, which finds nothing.
    """
    path = _two_column_file(tmp_path, block_primary_epsg=3857)
    assert spec_problems(path) == [], "the fixture itself must be a valid file"

    with caplog.at_level(logging.WARNING):
        resolved = extract_crs_from_parquet(str(path))

    assert (resolved or {}).get("id") == EPSG_3857
    assert caplog.records == []


def test_the_disagreement_is_judged_on_the_primary_column(tmp_path, caplog):
    """The block misdescribes the primary column while the earlier ``aux`` is fine.

    Matching the block's primary CRS against ``aux``'s logical type -- the first
    one in the schema -- would find EPSG:5070 on both sides and stay silent over
    a real disagreement.
    """
    path = _two_column_file(tmp_path, block_primary_epsg=5070)

    with caplog.at_level(logging.WARNING):
        resolved = extract_crs_from_parquet(str(path))

    assert (resolved or {}).get("id") == EPSG_5070, "the documented preference must not change"
    warnings = _disagreement_warnings(caplog.records)
    assert len(warnings) == 1, caplog.records
    assert "Using EPSG:5070" in warnings[0]
    assert "logical type says EPSG:3857" in warnings[0]


# ---------------------------------------------------------------------------
# The write path the issue was filed about
# ---------------------------------------------------------------------------


def test_a_rewrite_of_a_disagreeing_input_says_so(disagreeing, tmp_path, caplog):
    """``gpio add bbox`` writes the winner into both places; it must say which.

    This is the shape #1004 describes: the output is internally consistent and
    ``check spec`` passes it, while the coordinates are exactly the ones that
    were ambiguous on the way in.
    """
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    out = tmp_path / "bbox.parquet"
    with caplog.at_level(logging.WARNING):
        result = CliRunner().invoke(cli, ["add", "bbox", str(disagreeing), str(out)])
    assert result.exit_code == 0, result.output

    assert _disagreement_warnings(caplog.records), "the rewrite resolved the conflict in silence"
