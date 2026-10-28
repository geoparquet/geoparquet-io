"""An M/ZM 2.0 output with non-planar edges takes ONE footer patch, not two (#1270).

A GeoParquet 2.0 write of an M geometry with non-planar edges needs two footer
edits: the ``geo`` block DuckDB omits for M geometries (#589) and the
edges/GEOGRAPHY retype (#999). Each ``patch_footer`` call is a full copy of
every byte below the footer plus an atomic replace, so doing them separately
doubles the post-write I/O on large outputs. Both edits go into one
``patch_footer`` call.

The copy passes are counted at ``parquet_footer._copy_below_footer``, the one
routine every footer patch pays its full-file copy through.
"""

from __future__ import annotations

import json
from pathlib import Path

# Keeps pyarrow materialising GEOGRAPHY columns as GeoArrow extension types, so
# `native_geo_edges` can read the edge algorithm back.
import geoarrow.pyarrow  # noqa: F401
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core import parquet_footer, write_funnels
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.parquet_footer import patch_footer
from tests.native_geo_probes import geo_block, native_geo_edges, spec_problems

_ROWS = 300


@pytest.fixture
def xym_geography(tmp_path) -> Path:
    """A native GEOGRAPHY XYM input with no ``geo`` key.

    The shape a sedonadb-style 2.0 writer produces: the spherical edges live
    only in the Parquet GEOGRAPHY logical type. Built by writing an XYM 2.0
    COPY (whose ``geo`` key DuckDB omits, #589) and retyping the column with
    gpio's own footer patch.
    """
    path = tmp_path / "xym_geography.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
              SELECT i AS id,
                     ST_GeomFromText(
                       'POINT M (' || (i % 170 - 80) || ' ' || (i % 75) || ' ' || i || ')'
                     ) AS geometry
              FROM range({_ROWS}) t(i)
            ) TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
        """)
    finally:
        con.close()
    patch_footer(str(path), geography={"geometry": "spherical"})

    # Non-vacuity: the fixture is what the issue describes -- GEOGRAPHY logical
    # type, no geo key for the repair to find.
    assert native_geo_edges(path) == ("Geography", "EdgeType.SPHERICAL")
    assert geo_block(path) is None
    return path


def _count_footer_copies(monkeypatch) -> list:
    """Record one entry per full below-the-footer copy a patch performs."""
    copies = []
    real = parquet_footer._copy_below_footer

    def spy(source, sink, footer_start):
        copies.append(footer_start)
        return real(source, sink, footer_start)

    monkeypatch.setattr(parquet_footer, "_copy_below_footer", spy)
    return copies


def test_the_mzm_repair_and_the_edges_retype_share_one_footer_patch(
    xym_geography, tmp_path, monkeypatch
):
    """The write that needs both edits must copy the file below the footer once."""
    copies = _count_footer_copies(monkeypatch)
    out = tmp_path / "out.parquet"

    convert_to_geoparquet(str(xym_geography), str(out), skip_hilbert=True, geoparquet_version="2.0")

    assert len(copies) == 1, (
        f"the M/ZM geo repair and the edges retype took {len(copies)} footer-patch "
        "copies where one patch carries both edits"
    )


def test_the_single_patch_carries_both_edits(xym_geography, tmp_path):
    """The merged patch must lose neither half: the geo block nor the GEOGRAPHY."""
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(xym_geography), str(out), skip_hilbert=True, geoparquet_version="2.0", verbose=True
    )

    block = geo_block(out)
    assert block is not None, "the M/ZM repair's geo block never reached the footer"
    assert block["version"] == "2.0.0"
    assert block["columns"]["geometry"].get("edges") == "spherical"
    assert native_geo_edges(out) == ("Geography", "EdgeType.SPHERICAL")
    assert pq.read_table(str(out)).num_rows == _ROWS
    assert spec_problems(out) == []
    assert spec_problems(out, status="warning") == []


def test_a_verbose_write_says_the_repair_is_deferred(xym_geography, tmp_path, caplog):
    """Under --verbose the deferral is named, and the merged patch still lands both edits."""
    out = tmp_path / "out.parquet"
    with caplog.at_level("DEBUG"):
        convert_to_geoparquet(
            str(xym_geography),
            str(out),
            skip_hilbert=True,
            geoparquet_version="2.0",
            verbose=True,
        )

    assert "Deferring the omitted geo metadata to the edges footer patch" in caplog.text
    assert geo_block(out)["columns"]["geometry"].get("edges") == "spherical"
    assert native_geo_edges(out) == ("Geography", "EdgeType.SPHERICAL")


def test_an_edges_free_mzm_write_still_patches_once(tmp_path, monkeypatch):
    """Control: with no edges to restore, the repair alone still patches once."""
    source = tmp_path / "xym.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
              SELECT i AS id,
                     ST_GeomFromText('POINT M (' || (i % 170 - 80) || ' ' || (i % 75) || ' ' || i || ')') AS geometry
              FROM range({_ROWS}) t(i)
            ) TO {sql_path(str(source))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
        """)
    finally:
        con.close()
    copies = _count_footer_copies(monkeypatch)
    out = tmp_path / "out.parquet"

    convert_to_geoparquet(str(source), str(out), skip_hilbert=True, geoparquet_version="2.0")

    assert len(copies) == 1
    assert geo_block(out) is not None


def test_an_unpatchable_merged_footer_keeps_both_edits_in_the_block(
    xym_geography, tmp_path, monkeypatch, caplog
):
    """The rewrite fallback must carry the merged block: geo repair AND edges.

    The GEOGRAPHY retype is lost, as it always is when the footer cannot be
    patched, and the warning says so.
    """
    from geoparquet_io.core.parquet_footer import FooterPatchUnsupported

    def refuse(*args, **kwargs):
        raise FooterPatchUnsupported("refused for the test")

    monkeypatch.setattr(write_funnels, "patch_footer", refuse)
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(xym_geography), str(out), skip_hilbert=True, geoparquet_version="2.0")

    block = geo_block(out)
    assert block is not None, "the fallback rewrite lost the M/ZM repair's geo block"
    assert block["columns"]["geometry"].get("edges") == "spherical"
    assert "stay planar in the Parquet schema" in caplog.text


def test_a_raced_merged_patch_propagates_without_a_rewrite(xym_geography, tmp_path, monkeypatch):
    """FooterPatchRaced keeps its meaning through the merge: no fallback rewrite.

    ``convert`` wraps every failure in a ``GeoParquetError``, so the race is
    asserted as that error's cause; the load-bearing check is that the rewrite
    fallback was never taken.
    """
    from geoparquet_io.core.exceptions import GeoParquetError
    from geoparquet_io.core.parquet_footer import FooterPatchRaced

    def race(*args, **kwargs):
        raise FooterPatchRaced("changed for the test")

    monkeypatch.setattr(write_funnels, "patch_footer", race)
    monkeypatch.setattr(
        write_funnels,
        "_rewrite_file_with_geo_metadata",
        lambda *a, **k: pytest.fail("a raced patch must not fall back to a rewrite"),
    )

    with pytest.raises((FooterPatchRaced, GeoParquetError)) as excinfo:
        convert_to_geoparquet(
            str(xym_geography),
            str(tmp_path / "out.parquet"),
            skip_hilbert=True,
            geoparquet_version="2.0",
        )
    raced = excinfo.value
    assert isinstance(raced, FooterPatchRaced) or isinstance(raced.__cause__, FooterPatchRaced)


def test_a_pending_block_whose_edges_already_match_is_still_written(tmp_path):
    """The deferred repair block reaches the footer even when edges change nothing."""
    path = tmp_path / "v2.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            "COPY (SELECT 1 AS id, ST_GeomFromText('LINESTRING (0 0, 10 10)') AS geometry) "
            f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"
        )
    finally:
        con.close()
    pending = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["LineString"], "edges": "spherical"}
        },
    }

    write_funnels.apply_nonplanar_edges(
        {"geometry": "spherical"}, str(path), pending_geo=json.loads(json.dumps(pending))
    )

    block = geo_block(path)
    # The geodesic bbox rides along on the same patch (#1272); the assertion
    # here is that the deferred block reached the footer at all.
    block["columns"]["geometry"].pop("bbox", None)
    assert block == pending
    assert native_geo_edges(path) == ("Geography", "EdgeType.SPHERICAL")
