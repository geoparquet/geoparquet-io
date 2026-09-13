"""GeoParquet 2.0 writes of M/ZM data must not lose the geo metadata (gpio #589).

DuckDB 1.5.4's V2 writer omits the `geo` key-value metadata when geometries
have an M dimension (XY and XYZ are fine). gpio must guarantee the metadata
regardless, or the output silently degrades to parquet-geo-only.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from tests.conftest import get_geo_metadata, get_geoparquet_version, has_native_geo_types


@pytest.fixture
def zm_source(tmp_path):
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('LINESTRING ZM (0 0 100 0, 1 1 110 10)')),
            (2, ST_GeomFromText('LINESTRING ZM (5 5 50 0, 6 5 60 5)'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    return path


@pytest.fixture
def xym_source(tmp_path):
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('LINESTRING M (0 0 0, 1 1 10)'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    return path


@pytest.mark.parametrize("source", ["zm_source", "xym_source"])
def test_v2_write_of_m_data_keeps_geo_metadata(source, tmp_path, request):
    src = request.getfixturevalue(source)
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(src), str(out), skip_hilbert=True, geoparquet_version="2.0")

    assert get_geoparquet_version(str(out)) == "2.0.0", "geo metadata missing from 2.0 output"
    assert has_native_geo_types(str(out)), "native logical type lost"

    geo = get_geo_metadata(str(out))
    col = geo["columns"][geo["primary_column"]]
    assert col["encoding"] == "WKB"
    suffix = "ZM" if source == "zm_source" else "M"
    assert col["geometry_types"] == [f"LineString {suffix}"], col["geometry_types"]


def test_repaired_metadata_matches_duckdb_shape(zm_source, tmp_path):
    """The repaired geo block mirrors what DuckDB writes for XY/Z data."""
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(zm_source), str(out), skip_hilbert=True, geoparquet_version="2.0")
    kv = pq.ParquetFile(str(out)).metadata.metadata
    geo = json.loads(kv[b"geo"])
    assert geo["version"] == "2.0.0"
    assert geo["primary_column"] == "geometry"
    assert set(geo["columns"]) == {"geometry"}


def test_repair_preserves_lz4_codec_and_row_groups(zm_source, tmp_path):
    """The metadata repair rewrite must not change codec or row-group layout (todo 041)."""
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(zm_source),
        str(out),
        skip_hilbert=True,
        geoparquet_version="2.0",
        compression="LZ4",
        row_group_rows=1,
    )
    pf = pq.ParquetFile(str(out))
    assert b"geo" in pf.metadata.metadata, "geo metadata missing after repair"
    assert pf.metadata.num_row_groups == 2, "explicit row-group size not preserved"
    codecs = {
        pf.metadata.row_group(i).column(0).compression for i in range(pf.metadata.num_row_groups)
    }
    assert codecs <= {"LZ4", "LZ4_RAW"}, f"LZ4 silently rewritten: {codecs}"


def test_rewrite_keeps_row_group_structure_when_unspecified(tmp_path):
    """With row_group_rows=None the rewrite must mirror the file's own layout."""
    import pyarrow as pa

    from geoparquet_io.core.derive_geo_from_file import _rewrite_file_with_geo_metadata

    path = tmp_path / "plain.parquet"
    pq.write_table(pa.table({"a": list(range(30))}), str(path), row_group_size=10)
    assert pq.ParquetFile(str(path)).metadata.num_row_groups == 3

    _rewrite_file_with_geo_metadata(str(path), {"version": "2.0.0"}, "ZSTD", None, None)
    assert pq.ParquetFile(str(path)).metadata.num_row_groups == 3


def test_rewrite_preserves_lz4_codec_unit(tmp_path):
    """codec_map must cover the normalized 'LZ4' name callers actually pass."""
    import pyarrow as pa

    from geoparquet_io.core.derive_geo_from_file import _rewrite_file_with_geo_metadata

    path = tmp_path / "plain.parquet"
    pq.write_table(pa.table({"a": [1, 2, 3]}), str(path), compression="lz4")

    _rewrite_file_with_geo_metadata(str(path), {"version": "2.0.0"}, "LZ4", None, None)
    codec = pq.ParquetFile(str(path)).metadata.row_group(0).column(0).compression
    assert codec in ("LZ4", "LZ4_RAW"), codec


# --- Primary-column choice for multi-geometry repairs (todo 047-C6) ---


def test_repair_uses_real_primary_column_not_alphabetical(tmp_path):
    """With two geometry columns, the repair must honor the caller's primary
    column instead of picking the alphabetically-first one."""
    from geoparquet_io.core.derive_geo_from_file import _ensure_v2_geo_metadata

    path = tmp_path / "two_geoms.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('LINESTRING M (0 0 0, 1 1 10)'),
                ST_GeomFromText('POINT M (5 5 1)'))
          ) t(id, a_geom, the_geom)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    # Precondition: the DuckDB M-dimension bug leaves the file without geo KV.
    assert (pq.ParquetFile(str(path)).metadata.metadata or {}).get(b"geo") is None

    _ensure_v2_geo_metadata(str(path), primary_column="the_geom")

    geo = json.loads(pq.ParquetFile(str(path)).metadata.metadata[b"geo"])
    assert set(geo["columns"]) == {"a_geom", "the_geom"}
    assert geo["primary_column"] == "the_geom"


# --- CRS must survive the repair, for every dimension and version (#785) ---
#
# The repair rebuilds the geo block from scratch, so anything it does not carry
# over is *lost*, and a missing `crs` is not a gap: the spec reads it as the
# OGC:CRS84 default. On M/ZM data in a projected CRS that silently relabelled
# UTM metres as lon/lat while the Parquet logical type still said EPSG:25830.

_PROJECTED_WKT = {
    "XY": "POLYGON ((440000 4470000, 440010 4470000, 440010 4470010, 440000 4470000))",
    "Z": "POLYGON Z ((440000 4470000 1, 440010 4470000 2, 440010 4470010 3, 440000 4470000 1))",
    "M": "POLYGON M ((440000 4470000 10, 440010 4470000 20, 440010 4470010 30, 440000 4470000 10))",
    "ZM": "POLYGON ZM ((440000 4470000 1 10, 440010 4470000 2 20, 440010 4470010 3 30, "
    "440000 4470000 1 10))",
}
_WRITE_VERSIONS = ["1.1", "1.1-geoarrow", "2.0"]


def _wkb_source(path, wkt: str, crs) -> str:
    """A minimal GeoParquet 1.1 WKB source declaring ``crs`` (omitted when None)."""
    con = get_duckdb_connection(load_spatial=True)
    table = (
        con.execute(f"SELECT 1 AS id, ST_AsWKB(ST_GeomFromText('{wkt}')) AS geometry")
        .arrow()
        .read_all()
    )
    con.close()
    col_meta: dict = {"encoding": "WKB", "geometry_types": []}
    if crs is not None:
        col_meta["crs"] = crs
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": col_meta}}
    table = table.replace_schema_metadata({"geo": json.dumps(geo)})
    pq.write_table(table, str(path))
    return str(path)


@pytest.fixture
def epsg_25830():
    from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson

    return parse_crs_string_to_projjson("EPSG:25830")


def _primary_col_meta(parquet_file: str) -> dict:
    geo = get_geo_metadata(parquet_file)
    return geo["columns"][geo["primary_column"]]


def _schema_crs(parquet_file: str):
    """The CRS the Parquet native GEOMETRY logical type declares, or None."""
    from geoparquet_io.core.duckdb_metadata import (
        parse_geometry_logical_type,
        resolve_crs_reference,
    )

    pf = pq.ParquetFile(parquet_file)
    try:
        schema = pf.metadata.schema
        for i in range(len(schema)):
            parsed = parse_geometry_logical_type(str(schema.column(i).logical_type))
            if parsed:
                return resolve_crs_reference(parquet_file, parsed.get("crs"))
    finally:
        pf.close()
    return None


@pytest.mark.parametrize("dim", list(_PROJECTED_WKT))
@pytest.mark.parametrize("version", _WRITE_VERSIONS)
def test_projected_crs_survives_every_dimension_and_version(dim, version, tmp_path, epsg_25830):
    src = _wkb_source(tmp_path / "src.parquet", _PROJECTED_WKT[dim], epsg_25830)
    out = tmp_path / f"out_{dim}_{version}.parquet"
    convert_to_geoparquet(src, str(out), skip_hilbert=True, geoparquet_version=version)

    col = _primary_col_meta(str(out))
    assert "crs" in col, f"{dim} at {version}: geo block lost `crs` (reads as OGC:CRS84)"
    assert col["crs"] is not None, f"{dim} at {version}: geo `crs` is null (unknown)"
    assert col["crs"].get("id") == {"authority": "EPSG", "code": 25830}, col["crs"]


@pytest.mark.parametrize("dim", list(_PROJECTED_WKT))
def test_v2_geo_crs_agrees_with_parquet_logical_type(dim, tmp_path, epsg_25830):
    """At 2.0 both halves of the file are authoritative; they must not disagree."""
    src = _wkb_source(tmp_path / "src.parquet", _PROJECTED_WKT[dim], epsg_25830)
    out = tmp_path / f"out_{dim}.parquet"
    convert_to_geoparquet(src, str(out), skip_hilbert=True, geoparquet_version="2.0")

    from geoparquet_io.core.validate import _crs_equals

    schema_crs = _schema_crs(str(out))
    assert schema_crs is not None, f"{dim}: native logical type carries no CRS"
    assert _crs_equals(_primary_col_meta(str(out))["crs"], schema_crs)


@pytest.mark.parametrize("dim", list(_PROJECTED_WKT))
def test_v2_projected_output_passes_crs_and_range_validation(dim, tmp_path, epsg_25830):
    from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

    src = _wkb_source(tmp_path / "src.parquet", _PROJECTED_WKT[dim], epsg_25830)
    out = tmp_path / f"out_{dim}.parquet"
    convert_to_geoparquet(src, str(out), skip_hilbert=True, geoparquet_version="2.0")

    failed = [
        f"{c.name}: {c.message}"
        for c in validate_geoparquet(str(out)).checks
        if c.status is CheckStatus.FAILED and ("crs" in c.name or "coordinate" in c.name)
    ]
    assert not failed, f"{dim}: {failed}"


@pytest.mark.parametrize("dim", list(_PROJECTED_WKT))
@pytest.mark.parametrize("version", _WRITE_VERSIONS)
def test_crs84_input_still_omits_the_crs_key(dim, version, tmp_path):
    """Absent *is* the spec spelling of OGC:CRS84 -- the repair must not invent one."""
    wkt = _PROJECTED_WKT[dim].replace("440000 4470000", "1 2").replace("440010 4470000", "3 2")
    wkt = wkt.replace("440010 4470010", "3 4")
    src = _wkb_source(tmp_path / "src.parquet", wkt, None)
    out = tmp_path / f"out_{dim}_{version}.parquet"
    convert_to_geoparquet(src, str(out), skip_hilbert=True, geoparquet_version=version)

    assert "crs" not in _primary_col_meta(str(out))


def test_unresolvable_native_crs_becomes_explicit_null_not_the_default(tmp_path):
    """A CRS reference that resolves to no PROJJSON must not read as OGC:CRS84.

    Omitting the key would *mean* the default; an explicit null says "unknown",
    which is what the file actually tells us.
    """
    from geoparquet_io.core.derive_geo_from_file import _crs_from_geo_logical

    present, crs = _crs_from_geo_logical(
        "Geometry(crs=projjson:absent_key)", str(tmp_path / "nonexistent.parquet")
    )
    assert (present, crs) == (True, None)
