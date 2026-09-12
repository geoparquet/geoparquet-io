"""Rewrites must not silently drop non-planar edges semantics (gpio #588).

DuckDB has no GEOGRAPHY type, so a native-GEOGRAPHY input inevitably comes
back as GEOMETRY — but the `edges` declaration (spherical/ellipsoidal edge
interpretation) must survive in the geo metadata, or consumers will read
great-circle edges as planar.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.convert import convert_to_geoparquet
from tests.conftest import get_geo_metadata


def _make_edges_input(tmp_path, edges):
    """A 2.0 file whose geo metadata declares the given edges value.

    (Native GEOGRAPHY logical types can't be written locally — only sedonadb
    does that — so this simulates the metadata half of a geography input; the
    corpus suite covers the logical-type half.)
    """
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('LINESTRING (0 0, 10 10)')),
            (2, ST_GeomFromText('LINESTRING (-179 0, 179 5)'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()

    import geoarrow.pyarrow  # noqa: F401  (keep native types through rewrite)

    table = pq.read_table(str(path))
    meta = dict(table.schema.metadata)
    geo = json.loads(meta[b"geo"])
    geo["columns"]["geometry"]["edges"] = edges
    meta[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(meta), str(path))
    return path


@pytest.fixture
def spherical_input(tmp_path):
    return _make_edges_input(tmp_path, "spherical")


@pytest.fixture
def vincenty_input(tmp_path):
    return _make_edges_input(tmp_path, "vincenty")


def test_v2_rewrite_preserves_spherical_edges(spherical_input, tmp_path):
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(spherical_input), str(out), skip_hilbert=True, geoparquet_version="2.0"
    )
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]


def test_v1_1_rewrite_preserves_spherical_edges(spherical_input, tmp_path):
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(spherical_input), str(out), skip_hilbert=True, geoparquet_version="1.1"
    )
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]


def test_auto_rewrite_preserves_spherical_edges(spherical_input, tmp_path):
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(spherical_input), str(out), skip_hilbert=True, geoparquet_version=None
    )
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]


def test_v1_1_maps_ellipsoidal_edges_to_spherical(vincenty_input, tmp_path):
    """1.x only allows planar/spherical; ellipsoidal algorithms map to spherical (todo 039)."""
    from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(vincenty_input), str(out), skip_hilbert=True, geoparquet_version="1.1"
    )
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]

    result = validate_geoparquet(str(out), validate_data=False)
    edges_failures = [
        c for c in result.checks if "edges" in c.name and c.status == CheckStatus.FAILED
    ]
    assert not edges_failures, [f"{c.name}: {c.message}" for c in edges_failures]


def test_v2_keeps_ellipsoidal_edges_verbatim(vincenty_input, tmp_path):
    """2.0 outputs support the full edges vocabulary; keep the algorithm as-is."""
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(vincenty_input), str(out), skip_hilbert=True, geoparquet_version="2.0"
    )
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "vincenty", geo["columns"]["geometry"]


# --- Edges preservation must cover every rewrite path, not just convert (todo 037) ---


def test_extract_preserves_spherical_edges(spherical_input, tmp_path):
    from geoparquet_io.core.extract import extract

    out = tmp_path / "out.parquet"
    extract(str(spherical_input), str(out))
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]


def test_sort_hilbert_preserves_spherical_edges(spherical_input, tmp_path):
    from geoparquet_io.core.hilbert_order import hilbert_order

    out = tmp_path / "out.parquet"
    hilbert_order(str(spherical_input), str(out), geoparquet_version="2.0")
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]


def test_sort_by_column_preserves_spherical_edges(spherical_input, tmp_path):
    from geoparquet_io.core.sort_by_column import sort_by_column

    out = tmp_path / "out.parquet"
    sort_by_column(str(spherical_input), str(out), columns="id")
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]


def test_reproject_to_geographic_crs_preserves_spherical_edges(spherical_input, tmp_path):
    """A datum shift keeps great-circle semantics, so the declaration stands.

    Reprojecting to a *projected* CRS drops it instead (#601) — see
    tests/test_reproject_spherical_edges.py.
    """
    from geoparquet_io.core.reproject import reproject

    out = tmp_path / "out.parquet"
    reproject(str(spherical_input), str(out), target_crs="EPSG:4269")
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]


def test_remote_output_preserves_spherical_edges(spherical_input, tmp_path, monkeypatch):
    """Remote outputs are patched on the local temp file before upload."""
    import shutil

    import geoparquet_io.core.parquet_write as parquet_write

    uploaded = {}

    def fake_upload(local, _remote, **kwargs):
        dest = tmp_path / "uploaded.parquet"
        shutil.copy(str(local), str(dest))
        uploaded["dest"] = dest

    # Patched where ``write_parquet_with_metadata`` resolves the name, which is
    # the module that defines it.
    monkeypatch.setattr(parquet_write, "upload_if_remote", fake_upload)

    convert_to_geoparquet(
        str(spherical_input),
        "s3://fake-bucket/out.parquet",
        skip_hilbert=True,
        geoparquet_version="2.0",
    )
    assert "dest" in uploaded, "upload hook was not invoked for the remote output"
    geo = get_geo_metadata(str(uploaded["dest"]))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]


# --- Geography edges synthesis in metadata repair (todo 047-C7) ---


class TestGeographyEdgesFromLogical:
    """_ensure_v2_geo_metadata must synthesize `edges` for Geography columns."""

    def test_algorithm_extracted(self):
        from geoparquet_io.core.common import _geography_edges_from_logical

        assert (
            _geography_edges_from_logical("Geography(crs=OGC:CRS84, algorithm=vincenty)")
            == "vincenty"
        )

    def test_defaults_to_spherical_without_algorithm(self):
        from geoparquet_io.core.common import _geography_edges_from_logical

        assert _geography_edges_from_logical("Geography(crs=OGC:CRS84)") == "spherical"

    def test_geometry_logical_type_yields_none(self):
        from geoparquet_io.core.common import _geography_edges_from_logical

        assert _geography_edges_from_logical("Geometry(crs=OGC:CRS84)") is None
        assert _geography_edges_from_logical("") is None
