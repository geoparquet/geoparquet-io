"""Auto version mode must preserve a parquet input's GeoParquet version (gpio #587).

The --geoparquet-version help promises: "preserves the version of input files,
upgrades native geo types to 2.0, defaults to 1.1". Before the fix, convert
silently downgraded 2.0 inputs to 1.1, stripping native logical types and the
schema-level CRS.
"""

import pytest

from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from tests.conftest import get_geoparquet_version, has_native_geo_types


def _write_native(path, version_option):
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('POINT (30 10)')),
            (2, ST_GeomFromText('POINT (40 40)'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION '{version_option}')
    """)
    con.close()


@pytest.fixture
def v2_input(tmp_path):
    path = tmp_path / "v2.parquet"
    _write_native(path, "V2")
    assert get_geoparquet_version(str(path)) == "2.0.0"
    return path


@pytest.fixture
def pgo_input(tmp_path):
    path = tmp_path / "pgo.parquet"
    _write_native(path, "NONE")
    assert get_geoparquet_version(str(path)) is None
    assert has_native_geo_types(str(path))
    return path


def test_auto_preserves_v2(v2_input, tmp_path):
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(v2_input), str(out), skip_hilbert=True, geoparquet_version=None)
    assert get_geoparquet_version(str(out)) == "2.0.0"
    assert has_native_geo_types(str(out))


def test_auto_upgrades_parquet_geo_only_to_v2(pgo_input, tmp_path):
    """Documented behavior: native-geo inputs upgrade to 2.0 in auto mode."""
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(pgo_input), str(out), skip_hilbert=True, geoparquet_version=None)
    assert get_geoparquet_version(str(out)) == "2.0.0"
    assert has_native_geo_types(str(out))


def test_auto_keeps_v1_1_at_1_1(v2_input, tmp_path):
    v11 = tmp_path / "v11.parquet"
    convert_to_geoparquet(str(v2_input), str(v11), skip_hilbert=True, geoparquet_version="1.1")
    assert get_geoparquet_version(str(v11)) == "1.1.0"

    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(v11), str(out), skip_hilbert=True, geoparquet_version=None)
    assert get_geoparquet_version(str(out)) == "1.1.0"
    assert not has_native_geo_types(str(out))


def test_explicit_version_still_wins(v2_input, tmp_path):
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(v2_input), str(out), skip_hilbert=True, geoparquet_version="1.1")
    assert get_geoparquet_version(str(out)) == "1.1.0"
    assert not has_native_geo_types(str(out))


# --- Reproject auto mode must match convert's contract (todo 040) ---


def test_reproject_auto_upgrades_parquet_geo_only_to_v2(pgo_input, tmp_path):
    """Auto + native-geo-only input must write 2.0, not strip to 1.1 WKB."""
    from geoparquet_io.core.reproject import reproject

    out = tmp_path / "out.parquet"
    reproject(str(pgo_input), str(out), target_crs="EPSG:3857", geoparquet_version=None)
    assert get_geoparquet_version(str(out)) == "2.0.0"
    assert has_native_geo_types(str(out))


def test_reproject_auto_preserves_v2(v2_input, tmp_path):
    from geoparquet_io.core.reproject import reproject

    out = tmp_path / "out.parquet"
    reproject(str(v2_input), str(out), target_crs="EPSG:3857", geoparquet_version=None)
    assert get_geoparquet_version(str(out)) == "2.0.0"
    assert has_native_geo_types(str(out))


def test_reproject_explicit_version_still_wins(pgo_input, tmp_path):
    from geoparquet_io.core.reproject import reproject

    out = tmp_path / "out.parquet"
    reproject(str(pgo_input), str(out), target_crs="EPSG:3857", geoparquet_version="1.1")
    assert get_geoparquet_version(str(out)) == "1.1.0"
    assert not has_native_geo_types(str(out))


# --- 2.x normalization must agree across detection paths (todo 047-C2) ---


def test_streaming_metadata_path_flattens_future_2x_to_2_0():
    """extract_version_from_metadata must agree with the file-based twin
    (resolve_geoparquet_version_from_file), which flattens any 2.x to "2.0"."""
    import json

    from geoparquet_io.core.streaming import extract_version_from_metadata

    meta = {b"geo": json.dumps({"version": "2.1.0"}).encode()}
    assert extract_version_from_metadata(meta) == "2.0"


def test_streaming_metadata_path_known_versions_unchanged():
    import json

    from geoparquet_io.core.streaming import extract_version_from_metadata

    for declared, expected in [("1.0.0", "1.1"), ("1.1.0", "1.1"), ("2.0.0", "2.0")]:
        meta = {b"geo": json.dumps({"version": declared}).encode()}
        assert extract_version_from_metadata(meta) == expected, declared
