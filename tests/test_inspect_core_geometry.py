"""`gpio inspect` head/tail/meta skip the spatial extension on DuckDB >= 2.0 (#1353).

DuckDB 2.0's core ships the GEOMETRY type and the serialize/deserialize
functions the preview and metadata paths use (ST_AsText, ST_GeomFromWKB, ...),
so those read-only paths open their connections without installing or loading
spatial: faster start-up, and no failure on hosts that cannot download
extensions. On DuckDB 1.5 nothing changes: spatial is loaded exactly as before.
"""

import struct

import duckdb
import pytest
from click.testing import CliRunner

import geoparquet_io.core.duckdb_utils as duckdb_utils
from geoparquet_io.cli.main import cli
from geoparquet_io.core.duckdb_utils import duckdb_capabilities, get_duckdb_connection
from geoparquet_io.core.inspect_utils import _preview_table_from_query, wkb_to_wkt_preview
from tests.test_duckdb_capabilities import pretend_duckdb_version

CORE_GEOMETRY = duckdb_capabilities().core_geometry

# Standard ISO WKB point (x=1, y=2).
WKB_POINT = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"

# A spatial-1.x *internal*-format POLYGON blob (type byte 0x02 first, not a WKB
# byte-order byte): header (type, properties, hash, padding), body (type,
# ring count, ring length + pad, vertex doubles).
SPATIAL_1X_INTERNAL_POLYGON = (
    struct.pack("<BBHI", 2, 0, 0, 0)
    + struct.pack("<II", 2, 1)
    + struct.pack("<II", 4, 0)
    + struct.pack("<8d", 0, 0, 1, 0, 1, 1, 0, 0)
)

requires_core_geometry = pytest.mark.skipif(
    not CORE_GEOMETRY, reason="DuckDB without core GEOMETRY (needs >= 2.0)"
)
requires_no_core_geometry = pytest.mark.skipif(
    CORE_GEOMETRY, reason="DuckDB with core GEOMETRY (needs < 2.0)"
)


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def extension_loads(monkeypatch):
    """Record every extension name `_install_and_load_extension` is asked for."""
    loaded = []
    original = duckdb_utils._install_and_load_extension

    def recording(con, name, *args, **kwargs):
        loaded.append(name)
        return original(con, name, *args, **kwargs)

    monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", recording)
    return loaded


@pytest.fixture
def spatial_unavailable(monkeypatch):
    """Sabotage the install path: any attempt to load spatial raises.

    Simulates an air-gapped host where the extension cannot be downloaded.
    """
    original = duckdb_utils._install_and_load_extension

    def refusing(con, name, *args, **kwargs):
        if name == "spatial":
            raise RuntimeError("spatial extension unavailable (sabotaged for test)")
        return original(con, name, *args, **kwargs)

    monkeypatch.setattr(duckdb_utils, "_install_and_load_extension", refusing)


# --- the version gate itself -------------------------------------------------


@pytest.mark.parametrize(
    ("version", "expected"), [("2.0.0", True), ("2.1.3", True), ("1.5.6", False)]
)
def test_core_geometry_follows_the_major_version(version, expected):
    with pretend_duckdb_version(version) as caps:
        assert caps.core_geometry is expected


def test_core_geometry_matches_installed_duckdb():
    assert CORE_GEOMETRY is (int(duckdb.__version__.split(".")[0]) >= 2)


# --- inspect paths never load spatial on DuckDB >= 2.0 ------------------------


@requires_core_geometry
@pytest.mark.fresh_duckdb
@pytest.mark.parametrize(
    "args",
    [
        ["inspect", "head"],
        ["inspect", "tail"],
        ["inspect", "meta"],
        ["inspect", "meta", "--geo-stats"],
    ],
    ids=["head", "tail", "meta", "meta-geo-stats"],
)
def test_inspect_skips_spatial_on_core_geometry(runner, places_test_file, extension_loads, args):
    result = runner.invoke(cli, [*args, places_test_file], catch_exceptions=False)

    assert result.exit_code == 0
    assert "spatial" not in extension_loads


@requires_core_geometry
@pytest.mark.fresh_duckdb
def test_inspect_head_output_intact_without_spatial(runner, places_test_file, extension_loads):
    result = runner.invoke(cli, ["inspect", "head", places_test_file, "3"], catch_exceptions=False)

    assert result.exit_code == 0
    assert "spatial" not in extension_loads
    # Geometry still renders as WKT, which needs ST_AsText from DuckDB core.
    assert "POINT" in result.output


@requires_core_geometry
@pytest.mark.fresh_duckdb
@pytest.mark.parametrize(
    "args",
    [
        ["inspect", "head"],
        ["inspect", "tail"],
        ["inspect", "meta"],
        ["inspect", "meta", "--geo-stats"],
    ],
    ids=["head", "tail", "meta", "meta-geo-stats"],
)
@pytest.mark.usefixtures("spatial_unavailable")
def test_inspect_works_with_spatial_genuinely_unavailable(runner, places_test_file, args):
    """The air-gap case: the spatial extension cannot be installed at all."""
    result = runner.invoke(cli, [*args, places_test_file], catch_exceptions=False)

    assert result.exit_code == 0


# --- DuckDB 1.5 behavior is unchanged -----------------------------------------


@requires_no_core_geometry
@pytest.mark.fresh_duckdb
def test_inspect_head_still_loads_spatial_without_core_geometry(
    runner, places_test_file, extension_loads
):
    result = runner.invoke(cli, ["inspect", "head", places_test_file], catch_exceptions=False)

    assert result.exit_code == 0
    assert "spatial" in extension_loads


# --- the on-demand fallback ----------------------------------------------------


def test_preview_query_loads_spatial_on_demand(extension_loads):
    """A function outside DuckDB's core set loads spatial and retries (any version)."""
    con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
    try:
        # ST_Area is not in DuckDB core on any supported version.
        table = _preview_table_from_query(
            con,
            "SELECT ST_Area('POLYGON((0 0, 1 0, 1 1, 0 0))'::GEOMETRY) AS area",
            has_spatial=False,
        )
        assert table.column("area")[0].as_py() == pytest.approx(0.5)
    finally:
        con.close()
    # The fallback path must actually have run: if a future DuckDB adds the
    # function to core, this test would otherwise go vacuous.
    assert "spatial" in extension_loads


def test_preview_query_with_spatial_reraises():
    """When spatial is already loaded a failure is real and propagates."""
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        with pytest.raises(duckdb.Error):
            _preview_table_from_query(con, "SELECT ST_NoSuchFunction(1) AS x", has_spatial=True)
    finally:
        con.close()


@pytest.mark.usefixtures("spatial_unavailable")
def test_preview_fallback_surfaces_the_querys_own_error_when_load_fails():
    """Air-gapped host + a file that genuinely needs a non-core function.

    The user's problem is the missing function, so the original
    CatalogException must surface -- not a "cannot download extension spatial"
    error from the retry's load. The failed load rides along as __context__.
    """
    con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
    try:
        with pytest.raises(duckdb.CatalogException) as excinfo:
            _preview_table_from_query(
                con,
                "SELECT ST_Area('POLYGON((0 0, 1 0, 1 1, 0 0))'::GEOMETRY) AS area",
                has_spatial=False,
            )
    finally:
        con.close()
    message = str(excinfo.value).lower()
    assert "st_area" in message or "geometry" in message
    assert "sabotaged" not in message
    assert excinfo.value.__context__ is not None


def test_preview_fallback_ignores_non_catalog_errors(extension_loads):
    """Only a missing-function CatalogException means "this needs spatial".

    Any other duckdb.Error (a syntax error here; an interrupt or an IO/OOM
    failure in the field) must propagate as itself, without an INSTALL/LOAD
    attempt and without re-running the whole scan.
    """
    con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
    try:
        with pytest.raises(duckdb.Error) as excinfo:
            _preview_table_from_query(con, "SELECT FROM WHERE", has_spatial=False)
    finally:
        con.close()
    assert not isinstance(excinfo.value, duckdb.CatalogException)
    assert "spatial" not in extension_loads


# --- the load_spatial=None auto sentinel ----------------------------------------


@pytest.mark.fresh_duckdb
def test_factory_resolves_auto_spatial_from_core_geometry(extension_loads):
    """load_spatial=None loads spatial exactly where DuckDB's core lacks GEOMETRY."""
    get_duckdb_connection(load_spatial=None, load_httpfs=False).close()
    assert ("spatial" in extension_loads) is (not CORE_GEOMETRY)


# --- wkb_to_wkt_preview on a no-spatial connection (DuckDB >= 2.0) ---------------


@requires_core_geometry
@pytest.mark.fresh_duckdb
def test_wkb_preview_renders_iso_wkb_without_spatial(extension_loads):
    """ISO WKB renders to WKT through DuckDB core alone, no `<GEOMETRY>` fallback."""
    assert wkb_to_wkt_preview(WKB_POINT) == "POINT (1 2)"
    assert "spatial" not in extension_loads


def test_wkb_preview_internal_format_blob_falls_back_everywhere():
    """A spatial-1.x 0x02 internal blob never renders, with or without spatial.

    The BLOB -> GEOMETRY cast reads WKB only (DuckDB 1.5 with spatial:
    "Unimplemented type for cast"; 2.0 with or without spatial: "Unsupported
    geometry type in WKB"), so the type-tag fallback is the pinned behavior on
    every lane -- skipping spatial (#1353) changed nothing here.
    """
    assert wkb_to_wkt_preview(SPATIAL_1X_INTERNAL_POLYGON) == "<GEOMETRY>"
