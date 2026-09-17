"""Tests for `gpio pmtiles pyramid` (banded multi-level PMTiles archives)."""

import logging
import shutil
import sys

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.pmtiles_pyramid import (
    TileJoinNotFoundError,
    _band_zoom_args,
    _build_tile_join_command,
    _feature_layer_name,
    _layer_name,
    _resolve_feature_zooms,
)
from geoparquet_io.core.process.overview.levels import Band


def has_tippecanoe():
    return shutil.which("tippecanoe") is not None


def has_tile_join():
    return shutil.which("tile-join") is not None


def has_gpio():
    return shutil.which("gpio") is not None


# Skip integration tests on Windows (tippecanoe has no native Windows build)
skip_windows = pytest.mark.skipif(
    sys.platform == "win32",
    reason="tippecanoe not available on Windows",
)


class TestTileJoinNotFoundError:
    def test_error_message_content(self):
        error = TileJoinNotFoundError()
        message = str(error)
        assert "tile-join not found" in message
        assert "tippecanoe" in message
        assert "brew install tippecanoe" in message

    def test_check_tile_join_returns_bool(self):
        from geoparquet_io.core.pmtiles_pyramid import _check_tile_join

        assert isinstance(_check_tile_join(), bool)


class TestLayerNaming:
    def test_single_mode_uses_output_stem(self):
        assert _layer_name("single", "a5", 6, "buildings") == "buildings"
        assert _feature_layer_name("single", "buildings") == "buildings"

    def test_grouped_mode(self):
        assert _layer_name("grouped", "a5", 6, "buildings") == "aggregate"
        assert _layer_name("grouped", "h3", 4, "buildings") == "aggregate"
        assert _feature_layer_name("grouped", "buildings") == "features"

    def test_per_level_mode_grid(self):
        assert _layer_name("per-level", "a5", 6, "buildings") == "r6"
        assert _layer_name("per-level", "h3", 10, "x") == "r10"

    def test_per_level_mode_admin(self):
        assert _layer_name("per-level", "admin", "country", "x") == "country"
        assert _layer_name("per-level", "admin", "region", "x") == "region"

    def test_per_level_features(self):
        assert _feature_layer_name("per-level", "x") == "features"


class TestBandZoomArgs:
    def test_bounded_band_passes_through(self):
        assert _band_zoom_args(Band(5, 2, 4), base_max=10) == (2, 4)

    def test_final_band_uses_base_max(self):
        assert _band_zoom_args(Band(9, 5, None), base_max=10) == (5, 10)

    def test_final_band_without_base_max(self):
        assert _band_zoom_args(Band(9, 5, None), base_max=None) == (5, None)


class TestFeatureZoomResolution:
    def test_features_min_defaults_to_base_max_plus_one(self):
        base_max, feat_min = _resolve_feature_zooms(
            max_zoom=8, features_min_zoom=None, include_features=True
        )
        assert (base_max, feat_min) == (8, 9)

    def test_base_max_derived_from_features_min(self):
        base_max, feat_min = _resolve_feature_zooms(
            max_zoom=None, features_min_zoom=9, include_features=True
        )
        assert (base_max, feat_min) == (8, 9)

    def test_no_features_passes_max_zoom_through(self):
        assert _resolve_feature_zooms(None, None, False) == (None, None)
        assert _resolve_feature_zooms(7, None, False) == (7, None)

    def test_both_anchors_given_pass_through(self):
        assert _resolve_feature_zooms(6, 8, True) == (6, 8)

    def test_features_without_any_zoom_errors(self):
        with pytest.raises(InvalidParameterError, match="features"):
            _resolve_feature_zooms(max_zoom=None, features_min_zoom=None, include_features=True)

    def test_features_min_zoom_zero_errors(self):
        # Deriving base_max = -1 would hand tippecanoe `-z -1`.
        with pytest.raises(InvalidParameterError, match="features_min_zoom"):
            _resolve_feature_zooms(max_zoom=None, features_min_zoom=0, include_features=True)

    def test_features_min_zoom_not_above_max_zoom_errors(self):
        # Overlapping bands would double-render zooms into both bands.
        with pytest.raises(InvalidParameterError, match="features_min_zoom"):
            _resolve_feature_zooms(max_zoom=8, features_min_zoom=8, include_features=True)
        with pytest.raises(InvalidParameterError, match="features_min_zoom"):
            _resolve_feature_zooms(max_zoom=8, features_min_zoom=5, include_features=True)


class TestTileJoinCommand:
    def test_basic_command(self):
        cmd = _build_tile_join_command("out.pmtiles", ["a.pmtiles", "b.pmtiles"], name="out")
        assert cmd[0] == "tile-join"
        assert cmd[1:3] == ["-o", "out.pmtiles"]
        assert "-pk" in cmd
        assert "--name=out" in cmd
        assert cmd[-2:] == ["a.pmtiles", "b.pmtiles"]
        assert "--force" not in cmd

    def test_no_scratch_flag_because_tile_join_rejects_one(self, monkeypatch):
        """#1115: tile-join takes neither -t nor --temporary-directory.

        Passing one makes it exit 101 with "invalid option -- t", so the
        pyramid steers tippecanoe's scratch and the band-archive parent and
        leaves tile-join's own spill where upstream puts it.
        """
        monkeypatch.setenv("TMPDIR", "/data/from-env")
        cmd = _build_tile_join_command("out.pmtiles", ["a.pmtiles"], name="out")
        assert "-t" not in cmd
        assert not any(c.startswith("--temporary-directory") for c in cmd)
        assert cmd[-1:] == ["a.pmtiles"]

    def test_force_and_attribution(self):
        cmd = _build_tile_join_command(
            "out.pmtiles",
            ["a.pmtiles"],
            name="out",
            attribution="<a>me</a>",
            force=True,
        )
        assert "--force" in cmd
        assert "--attribution=<a>me</a>" in cmd


class TestParamValidation:
    def test_include_features_requires_source(self):
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        with pytest.raises(InvalidParameterError, match="features.source|features_source"):
            create_pmtiles_pyramid("in.parquet", "out.pmtiles", include_features=True, max_zoom=8)

    def test_invalid_layer_mode(self):
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        with pytest.raises(InvalidParameterError, match="layer_mode"):
            create_pmtiles_pyramid("in.parquet", "out.pmtiles", layer_mode="bogus")

    def test_dangerous_paths_rejected(self):
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        with pytest.raises(ValueError, match="dangerous character"):
            create_pmtiles_pyramid("in.parquet", "out.pmtiles | cat")


# ---------------------------------------------------------------------------
# Orchestration (fast: tippecanoe/tile-join/metadata rewrite all faked)
# ---------------------------------------------------------------------------


def _write_admin_region_aggregate(path):
    """Region-level admin aggregate: two US regions + one FR region."""
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT admin_code, admin_code AS admin_name, count, sum_area,
                   ST_Buffer(ST_Point(lon, lat), 0.5) AS geometry
            FROM (VALUES
                ('US-CA', 2, 10.0, -120.0, 37.0),
                ('US-NV', 3, 30.0, -116.0, 39.0),
                ('FR-IDF', 4, 8.0, 2.3, 48.8)
            ) AS t(admin_code, count, sum_area, lon, lat)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def _write_country_cache(path):
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT country, ST_Buffer(ST_Point(lon, lat), 2.0) AS geometry
            FROM (VALUES ('US', -118.0, 38.0), ('FR', 2.3, 48.8)) AS t(country, lon, lat)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.fixture
def fake_country_cache(tmp_path, monkeypatch):
    from geoparquet_io.core.admin_datasets import OvertureAdminDataset

    cache = tmp_path / "country_cache.parquet"
    _write_country_cache(cache)
    monkeypatch.setattr(
        OvertureAdminDataset,
        "get_source_for_level",
        lambda self, level, no_cache=False: str(cache),
    )
    return cache


@pytest.fixture
def fake_tools(monkeypatch):
    """Fake out every external tool so orchestration runs without tippecanoe."""
    from pathlib import Path as _Path

    import geoparquet_io.core.pmtiles_pyramid as pp

    calls = {"tiles": [], "tile_join": [], "metadata": []}
    monkeypatch.setattr(pp, "_check_tippecanoe", lambda: True)
    monkeypatch.setattr(pp, "_check_tile_join", lambda: True)

    def fake_tiles(input_path, output_path, **kwargs):
        calls["tiles"].append({"input": input_path, "output": output_path, **kwargs})
        _Path(output_path).write_bytes(b"fake pmtiles")

    monkeypatch.setattr(pp, "create_pmtiles_from_geoparquet", fake_tiles)
    monkeypatch.setattr(pp, "_run_tile_join", lambda cmd, verbose: calls["tile_join"].append(cmd))
    monkeypatch.setattr(
        pp,
        "_merge_pyramid_metadata",
        lambda path, pyramid: calls["metadata"].append((path, pyramid)),
    )
    return calls


# With bytes_per_cell=200000 and the 500 KB budget: 2 country cells fit at z0
# (400 KB) but 3 region cells do not (600 KB); at z1 the worst region tile
# holds the 2 US regions (400 KB) so the base fits => country [0,0], region
# [1, None]. Deterministic for the fixture's geography.
_BAND_FORCING = {"bytes_per_cell": 200000.0, "max_zoom": 6}


@pytest.mark.usefixtures("fake_country_cache")
class TestOrchestration:
    def test_admin_pyramid_bands_layers_and_metadata(self, tmp_path, fake_tools):
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        src = tmp_path / "by_region.parquet"
        out = tmp_path / "pyramid.pmtiles"
        _write_admin_region_aggregate(src)

        create_pmtiles_pyramid(str(src), str(out), layer_mode="per-level", **_BAND_FORCING)

        tiles = fake_tools["tiles"]
        assert [t["layer"] for t in tiles] == ["country", "region"]
        assert (tiles[0]["min_zoom"], tiles[0]["max_zoom"]) == (0, 0)
        assert (tiles[1]["min_zoom"], tiles[1]["max_zoom"]) == (1, 6)
        # The country band was built on the fly (no sibling existed).
        assert tiles[0]["input"].endswith("by_region_country.parquet")
        assert tiles[0]["input"] != str(tmp_path / "by_region_country.parquet")
        # The base band tiles the input itself.
        assert tiles[1]["input"] == str(src)
        # Aggregate bands are exempt from the size cap (bands were chosen to fit).
        assert all(t["no_tile_size_limit"] is True for t in tiles)

        (cmd,) = fake_tools["tile_join"]
        assert cmd[0] == "tile-join"
        assert cmd[1:3] == ["-o", str(out)]
        assert "--name=pyramid" in cmd
        assert len([arg for arg in cmd if arg.endswith(".pmtiles") and arg != str(out)]) == 2

        ((meta_path, pyramid),) = fake_tools["metadata"]
        assert meta_path == str(out)
        assert pyramid["scheme"] == "admin"
        assert [band["level"] for band in pyramid["bands"]] == ["country", "region"]
        assert pyramid["bands"][0] == {
            "level": "country",
            "layer": "country",
            "minzoom": 0,
            "maxzoom": 0,
        }
        assert pyramid["bands"][1]["maxzoom"] == 6

    def test_explicit_bands_override_the_budget(self, tmp_path, fake_tools):
        # Same fixture and the same band-forcing numbers as the test above,
        # which the probe turns into country [0,0] + region [1,...]. Stating
        # the plan has to beat that, or --bands cannot do the job it exists
        # for: making two pyramids hand over at the same zooms.
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        src = tmp_path / "by_region.parquet"
        out = tmp_path / "pyramid.pmtiles"
        _write_admin_region_aggregate(src)

        create_pmtiles_pyramid(
            str(src),
            str(out),
            layer_mode="per-level",
            bands="country:0,region:3",
            **_BAND_FORCING,
        )

        tiles = fake_tools["tiles"]
        assert [t["layer"] for t in tiles] == ["country", "region"]
        assert (tiles[0]["min_zoom"], tiles[0]["max_zoom"]) == (0, 2)
        assert (tiles[1]["min_zoom"], tiles[1]["max_zoom"]) == (3, 6)

        ((_, pyramid),) = fake_tools["metadata"]
        assert [(b["level"], b["minzoom"], b["maxzoom"]) for b in pyramid["bands"]] == [
            ("country", 0, 2),
            ("region", 3, 6),
        ]

    def test_explicit_bands_reject_an_unusable_plan(self, tmp_path, fake_tools):
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        with pytest.raises(InvalidParameterError, match="must start at z0"):
            create_pmtiles_pyramid(
                str(src),
                str(tmp_path / "out.pmtiles"),
                layer_mode="per-level",
                bands="country:1,region:3",
                **_BAND_FORCING,
            )

    def test_explicit_bands_reject_levels_rather_than_drop_it(self, tmp_path, fake_tools):
        # Both name the plan, so honouring --bands means dropping --levels.
        # Saying so beats obeying the option the caller did not mean.
        from geoparquet_io.core.exceptions import InvalidParameterError
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        with pytest.raises(InvalidParameterError, match="cannot be combined with --levels"):
            create_pmtiles_pyramid(
                str(src),
                str(tmp_path / "out.pmtiles"),
                bands="country:0,region:3",
                levels="country",
                max_zoom=6,
            )

    def test_explicit_bands_say_bytes_per_cell_went_unused(self, tmp_path, fake_tools, caplog):
        # The probe it tunes never runs. Ignoring it is right; ignoring it
        # silently would leave the caller believing it did something.
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        with caplog.at_level(logging.WARNING):
            create_pmtiles_pyramid(
                str(src),
                str(tmp_path / "out.pmtiles"),
                layer_mode="per-level",
                bands="country:0,region:3",
                **_BAND_FORCING,
            )
        assert "Ignoring --bytes-per-cell" in caplog.text

    def test_existing_overview_sibling_is_reused(self, tmp_path, fake_tools):
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid
        from geoparquet_io.core.process.overview import create_overviews

        src = tmp_path / "by_region.parquet"
        out = tmp_path / "pyramid.pmtiles"
        _write_admin_region_aggregate(src)
        # Pre-build the sibling like `gpio process overview` would.
        (sibling,) = [path for _, path in create_overviews(str(src), levels="country")]
        assert sibling == str(tmp_path / "by_region_country.parquet")

        create_pmtiles_pyramid(str(src), str(out), **_BAND_FORCING)

        assert fake_tools["tiles"][0]["input"] == sibling

    def test_stale_overview_sibling_is_rebuilt(self, tmp_path, fake_tools):
        """A sibling older than the input aggregate would bake stale data into
        the coarse bands; it must be ignored and the level rebuilt."""
        import os as _os

        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid
        from geoparquet_io.core.process.overview import create_overviews

        src = tmp_path / "by_region.parquet"
        out = tmp_path / "pyramid.pmtiles"
        _write_admin_region_aggregate(src)
        (sibling,) = [path for _, path in create_overviews(str(src), levels="country")]
        stale = src.stat().st_mtime - 100
        _os.utime(sibling, (stale, stale))

        create_pmtiles_pyramid(str(src), str(out), **_BAND_FORCING)

        rebuilt = fake_tools["tiles"][0]["input"]
        assert rebuilt != sibling
        assert rebuilt.endswith("by_region_country.parquet")

    def test_schema_mismatched_sibling_is_rebuilt(self, tmp_path, fake_tools):
        import pyarrow as pa
        import pyarrow.parquet as pq

        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        src = tmp_path / "by_region.parquet"
        out = tmp_path / "pyramid.pmtiles"
        _write_admin_region_aggregate(src)
        # A sibling from an older aggregate run with different attributes.
        sibling = tmp_path / "by_region_country.parquet"
        pq.write_table(pa.table({"admin_code": ["US"], "count": [1]}), sibling)

        create_pmtiles_pyramid(str(src), str(out), **_BAND_FORCING)

        assert fake_tools["tiles"][0]["input"] != str(sibling)

    def test_band_planning_capped_by_derived_base_max(self, tmp_path, fake_tools):
        """--include-features --features-min-zoom N without --max-zoom must plan
        bands against the derived base max (N-1), not the full probe range --
        otherwise the base band can invert (minzoom > maxzoom) and intermediate
        bands overlap the features band."""
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        src = tmp_path / "by_region.parquet"
        features = tmp_path / "features.parquet"
        out = tmp_path / "pyramid.pmtiles"
        _write_admin_region_aggregate(src)
        features.write_bytes(b"")  # never read: tiling is faked

        create_pmtiles_pyramid(
            str(src),
            str(out),
            bytes_per_cell=1e9,  # nothing fits: coarse band stretches to the cap
            include_features=True,
            features_source=str(features),
            features_min_zoom=3,
        )

        tiles = fake_tools["tiles"]
        assert [t["layer"] for t in tiles] == ["aggregate", "aggregate", "features"]
        # Derived base max is 2: the country band ends at 1, the base region
        # band gets exactly zoom 2, and the features band starts at 3.
        assert (tiles[0]["min_zoom"], tiles[0]["max_zoom"]) == (0, 1)
        assert (tiles[1]["min_zoom"], tiles[1]["max_zoom"]) == (2, 2)
        assert tiles[2]["min_zoom"] == 3
        for tile in tiles[:2]:
            assert tile["min_zoom"] <= tile["max_zoom"]

    def test_grouped_mode_with_features(self, tmp_path, fake_tools):
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        src = tmp_path / "by_region.parquet"
        features = tmp_path / "features.parquet"
        out = tmp_path / "pyramid.pmtiles"
        _write_admin_region_aggregate(src)
        features.write_bytes(b"")  # never read: tiling is faked

        create_pmtiles_pyramid(
            str(src),
            str(out),
            include_features=True,
            features_source=str(features),
            **_BAND_FORCING,
        )

        tiles = fake_tools["tiles"]
        assert [t["layer"] for t in tiles] == ["aggregate", "aggregate", "features"]
        feat = tiles[-1]
        assert feat["input"] == str(features)
        assert feat["min_zoom"] == 7  # base max (6) + 1
        assert feat["max_zoom"] is None
        # The features band keeps tippecanoe's cap so drop-densest applies.
        assert feat["no_tile_size_limit"] is False
        assert feat["drop_densest_as_needed"] is True

        ((_, pyramid),) = fake_tools["metadata"]
        assert pyramid["bands"][-1] == {
            "level": "features",
            "layer": "features",
            "minzoom": 7,
            "maxzoom": None,
        }
        assert len(fake_tools["tile_join"][0]) >= 3


class TestPreflightAndErrors:
    def test_existing_output_errors_without_force(self, tmp_path):
        """Fail fast on an existing output instead of tiling every band first
        and only then having tile-join reject it."""
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        out = tmp_path / "pyramid.pmtiles"
        out.write_bytes(b"existing archive")
        with pytest.raises(InvalidParameterError, match="force"):
            create_pmtiles_pyramid("in.parquet", str(out))
        assert out.read_bytes() == b"existing archive"

    def test_missing_tippecanoe(self, monkeypatch):
        import geoparquet_io.core.pmtiles_pyramid as pp

        monkeypatch.setattr(pp, "_check_tippecanoe", lambda: False)
        from geoparquet_io.core.pmtiles import TippecanoeNotFoundError

        with pytest.raises(TippecanoeNotFoundError):
            pp.create_pmtiles_pyramid("in.parquet", "out.pmtiles")

    def test_missing_tile_join(self, monkeypatch):
        import geoparquet_io.core.pmtiles_pyramid as pp

        monkeypatch.setattr(pp, "_check_tippecanoe", lambda: True)
        monkeypatch.setattr(pp, "_check_tile_join", lambda: False)
        with pytest.raises(TileJoinNotFoundError):
            pp.create_pmtiles_pyramid("in.parquet", "out.pmtiles")

    def test_aggregate_without_geometry_errors(self, tmp_path, monkeypatch):
        import duckdb

        import geoparquet_io.core.pmtiles_pyramid as pp

        monkeypatch.setattr(pp, "_check_tippecanoe", lambda: True)
        monkeypatch.setattr(pp, "_check_tile_join", lambda: True)
        src = tmp_path / "by_region.parquet"
        con = duckdb.connect()
        con.execute(f"COPY (SELECT 'US-CA' AS admin_code, 1 AS count) TO '{src}' (FORMAT PARQUET)")
        con.close()
        with pytest.raises(InvalidParameterError, match="geometry"):
            pp.create_pmtiles_pyramid(str(src), str(tmp_path / "o.pmtiles"))


class TestRunTileJoin:
    class _FakeProc:
        def __init__(self, returncode, stderr=""):
            self.returncode = returncode
            self.stderr = stderr

    def test_failure_raises_with_stderr(self, monkeypatch):
        import geoparquet_io.core.pmtiles_pyramid as pp

        monkeypatch.setattr(pp.subprocess, "run", lambda *a, **k: self._FakeProc(1, "boom\n"))
        with pytest.raises(RuntimeError, match="(?s)tile-join failed.*boom") as exc:
            pp._run_tile_join(["tile-join", "-o", "x"], verbose=False)
        assert "exit code 1" in str(exc.value)

    def test_success_logs_stderr_when_verbose(self, monkeypatch):
        import geoparquet_io.core.pmtiles_pyramid as pp

        monkeypatch.setattr(
            pp.subprocess, "run", lambda *a, **k: self._FakeProc(0, "joined 2 tilesets\n")
        )
        pp._run_tile_join(["tile-join", "-o", "x"], verbose=True)  # must not raise


class TestMergePyramidMetadata:
    def test_roundtrip_preserves_tiles_and_merges_key(self, tmp_path):
        from pmtiles.reader import MemorySource, Reader
        from pmtiles.tile import Compression, TileType, zxy_to_tileid
        from pmtiles.writer import Writer

        from geoparquet_io.core.pmtiles_pyramid import _merge_pyramid_metadata

        archive = tmp_path / "t.pmtiles"
        header = {
            "tile_type": TileType.MVT,
            "tile_compression": Compression.GZIP,
            "min_zoom": 0,
            "max_zoom": 1,
        }
        with open(archive, "wb") as f:
            writer = Writer(f)
            writer.write_tile(zxy_to_tileid(0, 0, 0), b"tile-z0")
            writer.write_tile(zxy_to_tileid(1, 0, 0), b"tile-z1")
            writer.finalize(header, {"name": "orig", "vector_layers": []})

        pyramid = {"scheme": "a5", "bands": [{"level": 5, "minzoom": 0, "maxzoom": 3}]}
        _merge_pyramid_metadata(str(archive), pyramid)

        # MemorySource (not MmapSource): an mmap would keep the file locked
        # on Windows until GC, breaking tmp_path cleanup.
        reader = Reader(MemorySource(archive.read_bytes()))
        metadata = reader.metadata()
        assert metadata["gpio:pyramid"] == pyramid
        assert metadata["name"] == "orig"  # existing metadata preserved
        assert reader.get(0, 0, 0) == b"tile-z0"
        assert reader.get(1, 0, 0) == b"tile-z1"
        assert reader.header()["min_zoom"] == 0
        assert reader.header()["max_zoom"] == 1
        # No temp file strays behind.
        assert [p.name for p in tmp_path.iterdir()] == ["t.pmtiles"]

    def _write_minimal_archive(self, path):
        from pmtiles.tile import Compression, TileType, zxy_to_tileid
        from pmtiles.writer import Writer

        header = {
            "tile_type": TileType.MVT,
            "tile_compression": Compression.GZIP,
            "min_zoom": 0,
            "max_zoom": 0,
        }
        with open(path, "wb") as f:
            writer = Writer(f)
            writer.write_tile(zxy_to_tileid(0, 0, 0), b"tile-z0")
            writer.finalize(header, {"name": "orig"})

    def test_rewrite_never_memory_maps_the_archive(self, tmp_path, monkeypatch):
        """A live mmap keeps an OS handle on the file, so os.replace fails on
        Windows. Guards against MmapSource being reintroduced here."""
        import mmap

        from geoparquet_io.core.pmtiles_pyramid import _merge_pyramid_metadata

        archive = tmp_path / "t.pmtiles"
        self._write_minimal_archive(archive)

        def forbidden_mmap(*args, **kwargs):
            raise AssertionError("the metadata rewrite must not mmap the archive")

        monkeypatch.setattr(mmap, "mmap", forbidden_mmap)
        _merge_pyramid_metadata(str(archive), {"scheme": "a5", "bands": []})

        assert [p.name for p in tmp_path.iterdir()] == ["t.pmtiles"]

    def test_failed_rewrite_cleans_up_temp_file(self, tmp_path, monkeypatch):
        from pmtiles.writer import Writer

        from geoparquet_io.core.pmtiles_pyramid import _merge_pyramid_metadata

        archive = tmp_path / "t.pmtiles"
        self._write_minimal_archive(archive)
        original = archive.read_bytes()

        def boom(*args, **kwargs):
            raise OSError("No space left on device")

        monkeypatch.setattr(Writer, "write_tile", boom)
        with pytest.raises(OSError, match="No space left"):
            _merge_pyramid_metadata(str(archive), {"scheme": "a5", "bands": []})

        # The original is untouched and no temp file is stranded.
        assert [p.name for p in tmp_path.iterdir()] == ["t.pmtiles"]
        assert archive.read_bytes() == original

    def test_unreadable_archive_strands_nothing(self, tmp_path):
        from geoparquet_io.core.pmtiles_pyramid import _merge_pyramid_metadata

        archive = tmp_path / "bad.pmtiles"
        archive.write_bytes(b"this is not a pmtiles archive at all")
        with pytest.raises(Exception):  # noqa: B017 - any parse failure counts
            _merge_pyramid_metadata(str(archive), {"scheme": "a5", "bands": []})
        assert [p.name for p in tmp_path.iterdir()] == ["bad.pmtiles"]
        assert archive.read_bytes() == b"this is not a pmtiles archive at all"


class TestPyramidMetadataPayload:
    def test_bands_and_features_entries(self):
        from geoparquet_io.core.pmtiles_pyramid import _pyramid_metadata
        from geoparquet_io.core.process.overview.detect import AggregateInfo

        info = AggregateInfo(
            scheme="a5",
            cell_column="a5_cell",
            base_level=10,
            rollup_columns=(),
            out_geometry="polygon",
        )
        bands = [Band(6, 0, 4), Band(10, 5, None)]
        payload = _pyramid_metadata(info, bands, 8, "per-level", "out", features_min_zoom=9)
        assert payload["scheme"] == "a5"
        assert payload["bands"] == [
            {"level": 6, "layer": "r6", "minzoom": 0, "maxzoom": 4},
            {"level": 10, "layer": "r10", "minzoom": 5, "maxzoom": 8},
            {"level": "features", "layer": "features", "minzoom": 9, "maxzoom": None},
        ]

    def test_no_features_entry_without_min_zoom(self):
        from geoparquet_io.core.pmtiles_pyramid import _pyramid_metadata
        from geoparquet_io.core.process.overview.detect import AggregateInfo

        info = AggregateInfo(
            scheme="admin",
            cell_column="admin_code",
            base_level="region",
            rollup_columns=(),
            out_geometry="polygon",
        )
        payload = _pyramid_metadata(
            info, [Band("region", 0, None)], None, "grouped", "x", features_min_zoom=None
        )
        assert [band["level"] for band in payload["bands"]] == ["region"]
        assert payload["bands"][0]["maxzoom"] is None


class TestApiAndCliWrappers:
    def test_ops_wrapper_passes_through(self, monkeypatch):
        import geoparquet_io.core.pmtiles_pyramid as pp
        from geoparquet_io.api import ops

        recorded = {}

        def fake(input_path, output_path, **kwargs):
            recorded["input"] = input_path
            recorded["output"] = output_path
            recorded.update(kwargs)

        monkeypatch.setattr(pp, "create_pmtiles_pyramid", fake)
        ops.create_pmtiles_pyramid(
            "cells.parquet", "out.pmtiles", levels=[5], layer_mode="single", max_zoom=9
        )
        assert recorded["input"] == "cells.parquet"
        assert recorded["output"] == "out.pmtiles"
        assert recorded["levels"] == [5]
        assert recorded["layer_mode"] == "single"
        assert recorded["max_zoom"] == 9

    def test_cli_success(self, monkeypatch):
        import geoparquet_io.core.pmtiles_pyramid as pp

        monkeypatch.setattr(pp, "create_pmtiles_pyramid", lambda *a, **k: None)
        runner = CliRunner()
        result = runner.invoke(cli, ["pmtiles", "pyramid", "in.parquet", "out.pmtiles"])
        assert result.exit_code == 0, result.output
        assert "Created out.pmtiles" in result.output

    def test_cli_error_becomes_click_exception(self, monkeypatch):
        import geoparquet_io.core.pmtiles_pyramid as pp

        def boom(*args, **kwargs):
            raise RuntimeError("tile-join failed with exit code 1")

        monkeypatch.setattr(pp, "create_pmtiles_pyramid", boom)
        runner = CliRunner()
        result = runner.invoke(cli, ["pmtiles", "pyramid", "in.parquet", "out.pmtiles"])
        assert result.exit_code != 0
        assert "tile-join failed" in result.output


# ---------------------------------------------------------------------------
# Integration (tippecanoe + tile-join + DuckDB community extensions)
# ---------------------------------------------------------------------------

_POINTS = [
    (2.35, 48.85, 4.0),
    (2.36, 48.86, 2.0),
    (13.40, 52.52, 1.5),
    (13.41, 52.53, 3.5),
    (-3.70, 40.42, 7.25),
    (30.0, -10.0, 5.0),
    (100.5, 13.75, 8.0),
    (-71.0, -35.0, 6.0),
]


def _write_points(path):
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, {v})" for lon, lat, v in _POINTS)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, v
            FROM (VALUES {values}) AS t(lon, lat, v)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def _read_pyramid_metadata(path):
    """Read header + metadata via MemorySource: no mmap, so no lingering
    file lock on Windows (an mmap survives its file object until GC)."""
    from pathlib import Path as _Path

    from pmtiles.reader import MemorySource, Reader

    reader = Reader(MemorySource(_Path(path).read_bytes()))
    return reader.header(), reader.metadata()


integration = pytest.mark.skipif(
    not (has_gpio() and has_tippecanoe() and has_tile_join()),
    reason="requires gpio, tippecanoe, and tile-join on PATH",
)


@skip_windows
@integration
@pytest.mark.slow
@pytest.mark.network
def test_pyramid_end_to_end_grouped(tmp_path):
    """Aggregate -> pyramid: bands are pinned, joined, and recorded in metadata."""
    from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    cells = tmp_path / "cells.parquet"
    out = tmp_path / "cells.pmtiles"
    _write_points(src)
    aggregate_by_a5(str(src), str(cells), resolution=7, metric="sum:v")

    # Absurd bytes-per-cell => nothing fits inside the probed range, so the
    # coarse level takes z0..max_zoom-1 and the base starts at max_zoom.
    create_pmtiles_pyramid(
        str(cells),
        str(out),
        levels=[5],
        bytes_per_cell=1e9,
        max_zoom=3,
    )

    assert out.exists() and out.stat().st_size > 0
    header, metadata = _read_pyramid_metadata(out)
    assert header["min_zoom"] == 0
    assert header["max_zoom"] == 3
    pyramid = metadata["gpio:pyramid"]
    assert pyramid["scheme"] == "a5"
    assert [band["level"] for band in pyramid["bands"]] == [5, 7]
    assert pyramid["bands"][0]["minzoom"] == 0
    assert pyramid["bands"][-1]["maxzoom"] == 3
    # Grouped (default) mode: all aggregate bands share one layer.
    layers = {layer["id"] for layer in metadata["vector_layers"]}
    assert layers == {"aggregate"}
    assert all(band["layer"] == "aggregate" for band in pyramid["bands"])


@skip_windows
@integration
@pytest.mark.slow
@pytest.mark.network
def test_pyramid_per_level_with_features(tmp_path):
    from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    cells = tmp_path / "cells.parquet"
    out = tmp_path / "pyramid.pmtiles"
    _write_points(src)
    aggregate_by_a5(str(src), str(cells), resolution=7, metric="sum:v")

    create_pmtiles_pyramid(
        str(cells),
        str(out),
        levels=[5],
        bytes_per_cell=1e9,
        max_zoom=3,
        layer_mode="per-level",
        include_features=True,
        features_source=str(src),
    )

    header, metadata = _read_pyramid_metadata(out)
    pyramid = metadata["gpio:pyramid"]
    assert [band["layer"] for band in pyramid["bands"]] == ["r5", "r7", "features"]
    # Features band starts right after the base band.
    assert pyramid["bands"][-1]["minzoom"] == 4
    layers = {layer["id"] for layer in metadata["vector_layers"]}
    assert {"r5", "r7", "features"} <= layers
    assert header["min_zoom"] == 0
    assert header["max_zoom"] >= 4
