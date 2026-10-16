"""Tests for file-name extent decoding and glob pruning (issue #1309).

The decoder and pruning tests are ported from qgis_plugin_gpq_downloader's
``tests/test_s3_partitions.py`` (same author), with the plugin's
``parquet_source_for_bbox`` tests adapted to gpio's pure
``prune_files_by_extent`` (a list of files in, a list of files or None out,
instead of a DuckDB connection and a SQL literal).
"""

from __future__ import annotations

import pytest

from geoparquet_io.core.filename_extents import (
    extent_from_file_name,
    prune_files_by_extent,
    quadkey_bounds,
    s2_cell_bounds,
    s2_cell_level,
)

PREFIX = "s3://us-west-2.opendata.source.coop/tge-labs/openbuildingmap/"
GBA_PREFIX = "s3://us-west-2.opendata.source.coop/tge-labs/globalbuildingatlas-lod1/"
NAN_THAILAND = (100.76, 18.77, 100.79, 18.80)

VIDA_S2 = (
    "s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/"
    "geoparquet/by_country_s2/"
)


def contains(extent, x, y):
    xmin, ymin, xmax, ymax = extent
    return xmin <= x <= xmax and ymin <= y <= ymax


class TestQuadkeyBounds:
    def test_quadkey_bounds(self):
        # Seattle and Nan, Thailand fall in these zoom-6 tiles
        assert contains(quadkey_bounds("021230"), -122.33, 47.61)
        assert contains(quadkey_bounds("132201"), 100.77, 18.78)
        assert not contains(quadkey_bounds("132201"), -122.33, 47.61)
        # Zoom-1 tile "3" is the south-east quarter of the world
        xmin, ymin, xmax, ymax = quadkey_bounds("3")
        assert (xmin, xmax) == (0.0, 180.0)
        assert ymax == 0.0 and ymin < -85

    def test_quadkey_bounds_at_deep_zoom(self):
        # A zoom-23 tile (Bing's deepest) is tiny but still well-formed
        quadkey = "132201" + "0" * 17
        xmin, ymin, xmax, ymax = quadkey_bounds(quadkey)
        assert xmin < xmax and ymin < ymax
        assert xmax - xmin < 1e-4
        assert contains(quadkey_bounds(quadkey), xmin, ymin)


class TestGridFileNames:
    def test_extent_from_grid_file_name(self):
        assert extent_from_file_name("e100_n20_e105_n15.parquet") == (100, 15, 105, 20)
        assert extent_from_file_name("w170_n65_w165_n60.parquet") == (-170, 60, -165, 65)
        assert extent_from_file_name("e100_n00_e105_s05.parquet") == (100, -5, 105, 0)

    def test_extent_from_file_name_ignores_other_names(self):
        for name in [
            "0.parquet",
            "part_1.parquet",
            "THA.parquet",
            "building.012.parquet",
            "part-00000-170892fe-0fe0-43c1-999d-f0911ce43365-c000.zstd.parquet",
        ]:
            assert extent_from_file_name(name) is None

    def test_malformed_names_return_none_rather_than_raising(self):
        for name in [
            "",
            ".parquet",
            "..parquet",
            "building..parquet",
            "building.00a1.parquet",
            "e100_n20_e105.parquet",
            "x100_n20_e105_n15.parquet",
            "e100_n20_e105_n15.parquet.bak",
            "3544332906740580352",
            "e100_n20_e105_n15",
        ]:
            assert extent_from_file_name(name) is None


class TestS2Cells:
    def test_s2_cell_level(self):
        assert s2_cell_level(1 << 60) == 0  # face 0
        assert s2_cell_level(3544332906740580352) == 4  # VIDA THA cell
        assert s2_cell_level(-6052837899185946624) == 1  # signed int64 form (NZL)
        assert s2_cell_level(2) is None  # lowest set bit at an odd position
        assert s2_cell_level(0) is None

    def test_s2_cell_bounds_match_vida_cells(self):
        # Cells and the places VIDA puts in them (checked against the files'
        # bbox statistics)
        assert contains(s2_cell_bounds(3544332906740580352), 100.77, 18.78)  # THA: Nan
        assert not contains(s2_cell_bounds(3544332906740580352), -0.12, 51.5)
        assert contains(s2_cell_bounds(10466365534009032704), -77.04, -12.05)  # PER: Lima
        assert contains(s2_cell_bounds(-6052837899185946624), 174.76, -36.85)  # NZL: Auckland

    def test_s2_cells_on_the_antimeridian_stay_on_their_side(self):
        # Pacific cells with an edge on 180 degrees: west of it (Fiji) and east of it
        assert s2_cell_bounds(7782220156096217088) == pytest.approx((135, -45, 180, 0), abs=1e-6)
        assert s2_cell_bounds(8358680908399640576) == pytest.approx((-180, -45, -135, 0), abs=1e-6)
        # A level-4 cell in the Aleutians, touching 180 degrees from the west
        xmin, ymin, xmax, ymax = s2_cell_bounds(6354579074219769856)
        assert xmin > 170 and xmax == 180

    def test_s2_level_zero_face_covers_the_world(self):
        # A whole cube face; faces 2 and 5 contain a pole, so the bbox is global
        assert s2_cell_bounds(1 << 60) == (-180.0, -90.0, 180.0, 90.0)

    def test_s2_cell_touching_the_pole_spans_every_longitude(self):
        # A level-1 cell on face 2, whose corner is the north pole
        face2_level1 = (2 << 61) | (1 << 58)
        assert s2_cell_level(face2_level1) == 1
        xmin, ymin, xmax, ymax = s2_cell_bounds(face2_level1)
        assert (xmin, xmax) == (-180.0, 180.0)
        assert ymax == 90.0
        assert ymin < 46  # face corners sit at ~45 degrees

    def test_s2_cell_bounds_rejects_an_invalid_id(self):
        with pytest.raises(ValueError, match="not a valid S2 cell id"):
            s2_cell_bounds(2)  # lowest set bit at an odd position

    def test_numbered_files_are_not_s2_cells(self):
        for name in ["0.parquet", "17.parquet", "1024.parquet", "20240101.parquet"]:
            assert extent_from_file_name(name) is None


class TestPruneFilesByExtent:
    def test_prunes_quadkey_files(self):
        files = [PREFIX + f"building.{k}.parquet" for k in ("132201", "132203", "021230")]
        assert prune_files_by_extent(files, NAN_THAILAND) == [PREFIX + "building.132201.parquet"]

    def test_prunes_grid_files(self):
        names = ["e100_n20_e105_n15", "e095_n20_e100_n15", "e100_n25_e105_n20", "w170_n65_w165_n60"]
        files = [GBA_PREFIX + f"{n}.parquet" for n in names]
        assert prune_files_by_extent(files, NAN_THAILAND) == [
            GBA_PREFIX + "e100_n20_e105_n15.parquet"
        ]

    def test_includes_neighbour_within_buffer(self):
        # An extent touching the 100 degree E cell edge also reads the western
        # neighbour. The third cell is far away, so the two are a real selection
        # rather than everything the glob matched.
        names = ["e100_n20_e105_n15", "e095_n20_e100_n15", "w170_n65_w165_n60"]
        files = [GBA_PREFIX + f"{n}.parquet" for n in names]
        assert prune_files_by_extent(files, (100.005, 18.0, 100.1, 18.1)) == [
            GBA_PREFIX + "e095_n20_e100_n15.parquet",
            GBA_PREFIX + "e100_n20_e105_n15.parquet",
        ]

    def test_falls_back_for_mixed_quadkey_lengths(self):
        files = [PREFIX + "building.1322.parquet", PREFIX + "building.132201.parquet"]
        assert prune_files_by_extent(files, NAN_THAILAND) is None

    def test_no_overlap_returns_empty_list(self):
        files = [PREFIX + "building.021230.parquet"]
        assert prune_files_by_extent(files, NAN_THAILAND) == []

    def test_falls_back_for_other_file_names(self):
        files = [
            "s3://bucket/by_country/THA/THA.parquet",
            "s3://bucket/by_country/LAO/LAO.parquet",
        ]
        assert prune_files_by_extent(files, (100, 18, 101, 19)) is None

    def test_one_unrecognized_name_disables_pruning_for_the_glob(self):
        # Every other file decodes, but one stranger means the convention is
        # not proven, so nothing may be skipped
        files = [
            VIDA_S2 + "country_iso=THA/3544332906740580352.parquet",
            VIDA_S2 + "country_iso=PER/10466365534009032704.parquet",
            VIDA_S2 + "notes.parquet",
        ]
        assert prune_files_by_extent(files, NAN_THAILAND) is None

    def test_ignores_year_named_files(self):
        files = [f"s3://bucket/data/data_{y}.parquet" for y in (2020, 2021, 2022, 2023)]
        assert prune_files_by_extent(files, NAN_THAILAND) is None

    def test_ignores_repeated_basenames(self):
        files = [f"s3://bucket/data/country={c}/part_00000.parquet" for c in ("TH", "LA", "VN")]
        assert prune_files_by_extent(files, NAN_THAILAND) is None

    def test_prunes_s2_files(self):
        files = [
            VIDA_S2 + "country_iso=THA/3544332906740580352.parquet",
            VIDA_S2 + "country_iso=LAO/3746994889972252672.parquet",
            VIDA_S2 + "country_iso=PER/10466365534009032704.parquet",
            VIDA_S2 + "country_iso=NZL/-6052837899185946624.parquet",
            VIDA_S2 + "country_iso=USA/6354579074219769856.parquet",
        ]
        # sorted: LAO, THA
        assert prune_files_by_extent(files, NAN_THAILAND) == [files[1], files[0]]

    def test_returns_none_when_every_file_matches(self):
        # Nothing pruned: let the caller keep the glob rather than spell out
        # every file
        names = ["e100_n20_e105_n15", "e095_n20_e100_n15"]
        files = [GBA_PREFIX + f"{n}.parquet" for n in names]
        assert prune_files_by_extent(files, (-180, -90, 180, 90)) is None

    def test_empty_listing_returns_none(self):
        assert prune_files_by_extent([], NAN_THAILAND) is None

    def test_windows_paths_use_the_basename(self):
        files = [
            "C:\\data\\e100_n20_e105_n15.parquet",
            "C:\\data\\e095_n20_e100_n15.parquet",
            "C:\\data\\w170_n65_w165_n60.parquet",
        ]
        assert prune_files_by_extent(files, NAN_THAILAND) == ["C:\\data\\e100_n20_e105_n15.parquet"]
