"""Unit tests for overview zoom-band selection (pure functions, no DuckDB)."""

import re

import pytest

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.overview.levels import (
    Band,
    estimate_bytes_per_cell,
    parse_bands,
    select_bands,
)


def uniform_worst_counts(cells: int, max_zoom: int = 12) -> dict[int, int]:
    """Worst tile cell-count for cells spread globally/uniformly: total / 4^z."""
    return {z: max(1, cells // (4**z)) for z in range(max_zoom + 1)}


def point_mass_worst_counts(cells: int, max_zoom: int = 12) -> dict[int, int]:
    """Worst tile cell-count when every cell lands in the same tile at all zooms."""
    return dict.fromkeys(range(max_zoom + 1), cells)


class TestSelectBands:
    def test_global_uniform_steps_down_with_zoom(self):
        # A5-like global grids: 12 * 4^L cells, spread uniformly.
        candidates = [4, 6, 8, 10]
        worst = {lvl: uniform_worst_counts(12 * 4**lvl) for lvl in candidates}
        bands = select_bands(worst, candidates, bytes_per_cell=100.0, max_tile_kb=500)
        assert bands == [
            Band(4, 0, 1),
            Band(6, 2, 3),
            Band(8, 4, 5),
            Band(10, 6, None),
        ]

    def test_point_mass_coarsest_extends_to_zoom_zero(self):
        # Nothing fits at z0 -- the coarsest band must still start at z0
        # (a single oversized z0 tile is acceptable; the cap guards it).
        candidates = [3, 7]
        worst = {
            3: point_mass_worst_counts(6000),
            7: {z: (20000 if z < 4 else 1000) for z in range(13)},
        }
        bands = select_bands(worst, candidates, bytes_per_cell=100.0, max_tile_kb=500)
        assert bands[0].level == 3
        assert bands[0].minzoom == 0
        assert bands == [Band(3, 0, 3), Band(7, 4, None)]

    def test_base_fits_everywhere_yields_single_band(self):
        candidates = [2, 5]
        worst = {2: uniform_worst_counts(10), 5: uniform_worst_counts(100)}
        bands = select_bands(worst, candidates, bytes_per_cell=50.0, max_tile_kb=500)
        assert bands == [Band(5, 0, None)]

    def test_band_invariants(self):
        candidates = [4, 6, 8, 10]
        worst = {lvl: uniform_worst_counts(12 * 4**lvl) for lvl in candidates}
        bands = select_bands(worst, candidates, bytes_per_cell=100.0, max_tile_kb=500)

        # First band starts at z0; final band is open-ended.
        assert bands[0].minzoom == 0
        assert bands[-1].maxzoom is None
        # Contiguous zoom coverage with no gaps or overlaps.
        for prev, nxt in zip(bands, bands[1:], strict=False):
            assert prev.maxzoom is not None
            assert nxt.minzoom == prev.maxzoom + 1
        # Monotonically finer levels.
        levels = [b.level for b in bands]
        assert levels == sorted(levels)
        assert len(set(levels)) == len(levels)
        # Base level is the final band.
        assert bands[-1].level == candidates[-1]

    def test_bytes_per_cell_override_shifts_transitions(self):
        candidates = [4, 10]
        worst = {lvl: uniform_worst_counts(12 * 4**lvl) for lvl in candidates}
        cheap = select_bands(worst, candidates, bytes_per_cell=100.0, max_tile_kb=500)
        pricey = select_bands(worst, candidates, bytes_per_cell=400.0, max_tile_kb=500)
        # More bytes per cell means the fine level only fits at a higher zoom.
        assert pricey[-1].minzoom > cheap[-1].minzoom

    def test_probe_exhaustion_forces_base_band(self):
        # Nothing ever fits inside the probed zoom range: the coarsest level
        # covers the probed zooms and the base still gets an open-ended band.
        candidates = [2, 9]
        worst = {
            2: point_mass_worst_counts(50000, max_zoom=5),
            9: point_mass_worst_counts(100000, max_zoom=5),
        }
        bands = select_bands(worst, candidates, bytes_per_cell=100.0, max_tile_kb=500)
        assert bands == [Band(2, 0, 5), Band(9, 6, None)]

    def test_explicit_candidates_restrict_selection(self):
        # Only the passed candidates may appear, even if other levels would fit.
        candidates = [6, 10]
        worst = {lvl: uniform_worst_counts(12 * 4**lvl) for lvl in candidates}
        bands = select_bands(worst, candidates, bytes_per_cell=100.0, max_tile_kb=500)
        assert {b.level for b in bands} <= {6, 10}

    def test_band_is_frozen(self):
        band = Band(4, 0, 3)
        with pytest.raises(AttributeError):
            band.level = 5

    def test_empty_candidates_error(self):
        with pytest.raises(InvalidParameterError, match="candidate"):
            select_bands({}, [], bytes_per_cell=100.0)


class TestEstimateBytesPerCell:
    def test_geometry_mode_ordering(self):
        kwargs = {"num_attributes": 3}
        none = estimate_bytes_per_cell(out_geometry="none", **kwargs)
        centroid = estimate_bytes_per_cell(out_geometry="centroid", **kwargs)
        polygon = estimate_bytes_per_cell(out_geometry="polygon", **kwargs)
        both = estimate_bytes_per_cell(out_geometry="both", **kwargs)
        assert none < centroid < polygon < both

    def test_more_attributes_cost_more(self):
        small = estimate_bytes_per_cell(num_attributes=1, out_geometry="polygon")
        big = estimate_bytes_per_cell(num_attributes=10, out_geometry="polygon")
        assert big > small

    def test_estimator_sanity_gba_like_schema(self):
        # Loose regression against recorded GlobalBuildingAtlas measurements:
        # a5 res-5 z0 tile ~= 297 KB and res-6 z0 ~= 791 KB with all cells kept
        # in one tile. For plausible res-5/6 populated-cell counts those imply
        # roughly 25-125 compressed bytes per cell, so a GBA-like schema
        # (count + 4 numeric rollups, polygon geometry) must land in that
        # ballpark. Generous tolerance on purpose: this is a conservative
        # budgeting heuristic, not a codec model -- do not tighten it to match
        # one dataset.
        est = estimate_bytes_per_cell(num_attributes=5, out_geometry="polygon")
        assert 40 <= est <= 160


class TestParseBands:
    def test_pairs_become_contiguous_bands(self):
        assert parse_bands("5:0,8:6,10:9") == [
            Band(5, 0, 5),
            Band(8, 6, 8),
            Band(10, 9, None),
        ]

    def test_single_band_is_open_ended(self):
        assert parse_bands("6:0") == [Band(6, 0, None)]

    def test_admin_levels_stay_strings(self):
        assert parse_bands("country:0,region:4") == [
            Band("country", 0, 3),
            Band("region", 4, None),
        ]

    def test_whitespace_is_tolerated(self):
        assert parse_bands(" 5:0 , 8:6 ") == [Band(5, 0, 5), Band(8, 6, None)]

    def test_bands_are_contiguous_and_cover_every_zoom(self):
        bands = parse_bands("4:0,6:3,8:7,10:11")
        assert bands[0].minzoom == 0
        assert bands[-1].maxzoom is None
        for prev, cur in zip(bands, bands[1:], strict=False):
            assert prev.maxzoom == cur.minzoom - 1

    def test_matches_select_bands_shape(self):
        # The whole point is interchangeability: an explicit plan has to be
        # the same kind of thing the probe produces, not a parallel format.
        candidates = [4, 6, 8, 10]
        worst = {lvl: uniform_worst_counts(12 * 4**lvl) for lvl in candidates}
        probed = select_bands(worst, candidates, bytes_per_cell=100.0, max_tile_kb=500)
        stated = parse_bands(",".join(f"{b.level}:{b.minzoom}" for b in probed))
        assert stated == probed

    @pytest.mark.parametrize(
        "spec,message",
        [
            ("5:1,8:6", "must start at z0"),
            ("5:0,8:6,8:9", "only once"),
            ("5:0,8:0", "strictly increase"),
            ("5:0,8:4,10:2", "strictly increase"),
            ("abc", "level:minzoom"),
            ("5:x", "must be an integer"),
            ("5:-1", "zoom cannot be negative"),
            ("-5:0", "level cannot be negative"),
            ("8:0,5:4", "must get finer"),
            ("10:0,8:3,9:6", "must get finer"),
            ("", "no bands given"),
            ("   ", "no bands given"),
            (":4", "missing level"),
        ],
    )
    def test_rejects_unusable_specs(self, spec, message):
        with pytest.raises(InvalidParameterError, match=message):
            parse_bands(spec)

    def test_a_reversed_plan_says_so_and_prints_the_right_way_round(self):
        """Finest-first is the natural way to write a ladder, and it is wrong here.

        gpio#1103 spelled its own example `8:11,7:10,...,2:0`. That fails the
        z0 check, which is true but reads as a problem with the first pair
        rather than with the order of all of them.
        """
        with pytest.raises(InvalidParameterError) as exc:
            parse_bands("8:11,7:10,6:9,5:7,4:5,2:0")
        message = str(exc.value)
        assert "reversed" in message
        assert "2:0,4:5,5:7,6:9,7:10,8:11" in message

    def test_the_suggested_spelling_actually_parses(self):
        """The hint has to be a working plan, not just a plausible-looking one."""
        reversed_spec = "8:11,7:10,6:9,5:7,4:5,2:0"
        with pytest.raises(InvalidParameterError) as exc:
            parse_bands(reversed_spec)
        suggestion = re.search(r"try '([^']+)'", str(exc.value)).group(1)
        bands = parse_bands(suggestion)
        assert [b.level for b in bands] == [2, 4, 5, 6, 7, 8]
        assert [b.minzoom for b in bands] == [0, 5, 7, 9, 10, 11]

    def test_a_two_entry_reversed_plan_is_caught_too(self):
        with pytest.raises(InvalidParameterError, match="reversed"):
            parse_bands("8:6,5:0")

    def test_a_reversed_plan_that_is_also_broken_gets_its_real_error(self):
        """No point suggesting a spelling that fails too.

        Reversing "8:6,8:0" gives "8:0,8:6", which repeats a level -- so the
        hint is withheld and the plan's own problem is reported instead.
        """
        with pytest.raises(InvalidParameterError) as exc:
            parse_bands("8:6,8:0")
        assert "reversed" not in str(exc.value)

    def test_a_plan_that_is_merely_wrong_keeps_its_own_message(self):
        # Not descending -- this is a plan that starts late, not a flipped one.
        with pytest.raises(InvalidParameterError, match="must start at z0"):
            parse_bands("5:1,8:6")
        # Descending levels at ascending zooms is the "must get finer" case.
        with pytest.raises(InvalidParameterError, match="must get finer"):
            parse_bands("8:0,5:4")
