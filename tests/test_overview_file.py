"""#1117: emit a levelled overview GeoParquet so a DGGS pyramid tiles in one pass.

The contract is tylertoo's OVERVIEWS_SPEC: levels are additional *rows* tagged
by a `level` column (2.1), written coarse to fine with each level ending on a
row-group boundary (4.2), and described by a `geo:overviews` footer key (3.2).
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.process.overview.overview_file import (
    DEFAULT_CELL_DETAIL,
    OVERVIEWS_KEY,
    LevelInput,
    cell_width_meters,
    gsd_for_zoom,
    gsd_ladder,
    write_overview_file,
    zoom_for_gsd,
)


def _level_table(n, tag):
    return pa.table({"cell": [f"{tag}-{i}" for i in range(n)], "value": list(range(n))})


def _levels(tmp_path, sizes_and_zooms):
    """Build a ladder; the second element is a zoom, turned into its GSD."""
    out = []
    for idx, (n, zoom) in enumerate(sizes_and_zooms):
        p = tmp_path / f"lvl{idx}.parquet"
        pq.write_table(_level_table(n, f"l{idx}"), p)
        out.append(LevelInput(key=f"r{idx}", path=str(p), gsd=gsd_for_zoom(zoom)))
    return out


class TestGsd:
    def test_matches_the_spec_reference_table(self):
        """5.2: gsd(z) = 40075016.69 / 1024 / 2^z."""
        assert gsd_for_zoom(0) == pytest.approx(39135.76, abs=0.01)
        assert gsd_for_zoom(2) == pytest.approx(9783.94, abs=0.01)
        assert gsd_for_zoom(6) == pytest.approx(611.50, abs=0.01)
        assert gsd_for_zoom(9) == pytest.approx(76.44, abs=0.01)


class TestOverviewFile:
    def test_level_column_is_int32_not_null_and_zero_based(self, tmp_path):
        """4.1: physical INT32 `level`, NOT NULL, domain 0..len(levels)-1."""
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4)]), str(out))

        tbl = pq.read_table(out)
        field = tbl.schema.field("level")
        assert field.type == pa.int32()
        assert not field.nullable
        assert set(tbl.column("level").to_pylist()) == {0, 1}

    def test_levels_are_written_coarse_to_fine(self, tmp_path):
        """4.2: level 0's row groups come first."""
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4)]), str(out))

        levels = pq.read_table(out).column("level").to_pylist()
        assert levels == sorted(levels)
        assert levels[0] == 0 and levels[-1] == 1

    def test_no_row_group_mixes_two_levels(self, tmp_path):
        """4.2/4.1: a row group holds exactly one level, matching the footer."""
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4), (13, 7)]), str(out))

        pf = pq.ParquetFile(out)
        for rg in range(pf.metadata.num_row_groups):
            vals = set(pf.read_row_group(rg, columns=["level"]).column("level").to_pylist())
            assert len(vals) == 1, f"row group {rg} mixes levels {vals}"

    def test_footer_row_group_ends_are_monotonic_and_cover_all_groups(self, tmp_path):
        """3.3: strictly increasing, last == num_row_groups - 1, no gaps."""
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4), (13, 7)]), str(out))

        meta = json.loads(pq.read_schema(out).metadata[OVERVIEWS_KEY.encode()])
        ends = [lv["row_group_end"] for lv in meta["levels"]]
        assert ends == sorted(set(ends))
        assert ends[-1] == pq.ParquetFile(out).metadata.num_row_groups - 1

    def test_footer_level_assignment_matches_the_column(self, tmp_path):
        """4.1: column and footer MUST agree for every row group."""
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4), (13, 7)]), str(out))

        meta = json.loads(pq.read_schema(out).metadata[OVERVIEWS_KEY.encode()])
        pf = pq.ParquetFile(out)
        start = 0
        for k, lv in enumerate(meta["levels"]):
            for rg in range(start, lv["row_group_end"] + 1):
                vals = set(pf.read_row_group(rg, columns=["level"]).column("level").to_pylist())
                assert vals == {k}
            start = lv["row_group_end"] + 1

    def test_gsd_strictly_decreases_and_zoom_strictly_increases(self, tmp_path):
        """3.3: coarse->fine means larger->smaller meters."""
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4), (13, 7)]), str(out))

        meta = json.loads(pq.read_schema(out).metadata[OVERVIEWS_KEY.encode()])
        gsds = [lv["gsd"] for lv in meta["levels"]]
        zooms = [lv["zoom"] for lv in meta["levels"]]
        assert gsds == sorted(gsds, reverse=True) and len(set(gsds)) == len(gsds)
        assert zooms == sorted(zooms) and len(set(zooms)) == len(zooms)
        assert all(g > 0 for g in gsds)

    def test_mode_is_duplicating_with_the_finest_level_canonical(self, tmp_path):
        """3.4: duplicating MUST carry canonical_level == len(levels) - 1."""
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4)]), str(out))

        meta = json.loads(pq.read_schema(out).metadata[OVERVIEWS_KEY.encode()])
        assert meta["mode"] == "duplicating"
        assert meta["canonical_level"] == len(meta["levels"]) - 1
        assert meta["version"].count(".") == 2

    def test_no_cogp_key_in_duplicating_mode(self, tmp_path):
        """3.1: writers SHOULD omit `cogp` in duplicating mode."""
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4)]), str(out))

        keys = {k.decode() for k in (pq.read_schema(out).metadata or {})}
        assert "cogp" not in keys

    def test_rejects_a_case_colliding_level_column(self, tmp_path):
        """4.1: a source `LEVEL` would shadow the overview column in SQL."""
        p = tmp_path / "src.parquet"
        pq.write_table(pa.table({"LEVEL": [1, 2], "v": [3, 4]}), p)
        with pytest.raises(ValueError, match="level"):
            write_overview_file(
                [LevelInput(key="r0", path=str(p), gsd=1000.0)], str(tmp_path / "o.parquet")
            )

    def test_rejects_a_single_level_with_no_levels_at_all(self, tmp_path):
        """3.3: `levels` MUST be non-empty."""
        with pytest.raises(ValueError, match="empty"):
            write_overview_file([], str(tmp_path / "o.parquet"))

    def test_rejects_non_decreasing_gsd(self, tmp_path):
        """3.3: gsd MUST be strictly decreasing coarse->fine."""
        with pytest.raises(ValueError, match="gsd"):
            write_overview_file(_levels(tmp_path, [(5, 4), (9, 4)]), str(tmp_path / "o.parquet"))

    def test_row_count_is_the_sum_of_the_levels(self, tmp_path):
        """Nothing is dropped in assembly."""
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4), (13, 7)]), str(out))
        assert pq.read_table(out).num_rows == 5 + 9 + 13


class TestCreateOverviewFileEndToEnd:
    """#1117: a real H3 aggregate becomes one levelled, spec-conformant file."""

    def test_h3_aggregate_ladder_assembles(self, tmp_path):
        import shutil as _shutil
        import subprocess as _sp

        if _shutil.which("gpio") is None:
            pytest.skip("gpio not installed")

        from geoparquet_io.core.process.overview.run import create_overview_file

        cells = tmp_path / "cells.parquet"
        res = _sp.run(
            [
                "gpio",
                "process",
                "aggregate",
                "h3",
                "tests/data/buildings_test.parquet",
                str(cells),
                "--resolution",
                "9",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode != 0:
            pytest.skip(f"h3 aggregate unavailable: {res.stderr[-200:]}")

        out = tmp_path / "pyramid.parquet"
        create_overview_file(str(cells), str(out), levels="8,7,6", force=True)

        tbl = pq.read_table(out)
        meta = json.loads(pq.read_schema(out).metadata[OVERVIEWS_KEY.encode()])

        # Four levels: the three built plus the r9 base, coarse to fine.
        assert len(meta["levels"]) == 4
        assert meta["mode"] == "duplicating"
        assert meta["canonical_level"] == 3
        assert [lv["zoom"] for lv in meta["levels"]] == sorted(lv["zoom"] for lv in meta["levels"])
        assert [lv["gsd"] for lv in meta["levels"]] == sorted(
            (lv["gsd"] for lv in meta["levels"]), reverse=True
        )

        # Column and footer agree for every row group (OVERVIEWS_SPEC 4.1).
        pf = pq.ParquetFile(out)
        assert meta["levels"][-1]["row_group_end"] == pf.metadata.num_row_groups - 1
        start = 0
        for k, lv in enumerate(meta["levels"]):
            for rg in range(start, lv["row_group_end"] + 1):
                vals = set(pf.read_row_group(rg, columns=["level"]).column("level").to_pylist())
                assert vals == {k}
            start = lv["row_group_end"] + 1

        # The finest level is the base aggregate, unchanged in row count.
        finest = tbl.filter(pa.compute.equal(tbl.column("level"), 3))
        assert finest.num_rows == pq.read_table(cells).num_rows


class TestDataDrivenGsd:
    """The GSD ladder is derived from the data, with one detail knob."""

    BOUNDS = (0.0, 0.0, 1.0, 1.0)

    def test_cell_width_shrinks_as_levels_refine(self):
        wide = cell_width_meters(self.BOUNDS, 10)
        narrow = cell_width_meters(self.BOUNDS, 1000)
        assert wide > narrow > 0

    def test_ladder_is_strictly_decreasing(self):
        gsds = gsd_ladder(self.BOUNDS, [10, 70, 490, 3430])
        assert gsds == sorted(gsds, reverse=True)
        assert len(set(gsds)) == len(gsds)

    def test_equal_cell_counts_are_still_separated(self):
        """Two levels with the same count must not collide in the footer."""
        gsds = gsd_ladder(self.BOUNDS, [100, 100, 100])
        assert all(gsds[i] < gsds[i - 1] for i in range(1, len(gsds)))

    def test_more_detail_means_smaller_gsd(self):
        """Mirrors tylertoo's --gsd-base direction: larger knob, denser level.

        Chris's case: sometimes more hexes should appear even if the tiles get
        heavier. A larger cell_detail lowers the GSD, so cells clear the
        visibility gates at coarser scales.
        """
        base = gsd_ladder(self.BOUNDS, [100, 900], cell_detail=DEFAULT_CELL_DETAIL)
        denser = gsd_ladder(self.BOUNDS, [100, 900], cell_detail=DEFAULT_CELL_DETAIL * 4)
        assert all(d < b for d, b in zip(denser, base, strict=True))

    def test_detail_must_be_positive(self):
        with pytest.raises(ValueError, match="cell_detail"):
            gsd_ladder(self.BOUNDS, [100], cell_detail=0)

    def test_zoom_round_trips_through_the_spec_formula(self):
        for z in (0, 3, 7, 12):
            assert zoom_for_gsd(gsd_for_zoom(z)) == z

    def test_zoom_never_goes_negative(self):
        """A GSD coarser than z0 still has to report a valid zoom."""
        assert zoom_for_gsd(gsd_for_zoom(0) * 8) == 0


class TestZoomLadder:
    def test_close_gsds_still_yield_strictly_increasing_zooms(self, tmp_path):
        """3.3: zoom MUST be strictly increasing when present.

        Independent rounding does not give that -- a four-level H3 ladder over
        a small extent rounds to 7, 7, 8, 8. gsd stays authoritative.
        """
        from geoparquet_io.core.process.overview.overview_file import _zoom_ladder

        zooms = _zoom_ladder([334.46, 289.65, 183.19, 140.50])
        assert zooms == sorted(zooms)
        assert len(set(zooms)) == len(zooms)

    def test_written_file_has_strictly_increasing_zooms(self, tmp_path):
        out = tmp_path / "ov.parquet"
        levels = []
        for idx, (n, g) in enumerate([(5, 334.46), (9, 289.65), (13, 183.19), (17, 140.5)]):
            p = tmp_path / f"z{idx}.parquet"
            pq.write_table(_level_table(n, f"z{idx}"), p)
            levels.append(LevelInput(key=f"r{idx}", path=str(p), gsd=g))
        write_overview_file(levels, str(out))

        meta = json.loads(pq.read_schema(out).metadata[OVERVIEWS_KEY.encode()])
        zooms = [lv["zoom"] for lv in meta["levels"]]
        assert zooms == sorted(zooms) and len(set(zooms)) == len(zooms)
