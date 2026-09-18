"""#1117: emit a levelled overview GeoParquet so a DGGS pyramid tiles in one pass.

The contract is tylertoo's OVERVIEWS_SPEC: levels are additional *rows* tagged
by a `level` column (2.1), written coarse to fine with each level ending on a
row-group boundary (4.2), and described by a `geo:overviews` footer key (3.2).
"""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.api import ops, read
from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.geo_metadata import OVERVIEWS_KEY
from geoparquet_io.core.process.overview import run as run_module
from geoparquet_io.core.process.overview.overview_file import (
    OVERVIEWS_VERSION,
    LevelInput,
    gsd_for_zoom,
    write_overview_file,
    zoom_for_gsd,
)
from geoparquet_io.core.process.overview.run import create_overview_file

BUILDINGS = str(Path(__file__).parent / "data" / "buildings_test.parquet")


def _level_table(n, tag):
    return pa.table({"cell": [f"{tag}-{i}" for i in range(n)], "value": list(range(n))})


def _levels(tmp_path, sizes_and_zooms):
    """A plain (no geometry) ladder; the second element is a zoom, turned into its GSD."""
    out = []
    for idx, (n, zoom) in enumerate(sizes_and_zooms):
        p = tmp_path / f"lvl{idx}.parquet"
        pq.write_table(_level_table(n, f"l{idx}"), p)
        out.append(LevelInput(path=str(p), gsd=gsd_for_zoom(zoom)))
    return out


def _footer(path):
    return json.loads(pq.read_schema(path).metadata[OVERVIEWS_KEY.encode()])


def _assert_footer_matches_column(path):
    """OVERVIEWS_SPEC 4.1/4.2: every row group holds one level, the footer's."""
    meta = _footer(path)
    pf = pq.ParquetFile(path)
    assert meta["levels"][-1]["row_group_end"] == pf.metadata.num_row_groups - 1
    start = 0
    for k, lv in enumerate(meta["levels"]):
        for rg in range(start, lv["row_group_end"] + 1):
            column = pf.metadata.row_group(rg).column(pf.schema_arrow.get_field_index("level"))
            assert (column.statistics.min, column.statistics.max) == (k, k)
        start = lv["row_group_end"] + 1


@pytest.fixture
def cells(tmp_path):
    """An H3 r9 aggregate of the buildings fixture (17 cells)."""
    path = tmp_path / "cells.parquet"
    read(BUILDINGS).aggregate_h3(9).write(str(path))
    return path


class TestGsd:
    def test_matches_the_spec_reference_table(self):
        """5.2: gsd(z) = 40075016.69 / 1024 / 2^z."""
        assert gsd_for_zoom(0) == pytest.approx(39135.76, abs=0.01)
        assert gsd_for_zoom(6) == pytest.approx(611.50, abs=0.01)
        assert gsd_for_zoom(9) == pytest.approx(76.44, abs=0.01)

    def test_zoom_round_trips_and_never_goes_negative(self):
        assert [zoom_for_gsd(gsd_for_zoom(z)) for z in (0, 5, 14, 22)] == [0, 5, 14, 22]
        assert zoom_for_gsd(1e9) == 0

    @pytest.mark.parametrize("bad", [0.0, -1.0, float("nan"), float("inf")])
    def test_rejects_a_non_positive_or_non_finite_gsd(self, bad):
        with pytest.raises(ValueError, match="positive"):
            zoom_for_gsd(bad)


class TestOverviewFile:
    def test_level_column_is_int32_not_null_and_zero_based(self, tmp_path):
        """4.1: physical INT32 `level`, NOT NULL, domain 0..len(levels)-1."""
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4)]), str(out))

        tbl = pq.read_table(out)
        field = tbl.schema.field("level")
        assert field.type == pa.int32()
        assert not field.nullable
        assert tbl.column("level").to_pylist() == [0] * 5 + [1] * 9

    def test_footer_matches_the_column_across_multi_group_levels(self, tmp_path, monkeypatch):
        """4.1/4.2 with a level that spans several row groups, and a source
        file whose own row groups do not line up with the output's."""
        monkeypatch.setattr(run_module, "resolve_row_group_rows", lambda *a: 4, raising=False)
        import geoparquet_io.core.process.overview.overview_file as of

        monkeypatch.setattr(of, "resolve_row_group_rows", lambda *a: 4)
        levels = _levels(tmp_path, [(5, 0), (9, 4), (13, 7)])
        # Rewrite the middle level with 2-row groups so slices cross them.
        pq.write_table(pq.read_table(levels[1].path), levels[1].path, row_group_size=2)
        out = tmp_path / "ov.parquet"
        write_overview_file(levels, str(out))

        assert pq.ParquetFile(out).metadata.num_row_groups == 2 + 3 + 4
        assert [lv["row_group_end"] for lv in _footer(out)["levels"]] == [1, 4, 8]
        _assert_footer_matches_column(out)
        assert pq.read_table(out).num_rows == 5 + 9 + 13

    def test_footer_fields(self, tmp_path):
        out = tmp_path / "ov.parquet"
        write_overview_file(_levels(tmp_path, [(5, 0), (9, 4), (13, 7)]), str(out))
        meta = _footer(out)
        assert meta["version"] == OVERVIEWS_VERSION == "0.2.0"
        # 3.4: duplicating names the finest level canonical; 3.1: no `cogp` key.
        assert meta["mode"] == "duplicating" and meta["canonical_level"] == 2
        assert "cogp" not in {k.decode() for k in pq.read_schema(out).metadata}
        gsds = [lv["gsd"] for lv in meta["levels"]]
        assert gsds == sorted(gsds, reverse=True) and len(set(gsds)) == 3
        assert [lv["zoom"] for lv in meta["levels"]] == [0, 4, 7]

    def test_zoom_is_left_out_when_two_levels_round_to_one(self, tmp_path):
        """3.3 makes zoom OPTIONAL and strictly increasing; tylertoo serves a
        level at exactly its zoom when present, so a nudged tie would put a
        level at a zoom its gsd does not denote. Ties drop the field."""
        out = tmp_path / "ov.parquet"
        close = [
            LevelInput(path=lv.path, gsd=g)
            for lv, g in zip(
                _levels(tmp_path, [(5, 0), (9, 0), (13, 0)]), [1000.0, 950.0, 100.0], strict=True
            )
        ]
        write_overview_file(close, str(out))
        assert all("zoom" not in lv for lv in _footer(out)["levels"])
        assert [lv["gsd"] for lv in _footer(out)["levels"]] == [1000.0, 950.0, 100.0]

    def test_rejects_a_case_colliding_level_column(self, tmp_path):
        """4.1: a source `LEVEL` would shadow the overview column in SQL."""
        p = tmp_path / "src.parquet"
        pq.write_table(pa.table({"LEVEL": [1, 2], "v": [3, 4]}), p)
        with pytest.raises(ValueError, match="LEVEL"):
            write_overview_file([LevelInput(path=str(p), gsd=1000.0)], str(tmp_path / "o.parquet"))

    def test_rejects_an_empty_level(self, tmp_path):
        """7.3: an empty level MUST NOT be written."""
        levels = _levels(tmp_path, [(5, 0), (9, 4)])
        pq.write_table(_level_table(0, "x"), levels[1].path)
        with pytest.raises(ValueError, match="7.3"):
            write_overview_file(levels, str(tmp_path / "o.parquet"))

    @pytest.mark.parametrize(
        ("ladder", "match"),
        [([], "empty"), ([(5, 4), (9, 4)], "decreasing")],
        ids=["no-levels", "non-decreasing"],
    )
    def test_rejects_a_bad_ladder(self, tmp_path, ladder, match):
        with pytest.raises(ValueError, match=match):
            write_overview_file(_levels(tmp_path, ladder), str(tmp_path / "o.parquet"))

    def test_output_that_is_a_level_is_refused(self, tmp_path):
        levels = _levels(tmp_path, [(5, 0), (9, 4)])
        with pytest.raises(ValueError, match="also a level"):
            write_overview_file(levels, levels[1].path)
        assert pq.read_table(levels[1].path).num_rows == 9

    def test_a_failed_write_leaves_nothing_at_the_output(self, tmp_path, monkeypatch):
        import geoparquet_io.core.process.overview.overview_file as of

        def boom(*args, **kwargs):
            raise RuntimeError("disk full")

        monkeypatch.setattr(of, "_write_levels", boom)
        out = tmp_path / "ov.parquet"
        with pytest.raises(RuntimeError, match="disk full"):
            write_overview_file(_levels(tmp_path, [(5, 0), (9, 4)]), str(out))
        assert not out.exists()
        assert not list(tmp_path.glob(".gpio-overview-*"))

    def test_schema_drift_is_named_before_anything_is_written(self, tmp_path):
        """A column the coarsest level lacks is projected away; one it has
        and a finer level lacks is an error naming the level."""
        levels = _levels(tmp_path, [(5, 0), (9, 4)])
        pq.write_table(pa.table({"cell": ["a"], "other": [1]}), levels[1].path)
        out = tmp_path / "o.parquet"
        with pytest.raises(ValueError, match="lvl1.parquet lacks column"):
            write_overview_file(levels, str(out))
        assert not out.exists()


class TestGeoLevels:
    """Real GeoParquet levels: the `geo` block, casts and encodings."""

    def _ladder(self, tmp_path, cells, **kwargs):
        siblings = ops.create_overviews(str(cells), levels="8,7", force=True, **kwargs)
        paths = [p for _, p in siblings] + [str(cells)]
        return [
            LevelInput(path=p, gsd=g) for p, g in zip(paths, [900.0, 400.0, 150.0], strict=True)
        ]

    def test_geo_block_is_the_union_and_the_geometry_is_not_dictionary_encoded(
        self, tmp_path, cells
    ):
        out = tmp_path / "ov.parquet"
        write_overview_file(self._ladder(tmp_path, cells), str(out))

        pf = pq.ParquetFile(out)
        geo = json.loads(pf.schema_arrow.metadata[b"geo"])["columns"]["geometry"]
        per_level = [
            json.loads(pq.read_schema(lv.path).metadata[b"geo"])["columns"]["geometry"]["bbox"]
            for lv in self._ladder(tmp_path, cells)
        ]
        assert geo["bbox"][0] == min(b[0] for b in per_level)
        assert geo["bbox"][2] == max(b[2] for b in per_level)
        assert geo["geometry_types"] == ["Polygon"]
        gi = pf.schema_arrow.get_field_index("geometry")
        for rg in range(pf.metadata.num_row_groups):
            assert "RLE_DICTIONARY" not in pf.metadata.row_group(rg).column(gi).encodings
        _assert_footer_matches_column(out)

    def test_a_2_0_input_and_1_1_siblings_become_one_2_0_file(self, tmp_path, cells):
        """The base is the user's file, the siblings the funnel's; when they
        disagree on the geometry encoding every level is cast to the version
        the facade resolves from the base."""
        native = tmp_path / "cells20.parquet"
        read(str(cells)).write(str(native), geoparquet_version="2.0")
        levels = self._ladder(tmp_path, native)
        assert pq.read_schema(levels[0].path).field("geometry").type == pa.binary()
        out = tmp_path / "ov.parquet"
        write_overview_file(levels, str(out))
        assert json.loads(pq.read_schema(out).metadata[b"geo"])["version"] == "2.0.0"
        assert pq.read_table(out).num_rows == sum(
            pq.read_metadata(lv.path).num_rows for lv in levels
        )

    def test_a_base_with_a_bbox_column_the_rollup_dropped_still_assembles(self, tmp_path, cells):
        with_bbox = tmp_path / "cells_bbox.parquet"
        read(str(cells)).add_bbox().write(str(with_bbox))
        out = tmp_path / "ov.parquet"
        write_overview_file(self._ladder(tmp_path, with_bbox), str(out))
        assert "bbox" not in pq.read_schema(out).names
        _assert_footer_matches_column(out)


class TestCreateOverviewFile:
    """The orchestrator: knobs validated first, siblings placed, GSDs measured."""

    def test_h3_aggregate_ladder_assembles_and_measures_gsds(self, tmp_path, cells):
        out = tmp_path / "pyramid.parquet"
        create_overview_file(str(cells), str(out), levels="8,7,6")

        meta = _footer(out)
        assert len(meta["levels"]) == 4 and meta["canonical_level"] == 3
        gsds = [lv["gsd"] for lv in meta["levels"]]
        assert gsds == sorted(gsds, reverse=True)
        # H3 edge lengths shrink ~2.6x per resolution; measured widths follow.
        for coarse, fine in zip(gsds, gsds[1:], strict=False):
            assert 2.0 < coarse / fine < 3.5
        # r9 cells are ~350 m across: gsd = width / cell_detail(4).
        assert 60 < gsds[-1] < 120
        _assert_footer_matches_column(out)
        tbl = pq.read_table(out)
        finest = tbl.filter(pc.equal(tbl.column("level"), 3)).drop_columns(["level"])
        assert finest.equals(pq.read_table(cells).select(finest.schema.names))

    def test_cell_detail_scales_every_gsd(self, tmp_path, cells):
        a = create_overview_file(str(cells), str(tmp_path / "a.parquet"), levels="8", cell_detail=2)
        b = create_overview_file(
            str(cells), str(tmp_path / "b.parquet"), levels="8", cell_detail=8, force=True
        )
        ga = [lv["gsd"] for lv in _footer(a)["levels"]]
        gb = [lv["gsd"] for lv in _footer(b)["levels"]]
        assert all(x == pytest.approx(y * 4) for x, y in zip(ga, gb, strict=True))

    def test_explicit_gsd_overrides_measurement(self, tmp_path, cells):
        out = create_overview_file(
            str(cells), str(tmp_path / "o.parquet"), levels="8,7", explicit_gsd="2000,800,300"
        )
        assert [lv["gsd"] for lv in _footer(out)["levels"]] == [2000.0, 800.0, 300.0]

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"explicit_gsd": "abc"}, "comma-separated"),
            ({"explicit_gsd": "nan,300,200"}, "positive"),
            ({"explicit_gsd": "300,800,200"}, "decreasing"),
            ({"cell_detail": 0}, "positive"),
            ({"cell_detail": float("nan")}, "positive"),
        ],
    )
    def test_bad_knobs_fail_before_any_rollup_runs(self, tmp_path, cells, kwargs, match):
        with pytest.raises(InvalidParameterError, match=match):
            create_overview_file(str(cells), str(tmp_path / "o.parquet"), levels="8,7", **kwargs)
        assert not list(tmp_path.glob("cells_r*.parquet")), "no sibling was built"

    def test_gsd_count_is_checked_against_the_ladder_before_building(self, tmp_path, cells):
        with pytest.raises(InvalidParameterError, match="expected 3 GSD value"):
            create_overview_file(
                str(cells), str(tmp_path / "o.parquet"), levels="8,7", explicit_gsd="2000,800"
            )
        assert not list(tmp_path.glob("cells_r*.parquet"))

    def test_output_equal_to_the_input_is_refused(self, tmp_path, cells):
        before = pq.read_table(cells)
        with pytest.raises(InvalidParameterError, match="is the input"):
            create_overview_file(str(cells), str(cells), levels="8")
        assert pq.read_table(cells).equals(before)

    def test_existing_output_needs_force(self, tmp_path, cells):
        out = tmp_path / "precious.parquet"
        out.write_text("precious")
        with pytest.raises(InvalidParameterError, match="already exists"):
            create_overview_file(str(cells), str(out), levels="8")
        assert out.read_text() == "precious"
        create_overview_file(str(cells), str(out), levels="8", force=True)
        assert OVERVIEWS_KEY.encode() in pq.read_schema(out).metadata

    def test_input_level_column_is_refused_before_building(self, tmp_path, cells):
        tbl = pq.read_table(cells)
        src = tmp_path / "storeys.parquet"
        pq.write_table(tbl.append_column("Level", pa.array([1] * tbl.num_rows)), src)
        with pytest.raises(InvalidParameterError, match="'Level'"):
            create_overview_file(str(src), str(tmp_path / "o.parquet"), levels="8")
        assert not list(tmp_path.glob("storeys_r*.parquet"))

    def test_output_dir_places_the_siblings(self, tmp_path, cells):
        sibs = tmp_path / "sibs"
        create_overview_file(
            str(cells), str(tmp_path / "o.parquet"), levels="8", output_dir=str(sibs)
        )
        assert (sibs / "cells_r8.parquet").exists()
        assert not (tmp_path / "cells_r8.parquet").exists()


class TestCli:
    def _run(self, args):
        return CliRunner().invoke(cli, ["process", "overview", *args])

    def test_overview_out_with_knobs_and_output_dir(self, tmp_path, cells):
        out = tmp_path / "pyr.parquet"
        result = self._run(
            [
                str(cells),
                "--levels",
                "8,7",
                "--overview-out",
                str(out),
                "--cell-detail",
                "2",
                "--output-dir",
                str(tmp_path / "sibs"),
            ]
        )
        assert result.exit_code == 0, result.output
        assert len(_footer(out)["levels"]) == 3
        assert (tmp_path / "sibs" / "cells_r7.parquet").exists()

    def test_gsd_arity_message_names_the_base(self, tmp_path, cells):
        result = self._run(
            [
                str(cells),
                "--levels",
                "8,7",
                "--overview-out",
                str(tmp_path / "o.parquet"),
                "--gsd",
                "4000,1500",
            ]
        )
        assert result.exit_code == 1
        assert "expected 3 GSD value(s)" in result.output and "plus the base" in result.output

    @pytest.mark.parametrize("flag", [["--cell-detail", "2"], ["--gsd", "1000,100"]])
    def test_knobs_without_overview_out_are_refused(self, cells, flag):
        result = self._run([str(cells), "--levels", "8", *flag])
        assert result.exit_code == 1
        assert "pass --overview-out" in result.output


class TestRewritesDropTheFooter:
    """The footer describes the file's own row-group layout, so a rewrite
    that regroups or filters rows must not carry it along (it would then
    describe a layout the file no longer has, and the spec calls the key
    authoritative)."""

    def test_sort_and_extract_leave_a_plain_file(self, tmp_path, cells):
        pyramid = tmp_path / "pyr.parquet"
        create_overview_file(str(cells), str(pyramid), levels="8,7")
        sorted_out = tmp_path / "sorted.parquet"
        read(str(pyramid)).sort_hilbert().write(str(sorted_out))
        assert OVERVIEWS_KEY.encode() not in (pq.read_schema(sorted_out).metadata or {})
        assert "level" in pq.read_schema(sorted_out).names


def test_ops_twin_forwards_every_knob(tmp_path, cells, monkeypatch):
    seen = {}

    def fake(input_parquet, overview_out, **kwargs):
        seen.update(kwargs)
        return overview_out

    monkeypatch.setattr(run_module, "create_overview_file", fake)
    ops.create_overview_file(
        str(cells),
        "o.parquet",
        levels="8",
        cell_detail=3,
        explicit_gsd=None,
        output_dir="d",
        force=True,
    )
    assert seen["cell_detail"] == 3 and seen["output_dir"] == "d" and seen["force"] is True
