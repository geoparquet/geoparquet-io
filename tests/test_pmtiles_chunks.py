"""``gpio pmtiles create --chunks`` (#1116): split, tile per chunk, join, resume."""

import json
import os
import shutil
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import geoparquet_io.core.pmtiles_chunks as pc
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.geo_metadata import OVERVIEWS_KEY, is_levelled_overview
from geoparquet_io.core.pmtiles_chunks import (
    MAX_CHUNK_CELLS,
    chunk_key_sql,
    create_pmtiles_chunked,
    parse_chunks,
)

BUILDINGS = str(Path(__file__).parent / "data" / "buildings_test.parquet")

skip_windows = pytest.mark.skipif(sys.platform == "win32", reason="tippecanoe not on Windows")
needs_tippecanoe = pytest.mark.skipif(
    shutil.which("tippecanoe") is None or shutil.which("tile-join") is None,
    reason="tippecanoe/tile-join not installed",
)


def _write_points(path, xs, ys, bbox_float32=False):
    """Points with an ``id`` each, optionally with a float32 bbox covering that
    rounds the extreme coordinates *inside* the true extent (GDAL's default)."""
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection()
    rows = ", ".join(f"({i}, {x!r}, {y!r})" for i, (x, y) in enumerate(zip(xs, ys, strict=True)))
    con.execute(
        f"COPY (SELECT id, ST_Point(x, y) AS geometry FROM (VALUES {rows}) t(id, x, y)) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    if bbox_float32:
        table = pq.read_table(path)
        f32 = pa.float32()
        bbox = pa.StructArray.from_arrays(
            [pa.array(xs, f32), pa.array(ys, f32), pa.array(xs, f32), pa.array(ys, f32)],
            names=["xmin", "ymin", "xmax", "ymax"],
        )
        table = table.append_column("bbox", bbox)
        geo = json.loads(table.schema.metadata[b"geo"])
        geo["columns"]["geometry"]["covering"] = {
            "bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}
        }
        pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), path)


class TestParseChunks:
    @pytest.mark.parametrize(("spec", "expected"), [("4x3", (4, 3)), (" 2X2 ", (2, 2))])
    def test_grid_forms(self, spec, expected):
        assert parse_chunks(spec) == expected

    @pytest.mark.parametrize("spec", ["4", "4x3x2", "axb", "0x3", "3x0", "-1x2", "auto", ""])
    def test_rejects_anything_but_a_positive_grid(self, spec):
        with pytest.raises(InvalidParameterError, match="chunks"):
            parse_chunks(spec)

    def test_caps_the_cell_count(self):
        """400x300 is a typo for 4x3, not a request for 120,000 tippecanoe runs."""
        with pytest.raises(InvalidParameterError, match=str(MAX_CHUNK_CELLS)):
            parse_chunks("400x300")


class TestChunkKey:
    """The centroid key assigns every row to exactly one cell."""

    def _keys(self, path, bounds, nx, ny):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

        con = get_duckdb_connection()
        try:
            key = chunk_key_sql("geometry", bounds, nx, ny)
            return dict(
                con.execute(
                    f"SELECT {key} AS k, count(*) FROM {sql_path(path)} GROUP BY 1 ORDER BY 1"
                ).fetchall()
            )
        finally:
            con.close()

    def test_grid_line_lands_in_exactly_one_cell(self, tmp_path):
        path = tmp_path / "p.parquet"
        _write_points(path, [0.0, 5.0, 10.0], [0.0, 5.0, 10.0])
        keys = self._keys(path, (0.0, 0.0, 10.0, 10.0), 2, 2)
        assert keys == {"chunk_0_0": 1, "chunk_1_1": 2}
        assert sum(keys.values()) == 3

    def test_centroids_outside_the_bounds_clamp_to_edge_cells(self, tmp_path):
        """A float32 bbox covering rounds 0.1 up and 0.9 down, so the footer
        bounds exclude the extreme points; they must still tile somewhere."""
        path = tmp_path / "p.parquet"
        xs = [0.1, 0.3, 0.5, 0.7, 0.9]
        _write_points(path, xs, xs)
        keys = self._keys(path, (0.10000000149, 0.10000000149, 0.89999997616, 0.89999997616), 2, 2)
        assert sum(keys.values()) == 5
        assert keys["chunk_0_0"] == 2 and keys["chunk_1_1"] == 3

    def test_degenerate_axis_collapses_to_one_cell(self):
        assert chunk_key_sql("g", (1.0, 0.0, 1.0, 10.0), 3, 2).startswith("'chunk_' || CAST(LEAST")
        assert "|| '_' || CAST(0 AS VARCHAR)" in chunk_key_sql("g", (1.0, 0.0, 1.0, 10.0), 3, 2)

    def test_geometry_column_is_quoted(self):
        assert '"geom""; DROP"' in chunk_key_sql('geom"; DROP', (0, 0, 1, 1), 2, 2)


class TestLevelledInputMarker:
    def test_keyed_on_the_footer_not_a_level_column(self):
        schema = pa.schema([("level", pa.int32())])
        assert not is_levelled_overview(schema)
        assert not is_levelled_overview({})
        assert is_levelled_overview(schema.with_metadata({OVERVIEWS_KEY: "{}"}))
        assert is_levelled_overview({OVERVIEWS_KEY: "{}"})
        assert is_levelled_overview({OVERVIEWS_KEY.encode(): b"{}"})


@pytest.fixture
def fake_tools(monkeypatch, tmp_path):
    """Everything below the orchestrator is faked: the split writes one chunk
    file per requested cell name, tiling writes bytes, tile-join writes its
    ``-o``. ``calls`` records what ran."""
    calls = {"split": [], "tiles": [], "joins": []}
    cells = ["chunk_0_0", "chunk_0_1"]

    def fake_split(input_path, parts_dir, **kwargs):
        calls["split"].append(kwargs)
        for name in cells:
            pq.write_table(pa.table({"id": [1]}), os.path.join(parts_dir, f"{name}.parquet"))

    def fake_tiles(chunk, output, **kwargs):
        calls["tiles"].append({"input": chunk, "output": output, **kwargs})
        Path(output).write_bytes(b"pmtiles")

    def fake_join(cmd, verbose):
        calls["joins"].append(cmd)
        Path(cmd[cmd.index("-o") + 1]).write_bytes(b"joined")

    monkeypatch.setattr(pc, "_check_tile_join", lambda: True)
    monkeypatch.setattr(pc, "_split_input", fake_split)
    monkeypatch.setattr(pc, "create_pmtiles_from_geoparquet", fake_tiles)
    monkeypatch.setattr(pc, "_run_tile_join", fake_join)
    calls["cells"] = cells
    return calls


class _failing_join:
    """Context: tile-join raises, so parts stay on disk for the next run."""

    def __init__(self, monkeypatch):
        self.monkeypatch = monkeypatch

    def __enter__(self):
        self.previous = pc._run_tile_join

        def fail(cmd, verbose):
            raise RuntimeError("tile-join failed with exit code 101")

        self.monkeypatch.setattr(pc, "_run_tile_join", fail)

    def __exit__(self, *exc):
        self.monkeypatch.setattr(pc, "_run_tile_join", self.previous)
        return False


def _kw(**kwargs):
    return kwargs


def _run(tmp_path, out, **overrides):
    kwargs = _kw(
        bbox=None,
        where=None,
        include_cols=None,
        layer=None,
        attribution=None,
        force=False,
        verbose=False,
        profile=None,
        tiling={"max_zoom": 12, "min_zoom": None, "temporary_directory": str(tmp_path)},
    )
    kwargs.update(overrides)
    chunks = kwargs.pop("chunks", "2x1")
    return create_pmtiles_chunked(BUILDINGS, str(out), chunks, **kwargs)


class TestGuards:
    def test_bbox_is_refused(self, tmp_path):
        with pytest.raises(InvalidParameterError, match="gpio extract --bbox"):
            _run(tmp_path, tmp_path / "o.pmtiles", bbox="0,0,1,1")

    def test_max_zoom_is_required(self, tmp_path):
        """``-zg`` per chunk gives the sparse chunks no high-zoom tiles."""
        with pytest.raises(InvalidParameterError, match="max-zoom"):
            _run(tmp_path, tmp_path / "o.pmtiles", tiling={"max_zoom": None})

    def test_existing_output_fails_before_any_tiling(self, tmp_path, fake_tools):
        out = tmp_path / "o.pmtiles"
        out.write_bytes(b"old")
        with pytest.raises(InvalidParameterError, match="already exists"):
            _run(tmp_path, out)
        assert fake_tools["split"] == []

    def test_levelled_overview_input_is_refused(self, tmp_path, fake_tools):
        src = tmp_path / "ov.parquet"
        table = pq.read_table(BUILDINGS)
        meta = {**table.schema.metadata, OVERVIEWS_KEY.encode(): b'{"version":"0.2.0"}'}
        pq.write_table(table.replace_schema_metadata(meta), src)
        with pytest.raises(InvalidParameterError, match="levelled overview"):
            create_pmtiles_chunked(
                str(src), str(tmp_path / "o.pmtiles"), "2x1", **_defaults(tmp_path)
            )

    def test_a_plain_level_column_is_not_an_overview(self, tmp_path, fake_tools):
        """Buildings carry storeys as ``level``; that is not the spec's marker."""
        src = tmp_path / "storeys.parquet"
        table = pq.read_table(BUILDINGS)
        pq.write_table(table.append_column("level", pa.array([1] * table.num_rows)), src)
        create_pmtiles_chunked(str(src), str(tmp_path / "o.pmtiles"), "2x1", **_defaults(tmp_path))
        assert (tmp_path / "o.pmtiles").exists()

    def test_parts_path_that_is_a_symlink_is_refused(self, tmp_path, fake_tools):
        out = tmp_path / "o.pmtiles"
        target = tmp_path / "elsewhere"
        target.mkdir()
        os.symlink(target, f"{out}.parts")
        with pytest.raises(InvalidParameterError, match="not a directory"):
            _run(tmp_path, out)
        assert list(target.iterdir()) == []

    def test_parts_path_that_is_a_file_is_refused(self, tmp_path, fake_tools):
        out = tmp_path / "o.pmtiles"
        Path(f"{out}.parts").write_text("junk")
        with pytest.raises(InvalidParameterError, match="not a directory"):
            _run(tmp_path, out)


def _defaults(tmp_path):
    return _kw(
        bbox=None,
        where=None,
        include_cols=None,
        layer=None,
        attribution=None,
        force=False,
        verbose=False,
        profile=None,
        tiling={"max_zoom": 12, "temporary_directory": str(tmp_path)},
    )


class TestOrchestration:
    def test_split_tile_join_and_clean_up(self, tmp_path, fake_tools):
        out = tmp_path / "out.pmtiles"
        _run(tmp_path, out, chunks="2x1", attribution="<a>x</a>")

        assert out.read_bytes() == b"joined"
        assert not os.path.exists(f"{out}.parts")
        (split,) = fake_tools["split"]
        assert "chunk_" in split["key_sql"] and "ST_Centroid" in split["key_sql"]
        tiles = fake_tools["tiles"]
        assert [Path(t["input"]).name for t in tiles] == [
            f"{c}.parquet" for c in fake_tools["cells"]
        ]
        # Tiled under a temporary name; the final name means "complete".
        assert all(t["output"].endswith(".building.pmtiles") for t in tiles)
        assert all(t["layer"] == "out" and t["force"] is True for t in tiles)
        assert all(t["max_zoom"] == 12 for t in tiles)
        (join,) = fake_tools["joins"]
        assert join[join.index("-o") + 1].endswith("joined.pmtiles")
        assert "--name=out" in join and "--attribution=<a>x</a>" in join
        assert [Path(p).name for p in join if p.endswith(".pmtiles") and "joined" not in p] == [
            f"{c}.pmtiles" for c in fake_tools["cells"]
        ]

    def test_layer_by_column_leaves_the_layer_unset(self, tmp_path, fake_tools):
        """Defaulting ``layer`` to the stem made every chunk fail the
        layer/layer_by_column guard for a user who only asked for the latter."""
        _run(tmp_path, tmp_path / "out.pmtiles", tiling={"max_zoom": 12, "layer_by_column": "id"})
        assert all(t["layer"] is None for t in fake_tools["tiles"])
        assert all(t["layer_by_column"] == "id" for t in fake_tools["tiles"])

    def test_force_replaces_an_existing_output(self, tmp_path, fake_tools):
        out = tmp_path / "out.pmtiles"
        out.write_bytes(b"old")
        _run(tmp_path, out, force=True)
        assert out.read_bytes() == b"joined"

    def test_success_leaves_no_parts_but_a_failed_join_keeps_them(
        self, tmp_path, fake_tools, monkeypatch
    ):
        out = tmp_path / "out.pmtiles"
        with _failing_join(monkeypatch), pytest.raises(RuntimeError, match="tile-join"):
            _run(tmp_path, out)
        # No partial archive at the output path, every part kept for resume.
        assert not out.exists()
        parts = sorted(p.name for p in Path(f"{out}.parts").iterdir())
        assert parts == [
            "chunk_0_0.parquet",
            "chunk_0_0.pmtiles",
            "chunk_0_1.parquet",
            "chunk_0_1.pmtiles",
            "manifest.json",
        ]

    def test_resume_reuses_complete_parts_and_ignores_leftovers(
        self, tmp_path, fake_tools, monkeypatch
    ):
        out = tmp_path / "out.pmtiles"
        parts_dir = Path(f"{out}.parts")
        # First run: the join fails after both chunks are tiled.
        with _failing_join(monkeypatch), pytest.raises(RuntimeError):
            _run(tmp_path, out, force=True)
        # Simulate a crash while re-tiling the second chunk: its final part is
        # gone and tippecanoe's in-progress file sits at the temporary name.
        (parts_dir / "chunk_0_1.pmtiles").unlink()
        (parts_dir / "chunk_0_1.building.pmtiles").write_bytes(b"sqlite junk")
        fake_tools["tiles"].clear()
        fake_tools["split"].clear()

        _run(tmp_path, out, force=True)

        assert out.read_bytes() == b"joined"
        assert fake_tools["split"] == [], "the split is not repeated once complete"
        assert [Path(t["input"]).name for t in fake_tools["tiles"]] == ["chunk_0_1.parquet"]

    def test_parts_built_with_other_options_are_refused(self, tmp_path, fake_tools, monkeypatch):
        out = tmp_path / "out.pmtiles"
        with _failing_join(monkeypatch), pytest.raises(RuntimeError):
            _run(tmp_path, out, force=True, chunks="2x1")
        for spec, tiling in [("3x1", {"max_zoom": 12}), ("2x1", {"max_zoom": 14})]:
            with pytest.raises(InvalidParameterError, match="different input, grid"):
                _run(tmp_path, out, force=True, chunks=spec, tiling=tiling)
        with pytest.raises(InvalidParameterError, match="different input, grid"):
            _run(tmp_path, out, force=True, where="id = 1")

    def test_all_chunks_empty_is_an_error_and_leaves_nothing(self, tmp_path, fake_tools):
        fake_tools["cells"].clear()
        out = tmp_path / "out.pmtiles"
        with pytest.raises(RuntimeError, match="contained any features"):
            _run(tmp_path, out)
        assert not os.path.exists(f"{out}.parts")

    def test_manifest_records_what_the_parts_were_built_from(
        self, tmp_path, fake_tools, monkeypatch
    ):
        out = tmp_path / "out.pmtiles"
        with _failing_join(monkeypatch), pytest.raises(RuntimeError):
            _run(tmp_path, out, where="id > 1", include_cols="id")
        manifest = json.loads((Path(f"{out}.parts") / "manifest.json").read_text())
        assert manifest["chunks"] == "2x1"
        assert manifest["where"] == "id > 1"
        assert manifest["include_cols"] == "id"
        assert manifest["input"] == os.path.abspath(BUILDINGS)
        assert manifest["split_complete"] is True
        assert "temporary_directory" not in manifest["tiling"]


class TestSplit:
    """The real single-pass split, no tippecanoe needed."""

    def test_every_row_lands_in_one_chunk_file_with_metadata(self, tmp_path):
        parts_dir = tmp_path / "out.pmtiles.parts"
        parts_dir.mkdir()
        pc._split_input(
            BUILDINGS,
            str(parts_dir),
            geometry_column="geometry",
            key_sql=chunk_key_sql("geometry", tuple(_bounds(BUILDINGS)), 2, 2),
            where=None,
            projection="*",
            scratch=str(tmp_path),
            verbose=False,
        )
        files = sorted(parts_dir.glob("chunk_*.parquet"))
        assert 2 <= len(files) <= 4
        tables = [pq.read_table(f) for f in files]
        assert sum(t.num_rows for t in tables) == pq.read_metadata(BUILDINGS).num_rows
        source = pq.read_schema(BUILDINGS)
        for f, t in zip(files, tables, strict=True):
            assert t.schema.names == source.names, f
            assert b"geo" in t.schema.metadata, f
        assert not list(parts_dir.glob(".staging_*")), "staging removed"

    def test_where_and_include_cols_shape_the_chunk_files(self, tmp_path):
        parts_dir = tmp_path / "out.pmtiles.parts"
        parts_dir.mkdir()
        pc._split_input(
            BUILDINGS,
            str(parts_dir),
            geometry_column="geometry",
            key_sql=chunk_key_sql("geometry", tuple(_bounds(BUILDINGS)), 1, 1),
            where="id LIKE '6%'",
            projection=pc._projection_sql("id", "geometry", None),
            scratch=str(tmp_path),
            verbose=False,
        )
        (only,) = parts_dir.glob("chunk_*.parquet")
        table = pq.read_table(only)
        expected = sum(1 for v in pq.read_table(BUILDINGS).column("id").to_pylist() if v[0] == "6")
        assert 0 < table.num_rows == expected
        assert table.schema.names == ["id", "geometry"]


def _bounds(path):
    from geoparquet_io.core.common import get_dataset_bounds

    return get_dataset_bounds(path, geometry_column="geometry")


class TestSplitDropsUnplaceableRows:
    def test_null_and_empty_geometries_are_left_out_like_the_single_pass(self, tmp_path):
        """A NULL or EMPTY geometry has no centroid to place; the single-pass
        path drops such rows in ``convert geojson`` and the split does too."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        src = tmp_path / "mixed.parquet"
        con = get_duckdb_connection()
        con.execute(
            f"""COPY (
                SELECT 1 AS id, ST_Point(1.0, 1.0) AS geometry
                UNION ALL SELECT 2, ST_Point(9.0, 9.0)
                UNION ALL SELECT 3, NULL::GEOMETRY
                UNION ALL SELECT 4, ST_GeomFromText('POLYGON EMPTY')
            ) TO '{src}' (FORMAT PARQUET)"""
        )
        con.close()
        parts_dir = tmp_path / "out.pmtiles.parts"
        parts_dir.mkdir()
        pc._split_input(
            str(src),
            str(parts_dir),
            geometry_column="geometry",
            key_sql=chunk_key_sql("geometry", (0.0, 0.0, 10.0, 10.0), 2, 2),
            where=None,
            projection="*",
            scratch=str(tmp_path),
            verbose=False,
        )
        files = sorted(f.name for f in parts_dir.glob("chunk_*.parquet"))
        assert files == ["chunk_0_0.parquet", "chunk_1_1.parquet"]
        ids = sorted(
            v for f in parts_dir.glob("chunk_*.parquet") for v in pq.read_table(f)["id"].to_pylist()
        )
        assert ids == [1, 2]
        assert not any(p.name.startswith("__HIVE") for p in parts_dir.iterdir())


class TestProjection:
    def test_include_cols_always_keep_geometry_and_the_layer_column(self):
        assert pc._projection_sql(None, "geometry", None) == "*"
        assert pc._projection_sql("id,name", "geom", "name") == '"id", "name", "geom"'
        assert pc._projection_sql("id", "geometry", "kind") == '"id", "geometry", "kind"'


def _feature_ids(pmtiles_path):
    """Distinct source ids in an archive (via tippecanoe-decode)."""
    import subprocess

    out = subprocess.run(
        ["tippecanoe-decode", "-c", str(pmtiles_path)], capture_output=True, text=True, check=True
    )
    ids = set()
    for line in out.stdout.splitlines():
        line = line.strip().rstrip(",")
        if not line.startswith("{"):
            continue
        props = json.loads(line).get("properties") or {}
        if "id" in props:
            ids.add(str(props["id"]))
    return ids


@needs_tippecanoe
@skip_windows
class TestEndToEnd:
    def test_chunked_equals_single_pass_and_resumes_after_a_failed_join(
        self, tmp_path, monkeypatch, caplog
    ):
        """Chunking is an optimisation, not a change in output: every source
        feature survives exactly once. A failed join keeps the parts and the
        re-run tiles nothing."""
        import logging

        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        whole = tmp_path / "whole.pmtiles"
        chunked = tmp_path / "chunked.pmtiles"
        create_pmtiles_from_geoparquet(BUILDINGS, str(whole), max_zoom=12)
        whole_ids = _feature_ids(whole)
        assert whole_ids

        real_join = pc._run_tile_join
        monkeypatch.setattr(
            pc, "_run_tile_join", lambda cmd, v: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        with pytest.raises(RuntimeError, match="boom"):
            create_pmtiles_from_geoparquet(BUILDINGS, str(chunked), max_zoom=12, chunks="2x2")
        assert not chunked.exists()
        parts = sorted(Path(f"{chunked}.parts").glob("chunk_*.pmtiles"))
        assert parts and not any(p.name.endswith(".building.pmtiles") for p in parts)

        monkeypatch.setattr(pc, "_run_tile_join", real_join)
        with caplog.at_level(logging.DEBUG):
            create_pmtiles_from_geoparquet(BUILDINGS, str(chunked), max_zoom=12, chunks="2x2")

        assert caplog.text.count("reusing") == len(parts)
        assert "tiling" not in caplog.text.replace("Tiling", "")
        assert not os.path.exists(f"{chunked}.parts")
        assert _feature_ids(chunked) == whole_ids

    def test_float32_bbox_covering_loses_no_edge_feature(self, tmp_path):
        """GDAL writes float32 bbox coverings; the footer bounds then exclude
        the extreme centroids, which used to fall outside every cell."""
        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        src = tmp_path / "pts.parquet"
        xs = [0.1, 0.3, 0.5, 0.7, 0.9]
        _write_points(src, xs, xs, bbox_float32=True)
        out = tmp_path / "out.pmtiles"
        create_pmtiles_from_geoparquet(str(src), str(out), max_zoom=6, chunks="2x2")
        assert _feature_ids(out) == {"0", "1", "2", "3", "4"}
