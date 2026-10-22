"""The ``gpio process`` group's ZSTD level, and the global default it differs from (#1299).

Two separate facts are pinned here.

The first is the group's own default. Benchmarking ``process simplify``,
``polygonize`` and ``contour`` after #1195 measured ZSTD 15 -- the global
default -- at 67-90% of the wall clock on the raster commands, for output no
smaller than level 9 gives: 9 writes 2.8-8.2x faster for at most 4.9% more
bytes, and on a 217k-row polygonize table it is *both* faster and smaller.
So the process group resolves an unset ZSTD level to 9, and every other
command keeps 15.

The second is that an unset level must reach pyarrow as a number at all.
``_rewrite_writer_kwargs`` used to omit the key, which is not "the default" --
it is pyarrow's own ZSTD default of 1, 14 levels below what the funnel writes
from the same data. These tests fail if either default drifts, in either
direction.

Both are checked on the level that reaches pyarrow, not on output size. Size
ordering does not hold at fixture scale: on a few thousand near-identical
triangle WKBs, level 9 writes a *larger* file than level 1 (23.9 vs 20.6 KB
here), the same inversion the benchmark found at level 15 on a 217k-row
polygonize table. The size argument for 9 is about real data, and asserting it
on synthetic data would pin the wrong fact.
"""

from importlib.util import find_spec

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.compression import (
    PROCESS_ZSTD_LEVEL,
    resolve_process_compression_level,
    validate_compression_settings,
)
from geoparquet_io.core.derive_geo_from_file import _rewrite_writer_kwargs
from geoparquet_io.core.parquet_writer import ParquetWriteSettings

requires_coarsen = pytest.mark.skipif(
    find_spec("coarsen") is None, reason="requires the simplify extra (coarsen)"
)

# A unit-square polygon, so the dependency-free tests need no shapely.
_WKB_SQUARE = bytes.fromhex(
    "010300000001000000050000000000000000000000000000000000000000000000000000"
    "00000000000000f03f000000000000f03f000000000000f03f000000000000f03f000000"
    "000000000000000000000000000000000000000000"
)


def _wkb_table(rows=3):
    """A minimal WKB table the write funnel accepts."""
    return pa.table(
        {
            "geometry": pa.array([_WKB_SQUARE] * rows, type=pa.binary()),
            "value": pa.array([float(i) for i in range(rows)], type=pa.float64()),
        }
    )


@pytest.fixture
def levels(monkeypatch):
    """Every ZSTD level that reaches pyarrow during the test, in call order.

    Spies on both write entry points -- ``pq.write_table`` for the in-memory
    funnel and ``pq.ParquetWriter`` for the streaming one -- because which of
    them a process command uses is exactly what this file must not depend on.
    A missing ``compression_level`` is recorded as ``None``, which is the bug
    the second half of #1299 is about, not an absence of evidence.
    """
    seen: list[int | None] = []
    real_write_table = pq.write_table
    real_writer = pq.ParquetWriter

    def spy_write_table(table, where, **kwargs):
        seen.append(kwargs.get("compression_level"))
        return real_write_table(table, where, **kwargs)

    class SpyWriter(real_writer):
        def __init__(self, where, schema, **kwargs):
            seen.append(kwargs.get("compression_level"))
            super().__init__(where, schema, **kwargs)

    monkeypatch.setattr(pq, "write_table", spy_write_table)
    monkeypatch.setattr(pq, "ParquetWriter", SpyWriter)
    return seen


class TestResolver:
    """``resolve_process_compression_level`` -- the group's one decision."""

    def test_unset_zstd_becomes_the_process_level(self):
        assert resolve_process_compression_level("ZSTD", None) == PROCESS_ZSTD_LEVEL

    def test_the_process_level_is_nine(self):
        # Pinned deliberately: the number is the measurement, and moving it is
        # a decision to re-measure, not a refactor.
        assert PROCESS_ZSTD_LEVEL == 9

    @pytest.mark.parametrize("level", [1, 3, 9, 15, 22])
    def test_an_explicit_level_is_never_touched(self, level):
        assert resolve_process_compression_level("ZSTD", level) == level

    def test_codec_case_does_not_matter(self):
        assert resolve_process_compression_level("zstd", None) == PROCESS_ZSTD_LEVEL

    @pytest.mark.parametrize("codec", ["GZIP", "BROTLI", "SNAPPY", "LZ4", "UNCOMPRESSED", "", None])
    def test_only_zstd_moves(self, codec):
        # The measurement is ZSTD's. GZIP and BROTLI have their own curves and
        # their own defaults in validate_compression_settings; nothing here
        # claims to have measured them.
        assert resolve_process_compression_level(codec, None) is None


class TestGlobalDefaultIsUnchanged:
    """15 stays the answer for every write that is not a process write."""

    def test_validate_still_resolves_an_unset_zstd_level_to_fifteen(self):
        _, level, desc = validate_compression_settings("ZSTD", None)
        assert level == 15
        assert desc == "ZSTD:15"

    def test_the_write_facade_still_defaults_to_fifteen(self):
        assert ParquetWriteSettings.DEFAULT_COMPRESSION_LEVEL == 15
        assert ParquetWriteSettings().compression_level == 15

    def test_a_plain_funnel_write_lands_on_fifteen(self, tmp_path, levels):
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        write_geoparquet_table(_wkb_table(), str(tmp_path / "out.parquet"))
        assert levels == [15]

    def test_the_process_level_is_below_the_global_one(self):
        _, global_level, _ = validate_compression_settings("ZSTD", None)
        assert PROCESS_ZSTD_LEVEL < global_level

    def test_a_levelless_codec_warns_only_about_a_level_the_caller_chose(self, caplog):
        """The warning compares against the global ZSTD default, from one place.

        Callers that pass a level unconditionally hand SNAPPY the ZSTD default,
        which is not something to warn about; anything else is.
        """
        import logging

        with caplog.at_level(logging.WARNING):
            _, level, desc = validate_compression_settings("SNAPPY", 9, verbose=True)
        assert level is None and desc == "SNAPPY"
        assert "does not support compression levels" in caplog.text

        caplog.clear()
        with caplog.at_level(logging.WARNING):
            validate_compression_settings("SNAPPY", 15, verbose=True)
        assert caplog.text == ""


class TestRewriteWriterKwargs:
    """An unset level must not fall through to pyarrow's own default of 1."""

    def test_unset_zstd_is_resolved_not_omitted(self):
        # pyarrow's default is 1 (an explicit 1 gives a byte-identical file),
        # so omitting the key silently wrote 14 levels below the funnel.
        assert _rewrite_writer_kwargs("ZSTD", None)["compression_level"] == 15

    def test_an_explicit_level_survives(self):
        assert _rewrite_writer_kwargs("ZSTD", 3)["compression_level"] == 3

    @pytest.mark.parametrize("codec", ["SNAPPY", "LZ4", "UNCOMPRESSED", "NONE"])
    def test_a_levelless_codec_gets_no_level(self, codec):
        assert "compression_level" not in _rewrite_writer_kwargs(codec, None)

    @pytest.mark.parametrize("codec", ["GZIP", "BROTLI"])
    def test_unset_gzip_and_brotli_resolve_to_their_codec_default(self, codec):
        # Deliberate: these now get gpio's documented default of 6 instead of
        # pyarrow's own implicit default, the same unset-means-the-documented-
        # number rule as ZSTD. Nothing has measured other levels for them.
        assert _rewrite_writer_kwargs(codec, None)["compression_level"] == 6

    def test_the_codec_still_round_trips(self):
        assert _rewrite_writer_kwargs("SNAPPY", None)["compression"] == "snappy"
        assert _rewrite_writer_kwargs("ZSTD", None)["compression"] == "zstd"
        assert _rewrite_writer_kwargs("UNCOMPRESSED", None)["compression"] == "none"


class TestVecorelSchemaFixLevel:
    """The in-place vecorel rewrite must not undo the file's level.

    ``_fix_vecorel_schema`` detects the codec from the file but Parquet
    footers carry no level, so an omitted ``compression_level`` silently fell
    to pyarrow's ZSTD 1 — undoing the level of the write it was fixing.
    """

    def _nullable_id_file(self, tmp_path):
        table = pa.table({"id": pa.array(["a", "b"], type=pa.string())})
        path = tmp_path / "vecorel.parquet"
        pq.write_table(table, str(path), compression="ZSTD")
        return str(path)

    def test_an_unset_level_resolves_to_the_codec_default(self, tmp_path, levels):
        from geoparquet_io.core.constants import _fix_vecorel_schema

        path = self._nullable_id_file(tmp_path)
        levels.clear()
        _fix_vecorel_schema(path, ["id"])
        assert levels == [15]

    def test_a_caller_resolved_level_passes_through(self, tmp_path, levels):
        from geoparquet_io.core.constants import _fix_vecorel_schema

        path = self._nullable_id_file(tmp_path)
        levels.clear()
        _fix_vecorel_schema(path, ["id"], compression_level=PROCESS_ZSTD_LEVEL)
        assert levels == [PROCESS_ZSTD_LEVEL]


class TestVecorelMetadataRewriteCodec:
    """The vecorel metadata rewrite keeps the caller's codec.

    Passing the settings through only when a level was set re-encoded a GZIP
    output as ZSTD on this one rewrite (the funnel's defaults took over).
    """

    def test_the_requested_codec_reaches_the_rewrite(self, tmp_path, monkeypatch):
        import geoparquet_io.core.add.geometry_metrics as gm
        import geoparquet_io.core.duckdb_utils as du

        src = tmp_path / "in.parquet"
        pq.write_table(pa.table({"id": ["a"]}), str(src), compression="GZIP")
        seen: dict = {}

        def fake_write(con, query, out, **kwargs):
            seen.update(kwargs)
            pq.write_table(pa.table({"id": ["a"]}), out, compression="GZIP")

        class FakeCon:
            def close(self):
                pass

        monkeypatch.setattr(gm, "write_parquet_with_metadata", fake_write)
        monkeypatch.setattr(du, "get_duckdb_connection", lambda **k: FakeCon())
        gm._add_vecorel_metadata_to_file(
            str(src), False, None, compression="GZIP", compression_level=None
        )
        assert seen["compression"] == "GZIP"
        assert seen["compression_level"] is None


class TestHelpTextNamesTheRightDefault:
    """``--help`` has to name the number the writers actually use.

    Both are imported rather than typed into the help string, so this is a
    check that the right *decorator* is on each command, not a spell-check.
    """

    def _help(self, args):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        result = CliRunner().invoke(cli, args + ["--help"])
        assert result.exit_code == 0, result.output
        return " ".join(result.output.split())

    @pytest.mark.parametrize(
        "args",
        [
            ["process", "simplify"],
            ["process", "polygonize"],
            ["process", "contour"],
            ["process", "overview"],
            ["process", "aggregate", "h3"],
            ["process", "aggregate", "a5"],
            ["process", "aggregate", "admin"],
        ],
    )
    def test_process_commands_name_the_process_level(self, args):
        text = self._help(args)
        assert f"ZSTD: 1-22 (default: {PROCESS_ZSTD_LEVEL})" in text
        assert "ZSTD: 1-22 (default: 15)" not in text

    @pytest.mark.parametrize(
        "args", [["convert", "geoparquet"], ["sort", "hilbert"], ["add", "bbox"]]
    )
    def test_other_commands_still_name_fifteen(self, args):
        text = self._help(args)
        assert "ZSTD: 1-22 (default: 15)" in text


class TestEveryProcessWriterResolves:
    """The ratchet: a new process command cannot forget the group's default.

    Checked statically, because the alternative is one end-to-end test per
    command and the DuckDB-backed ones (aggregate, overview) cost minutes and
    a community extension to run. Any function under ``core/process`` that
    takes a ``compression_level`` either resolves it or is listed below as
    receiving one already resolved by its caller.
    """

    #: Private helpers and inner writers, each reached only from a public
    #: entry in RESOLVERS that has already resolved the level.
    DOWNSTREAM = {
        ("overview/run.py", "_write_overview"),
        ("overview/run.py", "_build_levels"),
        ("simplify.py", "_footer_kv_via_funnel"),
        ("simplify.py", "_simplify_file_streaming"),
        ("simplify.py", "_simplify_then_refresh_metrics"),
    }

    def _writers(self):
        import ast
        from pathlib import Path

        root = Path(__file__).resolve().parents[1] / "geoparquet_io" / "core" / "process"
        found = {}
        for path in sorted(root.rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef):
                    continue
                args = [a.arg for a in node.args.args + node.args.kwonlyargs]
                if "compression_level" not in args:
                    continue
                body = ast.get_source_segment(source, node) or ""
                key = (str(path.relative_to(root)).replace("\\", "/"), node.name)
                found[key] = "resolve_process_compression_level(" in body
        return found

    def test_every_writer_is_accounted_for(self):
        writers = self._writers()
        assert writers, "found no process writers at all -- the walk is broken"
        for key, resolves in sorted(writers.items()):
            if key in self.DOWNSTREAM:
                assert not resolves, (
                    f"{key[0]}:{key[1]} is listed as downstream but resolves; "
                    "move it out of DOWNSTREAM"
                )
            else:
                assert resolves, (
                    f"{key[0]}:{key[1]} takes a compression_level but never calls "
                    "resolve_process_compression_level. Add the call at its entry, or "
                    "list it in DOWNSTREAM if its caller already resolved."
                )

    def test_the_known_entry_points_are_still_there(self):
        """Guards the walk itself: a rename must not silently empty it."""
        writers = self._writers()
        for key in [
            ("simplify.py", "simplify_file"),
            ("raster/polygonize.py", "polygonize_file"),
            ("raster/contour.py", "contour_file"),
            ("aggregate/by_a5.py", "aggregate_by_a5"),
            ("aggregate/by_h3.py", "aggregate_by_h3"),
            ("aggregate/by_admin.py", "aggregate_by_admin"),
            ("aggregate/grid_common.py", "aggregate_grid_file"),
            ("overview/run.py", "create_overviews"),
            ("overview/run.py", "create_overview_file"),
            ("overview/overview_file.py", "write_overview_file"),
        ]:
            assert writers.get(key) is True, f"{key[0]}:{key[1]} no longer resolves"


class TestRasterCommands:
    """polygonize and contour: always in-memory, so always the funnel."""

    def test_polygonize_writes_at_the_process_level(self, tmp_path, levels, monkeypatch):
        from geoparquet_io.core.process.raster import polygonize as mod

        monkeypatch.setattr(mod, "polygonize_raster", lambda *a, **k: _wkb_table())
        mod.polygonize_file("in.tif", str(tmp_path / "out.parquet"))
        assert levels == [PROCESS_ZSTD_LEVEL]

    def test_polygonize_honours_an_explicit_level(self, tmp_path, levels, monkeypatch):
        from geoparquet_io.core.process.raster import polygonize as mod

        monkeypatch.setattr(mod, "polygonize_raster", lambda *a, **k: _wkb_table())
        mod.polygonize_file("in.tif", str(tmp_path / "out.parquet"), compression_level=15)
        assert levels == [15]

    def test_contour_writes_at_the_process_level(self, tmp_path, levels, monkeypatch):
        from geoparquet_io.core.process.raster import contour as mod

        monkeypatch.setattr(mod, "contour_raster", lambda *a, **k: _wkb_table())
        mod.contour_file("in.tif", str(tmp_path / "out.parquet"), interval=100.0)
        assert levels == [PROCESS_ZSTD_LEVEL]

    def test_a_levelless_codec_still_gets_no_level(self, tmp_path, levels, monkeypatch):
        from geoparquet_io.core.process.raster import polygonize as mod

        monkeypatch.setattr(mod, "polygonize_raster", lambda *a, **k: _wkb_table())
        mod.polygonize_file("in.tif", str(tmp_path / "out.parquet"), compression="SNAPPY")
        assert levels == [None]


@requires_coarsen
class TestSimplify:
    """Both simplify paths -- the streaming one is the whole point."""

    def _input(self, tmp_path):
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        src = tmp_path / "in.parquet"
        write_geoparquet_table(_wkb_table(rows=8), str(src))
        return str(src)

    def test_streaming_simplify_writes_at_the_process_level(self, tmp_path, levels):
        from geoparquet_io.core.process.simplify import simplify_file

        src = self._input(tmp_path)
        levels.clear()  # drop the fixture write's own level
        simplify_file(src, str(tmp_path / "out.parquet"), 0.01)
        # The streaming writer is the path that used to inherit pyarrow's 1.
        assert levels and all(level == PROCESS_ZSTD_LEVEL for level in levels)
        assert None not in levels

    def test_coverage_simplify_writes_at_the_process_level(self, tmp_path, levels):
        from geoparquet_io.core.process.simplify import simplify_file

        src = self._input(tmp_path)
        levels.clear()
        simplify_file(src, str(tmp_path / "out.parquet"), 0.01, coverage=True)
        assert levels and all(level == PROCESS_ZSTD_LEVEL for level in levels)

    def test_simplify_honours_an_explicit_level(self, tmp_path, levels):
        from geoparquet_io.core.process.simplify import simplify_file

        src = self._input(tmp_path)
        levels.clear()
        simplify_file(src, str(tmp_path / "out.parquet"), 0.01, compression_level=15)
        assert levels and all(level == 15 for level in levels)

    def _metrics_input(self, tmp_path):
        """An input that actually has metric columns, so the detour runs.

        Without them ``_simplify_then_refresh_metrics`` warns "nothing to
        refresh" and does a plain simplify — which is how the first version of
        this test stayed green while the detour's final writes went out at
        DuckDB's 3 and pyarrow's 1.
        """
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        base = _wkb_table(rows=8)
        table = base.append_column(
            "metrics:area", pa.array([1.0] * base.num_rows, type=pa.float64())
        ).append_column("metrics:perimeter", pa.array([4.0] * base.num_rows, type=pa.float64()))
        src = tmp_path / "in_metrics.parquet"
        write_geoparquet_table(table, str(src))
        return str(src)

    def test_the_streamed_level_survives_a_refresh_metrics_detour(
        self, tmp_path, levels, monkeypatch
    ):
        """``--refresh-metrics`` re-enters through another command's core.

        The resolve happens once at ``simplify_file``'s entry, so the temp
        sibling *and* every write ``add geometry-metrics`` performs — its two
        DuckDB COPY passes, the vecorel metadata rewrite, the id column, and
        the schema fix — have to carry the resolved level rather than fall to
        their own defaults on the way through. The pq spy sees the pyarrow
        writes; the DuckDB COPY writes are spied at ``add_computed_column``.
        """
        import geoparquet_io.core.add.geometry_metrics as gm
        import geoparquet_io.core.constants as consts
        from geoparquet_io.core.process.simplify import simplify_file

        duckdb_levels: list[int | None] = []
        real_add = gm.add_computed_column

        def spy_add(*args, **kwargs):
            duckdb_levels.append(kwargs.get("compression_level"))
            return real_add(*args, **kwargs)

        monkeypatch.setattr(gm, "add_computed_column", spy_add)
        monkeypatch.setattr(consts, "add_computed_column", spy_add)
        src = self._metrics_input(tmp_path)
        levels.clear()
        simplify_file(src, str(tmp_path / "out.parquet"), 0.01, refresh_metrics=True)
        assert levels and all(level == PROCESS_ZSTD_LEVEL for level in levels)
        assert duckdb_levels and all(level == PROCESS_ZSTD_LEVEL for level in duckdb_levels)
