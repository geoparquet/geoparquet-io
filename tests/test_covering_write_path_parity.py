"""Four write paths, one answer to the covering question (#1172).

Every gpio write ends at one of four places, and each used to decide the
``covering`` question for itself:

* the **Arrow** block builders (``in-memory``, ``streaming``,
  ``write_geoparquet_table``) -- covered by
  ``tests/test_covering_bbox_field_order.py``, whose every-path slice is the
  other half of this issue;
* ``Table.write``, which hands a strategy an Arrow table and *no*
  ``original_metadata``, so three of the four strategies could not see what the
  table's own block declared;
* the 2.0 **fast path**, a plain DuckDB ``COPY`` whose generated ``geo`` key
  carries no covering at all, so the input's block is substituted -- unless the
  substitute was judged too thin to stand in, which happens exactly when a
  caller invalidated the derived stats;
* the **stdout** Arrow IPC stream, which carries the input's block forward and
  ran none of the gates a file write runs.

What each class pins is the *parity*: the same input, through any of them, comes
out declaring the same covering.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

from tests.fix_output_oracle import covering_of, run_cli, spec_failures

#: Every strategy ``Table.write`` accepts, default first.
STRATEGIES = ["duckdb-kv", "streaming", "disk-rewrite", "in-memory"]


def geo_block_of(path: Path) -> dict:
    meta = pq.read_metadata(str(path)).metadata or {}
    return json.loads(meta[b"geo"].decode("utf-8")) if b"geo" in meta else {}


def bbox_covering_column(path: Path) -> str | None:
    """The column the output's primary ``covering.bbox`` names, or None."""
    covering = covering_of(path) or {}
    bbox = covering.get("bbox")
    return bbox["xmin"][0] if isinstance(bbox, dict) and "xmin" in bbox else None


def bbox_covering_columns(path: Path) -> set[str]:
    """Every column named by a ``covering.bbox`` anywhere in the output's block.

    ``covering_of`` reads one column, and the primary by default; a defect on a
    SECONDARY column's entry is invisible to it (#953/#1035). Nothing in
    ``geo["columns"]`` may name a struct the spec forbids, so the assertion is
    over all of them.
    """
    found = set()
    for col_meta in (geo_block_of(path).get("columns") or {}).values():
        bbox = ((col_meta or {}).get("covering") or {}).get("bbox")
        if isinstance(bbox, dict) and "xmin" in bbox:
            found.add(bbox["xmin"][0])
    return found


# ---------------------------------------------------------------------------
# Table.write
# ---------------------------------------------------------------------------


class TestTableWriteKeepsADeclaredCovering:
    """``Table.write`` hands the strategy a table and ``original_metadata=None``.

    ``in-memory`` reads the covering off the table's own schema anyway; the other
    three rebuilt the block from nothing and could only fall back to the one
    self-evident name, so a covering over any other column was lost on the
    DEFAULT strategy.
    """

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_the_documented_add_bbox_call_keeps_its_column(
        self, buildings_test_file, strategy, tmp_path
    ):
        """``docs/guide/add.md``: ``add_bbox(column_name='bounds').write(...)``."""
        import geoparquet_io as gpio

        out = tmp_path / f"{strategy}.parquet"
        gpio.read(str(buildings_test_file)).add_bbox(column_name="bounds").write(
            str(out), write_strategy=strategy
        )

        assert bbox_covering_column(out) == "bounds"
        assert spec_failures(out) == {}

    def test_the_default_strategy_keeps_it(self, buildings_test_file, tmp_path):
        """The documented call says nothing about a strategy, so the default decides."""
        import geoparquet_io as gpio

        out = tmp_path / "default.parquet"
        gpio.read(str(buildings_test_file)).add_bbox(column_name="bounds").write(str(out))

        assert bbox_covering_column(out) == "bounds"
        assert spec_failures(out) == {}

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_a_gdal_style_covering_survives(self, gdal_style_covering, strategy, tmp_path):
        """GDAL writes ``geometry_bbox``; nothing but the input's block vouches for it."""
        import geoparquet_io as gpio

        out = tmp_path / f"gdal_{strategy}.parquet"
        gpio.read(str(gdal_style_covering)).write(str(out), write_strategy=strategy)

        assert bbox_covering_column(out) == "geometry_bbox"
        assert spec_failures(out) == {}

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_an_index_entry_beside_it_survives_too(self, quadkey_file, strategy, tmp_path):
        """``covering`` holds one entry per kind; carrying it must not lose the others."""
        import geoparquet_io as gpio

        out = tmp_path / f"qk_{strategy}.parquet"
        gpio.read(str(quadkey_file)).write(str(out), write_strategy=strategy)

        covering = covering_of(out) or {}
        assert set(covering) == {"bbox", "quadkey"}, covering
        assert spec_failures(out) == {}

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_an_illegal_declared_covering_is_still_dropped(
        self, overture_order_covering, strategy, tmp_path
    ):
        """Carrying the input's block is not a way around the struct-shape gate."""
        import geoparquet_io as gpio

        out = tmp_path / f"illegal_{strategy}.parquet"
        gpio.read(str(overture_order_covering)).write(str(out), write_strategy=strategy)

        assert covering_of(out) is None
        assert spec_failures(out) == {}


# ---------------------------------------------------------------------------
# The 2.0 fast path
# ---------------------------------------------------------------------------


class TestTheFastPathKeepsALegalCoveringAfterInvalidation:
    """A 2.0 input with a declared bbox covering, written by a row filter.

    The filter invalidates the carried stats, which strips the primary's
    ``geometry_types`` -- and the fast path's carry declined on exactly that,
    *before* the covering step ran. DuckDB's own bare block went out instead, and
    ``gpio check bbox`` then reported an undeclared bbox column.
    """

    @pytest.mark.parametrize(
        ("flag", "value"),
        [("--limit", "10"), ("--where", "name IS NOT NULL"), ("--bbox", "-180,-90,180,90")],
        ids=["limit", "where", "bbox"],
    )
    def test_extract_keeps_it(self, v2_with_declared_covering, flag, value, tmp_path):
        out = tmp_path / "extracted.parquet"

        run_cli("extract", "geoparquet", v2_with_declared_covering, out, flag, value)

        assert bbox_covering_column(out) == "bbox"
        assert "Point" in geo_block_of(out)["columns"]["geometry"]["geometry_types"]
        assert spec_failures(out) == {}

    def test_check_bbox_stops_calling_the_column_undeclared(
        self, v2_with_declared_covering, tmp_path
    ):
        """The user-visible symptom, before and after."""
        out = tmp_path / "extracted.parquet"
        run_cli("extract", "geoparquet", v2_with_declared_covering, out, "--limit", "10")

        output = run_cli("check", "bbox", out)

        assert "not declared in 'covering'" not in output, output

    @pytest.mark.parametrize(
        ("command", "extra"),
        [("quadkey", ["--auto"]), ("kdtree", ["--partitions", "2"])],
    )
    def test_partition_keeps_it(self, v2_with_declared_covering, command, extra, tmp_path):
        destination = tmp_path / command
        run_cli("partition", command, v2_with_declared_covering, destination, *extra)

        written = sorted(destination.rglob("*.parquet"))
        assert written, "the partition wrote nothing, so nothing is measured"
        for part in written:
            assert bbox_covering_column(part) == "bbox", part
            assert spec_failures(part) == {}, part

    def test_an_unfiltered_copy_keeps_it_too(self, v2_with_declared_covering, tmp_path):
        """The control: nothing invalidated, so the carry never declined here."""
        out = tmp_path / "copy.parquet"
        run_cli("extract", "geoparquet", v2_with_declared_covering, out)

        assert bbox_covering_column(out) == "bbox"

    @pytest.mark.parametrize("strategy", ["streaming", "in-memory", "disk-rewrite"])
    def test_write_memory_is_warned_not_refused(
        self, v2_with_declared_covering, strategy, tmp_path
    ):
        """Keeping the covering routes the write; the user's flags did not change.

        Taking the rewrite to keep the input's covering is a decision the funnel
        makes, so the memory-limit guard it lands in must behave as it does for
        the other funnel-made reroute (1.1-geoarrow): warn and drop the limit.
        Raising instead turned `extract --limit N --write-strategy streaming
        --write-memory 2GB` of a 2.0 input -- exit 0 before the carry existed --
        into exit 2.
        """
        out = tmp_path / f"wm_{strategy}.parquet"

        output = run_cli(
            "extract",
            "geoparquet",
            v2_with_declared_covering,
            out,
            "--limit",
            "10",
            "--write-strategy",
            strategy,
            "--write-memory",
            "2GB",
        )

        assert "--write-memory is ignored" in output, output
        assert bbox_covering_column(out) == "bbox"
        assert spec_failures(out) == {}

    def test_a_rewrite_the_user_asked_for_still_refuses_it(
        self, v2_with_declared_covering, tmp_path
    ):
        """The control: `--geoparquet-version 1.1` is the user's own rewrite.

        Nothing the funnel decided put this write on the streaming strategy, so
        the flag combination is a real error and stays one.
        """
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        out = tmp_path / "asked.parquet"
        result = CliRunner().invoke(
            cli,
            [
                "extract",
                "geoparquet",
                str(v2_with_declared_covering),
                str(out),
                "--limit",
                "10",
                "--geoparquet-version",
                "1.1",
                "--write-strategy",
                "streaming",
                "--write-memory",
                "2GB",
            ],
        )

        assert result.exit_code == 2, result.output
        assert "only supported with the 'duckdb-kv' write strategy" in result.output


# ---------------------------------------------------------------------------
# A secondary column's illegal covering
# ---------------------------------------------------------------------------


class TestASecondaryColumnsIllegalCoveringIsGatedEverywhere:
    """The struct-shape gate is per *column*, and every path has to run it.

    Each DuckDB path's declare step (``declare_carried_bbox_column``) is scoped
    to the primary, so a SECONDARY column's ``covering`` over an Overture-order
    struct was never judged at all: the fast path and the two DuckDB rewrite
    strategies wrote it verbatim and ``gpio check spec`` then failed the file
    gpio had just written (#1035/#1172). The Arrow builders ran the gate over
    every column already, which is the parity the four cases below pin.
    """

    ILLEGAL_STRUCT = "boundary_extent"

    def test_the_fast_path_drops_it(self, v2_with_illegal_secondary_covering, tmp_path):
        """A 2.0 unfiltered extract: DuckDB's block is replaced by the input's."""
        out = tmp_path / "fast.parquet"
        run_cli("extract", "geoparquet", v2_with_illegal_secondary_covering, out)

        assert self.ILLEGAL_STRUCT not in bbox_covering_columns(out)
        assert spec_failures(out) == {}

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_every_rewrite_strategy_drops_it(
        self, v2_with_illegal_secondary_covering, strategy, tmp_path
    ):
        """1.1 output, so every strategy rebuilds the block from the input's."""
        out = tmp_path / f"{strategy}.parquet"
        run_cli(
            "extract",
            "geoparquet",
            v2_with_illegal_secondary_covering,
            out,
            "--geoparquet-version",
            "1.1",
            "--write-strategy",
            strategy,
        )

        assert self.ILLEGAL_STRUCT not in bbox_covering_columns(out)
        assert spec_failures(out) == {}


# ---------------------------------------------------------------------------
# The stdout Arrow IPC stream
# ---------------------------------------------------------------------------


class TestTheStdoutStreamIsGatedLikeAFileWrite:
    """A stream is read, and persisted, exactly like the file a gpio write makes.

    ``geopandas.read_parquet`` indexes ``covering["bbox"]["xmin"][0]`` unguarded,
    so a covering whose only member is a spatial-index entry makes the file
    unopenable (#954). Every gpio *file* write re-gates that shape; the stream
    carried it through.
    """

    @staticmethod
    def _stream(monkeypatch, input_file, **kwargs) -> pa.Table:
        from geoparquet_io.core.extract import extract

        buffer = io.BytesIO()
        fake_stdout = mock.MagicMock()
        fake_stdout.buffer = buffer
        fake_stdout.isatty.return_value = False
        monkeypatch.setattr(sys, "stdout", fake_stdout)
        extract(str(input_file), "-", **kwargs)
        return ipc.open_stream(io.BytesIO(buffer.getvalue())).read_all()

    @staticmethod
    def _covering(table: pa.Table) -> dict | None:
        geo = json.loads((table.schema.metadata or {})[b"geo"].decode("utf-8"))
        column = (geo.get("columns") or {}).get(geo.get("primary_column")) or {}
        return column.get("covering")

    def test_a_projection_that_leaves_only_an_index_entry_drops_the_covering(
        self, quadkey_file, monkeypatch
    ):
        table = self._stream(monkeypatch, quadkey_file, exclude_cols="bbox")

        assert self._covering(table) is None

    def test_the_persisted_stream_is_readable(self, quadkey_file, monkeypatch, tmp_path):
        """The reason it matters: a stream is something people write to a file."""
        geopandas = pytest.importorskip("geopandas")

        table = self._stream(monkeypatch, quadkey_file, exclude_cols="bbox")
        persisted = tmp_path / "persisted.parquet"
        pq.write_table(table, str(persisted))

        assert len(geopandas.read_parquet(str(persisted))) == table.num_rows

    def test_a_covering_that_keeps_its_bbox_member_is_untouched(self, quadkey_file, monkeypatch):
        """The control: the gate drops the unreadable shape, not every covering."""
        table = self._stream(monkeypatch, quadkey_file)

        assert set(self._covering(table) or {}) == {"bbox", "quadkey"}

    def test_an_illegal_struct_is_not_streamed_as_a_covering_either(
        self, overture_order_covering, monkeypatch
    ):
        table = self._stream(monkeypatch, overture_order_covering)

        assert self._covering(table) is None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def buildings_test_file() -> Path:
    """1.0, CRS84, 42 rows, and -- the point here -- no bbox column of its own."""
    return Path("tests/data/buildings_test.parquet")


def _rewrite_covering(source: Path, target: Path, covering: dict | None, **columns) -> Path:
    """``source`` with the primary column's ``covering`` replaced."""
    table = pq.read_table(str(source))
    metadata = dict(table.schema.metadata or {})
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    col_meta = geo["columns"][geo["primary_column"]]
    if covering is None:
        col_meta.pop("covering", None)
    else:
        col_meta["covering"] = covering
    col_meta.update(columns)
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    pq.write_table(table.replace_schema_metadata(metadata), str(target))
    return target


@pytest.fixture
def gdal_style_covering(tmp_path) -> Path:
    """A ``geometry_bbox`` struct and a covering naming it -- what GDAL writes.

    Nothing but the input's own block vouches for that name: it is not the one
    self-evident ``bbox``, so a write that cannot see the block cannot declare it.
    """
    table = pq.read_table("tests/data/places_test.parquet")
    index = table.schema.get_field_index("bbox")
    table = table.set_column(index, "geometry_bbox", table.column("bbox"))
    metadata = dict(table.schema.metadata or {})
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    geo["version"] = "1.1.0"
    geo["columns"][geo["primary_column"]]["covering"] = {
        "bbox": {axis: ["geometry_bbox", axis] for axis in ("xmin", "ymin", "xmax", "ymax")}
    }
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    target = tmp_path / "gdal.parquet"
    pq.write_table(table.replace_schema_metadata(metadata), str(target))
    assert spec_failures(target) == {}, "the fixture must be clean, or nothing below is proved"
    return target


@pytest.fixture
def quadkey_file(tmp_path) -> Path:
    """A bbox covering with a ``quadkey`` index entry beside it."""
    target = tmp_path / "quadkey.parquet"
    run_cli("add", "quadkey", "tests/data/places_test.parquet", target)
    assert set(covering_of(target) or {}) == {"bbox", "quadkey"}
    return target


@pytest.fixture
def overture_order_covering(tmp_path) -> Path:
    """1.1, a covering declared over Overture's ``xmin, xmax, ymin, ymax`` struct."""
    target = tmp_path / "overture_declared.parquet"
    table = pq.read_table("tests/data/country_partition/Honduras.parquet")
    pq.write_table(table, str(target))
    assert "covering_bbox_structure_geometry" in spec_failures(target)
    return target


@pytest.fixture
def v2_with_illegal_secondary_covering(tmp_path) -> Path:
    """2.0, a SECONDARY geometry column, and its covering over an illegal struct.

    The #953 shape carrying #1035's defect on the secondary: primary Point
    ``geometry``, secondary Polygon ``boundary``, and a ``boundary_extent``
    struct whose fields run ``xmin, xmax, ymin, ymax`` -- Overture's order, which
    a 1.1 ``covering`` may not point at. The primary declares nothing, so every
    primary-scoped step leaves the secondary's entry untouched.

    The entry is footer-patched on at the end, after gpio has written the 2.0
    file: declaring it on the 1.1 source instead would let the conversion strip
    it, and the fixture would then prove nothing.
    """
    from geoparquet_io.core.parquet_footer import patch_footer_kv
    from tests.fixtures.multi_geometry import create_multi_geometry_with_secondary_bbox

    source = tmp_path / "multi_11.parquet"
    create_multi_geometry_with_secondary_bbox(str(source), bbox_name="boundary_extent")

    # The helper writes the spec's order; Overture's is what gpio must refuse.
    table = pq.read_table(str(source))
    index = table.schema.get_field_index("boundary_extent")
    struct = pa.concat_arrays(table.column(index).chunks)
    overture_order = ["xmin", "xmax", "ymin", "ymax"]
    table = table.set_column(
        index,
        "boundary_extent",
        pa.StructArray.from_arrays(
            [struct.field(name) for name in overture_order], names=overture_order
        ),
    )
    pq.write_table(table, str(source))
    assert spec_failures(source) == {}, "nothing declares the struct yet, so the source is clean"

    v2 = tmp_path / "multi_20.parquet"
    run_cli("extract", "geoparquet", source, v2, "--geoparquet-version", "2.0")
    assert "boundary_extent" in pq.read_schema(str(v2)).names

    geo = geo_block_of(v2)
    geo["columns"]["boundary"]["covering"] = {
        "bbox": {axis: ["boundary_extent", axis] for axis in ("xmin", "ymin", "xmax", "ymax")}
    }
    target = tmp_path / "multi_20_declared.parquet"
    patch_footer_kv(str(v2), {"geo": json.dumps(geo)}, output_file=str(target))

    assert set(spec_failures(target)) == {"covering_bbox_structure_boundary"}, (
        "the fixture must carry exactly the defect under test"
    )
    return target


@pytest.fixture
def v2_with_declared_covering(tmp_path) -> Path:
    """GeoParquet 2.0, a bbox column, and a covering the file itself declares."""
    v2 = tmp_path / "v2.parquet"
    run_cli(
        "convert",
        "geoparquet",
        "tests/data/places_test.parquet",
        v2,
        "--geoparquet-version",
        "2.0",
    )
    target = tmp_path / "v2_declared.parquet"
    run_cli("add", "bbox", v2, target)
    assert geo_block_of(target)["version"].startswith("2.")
    assert bbox_covering_column(target) == "bbox"
    return target
