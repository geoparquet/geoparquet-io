"""File-name extent pruning wired into ``gpio extract --bbox`` (issue #1309).

A bbox query over a multi-file glob whose names encode their extent (S2 cell
tokens, quadkeys, lon/lat grid tiles) must open only the overlapping files;
one unrecognized name disables pruning for the whole glob.
"""

from __future__ import annotations

import duckdb

import geoparquet_io.core.extract as extract_module
from geoparquet_io.core.extract import _prune_input_files, extract

NAN_THAILAND = (100.76, 18.77, 100.79, 18.80)

VIDA_S2 = (
    "s3://us-west-2.opendata.source.coop/vida/google-microsoft-osm-open-buildings/"
    "geoparquet/by_country_s2/"
)
VIDA_FILES = [
    VIDA_S2 + "country_iso=THA/3544332906740580352.parquet",
    VIDA_S2 + "country_iso=LAO/3746994889972252672.parquet",
    VIDA_S2 + "country_iso=PER/10466365534009032704.parquet",
    VIDA_S2 + "country_iso=NZL/-6052837899185946624.parquet",
]


def write_point_file(path, lon, lat, rows=3):
    """A tiny GeoParquet file of ``rows`` points around (lon, lat)."""
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"""
            COPY (
                SELECT i AS id, ST_Point({lon} + i * 0.0001, {lat} + i * 0.0001) AS geometry
                FROM range({rows}) t(i)
            ) TO '{path}' (FORMAT PARQUET)
        """)
    finally:
        con.close()


class TestPruneInputFiles:
    def test_mocked_vida_listing_opens_exactly_the_overlapping_paths(self, monkeypatch):
        """A VIDA-style S2-named listing is pruned to the files whose cell
        intersects the bbox - and nothing else."""
        monkeypatch.setattr(extract_module, "_list_partition_files", lambda path: list(VIDA_FILES))
        selected = _prune_input_files(VIDA_S2 + "*/*.parquet", NAN_THAILAND)
        assert selected == [VIDA_FILES[1], VIDA_FILES[0]]  # sorted: LAO, THA

    def test_one_unrecognized_name_disables_pruning(self, monkeypatch):
        files = [*VIDA_FILES, VIDA_S2 + "country_iso=THA/notes.parquet"]
        monkeypatch.setattr(extract_module, "_list_partition_files", lambda path: files)
        assert _prune_input_files(VIDA_S2 + "*/*.parquet", NAN_THAILAND) is None

    def test_single_file_input_never_lists(self, monkeypatch):
        def boom(path):
            raise AssertionError("a single file must not be listed")

        monkeypatch.setattr(extract_module, "_list_partition_files", boom)
        assert _prune_input_files("data/3544332906740580352.parquet", NAN_THAILAND) is None

    def test_no_bbox_disables_pruning(self, monkeypatch):
        monkeypatch.setattr(extract_module, "_list_partition_files", lambda path: list(VIDA_FILES))
        assert _prune_input_files(VIDA_S2 + "*/*.parquet", None) is None

    def test_projected_bbox_disables_pruning(self, monkeypatch):
        # File names encode WGS84 extents; a bbox in projected units cannot be
        # compared against them
        monkeypatch.setattr(extract_module, "_list_partition_files", lambda path: list(VIDA_FILES))
        assert (
            _prune_input_files(VIDA_S2 + "*/*.parquet", (500000, 4000000, 600000, 4100000)) is None
        )

    def test_no_overlap_keeps_one_file_for_the_schema(self, monkeypatch):
        monkeypatch.setattr(extract_module, "_list_partition_files", lambda path: list(VIDA_FILES))
        # The Indian Ocean: no cell overlaps, so one file is kept for a
        # zero-row read instead of opening every footer
        selected = _prune_input_files(VIDA_S2 + "*/*.parquet", (70.0, -30.0, 71.0, -29.0))
        assert selected is not None and len(selected) == 1
        assert selected[0] in VIDA_FILES

    def test_a_glob_matching_one_file_is_left_alone(self, tmp_path):
        write_point_file(tmp_path / "3544332906740580352.parquet", 0, 0, rows=1)
        assert _prune_input_files(str(tmp_path / "*.parquet"), NAN_THAILAND) is None

    def test_local_listing_comes_from_the_filesystem(self, tmp_path):
        for name in ("3544332906740580352", "10466365534009032704"):
            write_point_file(tmp_path / f"{name}.parquet", 0, 0, rows=1)
        selected = _prune_input_files(str(tmp_path / "*.parquet"), NAN_THAILAND)
        assert selected == [str(tmp_path / "3544332906740580352.parquet")]


class TestListPartitionFiles:
    def test_remote_glob_lists_through_duckdb(self, monkeypatch):
        class FakeConnection:
            def __init__(self):
                self.closed = False
                self.sql = None

            def execute(self, sql):
                self.sql = sql
                return self

            def fetchall(self):
                return [(f,) for f in VIDA_FILES]

            def close(self):
                self.closed = True

        conn = FakeConnection()
        monkeypatch.setattr(extract_module, "get_duckdb_connection_for_s3", lambda *a, **k: conn)
        files = extract_module._list_partition_files(VIDA_S2 + "*/*.parquet")
        assert files == VIDA_FILES
        assert "glob(" in conn.sql
        assert conn.closed

    def test_remote_listing_failure_returns_none(self, monkeypatch):
        def no_network(*args, **kwargs):
            raise RuntimeError("no network")

        monkeypatch.setattr(extract_module, "get_duckdb_connection_for_s3", no_network)
        assert extract_module._list_partition_files(VIDA_S2 + "*/*.parquet") is None

    def test_remote_path_without_a_glob_is_not_listed(self, monkeypatch):
        def boom(*args, **kwargs):
            raise AssertionError("must not open a connection")

        monkeypatch.setattr(extract_module, "get_duckdb_connection_for_s3", boom)
        assert extract_module._list_partition_files("s3://bucket/key=1/data.parquet") is None


class TestExtractWithPruning:
    def test_extract_query_reads_only_overlapping_files(self, tmp_path, monkeypatch):
        """End to end through extract(): the generated query names exactly the
        overlapping files, never the glob."""
        thai = tmp_path / "3544332906740580352.parquet"
        lima = tmp_path / "10466365534009032704.parquet"
        write_point_file(thai, 100.77, 18.78)
        write_point_file(lima, -77.04, -12.05)

        captured = {}

        def capture(input_parquet, output_parquet, query, *args, **kwargs):
            captured["query"] = query

        monkeypatch.setattr(extract_module, "_execute_extraction", capture)
        outdir = tmp_path / "out"
        outdir.mkdir()
        extract(
            str(tmp_path / "*.parquet"),
            str(outdir / "out.parquet"),
            bbox="100.76,18.77,100.79,18.80",
        )

        assert str(thai) in captured["query"]
        assert str(lima) not in captured["query"]
        assert "*.parquet" not in captured["query"]

    def test_extract_skips_the_footer_of_non_overlapping_files(self, tmp_path):
        """The pruned file's footer is never fetched: a corrupt far-away file
        does not break a bbox extract that cannot touch it."""
        write_point_file(tmp_path / "3544332906740580352.parquet", 100.77, 18.78)
        write_point_file(tmp_path / "10466365534009032704.parquet", -77.04, -12.05)
        # An east-Pacific cell, sorted last so no schema probe reads it first
        (tmp_path / "8358680908399640576.parquet").write_bytes(b"this is not a parquet file")

        outdir = tmp_path / "out"
        outdir.mkdir()
        out = outdir / "out.parquet"
        extract(str(tmp_path / "*.parquet"), str(out), bbox="100.76,18.77,100.79,18.80")

        con = duckdb.connect()
        try:
            rows = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
        finally:
            con.close()
        assert rows == 3

    def test_extract_opens_everything_when_a_name_is_unrecognized(self, tmp_path):
        """One stranger in the glob falls back to current behavior: every file
        is read and the bbox filter alone decides the rows."""
        write_point_file(tmp_path / "3544332906740580352.parquet", 100.77, 18.78)
        write_point_file(tmp_path / "nan_city_extras.parquet", 100.775, 18.785)

        outdir = tmp_path / "out"
        outdir.mkdir()
        out = outdir / "out.parquet"
        extract(str(tmp_path / "*.parquet"), str(out), bbox="100.76,18.77,100.79,18.80")

        con = duckdb.connect()
        try:
            rows = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
        finally:
            con.close()
        assert rows == 6  # both files' rows survive the filter

    def test_extract_with_no_overlap_writes_an_empty_file(self, tmp_path):
        write_point_file(tmp_path / "3544332906740580352.parquet", 100.77, 18.78)
        write_point_file(tmp_path / "10466365534009032704.parquet", -77.04, -12.05)

        outdir = tmp_path / "out"
        outdir.mkdir()
        out = outdir / "out.parquet"
        extract(str(tmp_path / "*.parquet"), str(out), bbox="70.0,-30.0,71.0,-29.0")

        con = duckdb.connect()
        try:
            rows = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
        finally:
            con.close()
        assert rows == 0
