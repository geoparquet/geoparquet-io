"""Tests for single-pass partitioning (issue #478).

Partitioning must read the input ONCE via DuckDB ``COPY ... PARTITION_BY``
instead of re-scanning the whole input per partition value. These tests pin the
behaviour that matters: row totals reconcile, only one partitioned scan is
issued, naming is unchanged, and per-partition geo metadata stays correct.
"""

from __future__ import annotations

import json
import os

import duckdb
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.exceptions import PartitionError
from geoparquet_io.core.partition.common import partition_by_column


def _write_points(path, rows):
    """Write a tiny GeoParquet from ``rows`` of (cat, x, y); cat may be None."""
    from geoparquet_io.core.write_funnels import write_parquet_with_metadata

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("CREATE TABLE t (cat VARCHAR, geometry GEOMETRY)")
    for cat, x, y in rows:
        if cat is None:
            con.execute("INSERT INTO t VALUES (NULL, ST_Point(?, ?))", [x, y])
        else:
            con.execute("INSERT INTO t VALUES (?, ST_Point(?, ?))", [cat, x, y])
    write_parquet_with_metadata(con, "SELECT * FROM t", path)
    con.close()
    return path


@pytest.fixture
def multi_value_file(temp_output_dir):
    """A small GeoParquet with a low-cardinality ``cat`` column, geometry, and a
    passthrough KV key (``collection``) to verify metadata preservation.

    Three categories with differing geographic extents so per-partition bbox
    must differ from the global bbox.
    """
    from geoparquet_io.core.write_funnels import write_parquet_with_metadata

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    path = os.path.join(temp_output_dir, "multi.parquet")
    # cat A in the lower-left, B in the middle, C in the upper-right.
    con.execute(
        """
        CREATE TABLE t AS
        SELECT cat, ST_Point(x, y) AS geometry FROM (
            SELECT 'AAA' AS cat, i AS x, i AS y FROM range(10) tbl(i)
            UNION ALL
            SELECT 'BBB', 100 + i, 100 + i FROM range(20) tbl(i)
            UNION ALL
            SELECT 'CCC', 200 + i, 200 + i FROM range(5) tbl(i)
        )
        """
    )
    write_parquet_with_metadata(
        con,
        "SELECT * FROM t",
        path,
        extra_kv_metadata={"collection": json.dumps({"id": "test-collection"})},
    )
    con.close()
    return path


def _rglob_parquet(folder):
    out = []
    for root, _dirs, files in os.walk(folder):
        out.extend(os.path.join(root, f) for f in files if f.endswith(".parquet"))
    return out


def _row_count(path):
    return pq.ParquetFile(path).metadata.num_rows


class TestReconciliation:
    """sum(partition rows) == input rows; no duplication or loss."""

    def test_flat_keep_column(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "flat")
        n = partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            keep_partition_column=True,
            skip_analysis=True,
        )
        files = _rglob_parquet(out)
        assert len(files) == 3
        assert n == 3
        assert sum(_row_count(f) for f in files) == 35
        # Files named by value, flat layout
        assert sorted(os.path.basename(f) for f in files) == [
            "AAA.parquet",
            "BBB.parquet",
            "CCC.parquet",
        ]
        # Partition column kept; internal alias never leaks into output.
        names = pq.ParquetFile(files[0]).schema_arrow.names
        assert "cat" in names
        assert not any(n.startswith("__gpio_part") for n in names)

    def test_flat_drop_column(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "drop")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            keep_partition_column=False,
            skip_analysis=True,
        )
        files = _rglob_parquet(out)
        assert sum(_row_count(f) for f in files) == 35
        assert "cat" not in pq.ParquetFile(files[0]).schema_arrow.names

    def test_hive_layout(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "hive")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            hive=True,
            skip_analysis=True,
        )
        # Hive dirs cat=AAA/AAA.parquet
        assert os.path.isdir(os.path.join(out, "cat=AAA"))
        assert os.path.isfile(os.path.join(out, "cat=AAA", "AAA.parquet"))
        files = _rglob_parquet(out)
        assert sum(_row_count(f) for f in files) == 35

    def test_chars_prefix(self, multi_value_file, temp_output_dir):
        # First char only -> A, B, C
        out = os.path.join(temp_output_dir, "chars")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            column_prefix_length=1,
            skip_analysis=True,
        )
        files = _rglob_parquet(out)
        assert sorted(os.path.basename(f) for f in files) == [
            "A.parquet",
            "B.parquet",
            "C.parquet",
        ]
        assert sum(_row_count(f) for f in files) == 35

    def test_filename_prefix(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "pfx")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            filename_prefix="places",
            skip_analysis=True,
        )
        files = _rglob_parquet(out)
        assert all(os.path.basename(f).startswith("places_") for f in files)
        assert sum(_row_count(f) for f in files) == 35


class TestSingleScan:
    """Exactly one partitioned scan of the input; no per-value WHERE writes."""

    def test_one_partition_by_no_per_value_loop(
        self, multi_value_file, temp_output_dir, monkeypatch
    ):
        executed: list[str] = []

        real_connect = duckdb.connect

        class SpyCon:
            def __init__(self, con):
                self._con = con

            def execute(self, sql, *args, **kwargs):
                executed.append(sql)
                return self._con.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._con, name)

        def fake_get_conn(*_args, **_kwargs):
            con = real_connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            con.execute("INSTALL httpfs; LOAD httpfs;")
            return SpyCon(con)

        # Patch the connection factory used inside partition_by_column.
        monkeypatch.setattr(
            "geoparquet_io.core.partition.common.get_duckdb_connection", fake_get_conn
        )

        out = os.path.join(temp_output_dir, "scan")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            skip_analysis=True,
        )

        partition_by_stmts = [s for s in executed if "PARTITION_BY" in s.upper()]
        assert len(partition_by_stmts) == 1, executed

        # No statement should filter the input by a single partition value
        # (the old O(N) per-value pattern).
        per_value = [s for s in executed if '"cat" =' in s or '"cat"=\'' in s]
        assert per_value == [], per_value


class TestMetadata:
    """Each partition keeps valid geo metadata, tight bbox, and passthrough KV."""

    def test_geo_and_passthrough_and_tight_bbox(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "meta")
        partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            skip_analysis=True,
        )
        files = {os.path.basename(f): f for f in _rglob_parquet(out)}

        bboxes = {}
        for name, path in files.items():
            md = pq.ParquetFile(path).schema_arrow.metadata or {}
            assert b"geo" in md, f"{name} missing geo metadata"
            geo = json.loads(md[b"geo"])
            col = geo["columns"][geo["primary_column"]]
            assert "geometry_types" in col
            assert "bbox" in col
            bboxes[name] = col["bbox"]
            # passthrough KV preserved
            assert b"collection" in md, f"{name} missing collection KV"

        # Per-partition bbox must be tight, not the global bbox.
        # AAA is in lower-left (~0..9), CCC upper-right (~200..204).
        assert bboxes["AAA.parquet"][0] < 50
        assert bboxes["CCC.parquet"][0] > 150
        assert bboxes["AAA.parquet"] != bboxes["CCC.parquet"]


class TestMemoryLimitValidation:
    """memory_limit is interpolated into SET, so it must be validated."""

    def test_rejects_injection(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "mem")
        with pytest.raises(ValueError, match="Invalid memory_limit"):
            partition_by_column(
                input_parquet=multi_value_file,
                output_folder=out,
                column_name="cat",
                skip_analysis=True,
                memory_limit="1GB'; ATTACH 'evil.db' AS evil; --",
            )

    def test_accepts_valid_sizes(self):
        from geoparquet_io.core.partition.staging import _validate_memory_limit

        assert _validate_memory_limit("512MB") == "512MB"
        assert _validate_memory_limit("2gb") == "2GB"
        assert _validate_memory_limit("4.5 GB") == "4.5GB"


class TestCollision:
    """Distinct values that sanitize to the same filename must NOT lose rows."""

    def test_colliding_values_raise(self, temp_output_dir):
        # "a b" and "a_b" both sanitize to "a_b.parquet" -> would collide.
        path = _write_points(
            os.path.join(temp_output_dir, "collide.parquet"),
            [("a b", 1, 1), ("a b", 2, 2), ("a_b", 3, 3)],
        )
        out = os.path.join(temp_output_dir, "out")
        with pytest.raises(PartitionError, match="map to the same output file"):
            partition_by_column(
                input_parquet=path,
                output_folder=out,
                column_name="cat",
                skip_analysis=True,
            )

    def test_empty_sanitized_value_does_not_become_dotfile(self, temp_output_dir):
        # A value of "." sanitizes to "" -> must fall back, not write ".parquet".
        path = _write_points(
            os.path.join(temp_output_dir, "dot.parquet"), [(".", 1, 1), ("ok", 2, 2)]
        )
        out = os.path.join(temp_output_dir, "out")
        partition_by_column(
            input_parquet=path, output_folder=out, column_name="cat", skip_analysis=True
        )
        names = sorted(os.path.basename(f) for f in _rglob_parquet(out))
        assert ".parquet" not in names
        assert "_empty.parquet" in names
        assert sum(_row_count(f) for f in _rglob_parquet(out)) == 2


class TestNullHandling:
    """NULL partition values are dropped; non-NULL rows still reconcile."""

    def test_null_values_dropped_rest_reconcile(self, temp_output_dir):
        path = _write_points(
            os.path.join(temp_output_dir, "nulls.parquet"),
            [("AAA", 1, 1), ("AAA", 2, 2), (None, 3, 3), (None, 4, 4), ("BBB", 5, 5)],
        )
        out = os.path.join(temp_output_dir, "out")
        n = partition_by_column(
            input_parquet=path, output_folder=out, column_name="cat", skip_analysis=True
        )
        assert n == 2  # AAA, BBB — the two NULL rows excluded
        files = _rglob_parquet(out)
        assert sum(_row_count(f) for f in files) == 3  # 2 + 1, NULLs gone

    def test_all_null_raises(self, temp_output_dir):
        path = _write_points(
            os.path.join(temp_output_dir, "allnull.parquet"), [(None, 1, 1), (None, 2, 2)]
        )
        out = os.path.join(temp_output_dir, "out")
        with pytest.raises(PartitionError, match="No non-NULL values"):
            partition_by_column(
                input_parquet=path, output_folder=out, column_name="cat", skip_analysis=True
            )


class TestOverwrite:
    """overwrite=False preserves existing files and counts only new writes."""

    def test_existing_partitions_skipped(self, multi_value_file, temp_output_dir):
        out = os.path.join(temp_output_dir, "ow")
        first = partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            skip_analysis=True,
        )
        assert first == 3
        mtimes = {f: os.path.getmtime(f) for f in _rglob_parquet(out)}

        # Re-run with overwrite=False: everything exists -> nothing rewritten.
        second = partition_by_column(
            input_parquet=multi_value_file,
            output_folder=out,
            column_name="cat",
            overwrite=False,
            skip_analysis=True,
        )
        assert second == 0
        for f, mtime in mtimes.items():
            assert os.path.exists(f)
            assert os.path.getmtime(f) == mtime  # untouched


def _write_two_column(path, rows):
    """Write a GeoParquet with two partitionable columns from ``rows`` of
    (country, subdivision, x, y)."""
    from geoparquet_io.core.write_funnels import write_parquet_with_metadata

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("CREATE TABLE t (country VARCHAR, subdivision VARCHAR, geometry GEOMETRY)")
    for country, subdivision, x, y in rows:
        con.execute("INSERT INTO t VALUES (?, ?, ST_Point(?, ?))", [country, subdivision, x, y])
    write_parquet_with_metadata(con, "SELECT * FROM t", path)
    con.close()
    return path


class TestRepartitionNoLeak:
    """Regression for #490: the internal ``__gpio_part`` alias must never leak
    into output files, and re-partitioning a partition must not compound it
    (``__gpio_part``, then ``__gpio_part_1``, …)."""

    def test_repartition_of_partition_has_no_alias_column(self, temp_output_dir):
        src = _write_two_column(
            os.path.join(temp_output_dir, "src.parquet"),
            [
                ("US", "US-NY", 1, 1),
                ("US", "US-NY", 2, 2),
                ("US", "US-CA", 3, 3),
                ("CA", "CA-ON", 4, 4),
            ],
        )

        # Pass 1: partition by country.
        out1 = os.path.join(temp_output_dir, "out1")
        partition_by_column(
            input_parquet=src,
            output_folder=out1,
            column_name="country",
            keep_partition_column=True,
            skip_analysis=True,
        )
        first_files = _rglob_parquet(out1)
        for f in first_files:
            names = pq.ParquetFile(f).schema_arrow.names
            assert not any(n.startswith("__gpio_part") for n in names), names

        # Pass 2: re-partition the US output (carried via SELECT *) by subdivision.
        us_file = os.path.join(out1, "US.parquet")
        assert us_file in first_files
        out2 = os.path.join(temp_output_dir, "out2")
        partition_by_column(
            input_parquet=us_file,
            output_folder=out2,
            column_name="subdivision",
            keep_partition_column=True,
            skip_analysis=True,
        )
        second_files = _rglob_parquet(out2)
        assert second_files
        for f in second_files:
            names = pq.ParquetFile(f).schema_arrow.names
            # No alias from either pass: __gpio_part, __gpio_part_1, …
            assert not any(n.startswith("__gpio_part") for n in names), names
        # Rows reconcile across the re-partition (only the 3 US rows).
        assert sum(_row_count(f) for f in second_files) == 3
