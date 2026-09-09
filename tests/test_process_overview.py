"""Tests for `gpio process overview` (aggregate rollups to coarser levels)."""

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.admin_datasets import OvertureAdminDataset
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.overview import create_overviews
from geoparquet_io.core.process.overview.detect import (
    detect_aggregate_file,
    detect_aggregate_info,
)
from geoparquet_io.core.process.overview.run import overview_output_path

# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _write_admin_region_aggregate(path, with_geometry=True, extra_column=False):
    """A tiny region-level `process aggregate admin` output.

    Rows: two US regions, one FR region, and an 'unassigned' bucket.
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    extra_col = ", 'x' AS notes" if extra_column else ""
    geom_col = (
        ", CASE WHEN admin_code = 'unassigned' THEN NULL "
        "ELSE ST_Buffer(ST_Point(lon, lat), 0.5) END AS geometry"
        if with_geometry
        else ""
    )
    con.execute(
        f"""
        COPY (
            SELECT admin_code, admin_code AS admin_name, count, sum_area,
                   avg_height, min_year, max_year, count_barn, count_other
                   {extra_col}{geom_col}
            FROM (VALUES
                ('US-CA', 2, 10.0, 4.0, 1990, 2000, 1, 1, -120.0, 37.0),
                ('US-NV', 3, 30.0, 6.0, 1980, 1995, 2, 1, -116.0, 39.0),
                ('FR-IDF', 4, 8.0, 2.5, 2001, 2020, 0, 4, 2.3, 48.8),
                ('unassigned', 1, 1.0, 1.0, 1970, 1970, 0, 1, 0.0, 0.0)
            ) AS t(admin_code, count, sum_area, avg_height, min_year,
                   max_year, count_barn, count_other, lon, lat)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def _write_country_cache(path):
    """A fake Overture per-level country cache (US split in two rows, FR in one)."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT country, 'country' AS subtype,
                   ST_Buffer(ST_Point(lon, lat), 2.0) AS geometry
            FROM (VALUES
                ('US', -120.0, 37.0),
                ('US', -116.0, 39.0),
                ('FR', 2.3, 48.8)
            ) AS t(country, lon, lat)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.fixture
def fake_country_cache(tmp_path, monkeypatch):
    cache = tmp_path / "country_cache.parquet"
    _write_country_cache(cache)
    monkeypatch.setattr(
        OvertureAdminDataset,
        "get_source_for_level",
        lambda self, level, no_cache=False: str(cache),
    )
    return cache


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


class TestDetection:
    def test_no_cell_column_errors_with_hint(self, tmp_path):
        path = tmp_path / "plain.parquet"
        pq.write_table(pa.table({"id": [1, 2], "count": [3, 4]}), path)
        with pytest.raises(InvalidParameterError, match="cell-column"):
            detect_aggregate_file(str(path))

    def test_missing_count_column_errors(self, tmp_path):
        path = tmp_path / "no_count.parquet"
        pq.write_table(pa.table({"admin_code": ["US-CA"], "sum_area": [1.0]}), path)
        with pytest.raises(InvalidParameterError, match="count"):
            detect_aggregate_file(str(path))

    def test_admin_region_aggregate_detected(self, tmp_path):
        path = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(path)
        info = detect_aggregate_file(str(path))
        assert info.scheme == "admin"
        assert info.cell_column == "admin_code"
        assert info.base_level == "region"
        assert info.out_geometry == "polygon"
        roles = {c.name: c.func for c in info.rollup_columns}
        assert roles == {
            "sum_area": "sum",
            "avg_height": "avg",
            "min_year": "min",
            "max_year": "max",
            "count_barn": "sum",
            "count_other": "sum",
        }

    def test_admin_country_level_input_errors(self, tmp_path):
        path = tmp_path / "by_country.parquet"
        pq.write_table(
            pa.table({"admin_code": ["US", "FR", "unassigned"], "count": [1, 2, 3]}),
            path,
        )
        with pytest.raises(InvalidParameterError, match="country level"):
            detect_aggregate_file(str(path))

    def test_admin_empty_input_distinct_error(self, tmp_path):
        """0 usable rows is not 'already at country level' -- say so."""
        path = tmp_path / "empty.parquet"
        pq.write_table(
            pa.table({"admin_code": pa.array(["unassigned", None]), "count": [1, 2]}),
            path,
        )
        with pytest.raises(InvalidParameterError, match="no admin codes"):
            detect_aggregate_file(str(path))

    def test_unclassifiable_columns_are_dropped(self, tmp_path):
        path = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(path, extra_column=True)
        info = detect_aggregate_file(str(path))
        assert "notes" in info.dropped_columns
        assert "notes" not in {c.name for c in info.rollup_columns}

    def test_no_geometry_infers_none(self, tmp_path):
        path = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(path, with_geometry=False)
        info = detect_aggregate_file(str(path))
        assert info.out_geometry == "none"


# ---------------------------------------------------------------------------
# Grid detection / rollup with stub UDFs (fast: no community extensions)
# ---------------------------------------------------------------------------
#
# The a5/h3 helper functions are stubbed with DuckDB Python UDFs so the SQL
# paths execute for real without installing community extensions. Stub cell id
# encoding: id = resolution * 1000 + n; parent(id, L) = L * 1000 + n // 4.


def _register_a5_stubs(con):
    con.create_function("a5_get_resolution", lambda c: c // 1000, ["UBIGINT"], "INTEGER")
    con.create_function(
        "a5_cell_to_parent",
        lambda c, lvl: lvl * 1000 + (c % 1000) // 4,
        ["UBIGINT", "INTEGER"],
        "UBIGINT",
    )
    con.create_function(
        "a5_cell_to_lonlat",
        lambda c: [float(c % 100), float(c % 60)],
        ["UBIGINT"],
        "DOUBLE[]",
    )


def _no_extension(monkeypatch):
    """Skip INSTALL/LOAD of grid community extensions (stubs stand in)."""
    from geoparquet_io.core.process.overview import detect as detect_mod

    monkeypatch.setattr(detect_mod, "ensure_grid_extension", lambda con, scheme: None)


def _stub_grid_aggregate_table(cells, counts, values):
    return pa.table(
        {
            "a5_cell": pa.array(cells, type=pa.uint64()),
            "count": pa.array(counts, type=pa.int64()),
            "sum_v": pa.array(values, type=pa.float64()),
            "avg_v": pa.array(values, type=pa.float64()),
            "min_v": pa.array(values, type=pa.float64()),
            "max_v": pa.array(values, type=pa.float64()),
        }
    )


class TestGridDetectionStubbed:
    def test_detect_a5(self, monkeypatch):
        import duckdb as _duckdb

        _no_extension(monkeypatch)
        con = _duckdb.connect()
        _register_a5_stubs(con)
        con.register("agg", _stub_grid_aggregate_table([7000, 7001], [1, 2], [1.0, 2.0]))
        info = detect_aggregate_info(con, "agg")
        assert info.scheme == "a5"
        assert info.cell_column == "a5_cell"
        assert info.base_level == 7
        assert info.out_geometry == "none"
        # count + sum/avg/min/max rollups feed the bytes-per-cell estimate.
        assert info.num_attributes == 5
        # An explicit --cell-column override resolves to the same scheme.
        override = detect_aggregate_info(con, "agg", cell_column="a5_cell")
        assert (override.scheme, override.base_level) == ("a5", 7)
        con.close()

    def test_detect_h3(self, monkeypatch):
        import duckdb as _duckdb

        _no_extension(monkeypatch)
        con = _duckdb.connect()
        con.create_function("h3_get_resolution", lambda c: 7, ["VARCHAR"], "INTEGER")
        con.register(
            "agg",
            pa.table({"h3_cell": ["871fb4662ffffff", "871fb4663ffffff"], "count": [1, 2]}),
        )
        info = detect_aggregate_info(con, "agg")
        assert info.scheme == "h3"
        assert info.cell_column == "h3_cell"
        assert info.base_level == 7
        con.close()

    def test_detect_mixed_resolutions_errors(self, monkeypatch):
        import duckdb as _duckdb

        _no_extension(monkeypatch)
        con = _duckdb.connect()
        _register_a5_stubs(con)
        con.register("agg", _stub_grid_aggregate_table([6000, 7000], [1, 2], [1.0, 2.0]))
        with pytest.raises(InvalidParameterError, match="[Mm]ixed"):
            detect_aggregate_info(con, "agg")
        con.close()

    def test_detect_all_null_cells_errors(self, monkeypatch):
        import duckdb as _duckdb

        _no_extension(monkeypatch)
        con = _duckdb.connect()
        _register_a5_stubs(con)
        con.register(
            "agg",
            pa.table({"a5_cell": pa.array([None, None], type=pa.uint64()), "count": [1, 2]}),
        )
        with pytest.raises(InvalidParameterError, match="non-NULL"):
            detect_aggregate_info(con, "agg")
        con.close()

    def test_cell_column_override_missing_errors(self, monkeypatch):
        import duckdb as _duckdb

        _no_extension(monkeypatch)
        con = _duckdb.connect()
        con.register("agg", pa.table({"count": [1]}))
        with pytest.raises(InvalidParameterError, match="not found"):
            detect_aggregate_info(con, "agg", cell_column="mycell")
        con.close()

    def test_cell_column_override_scheme_inference(self, monkeypatch):
        import duckdb as _duckdb

        from geoparquet_io.core.process.overview.detect import _scheme_for_column

        con = _duckdb.connect()
        con.register(
            "t",
            pa.table({"h3ish": ["871fb4662ffffff"], "adminish": ["US-CA"]}),
        )
        # Integer columns are a5 only when the name says so.
        assert _scheme_for_column(con, "t", "my_a5_cells", "UBIGINT") == "a5"
        # 15-hex-char strings are h3; anything else is admin.
        assert _scheme_for_column(con, "t", "h3ish", "VARCHAR") == "h3"
        assert _scheme_for_column(con, "t", "adminish", "VARCHAR") == "admin"
        con.close()

    def test_integer_cell_column_without_a5_name_refuses_to_guess(self):
        # H3 ids are commonly stored as UBIGINT too; routing them through the
        # a5 hierarchy would silently produce wrong output. Require --scheme.
        import duckdb as _duckdb

        con = _duckdb.connect()
        con.register(
            "agg",
            pa.table({"cells": pa.array([123, 456], type=pa.uint64()), "count": [1, 2]}),
        )
        with pytest.raises(InvalidParameterError, match="scheme"):
            detect_aggregate_info(con, "agg", cell_column="cells")
        con.close()

    def test_scheme_override_h3_integer_ids(self, monkeypatch):
        # Packed (UBIGINT) H3 ids only work via an explicit scheme override.
        import duckdb as _duckdb

        _no_extension(monkeypatch)
        con = _duckdb.connect()
        con.create_function("h3_get_resolution", lambda c: 7, ["UBIGINT"], "INTEGER")
        con.register(
            "agg",
            pa.table({"h3_id": pa.array([1, 2], type=pa.uint64()), "count": [1, 2]}),
        )
        info = detect_aggregate_info(con, "agg", cell_column="h3_id", scheme="h3")
        assert info.scheme == "h3"
        assert info.cell_column == "h3_id"
        assert info.base_level == 7
        con.close()

    def test_scheme_override_uses_default_column(self, monkeypatch):
        import duckdb as _duckdb

        _no_extension(monkeypatch)
        con = _duckdb.connect()
        _register_a5_stubs(con)
        con.register("agg", _stub_grid_aggregate_table([7000, 7001], [1, 2], [1.0, 2.0]))
        info = detect_aggregate_info(con, "agg", scheme="a5")
        assert (info.scheme, info.cell_column, info.base_level) == ("a5", "a5_cell", 7)
        # A scheme whose default column is missing fails loudly.
        with pytest.raises(InvalidParameterError, match="not found"):
            detect_aggregate_info(con, "agg", scheme="h3")
        con.close()

    def test_invalid_scheme_errors(self):
        import duckdb as _duckdb

        con = _duckdb.connect()
        con.register("agg", pa.table({"a5_cell": pa.array([1], type=pa.uint64()), "count": [1]}))
        with pytest.raises(InvalidParameterError, match="[Ii]nvalid scheme"):
            detect_aggregate_info(con, "agg", scheme="s2")
        con.close()

    def test_ensure_grid_extension_statements(self, monkeypatch):
        """Loads a5 through the shared helper, so the telemetry opt-out applies.

        A raw INSTALL/LOAD here brought back the #779 segfault on
        ``gpio process overview``, which is why this asserts the opt-out is in
        the environment by the time LOAD runs rather than pinning the SQL text.
        """
        import os

        from geoparquet_io.core.process.overview.detect import ensure_grid_extension

        monkeypatch.delenv("QUERY_FARM_TELEMETRY_OPT_OUT", raising=False)

        class RecordingCon:
            def __init__(self):
                self.statements = []

            def execute(self, sql):
                self.statements.append((sql, os.environ.get("QUERY_FARM_TELEMETRY_OPT_OUT")))

        con = RecordingCon()
        ensure_grid_extension(con, "a5")
        sql = [statement for statement, _ in con.statements]
        assert any(s.startswith("INSTALL a5 FROM community") for s in sql)
        assert any(s.startswith("LOAD a5") for s in sql)
        assert all(
            opt_out is not None for statement, opt_out in con.statements if "LOAD" in statement
        )


class TestOutGeometryInference:
    def test_both_when_centroid_column_present(self, tmp_path):
        src = tmp_path / "by_region.parquet"
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
        con.execute(
            f"""
            COPY (
                SELECT 'US-CA' AS admin_code, 1 AS count,
                       ST_Buffer(ST_Point(0, 0), 1.0) AS geometry,
                       ST_Point(0, 0) AS centroid
            ) TO '{src}' (FORMAT PARQUET)
            """
        )
        con.close()
        assert detect_aggregate_file(str(src)).out_geometry == "both"

    def test_centroid_when_geometry_is_points(self, tmp_path):
        src = tmp_path / "by_region.parquet"
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
        con.execute(
            f"""
            COPY (
                SELECT 'US-CA' AS admin_code, 1 AS count, ST_Point(0, 0) AS geometry
            ) TO '{src}' (FORMAT PARQUET)
            """
        )
        con.close()
        assert detect_aggregate_file(str(src)).out_geometry == "centroid"


class TestGridRollupStubbed:
    def test_create_overviews_explicit_level(self, tmp_path, monkeypatch):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.process.overview import detect as detect_mod

        _no_extension(monkeypatch)
        real_factory = get_duckdb_connection

        def stubbed_connection(**kwargs):
            con = real_factory(**kwargs)
            _register_a5_stubs(con)
            return con

        monkeypatch.setattr(detect_mod, "get_duckdb_connection", stubbed_connection)

        src = tmp_path / "cells.parquet"
        # Parents at level 5: 7000,7001 -> 5000; 7004,7005 -> 5001.
        pq.write_table(
            _stub_grid_aggregate_table(
                [7000, 7001, 7004, 7005], [1, 2, 3, 4], [1.0, 2.0, 3.0, 4.0]
            ),
            src,
        )
        results = create_overviews(str(src), levels="5", compression_level=9, show_sql=True)
        assert results == [(5, str(tmp_path / "cells_r5.parquet"))]

        out = pq.read_table(results[0][1])
        rows = dict(zip(out.column("a5_cell").to_pylist(), range(out.num_rows), strict=True))
        assert set(rows) == {5000, 5001}

        def col(name, cell):
            return out.column(name)[rows[cell]].as_py()

        assert col("count", 5000) == 3
        assert col("sum_v", 5000) == pytest.approx(3.0)
        # Count-weighted: (1*1.0 + 2*2.0) / 3.
        assert col("avg_v", 5000) == pytest.approx(5.0 / 3.0)
        assert col("min_v", 5000) == 1.0
        assert col("max_v", 5000) == 2.0
        assert col("count", 5001) == 7
        assert col("avg_v", 5001) == pytest.approx(25.0 / 7.0)

    def test_create_overviews_auto_levels(self, tmp_path, monkeypatch):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.process.overview import detect as detect_mod

        _no_extension(monkeypatch)
        real_factory = get_duckdb_connection

        def stubbed_connection(**kwargs):
            con = real_factory(**kwargs)
            _register_a5_stubs(con)
            return con

        monkeypatch.setattr(detect_mod, "get_duckdb_connection", stubbed_connection)

        src = tmp_path / "cells.parquet"
        pq.write_table(
            _stub_grid_aggregate_table(
                [7000, 7001, 7004, 7005], [1, 2, 3, 4], [1.0, 2.0, 3.0, 4.0]
            ),
            src,
        )
        # Absurd bytes-per-cell: nothing fits, so only the coarsest level (0)
        # is selected and the base is deferred past the probe range.
        results = create_overviews(str(src), max_tile_kb=1, bytes_per_cell=1e9)
        assert [lvl for lvl, _ in results] == [0]
        assert (tmp_path / "cells_r0.parquet").exists()

    def test_create_overviews_base_fits_builds_nothing(self, tmp_path, monkeypatch):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.process.overview import detect as detect_mod

        _no_extension(monkeypatch)
        real_factory = get_duckdb_connection

        def stubbed_connection(**kwargs):
            con = real_factory(**kwargs)
            _register_a5_stubs(con)
            return con

        monkeypatch.setattr(detect_mod, "get_duckdb_connection", stubbed_connection)

        src = tmp_path / "cells.parquet"
        pq.write_table(_stub_grid_aggregate_table([7000, 7001], [1, 2], [1.0, 2.0]), src)
        # Tiny bytes-per-cell: the base level fits at z0, so no overviews.
        assert create_overviews(str(src), bytes_per_cell=1.0) == []

    def test_plan_bands_grid_explicit_levels(self, monkeypatch):
        import duckdb as _duckdb

        from geoparquet_io.core.process.overview.run import plan_bands

        _no_extension(monkeypatch)
        con = _duckdb.connect()
        _register_a5_stubs(con)
        con.register("agg", _stub_grid_aggregate_table([7000, 7001], [1, 2], [1.0, 2.0]))
        info = detect_aggregate_info(con, "agg")
        bands = plan_bands(
            con, "SELECT * FROM agg", info, levels=[5], bytes_per_cell=1.0, verbose=True
        )
        # Everything fits immediately -> single base band; candidates were
        # restricted to the explicit level + base.
        assert [band.level for band in bands] == [7]
        assert bands[0].minzoom == 0 and bands[0].maxzoom is None
        con.close()

    def test_rollup_table_grid_stubbed(self, monkeypatch):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.process.overview import rollup as rollup_mod
        from geoparquet_io.core.process.overview import rollup_table

        _no_extension(monkeypatch)
        real_factory = get_duckdb_connection

        def stubbed_connection(**kwargs):
            con = real_factory(**kwargs)
            _register_a5_stubs(con)
            return con

        monkeypatch.setattr(rollup_mod, "get_duckdb_connection", stubbed_connection)

        result = rollup_table(
            _stub_grid_aggregate_table([7000, 7001, 7004], [1, 2, 3], [1.0, 2.0, 3.0]), 5
        )
        rows = dict(
            zip(
                result.column("a5_cell").to_pylist(),
                result.column("count").to_pylist(),
                strict=True,
            )
        )
        assert rows == {5000: 3, 5001: 3}

    def test_plan_bands_admin_without_geometry_errors(self, tmp_path):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.process.overview.run import plan_bands

        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src, with_geometry=False)
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            relation = f"read_parquet('{src}')"
            info = detect_aggregate_info(con, relation)
            with pytest.raises(InvalidParameterError, match="without geometry"):
                plan_bands(con, f"SELECT * FROM {relation}", info)
        finally:
            con.close()


class TestProbeWorstTileCounts:
    def test_counts_shrink_with_zoom(self):
        from geoparquet_io.core.partition.auto_resolution import _register_quadkey_udf
        from geoparquet_io.core.process.overview.levels import probe_worst_tile_counts

        con = duckdb.connect()
        _register_quadkey_udf(con)
        # Two nearby points, one far away, one beyond the WebMercator lat
        # domain (exercises the clamp).
        cells_sql = (
            "SELECT * FROM (VALUES (2.0, 48.0), (2.001, 48.001), (-100.0, 40.0), (100.0, 89.0)) "
            "t(lon, lat)"
        )
        counts = probe_worst_tile_counts(con, cells_sql, max_zoom=8)
        assert set(counts) == set(range(9))
        assert counts[0] == 4  # everything lands in the single z0 tile
        assert counts[8] >= 1
        assert all(counts[z + 1] <= counts[z] for z in range(8))
        con.close()

    def test_empty_input(self):
        from geoparquet_io.core.partition.auto_resolution import _register_quadkey_udf
        from geoparquet_io.core.process.overview.levels import probe_worst_tile_counts

        con = duckdb.connect()
        _register_quadkey_udf(con)
        counts = probe_worst_tile_counts(
            con, "SELECT NULL::DOUBLE AS lon, NULL::DOUBLE AS lat WHERE false", max_zoom=2
        )
        assert counts == {0: 0, 1: 0, 2: 0}
        con.close()


# ---------------------------------------------------------------------------
# Rollup SQL builders (pure string construction, nothing executed)
# ---------------------------------------------------------------------------


def _grid_info(scheme="a5", out_geometry="polygon"):
    from geoparquet_io.core.process.overview.detect import AggregateInfo, RollupColumn

    return AggregateInfo(
        scheme=scheme,
        cell_column=f"{scheme}_cell",
        base_level=7,
        rollup_columns=(
            RollupColumn("sum_area", "sum"),
            RollupColumn("avg_height", "avg"),
            RollupColumn("min_year", "min"),
            RollupColumn("max_year", "max"),
            RollupColumn("count_barn", "sum", cast_to_bigint=True),
        ),
        out_geometry=out_geometry,
    )


def _admin_info(out_geometry="polygon"):
    from geoparquet_io.core.process.overview.detect import AggregateInfo, RollupColumn

    return AggregateInfo(
        scheme="admin",
        cell_column="admin_code",
        base_level="region",
        rollup_columns=(RollupColumn("sum_area", "sum"),),
        out_geometry=out_geometry,
    )


class TestRollupSqlBuilders:
    def test_grid_rollup_sql_polygon(self):
        from geoparquet_io.core.process.overview.rollup import build_grid_rollup_sql

        sql = build_grid_rollup_sql(_grid_info(), "SELECT * FROM src", 5)
        assert 'a5_cell_to_parent("a5_cell", 5)' in sql
        assert 'CASE WHEN "a5_cell" IS NULL THEN NULL' in sql  # unassigned guard
        assert "GROUP BY __parent" in sql
        assert "CAST(SUM(count) AS BIGINT) AS count" in sql
        assert (
            'SUM("avg_height" * count) / '
            'NULLIF(SUM(count) FILTER (WHERE "avg_height" IS NOT NULL), 0)' in sql
        )
        assert 'CAST(SUM("count_barn") AS BIGINT)' in sql
        assert "a5_cell_to_boundary" in sql  # polygon regenerated from cell id

    def test_grid_rollup_sql_h3_both(self):
        from geoparquet_io.core.process.overview.rollup import build_grid_rollup_sql

        sql = build_grid_rollup_sql(_grid_info("h3", out_geometry="both"), "SELECT * FROM s", 4)
        assert 'h3_cell_to_parent("h3_cell", 4)' in sql
        assert "h3_cell_to_boundary_wkb" in sql
        assert "AS centroid" in sql
        # A rolled-up parent cell straddles the antimeridian as readily as a base
        # cell, so it goes through the same seam repair.
        assert "ST_CollectionExtract(" in sql

    def test_grid_rollup_sql_none_geometry(self):
        from geoparquet_io.core.process.overview.rollup import build_grid_rollup_sql

        sql = build_grid_rollup_sql(_grid_info(out_geometry="none"), "SELECT * FROM s", 5)
        assert "boundary" not in sql
        assert "geometry" not in sql

    def test_admin_rollup_sql_polygon(self):
        from geoparquet_io.core.process.overview.rollup import build_admin_rollup_sql

        sql = build_admin_rollup_sql(
            _admin_info(), "SELECT * FROM s", "read_parquet('c.parquet')", '"country"', "geometry"
        )
        assert "split_part(\"admin_code\", '-', 1)" in sql
        assert "'unassigned'" in sql  # unassigned passes through
        assert "ST_Union_Agg" in sql  # multi-row countries are unioned
        assert "LEFT JOIN" in sql
        assert "AS geometry" in sql

    def test_admin_rollup_sql_geometry_modes(self):
        from geoparquet_io.core.process.overview.rollup import build_admin_rollup_sql

        centroid = build_admin_rollup_sql(
            _admin_info("centroid"), "SELECT * FROM s", "read_parquet('c')", '"country"', "g"
        )
        assert "ST_Centroid" in centroid
        both = build_admin_rollup_sql(
            _admin_info("both"), "SELECT * FROM s", "read_parquet('c')", '"country"', "g"
        )
        assert "AS centroid" in both

    def test_admin_rollup_sql_respects_cell_column(self):
        from geoparquet_io.core.process.overview.detect import AggregateInfo
        from geoparquet_io.core.process.overview.rollup import build_admin_rollup_sql

        info = AggregateInfo(
            scheme="admin",
            cell_column="region_code",
            base_level="region",
            rollup_columns=(),
            out_geometry="polygon",
        )
        sql = build_admin_rollup_sql(
            info, "SELECT * FROM s", "read_parquet('c.parquet')", '"country"', "geometry"
        )
        assert "split_part(\"region_code\", '-', 1)" in sql
        assert '__parent AS "region_code"' in sql
        assert 'r."region_code" = c.__country_code' in sql
        assert "admin_code" not in sql

    def test_admin_probe_sql_respects_cell_column(self, tmp_path):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.process.overview.detect import detect_aggregate_info
        from geoparquet_io.core.process.overview.run import _admin_cells_probe_sql

        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        renamed = tmp_path / "renamed.parquet"
        table = pq.read_table(src)
        table = table.rename_columns(
            ["region_code" if n == "admin_code" else n for n in table.column_names]
        )
        pq.write_table(table, renamed)

        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            relation = f"read_parquet('{renamed}')"
            info = detect_aggregate_info(con, relation, cell_column="region_code")
            source_sql = f"SELECT * FROM {relation}"
            for level in ("region", "country"):
                sql = _admin_cells_probe_sql(con, info, source_sql, level)
                assert "admin_code" not in sql
                rows = con.execute(sql).fetchall()
                assert rows  # executes cleanly against the renamed column
        finally:
            con.close()

    def test_admin_rollup_sql_none_geometry_skips_join(self):
        from geoparquet_io.core.process.overview.rollup import (
            build_admin_rollup_sql,
            build_level_sql,
        )

        sql = build_admin_rollup_sql(_admin_info("none"), "SELECT * FROM s", None, None, None)
        assert "LEFT JOIN" not in sql
        # build_level_sql takes the no-country-cache shortcut (con unused).
        assert build_level_sql(None, _admin_info("none"), "SELECT * FROM s", "country") == sql

    def test_validate_level_grid_non_integer_errors(self):
        from geoparquet_io.core.process.overview.rollup import validate_level

        with pytest.raises(InvalidParameterError, match="integer"):
            validate_level(_grid_info(), "country")
        assert validate_level(_grid_info(), "5") == 5

    def test_validate_level_not_coarser_errors(self):
        from geoparquet_io.core.process.overview.rollup import validate_level

        with pytest.raises(InvalidParameterError, match="coarser"):
            validate_level(_grid_info(), 7)  # equal to base
        with pytest.raises(InvalidParameterError, match="coarser"):
            validate_level(_grid_info(), 9)  # finer than base

    def test_avg_rollup_ignores_null_children(self, monkeypatch):
        """Children avg=10/count=5 and avg=NULL/count=3 must roll up to 10.0,
        not 6.25: NULL-avg cells cannot count in the denominator."""
        import duckdb as _duckdb

        from geoparquet_io.core.process.overview.rollup import build_grid_rollup_sql

        _no_extension(monkeypatch)
        con = _duckdb.connect()
        _register_a5_stubs(con)
        con.register(
            "agg",
            pa.table(
                {
                    "a5_cell": pa.array([7000, 7001], type=pa.uint64()),
                    "count": pa.array([5, 3], type=pa.int64()),
                    "avg_v": pa.array([10.0, None], type=pa.float64()),
                }
            ),
        )
        info = detect_aggregate_info(con, "agg")
        sql = build_grid_rollup_sql(info, "SELECT * FROM agg", 5)
        ((cell, count, avg),) = con.execute(sql).fetchall()
        assert (cell, count) == (5000, 8)
        assert avg == pytest.approx(10.0)
        con.close()

    def test_admin_country_probe_handles_antimeridian(self):
        """Countries spanning the antimeridian must probe near +/-180, not at
        the AVG(lon) of their region centroids (which lands near lon 0)."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.process.overview.detect import AggregateInfo
        from geoparquet_io.core.process.overview.run import _admin_cells_probe_sql

        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.execute("SET geometry_always_xy = true")
            con.execute(
                """
                CREATE TEMP TABLE agg AS
                SELECT * FROM (VALUES
                    ('US-AK', 1, ST_Buffer(ST_Point(-179.5, 60.0), 0.1)),
                    ('US-XX', 1, ST_Buffer(ST_Point(179.5, 55.0), 0.1))
                ) t(admin_code, count, geometry)
                """
            )
            info = AggregateInfo(
                scheme="admin",
                cell_column="admin_code",
                base_level="region",
                rollup_columns=(),
                out_geometry="polygon",
            )
            sql = _admin_cells_probe_sql(con, info, "SELECT * FROM agg", "country")
            ((lon, lat),) = con.execute(sql).fetchall()
            assert abs(lon) > 170
            assert 55.0 <= lat <= 61.0
        finally:
            con.close()

    def test_grid_probe_sql_variants(self):
        from geoparquet_io.core.process.overview.run import _grid_cells_probe_sql

        non_base = _grid_cells_probe_sql(_grid_info(), "SELECT * FROM s", 5)
        assert 'a5_cell_to_parent("a5_cell", 5)' in non_base
        assert "a5_cell_to_lonlat" in non_base
        # At the base level the cell is its own parent -- no parent call.
        base = _grid_cells_probe_sql(_grid_info(), "SELECT * FROM s", 7)
        assert "a5_cell_to_parent" not in base
        # h3 returns [lat, lng]; lon must come from index 2.
        h3 = _grid_cells_probe_sql(_grid_info("h3"), "SELECT * FROM s", 5)
        assert "h3_cell_to_latlng" in h3
        assert "__ll[2] AS lon" in h3

    def test_parse_levels_duplicates_error(self):
        from geoparquet_io.core.process.overview.run import parse_levels

        with pytest.raises(InvalidParameterError, match="duplicate"):
            parse_levels([5, 5], _grid_info())
        with pytest.raises(InvalidParameterError, match="no levels"):
            parse_levels("", _grid_info())
        assert parse_levels("5, 3", _grid_info()) == [3, 5]


# ---------------------------------------------------------------------------
# In-memory rollups and API wrappers (fast)
# ---------------------------------------------------------------------------


class TestInMemoryAndWrappers:
    @pytest.mark.usefixtures("fake_country_cache")
    def test_rollup_table_admin(self, tmp_path):
        from geoparquet_io.core.process.overview import rollup_table

        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        result = rollup_table(pq.read_table(src), "country")
        codes = result.column("admin_code").to_pylist()
        assert set(codes) == {"US", "FR", "unassigned"}
        assert sum(result.column("count").to_pylist()) == 10

    @pytest.mark.usefixtures("fake_country_cache")
    def test_table_overview_admin(self, tmp_path):
        from geoparquet_io.api.table import Table

        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        result = Table(pq.read_table(src)).overview("country", scheme="admin")
        assert "admin_code" in result.column_names
        assert result.geometry_column == "geometry"

    def test_ops_create_overviews_wrapper(self, monkeypatch):
        from geoparquet_io.api import ops
        from geoparquet_io.core.process import overview as overview_pkg

        recorded = {}

        def fake(input_parquet, **kwargs):
            recorded["input"] = input_parquet
            recorded.update(kwargs)
            return [(4, "cells_r4.parquet")]

        monkeypatch.setattr(overview_pkg, "create_overviews", fake)
        result = ops.create_overviews(
            "cells.parquet", levels=[4], max_tile_kb=300, scheme="a5", show_sql=True
        )
        assert result == [(4, "cells_r4.parquet")]
        assert recorded["input"] == "cells.parquet"
        assert recorded["levels"] == [4]
        assert recorded["max_tile_kb"] == 300
        assert recorded["scheme"] == "a5"
        assert recorded["show_sql"] is True


# ---------------------------------------------------------------------------
# Output naming
# ---------------------------------------------------------------------------


class TestOutputNaming:
    # Compare Path objects / name+parent, not raw strings: the separator is
    # platform-specific (backslashes on Windows).

    def test_grid_naming(self):
        r7 = Path(overview_output_path("/x/cells.parquet", "a5", 7))
        assert r7.name == "cells_r7.parquet"
        assert r7.parent == Path("/x")
        assert Path(overview_output_path("/x/cells.parquet", "h3", 4)).name == "cells_r4.parquet"

    def test_admin_naming(self):
        out = Path(overview_output_path("/x/by_region.parquet", "admin", "country"))
        assert out.name == "by_region_country.parquet"
        assert out.parent == Path("/x")

    def test_output_dir_override(self):
        out = Path(overview_output_path("/x/cells.parquet", "a5", 4, output_dir="/y"))
        assert out.name == "cells_r4.parquet"
        assert out.parent == Path("/y")


# ---------------------------------------------------------------------------
# Admin rollup (fast: fake country cache, no network)
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("fake_country_cache")
class TestAdminRollup:
    def test_region_to_country_rollup(self, tmp_path):
        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)

        # bytes_per_cell forces the country level into the plan (3 region
        # cells blow the z0 budget, 2 countries fit).
        results = create_overviews(str(src), bytes_per_cell=200000.0)
        assert results == [("country", str(tmp_path / "by_region_country.parquet"))]

        table = pq.read_table(results[0][1])
        rows = {code: i for i, code in enumerate(table.column("admin_code").to_pylist())}
        assert set(rows) == {"US", "FR", "unassigned"}

        def col(name, code):
            return table.column(name)[rows[code]].as_py()

        # Exact rollups.
        assert col("count", "US") == 5
        assert col("sum_area", "US") == pytest.approx(40.0)
        assert col("min_year", "US") == 1980
        assert col("max_year", "US") == 2000
        assert col("count_barn", "US") == 3
        assert col("count_other", "US") == 2
        # Count-weighted average: (2*4.0 + 3*6.0) / 5.
        assert col("avg_height", "US") == pytest.approx(5.2)
        assert col("count", "FR") == 4
        assert col("avg_height", "FR") == pytest.approx(2.5)
        # admin_name mirrors the country code at this level.
        assert col("admin_name", "US") == "US"
        # Unassigned bucket passes through with NULL geometry.
        assert col("count", "unassigned") == 1
        assert col("geometry", "unassigned") is None
        # Assigned countries got a (unioned) polygon.
        assert col("geometry", "US") is not None
        assert col("geometry", "FR") is not None

    def test_custom_cell_column_rollup(self, tmp_path):
        """--cell-column region_code must drive the whole rollup, not just
        detection (the parent expr, output column, and country join)."""
        src = tmp_path / "orig.parquet"
        renamed = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        table = pq.read_table(src)
        table = table.rename_columns(
            ["region_code" if n == "admin_code" else n for n in table.column_names]
        )
        pq.write_table(table, renamed)

        results = create_overviews(str(renamed), levels="country", cell_column="region_code")
        out = pq.read_table(results[0][1])
        assert "region_code" in out.column_names
        assert "admin_code" not in out.column_names
        rows = {code: i for i, code in enumerate(out.column("region_code").to_pylist())}
        assert set(rows) == {"US", "FR", "unassigned"}
        assert out.column("count")[rows["US"]].as_py() == 5
        # The country join keyed on the custom column still attaches geometry.
        assert out.column("geometry")[rows["US"]].as_py() is not None

    def test_explicit_levels_country(self, tmp_path):
        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        results = create_overviews(str(src), levels="country")
        assert [lvl for lvl, _ in results] == ["country"]

    def test_admin_auto_skips_when_base_fits(self, tmp_path):
        """Admin auto mode probes like grid auto mode: when the region base
        already fits the budget at every zoom, no overview is built."""
        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        assert create_overviews(str(src), bytes_per_cell=1.0) == []

    def test_admin_auto_without_geometry_builds_country(self, tmp_path):
        """A geometry-less admin aggregate cannot be probed; auto mode falls
        back to the only coarser level."""
        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src, with_geometry=False)
        results = create_overviews(str(src))
        assert [lvl for lvl, _ in results] == ["country"]

    def test_invalid_admin_level_errors(self, tmp_path):
        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        with pytest.raises(InvalidParameterError, match="country"):
            create_overviews(str(src), levels="province")

    def test_country_cache_path_with_single_quote(self, tmp_path, monkeypatch):
        """The country cache path must be SQL-escaped like every other path
        (home dirs can contain apostrophes)."""
        import shutil

        quoted_dir = tmp_path / "o'brien"
        quoted_dir.mkdir()
        plain_cache = tmp_path / "country_cache.parquet"
        quoted_cache = quoted_dir / "country_cache.parquet"
        shutil.copy(plain_cache, quoted_cache)
        monkeypatch.setattr(
            OvertureAdminDataset,
            "get_source_for_level",
            lambda self, level, no_cache=False: str(quoted_cache),
        )

        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        results = create_overviews(str(src), levels="country")
        out = pq.read_table(results[0][1])
        assert set(out.column("admin_code").to_pylist()) == {"US", "FR", "unassigned"}

    def test_existing_output_errors_without_force(self, tmp_path):
        """Like pmtiles pyramid, refuse to silently overwrite derived sibling
        files the user never named unless --force is given."""
        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        (result,) = create_overviews(str(src), levels="country")
        first_bytes = Path(result[1]).read_bytes()

        with pytest.raises(InvalidParameterError, match="force"):
            create_overviews(str(src), levels="country")
        assert Path(result[1]).read_bytes() == first_bytes  # untouched

        forced = create_overviews(str(src), levels="country", force=True)
        assert forced == [result]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


class TestCli:
    @pytest.mark.usefixtures("fake_country_cache")
    def test_cli_admin_rollup(self, tmp_path):
        src = tmp_path / "by_region.parquet"
        outdir = tmp_path / "out"
        outdir.mkdir()
        _write_admin_region_aggregate(src)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "process",
                "overview",
                str(src),
                "--output-dir",
                str(outdir),
                "--bytes-per-cell",
                "200000",
            ],
        )
        assert result.exit_code == 0, result.output
        assert (outdir / "by_region_country.parquet").exists()

    def test_cli_bad_input_errors_cleanly(self, tmp_path):
        path = tmp_path / "plain.parquet"
        pq.write_table(pa.table({"id": [1]}), path)
        runner = CliRunner()
        result = runner.invoke(cli, ["process", "overview", str(path)])
        assert result.exit_code != 0
        assert "cell-column" in result.output


# ---------------------------------------------------------------------------
# Grid gold rollups (slow: DuckDB community extensions)
# ---------------------------------------------------------------------------

_GOLD_POINTS = [
    # Clusters around three cities plus outliers, with crop + area attributes.
    (2.35, 48.85, "wheat", 4.0),
    (2.36, 48.86, "corn", 2.0),
    (2.37, 48.84, "wheat", 6.5),
    (13.40, 52.52, "corn", 1.5),
    (13.41, 52.53, "corn", 3.5),
    (13.39, 52.51, "soy", 9.0),
    (-3.70, 40.42, "wheat", 7.25),
    (-3.71, 40.41, "soy", 0.75),
    (30.0, -10.0, "wheat", 5.0),
    (100.5, 13.75, "soy", 8.0),
]

_GOLD_METRIC = "sum:area,avg:area,min:area,max:area"


def _write_points_geoparquet(path, rows):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, '{crop}', {area})" for lon, lat, crop, area in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, crop, area
            FROM (VALUES {values}) AS t(lon, lat, crop, area)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def _assert_rollup_matches_direct(rolled_path, direct_path, cell_column):
    rolled = pq.read_table(rolled_path)
    direct = pq.read_table(direct_path)
    assert rolled.num_rows == direct.num_rows

    def by_cell(table):
        cells = table.column(cell_column).to_pylist()
        return {
            c: {n: table.column(n)[i].as_py() for n in table.column_names}
            for i, c in enumerate(cells)
        }

    rolled_rows = by_cell(rolled)
    direct_rows = by_cell(direct)
    assert set(rolled_rows) == set(direct_rows)
    compare_cols = [
        n for n in direct.column_names if n.startswith(("count", "sum_", "avg_", "min_", "max_"))
    ]
    assert compare_cols  # sanity: the fixture exercised every rollup kind
    for cell, expect in direct_rows.items():
        got = rolled_rows[cell]
        for name in compare_cols:
            if name.startswith("avg_"):
                assert got[name] == pytest.approx(expect[name]), (cell, name)
            else:
                assert got[name] == expect[name], (cell, name)


@pytest.mark.slow
@pytest.mark.network
def test_gold_rollup_a5(tmp_path):
    """Rolling res-7 cells up to res 5 must equal aggregating raw data at res 5."""
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    base = tmp_path / "cells.parquet"
    direct = tmp_path / "direct_r5.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    kwargs = {"metric": _GOLD_METRIC, "breakdown": "crop", "out_geometry": "both"}
    aggregate_by_a5(str(src), str(base), resolution=7, **kwargs)
    aggregate_by_a5(str(src), str(direct), resolution=5, **kwargs)

    results = create_overviews(str(base), levels=[5])
    assert results == [(5, str(tmp_path / "cells_r5.parquet"))]
    _assert_rollup_matches_direct(results[0][1], str(direct), "a5_cell")


@pytest.mark.slow
@pytest.mark.network
def test_gold_rollup_h3(tmp_path):
    """H3 hexagons do not nest exactly, so a point near a cell edge can have a
    res-7 cell whose res-5 *ancestor* differs from the point's direct res-5
    cell. The rollup follows the true hierarchy (h3_cell_to_parent), so the
    gold expectation here is computed hierarchy-faithfully in SQL: key every
    raw point by parent-of-its-res-7-cell, then aggregate. This also proves
    the count-weighted avg equals the true mean when the metric has no NULLs.
    """
    from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3

    src = tmp_path / "points.parquet"
    base = tmp_path / "cells.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    kwargs = {"metric": _GOLD_METRIC, "breakdown": "crop", "out_geometry": "both"}
    aggregate_by_h3(str(src), str(base), resolution=7, **kwargs)

    results = create_overviews(str(base), levels="5")
    assert results == [(5, str(tmp_path / "cells_r5.parquet"))]

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute("INSTALL h3 FROM community; LOAD h3")
    expected_rows = con.execute(
        f"""
        SELECT h3_cell_to_parent(
                   h3_latlng_to_cell_string(ST_Y(geometry), ST_X(geometry), 7), 5
               ) AS h3_cell,
               COUNT(*) AS count,
               SUM(area) AS sum_area,
               AVG(area) AS avg_area,
               MIN(area) AS min_area,
               MAX(area) AS max_area,
               COUNT(*) FILTER (WHERE crop = 'wheat') AS count_wheat,
               COUNT(*) FILTER (WHERE crop = 'corn') AS count_corn,
               COUNT(*) FILTER (WHERE crop = 'soy') AS count_soy
        FROM read_parquet('{src}')
        GROUP BY 1
        """
    ).fetchall()
    columns = [
        "h3_cell",
        "count",
        "sum_area",
        "avg_area",
        "min_area",
        "max_area",
        "count_wheat",
        "count_corn",
        "count_soy",
    ]
    expected = {row[0]: dict(zip(columns, row, strict=True)) for row in expected_rows}
    con.close()

    rolled = pq.read_table(results[0][1])
    assert rolled.num_rows == len(expected)
    cells = rolled.column("h3_cell").to_pylist()
    assert set(cells) == set(expected)
    for i, cell in enumerate(cells):
        for name in columns[1:]:
            got = rolled.column(name)[i].as_py()
            if name == "avg_area":
                assert got == pytest.approx(expected[cell][name]), (cell, name)
            else:
                assert got == expected[cell][name], (cell, name)


@pytest.mark.slow
@pytest.mark.network
def test_level_not_coarser_than_base_errors(tmp_path):
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    base = tmp_path / "cells.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    aggregate_by_a5(str(src), str(base), resolution=7)
    with pytest.raises(InvalidParameterError, match="coarser"):
        create_overviews(str(base), levels=[7])


@pytest.mark.slow
@pytest.mark.network
def test_mixed_resolution_input_errors(tmp_path):
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    a = tmp_path / "r6.parquet"
    b = tmp_path / "r7.parquet"
    mixed = tmp_path / "mixed.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    aggregate_by_a5(str(src), str(a), resolution=6)
    aggregate_by_a5(str(src), str(b), resolution=7)
    pq.write_table(
        pa.concat_tables([pq.read_table(a), pq.read_table(b)], promote_options="default"),
        mixed,
    )
    with pytest.raises(InvalidParameterError, match="[Mm]ixed"):
        create_overviews(str(mixed), levels=[4])


@pytest.mark.slow
@pytest.mark.network
def test_auto_level_selection_a5(tmp_path):
    """With an absurd bytes-per-cell nothing fits, so the coarsest level is built."""
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    base = tmp_path / "cells.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    aggregate_by_a5(str(src), str(base), resolution=7)

    results = create_overviews(str(base), max_tile_kb=1, bytes_per_cell=1e6)
    assert [lvl for lvl, _ in results] == [0]
    assert (tmp_path / "cells_r0.parquet").exists()


@pytest.mark.slow
@pytest.mark.network
def test_table_overview_api(tmp_path):
    from geoparquet_io.api.table import Table
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    base = tmp_path / "cells.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    aggregate_by_a5(str(src), str(base), resolution=7, metric="sum:area")

    result = Table(pq.read_table(base)).overview(5)
    assert "a5_cell" in result.column_names
    assert "count" in result.column_names
    assert "sum_area" in result.column_names
    base_count = sum(pq.read_table(base).column("count").to_pylist())
    assert sum(result.to_arrow().column("count").to_pylist()) == base_count
