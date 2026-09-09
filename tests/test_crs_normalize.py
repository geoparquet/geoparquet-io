#!/usr/bin/env python3

"""Tests for the shared CRS-normalization helpers (issue #525).

Spatial operations (admin joins, lon/lat grid keying) assume OGC:CRS84 input.
These helpers produce the SQL needed to reproject non-CRS84 input to the
operation's expected CRS before the spatial work happens.
"""

from unittest import mock

import pytest

from geoparquet_io.core.crs_utils import (
    crs_string_for_transform,
    source_crs_string,
    transform_geom_sql,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection

EPSG_28992 = {"id": {"authority": "EPSG", "code": 28992}}


class TestCrsStringForTransform:
    def test_projected_dict_returns_auth_code(self):
        assert crs_string_for_transform(EPSG_28992) == "EPSG:28992"

    def test_projected_string_returns_auth_code(self):
        assert crs_string_for_transform("EPSG:28992") == "EPSG:28992"

    def test_none_returns_none(self):
        assert crs_string_for_transform(None) is None

    @pytest.mark.parametrize(
        "crs", ["EPSG:4326", "OGC:CRS84", {"id": {"authority": "OGC", "code": "CRS84"}}]
    )
    def test_default_crs_returns_none(self, crs):
        # The default CRS needs no transform when the target is lon/lat.
        assert crs_string_for_transform(crs) is None


class TestTransformGeomSql:
    def test_noop_for_none_source(self):
        assert transform_geom_sql('"geom"', None) == '"geom"'

    def test_noop_for_default_source(self):
        assert transform_geom_sql('"geom"', "EPSG:4326") == '"geom"'

    def test_projected_source_emits_st_transform(self):
        assert (
            transform_geom_sql('"geom"', EPSG_28992)
            == "ST_Transform(\"geom\", 'EPSG:28992', 'OGC:CRS84')"
        )

    def test_custom_target_crs(self):
        assert (
            transform_geom_sql("g", "EPSG:28992", target_crs="EPSG:3857")
            == "ST_Transform(g, 'EPSG:28992', 'EPSG:3857')"
        )

    def test_transform_runs_in_duckdb_and_yields_lonlat(self, fields_5070_file):
        """A projected (EPSG:5070) geometry transforms to lon/lat degrees."""
        src = source_crs_string(fields_5070_file)
        assert src == "EPSG:5070"
        expr = transform_geom_sql('"geometry"', src)
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.execute("SET geometry_always_xy = true;")
            raw_x, lon, lat = con.execute(
                f'SELECT ST_X(ST_Centroid("geometry")), '
                f"ST_X(ST_Centroid({expr})), ST_Y(ST_Centroid({expr})) "
                f"FROM '{fields_5070_file}' LIMIT 1"
            ).fetchone()
        finally:
            con.close()
        # Raw coords are projected metres (millions); transformed are valid degrees.
        assert abs(raw_x) > 1000
        assert -180 <= lon <= 180
        assert -90 <= lat <= 90


class TestSourceCrsString:
    def test_detects_projected_fixture(self, fields_5070_file):
        assert source_crs_string(fields_5070_file) == "EPSG:5070"

    def test_default_crs_input_needs_no_transform(self, buildings_test_file):
        # The common case: a CRS84 input is not reprojected (hot path stays free).
        assert source_crs_string(buildings_test_file) is None


class TestCrsStringFromTable:
    """Table-API CRS detection must be independent of ``geoarrow.pyarrow`` import.

    Importing ``geoarrow.pyarrow`` (which many code paths and tests do
    transitively) registers its extension types, so ``pyarrow.parquet`` returns
    a parquet-geo-only geometry as a registered extension type — the CRS then
    lives on ``field.type.crs`` and the raw ``ARROW:extension:metadata`` key is
    consumed off ``field.metadata``. Detection must work either way; otherwise
    the table-API grid ops silently skip reprojection (#525).
    """

    def _read_table(self, parquet_file):
        import pyarrow.parquet as pq

        return pq.read_table(parquet_file)

    def test_detects_projected_without_geoarrow_extension(self, fields_5070_file):
        # geoarrow.pyarrow's extension-type registration is global and
        # irreversible, and several tests in the suite import it; under -n auto
        # the import order is nondeterministic. Run in a fresh subprocess that
        # never imports geoarrow.pyarrow, so this provably exercises the raw
        # ARROW:extension:metadata (Case 2) branch rather than the registered
        # extension type (Case 1).
        import subprocess
        import sys
        import textwrap

        script = textwrap.dedent(
            """
            import sys
            import pyarrow.parquet as pq
            from geoparquet_io.core.crs_utils import crs_string_from_table

            table = pq.read_table(sys.argv[1])
            field = table.schema.field("geometry")
            # Guard: confirm we are actually in Case 2 (plain binary field with
            # raw extension metadata), not the registered-extension-type Case 1.
            assert getattr(field.type, "extension_name", None) is None, (
                "expected a plain binary geometry field (Case 2), got a "
                "registered extension type"
            )
            md = field.metadata or {}
            assert md.get(b"ARROW:extension:metadata"), (
                "expected raw ARROW:extension:metadata on the geometry field"
            )
            assert "geoarrow.pyarrow" not in sys.modules
            assert crs_string_from_table(table, "geometry") == "EPSG:5070"
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", script, fields_5070_file],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"subprocess failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

    def test_detects_projected_with_geoarrow_extension_registered(self, fields_5070_file):
        # Force the registered-extension-type carrier (Case 1).
        import geoarrow.pyarrow  # noqa: F401

        from geoparquet_io.core.crs_utils import crs_string_from_table

        table = self._read_table(fields_5070_file)
        assert crs_string_from_table(table, "geometry") == "EPSG:5070"


class TestHotPathUnchanged:
    """Regression guard (#525 perf): CRS84 input must not pay for reprojection.

    The reprojection is keyed off a detected source CRS. For default/CRS84 input
    the join SQL must keep its bbox pre-filter and contain no ST_Transform, so the
    common path is byte-for-byte what it was before this change.
    """

    def test_join_condition_keeps_bbox_prefilter_without_transform(self):
        from geoparquet_io.core.duckdb_utils import build_spatial_join_condition

        sql = build_spatial_join_condition("geom", "geom", "bbox", "bbox")
        assert "ST_Transform" not in sql
        assert ".xmin" in sql and ".xmax" in sql  # bbox pre-filter retained

    def test_join_condition_reprojects_and_drops_bbox_when_transforming(self):
        from geoparquet_io.core.duckdb_utils import build_spatial_join_condition

        sql = build_spatial_join_condition(
            "geom",
            "geom",
            "bbox",
            "bbox",
            input_geom_sql="ST_Transform(a.geom, 'EPSG:5070', 'OGC:CRS84')",
        )
        assert "ST_Transform" in sql
        assert ".xmin" not in sql  # bbox pre-filter skipped (source-CRS bbox)


def _read_column(parquet_file, column):
    con = get_duckdb_connection(load_spatial=True)
    try:
        rows = con.execute(f"SELECT \"{column}\" FROM '{parquet_file}' ORDER BY 1").fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


class TestGridKeyingIsCrsAware:
    """Class B (#525): lon/lat grid keying must reproject projected input.

    Keying a projected (EPSG:5070) file must yield the same cells as keying a
    reprojected-to-CRS84 copy of the same data. Before the fix the projected
    metres were fed to ``*_lonlat_to_cell`` as degrees, producing different
    (wrong) cells.
    """

    @pytest.fixture
    def reprojected_4326(self, fields_5070_file, tmp_path):
        from geoparquet_io.core.reproject import reproject

        out = tmp_path / "fields_4326.parquet"
        reproject(fields_5070_file, str(out), target_crs="EPSG:4326")
        return str(out)

    @pytest.mark.parametrize(
        "module,func_name,column,res_kw",
        [
            ("a5", "add_a5_column", "a5_cell", {"a5_resolution": 12}),
            ("h3", "add_h3_column", "h3_cell", {"h3_resolution": 9}),
            ("s2", "add_s2_column", "s2_cell", {"s2_level": 14}),
            ("quadkey", "add_quadkey_column", "quadkey", {"resolution": 16}),
        ],
    )
    def test_projected_cells_match_reprojected(
        self, fields_5070_file, reprojected_4326, tmp_path, module, func_name, column, res_kw
    ):
        import importlib

        mod = importlib.import_module(f"geoparquet_io.core.add.{module}")
        add_func = getattr(mod, func_name)

        proj_out = tmp_path / f"proj_{module}.parquet"
        wgs_out = tmp_path / f"wgs_{module}.parquet"
        add_func(fields_5070_file, str(proj_out), **res_kw)
        add_func(reprojected_4326, str(wgs_out), **res_kw)

        proj_cells = _read_column(str(proj_out), column)
        wgs_cells = _read_column(str(wgs_out), column)

        # Keying the projected file (now reprojected internally) must agree with
        # keying a reprojected copy. A few fine-resolution boundary cells can flip
        # due to the reproject-to-file coordinate rounding in the oracle, so allow
        # a tiny mismatch — without the fix, ~every cell differs (metres keyed as
        # degrees), so this still cleanly distinguishes fixed from broken.
        assert len(proj_cells) == len(wgs_cells)
        mismatches = sum(1 for a, b in zip(proj_cells, wgs_cells, strict=True) if a != b)
        assert mismatches <= max(1, len(proj_cells) // 100)
        # And the keying actually produced a cell for every row (not null).
        assert all(c is not None for c in proj_cells)


class TestGridKeyingStreamingIsCrsAware:
    """Class B (#530), streaming path: stdout streaming must reproject too.

    ``gpio add quadkey projected.parquet -`` routes through the streaming path
    (output ``-``). It must reproject projected input to lon/lat before keying,
    exactly like the file path — otherwise the documented stdin/stdout feature
    silently emits wrong cells (metres keyed as degrees) for projected input.
    """

    def _stream_quadkeys(self, input_file, monkeypatch, resolution):
        """Run the streaming quadkey path to stdout and read the cells back."""
        import io

        import pyarrow.ipc as ipc

        from geoparquet_io.core.add.quadkey import add_quadkey_column

        buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.isatty.return_value = False
        mock_stdout.buffer = buffer
        monkeypatch.setattr("sys.stdout", mock_stdout)

        add_quadkey_column(input_file, "-", resolution=resolution)

        buffer.seek(0)
        table = ipc.open_stream(buffer).read_all()
        return table.column("quadkey").to_pylist()

    def test_projected_streaming_matches_file_path(self, fields_5070_file, tmp_path, monkeypatch):
        from geoparquet_io.core.add.quadkey import add_quadkey_column

        # File-based path (already CRS-aware) is the oracle.
        file_out = tmp_path / "file.parquet"
        add_quadkey_column(fields_5070_file, str(file_out), resolution=16)
        file_cells = _read_column(str(file_out), "quadkey")

        stream_cells = self._stream_quadkeys(fields_5070_file, monkeypatch, resolution=16)

        assert len(stream_cells) == len(file_cells)
        assert all(c is not None for c in stream_cells)
        # The streaming path must reproject like the file path, not key metres as
        # degrees — so the same set of cells comes out (file oracle is sorted).
        assert sorted(stream_cells) == file_cells

    def test_projected_streaming_does_not_raise(self, fields_5070_file, monkeypatch):
        """Projected input must reproject, not raise GeoParquetError (the old guard)."""
        cells = self._stream_quadkeys(fields_5070_file, monkeypatch, resolution=16)
        assert all(c is not None for c in cells)

    @staticmethod
    def _stream_grid(input_file, monkeypatch, module, func_name, column, res_kw):
        """Run a grid add to stdout (streaming) and read the cells back."""
        import importlib
        import io

        import pyarrow.ipc as ipc

        buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.isatty.return_value = False
        mock_stdout.buffer = buffer
        monkeypatch.setattr("sys.stdout", mock_stdout)

        add_func = getattr(importlib.import_module(f"geoparquet_io.core.add.{module}"), func_name)
        add_func(input_file, "-", **res_kw)

        buffer.seek(0)
        return ipc.open_stream(buffer).read_all().column(column).to_pylist()

    @pytest.mark.parametrize(
        "module,func_name,column,res_kw",
        [
            ("a5", "add_a5_column", "a5_cell", {"a5_resolution": 12}),
            ("h3", "add_h3_column", "h3_cell", {"h3_resolution": 9}),
            ("s2", "add_s2_column", "s2_cell", {"s2_level": 14}),
        ],
    )
    def test_a5_h3_s2_streaming_matches_file_path(
        self, fields_5070_file, tmp_path, monkeypatch, module, func_name, column, res_kw
    ):
        import importlib

        add_func = getattr(importlib.import_module(f"geoparquet_io.core.add.{module}"), func_name)

        # File-based path (already CRS-aware) is the oracle.
        file_out = tmp_path / f"file_{module}.parquet"
        add_func(fields_5070_file, str(file_out), **res_kw)
        file_cells = _read_column(str(file_out), column)

        stream_cells = self._stream_grid(
            fields_5070_file, monkeypatch, module, func_name, column, res_kw
        )

        assert len(stream_cells) == len(file_cells)
        assert all(c is not None for c in stream_cells)
        # Streaming must reproject like the file path (not key metres as degrees),
        # so the same multiset of cells comes out.
        assert sorted(stream_cells) == sorted(file_cells)


class TestGridKeyingTableApiIsCrsAware:
    """Class B (#525), Python API: the table-centric grid ops must reproject too.

    ``gpio.read(projected).add_h3(...)`` routes through ``add_*_table`` (not the
    file path). These must reproject projected input to lon/lat before keying,
    exactly like the CLI/file path — otherwise the public API silently emits
    wrong cells (metres fed to ``*_lonlat_to_cell`` as degrees), and quadkey
    rejects projected input outright.
    """

    @pytest.fixture
    def reprojected_4326(self, fields_5070_file, tmp_path):
        from geoparquet_io.core.reproject import reproject

        out = tmp_path / "fields_4326.parquet"
        reproject(fields_5070_file, str(out), target_crs="EPSG:4326")
        return str(out)

    @pytest.mark.parametrize(
        "op_name,column,kwargs",
        [
            ("add_a5", "a5_cell", {"resolution": 12}),
            ("add_h3", "h3_cell", {"resolution": 9}),
            ("add_s2", "s2_cell", {"level": 14}),
            ("add_quadkey", "quadkey", {"resolution": 16}),
        ],
    )
    def test_projected_table_cells_match_reprojected(
        self, fields_5070_file, reprojected_4326, op_name, column, kwargs
    ):
        import geoparquet_io as gpio
        from geoparquet_io.api import ops

        op = getattr(ops, op_name)
        proj_cells = op(gpio.read(fields_5070_file)._table, **kwargs).column(column).to_pylist()
        wgs_cells = op(gpio.read(reprojected_4326)._table, **kwargs).column(column).to_pylist()

        # Reprojecting internally must agree with keying a reprojected copy. Allow
        # a tiny boundary-cell tolerance (same as the file-path oracle); without
        # the fix ~every projected cell differs (metres keyed as degrees).
        assert len(proj_cells) == len(wgs_cells)
        mismatches = sum(1 for a, b in zip(proj_cells, wgs_cells, strict=True) if a != b)
        assert mismatches <= max(1, len(proj_cells) // 100)
        assert all(c is not None for c in proj_cells)

    def test_projected_quadkey_table_does_not_raise(self, fields_5070_file):
        """Projected input must reproject, not raise GeoParquetError (the old guard)."""
        import geoparquet_io as gpio
        from geoparquet_io.api import ops

        result = ops.add_quadkey(gpio.read(fields_5070_file)._table, resolution=16)
        assert "quadkey" in result.column_names
        assert all(c is not None for c in result.column("quadkey").to_pylist())


def _con_with_admin():
    """Connection with a CRS84 admin polygon covering the 5070 fixture's extent.

    The fixture's geometries reproject to roughly lon 18 / lat 47, so the admin
    polygon spans (17..19, 46..48).
    """
    con = get_duckdb_connection(load_spatial=True)
    con.execute("SET geometry_always_xy = true;")
    con.execute(
        "CREATE TEMP TABLE _admin AS "
        "SELECT ST_GeomFromText('POLYGON((17 46, 19 46, 19 48, 17 48, 17 46))') AS geom, "
        "'R1' AS region"
    )
    return con


class TestAdminJoinIsCrsAware:
    """Class A (#525): admin spatial joins must reproject projected input.

    Without reprojection a projected (EPSG:5070) input either errors on a CRS
    mismatch or silently matches nothing (metres compared against degrees).
    """

    def test_admin_divisions_join_assigns_with_reprojection(self, fields_5070_file):
        from geoparquet_io.core.add.admin_divisions import _build_spatial_join_query

        con = _con_with_admin()
        try:
            kwargs = {
                "input_source": fields_5070_file,
                "admin_subquery": "(SELECT geom, region FROM _admin)",
                "admin_select_clause": 'b."region" AS region',
                "input_bbox_col": None,
                "admin_bbox_col": None,
                "input_geom_col": "geometry",
                "admin_geom_col": "geom",
            }
            with_crs = _build_spatial_join_query(source_crs="EPSG:5070", **kwargs)
            without = _build_spatial_join_query(source_crs=None, **kwargs)
            assigned = con.execute(
                f"SELECT COUNT(*) FROM ({with_crs}) WHERE region IS NOT NULL"
            ).fetchone()[0]
            not_assigned = con.execute(
                f"SELECT COUNT(*) FROM ({without}) WHERE region IS NOT NULL"
            ).fetchone()[0]
        finally:
            con.close()
        assert assigned > 0  # reprojected input intersects the admin polygon
        assert not_assigned == 0  # raw projected metres match nothing

    def test_admin_hierarchical_enrichment_assigns_with_reprojection(self, fields_5070_file):
        from geoparquet_io.core.partition.admin_hierarchical import _build_enrichment_query

        con = _con_with_admin()
        try:
            sql = _build_enrichment_query(
                input_source=fields_5070_file,
                admin_table_ref="_admin",
                admin_where_clause="",
                admin_select_clause='b."region" AS region',
                admin_geom_col="geom",
                admin_bbox_col=None,
                boundary_columns=["region"],
                input_geom_col="geometry",
                input_bbox_col=None,
                enriched_table="_enriched",
                source_crs="EPSG:5070",
            )
            con.execute(sql)
            assigned = con.execute(
                "SELECT COUNT(*) FROM _enriched WHERE region IS NOT NULL"
            ).fetchone()[0]
        finally:
            con.close()
        assert assigned > 0


class TestAdminReprojectsAdminSideWithBbox:
    """Class A perf (#525): with bboxes on both sides the admin side is reprojected.

    Reprojecting the (small) admin polygons into the input CRS — instead of the
    large input into CRS84 — keeps the cheap bbox pre-filter, so the join does
    not degrade to a full nested-loop ST_Intersects (the #460 regression). The
    input geometry must be left untransformed.
    """

    def test_join_query_keeps_bbox_prefilter_and_leaves_input_untransformed(self):
        from geoparquet_io.core.add.admin_divisions import _build_spatial_join_query

        sql = _build_spatial_join_query(
            input_source="x.parquet",
            admin_subquery="(SELECT geom, bbox, region FROM admin)",
            admin_select_clause='b."region" AS region',
            input_bbox_col="bbox",
            admin_bbox_col="bbox",
            input_geom_col="geometry",
            admin_geom_col="geom",
            source_crs="EPSG:5070",
        )
        # The join itself does not transform the input (admin reproject lives in
        # the subquery); the bbox pre-filter is retained.
        assert "ST_Transform" not in sql
        assert ".xmin" in sql and ".xmax" in sql

    def test_admin_subquery_reprojects_admin_and_synthesizes_bbox(self):
        from geoparquet_io.core.add.admin_divisions import _build_admin_subquery

        sq = _build_admin_subquery(
            dataset=None,
            levels=None,
            boundary_columns=["region"],
            admin_table_ref="'admin.parquet'",
            admin_geom_col="geom",
            admin_bbox_col="bbox",
            admin_where_clauses=[],
            source_crs="EPSG:5070",
        )
        assert "ST_Transform" in sq  # admin polygons reprojected into input CRS
        assert "struct_pack" in sq  # source-CRS bbox synthesized for the pre-filter

    def test_enrichment_query_reprojects_admin_side_keeps_prefilter(self):
        from geoparquet_io.core.partition.admin_hierarchical import _build_enrichment_query

        sql = _build_enrichment_query(
            input_source="x.parquet",
            admin_table_ref="admin",
            admin_where_clause="",
            admin_select_clause='b."region" AS region',
            admin_geom_col="geom",
            admin_bbox_col="bbox",
            boundary_columns=["region"],
            input_geom_col="geometry",
            input_bbox_col="bbox",
            enriched_table="_enriched",
            source_crs="EPSG:5070",
        )
        assert "ST_Transform" in sql  # admin reprojected
        # Identifiers are quoted (#926); the input geom is still untransformed.
        assert 'ST_Intersects(b."geom", a."geometry")' in sql
        assert 'a."bbox".xmin <= b."bbox".xmax' in sql  # bbox pre-filter restored
        assert "struct_pack" in sql

    def test_enrichment_reproject_path_runs_and_assigns(self, fields_5070_file):
        """The reproject-admin SQL executes in DuckDB and assigns via the prefilter."""
        from geoparquet_io.core.partition.admin_hierarchical import _build_enrichment_query

        con = _con_with_admin()
        try:
            # Admin polygon with a CRS84 bbox struct.
            con.execute(
                "CREATE TEMP TABLE _admin_b AS SELECT geom, region, "
                "struct_pack(xmin := ST_XMin(geom), xmax := ST_XMax(geom), "
                "ymin := ST_YMin(geom), ymax := ST_YMax(geom)) AS bbox FROM _admin"
            )
            # Projected input with a source-CRS bbox struct.
            con.execute(
                "CREATE TEMP TABLE _inp AS SELECT geometry, "
                "struct_pack(xmin := ST_XMin(geometry), xmax := ST_XMax(geometry), "
                "ymin := ST_YMin(geometry), ymax := ST_YMax(geometry)) AS bbox "
                f"FROM '{fields_5070_file}'"
            )
            sql = _build_enrichment_query(
                input_source="_inp",
                admin_table_ref="_admin_b",
                admin_where_clause="",
                admin_select_clause='b."region" AS region',
                admin_geom_col="geom",
                admin_bbox_col="bbox",
                boundary_columns=["region"],
                input_geom_col="geometry",
                input_bbox_col="bbox",
                enriched_table="_enriched",
                input_is_table_ref=True,
                source_crs="EPSG:5070",
            )
            con.execute(sql)
            assigned = con.execute(
                "SELECT COUNT(*) FROM _enriched WHERE region IS NOT NULL"
            ).fetchone()[0]
        finally:
            con.close()
        assert assigned > 0
