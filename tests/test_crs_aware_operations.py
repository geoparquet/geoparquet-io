"""CRS-awareness of spatial operations (issue #525).

Several spatial operations assumed the input was OGC:CRS84 (lon/lat degrees):

- Grid keying (``add a5``/``h3``/``s2``/``quadkey`` and the ``partition``
  equivalents that build on them) fed projected metres straight into
  ``*_lonlat_to_cell``, producing silently-wrong cells.
- Admin joins (``add admin-divisions``, ``partition admin``) ran
  ``ST_Intersects`` between a projected input and CRS84 admin boundaries, which
  DuckDB 1.5 refuses with a CRS-mismatch error.

These tests exercise the shared reprojection on the projected (EPSG:5070) test
fixtures, which are the same fields as the CRS84 fixture in a different CRS.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add.a5 import add_a5_column, add_a5_table
from geoparquet_io.core.add.admin_divisions import add_admin_divisions_multi
from geoparquet_io.core.add.h3 import add_h3_column, add_h3_table
from geoparquet_io.core.add.quadkey import add_quadkey_column, add_quadkey_table
from geoparquet_io.core.add.s2 import add_s2_column, add_s2_table
from geoparquet_io.core.common import add_bbox
from geoparquet_io.core.crs_utils import (
    crs_transform_sql_expr,
    extract_crs_from_table,
    parse_crs_string_to_projjson,
)
from geoparquet_io.core.duckdb_utils import build_spatial_join_condition
from geoparquet_io.core.reproject import reproject


def _column(parquet_file: str, column: str) -> list:
    con = duckdb.connect()
    con.execute("LOAD spatial")
    try:
        rows = con.execute(f"SELECT \"{column}\" FROM read_parquet('{parquet_file}')").fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


def _agreement(a: list, b: list) -> float:
    assert len(a) == len(b)
    return sum(1 for x, y in zip(a, b, strict=True) if x == y) / len(a)


# --------------------------------------------------------------------------- #
# Shared helper
# --------------------------------------------------------------------------- #


class TestCrsTransformSqlExpr:
    """Unit tests for the shared ``crs_transform_sql_expr`` helper."""

    def test_none_crs_is_untransformed(self):
        assert crs_transform_sql_expr('"geometry"', None) == '"geometry"'

    def test_default_crs_is_untransformed(self):
        # EPSG:4326 / OGC:CRS84 are the target — never transformed.
        assert crs_transform_sql_expr('"g"', "EPSG:4326") == '"g"'
        assert crs_transform_sql_expr('"g"', "OGC:CRS84") == '"g"'
        assert crs_transform_sql_expr('"g"', {"id": {"authority": "EPSG", "code": 4326}}) == '"g"'

    def test_projected_crs_wraps_in_transform(self):
        expr = crs_transform_sql_expr('ST_Centroid("g")', "EPSG:5070")
        assert expr == "ST_Transform(ST_Centroid(\"g\"), 'EPSG:5070', 'OGC:CRS84')"

    def test_projjson_dict_is_resolved(self):
        crs = parse_crs_string_to_projjson("EPSG:5070")
        expr = crs_transform_sql_expr('"g"', crs)
        assert "ST_Transform" in expr and "EPSG:5070" in expr

    def test_custom_target(self):
        expr = crs_transform_sql_expr('"g"', "EPSG:4326", target_crs="EPSG:3857")
        # 4326 source is the default and is skipped regardless of target.
        assert expr == '"g"'
        expr = crs_transform_sql_expr('"g"', "EPSG:5070", target_crs="EPSG:3857")
        assert expr == "ST_Transform(\"g\", 'EPSG:5070', 'EPSG:3857')"


# --------------------------------------------------------------------------- #
# Class B — grid keying must reproject projected input before keying
# --------------------------------------------------------------------------- #

_GRID_FILE_CASES = [
    ("a5", add_a5_column, "a5_cell", {"a5_resolution": 12}),
    ("h3", add_h3_column, "h3_cell", {"h3_resolution": 9}),
    ("s2", add_s2_column, "s2_cell", {"s2_level": 15}),
    ("quadkey", add_quadkey_column, "quadkey", {"resolution": 14, "use_centroid": True}),
]


@pytest.fixture
def crs84_reference(fields_5070_file, tmp_path):
    """The projected fixture reprojected to OGC:CRS84 (the keying baseline)."""
    ref = str(tmp_path / "ref_crs84.parquet")
    reproject(fields_5070_file, ref, target_crs="EPSG:4326", verbose=False)
    return ref


@pytest.mark.parametrize(("name", "fn", "col", "kwargs"), _GRID_FILE_CASES)
def test_grid_keying_reprojects_projected_file(
    name, fn, col, kwargs, fields_5070_file, crs84_reference, tmp_path
):
    """Keying a projected file matches keying it after reprojection to CRS84."""
    out_proj = str(tmp_path / f"{name}_proj.parquet")
    out_ref = str(tmp_path / f"{name}_ref.parquet")
    fn(fields_5070_file, out_proj, **kwargs)
    fn(crs84_reference, out_ref, **kwargs)

    proj_cells = _column(out_proj, col)
    ref_cells = _column(out_ref, col)

    assert all(c is not None for c in proj_cells)
    # Centroid-then-transform vs transform-then-centroid differ for a few cells
    # near grid boundaries; the overwhelming majority must agree.
    assert _agreement(proj_cells, ref_cells) >= 0.97


def test_grid_keying_is_load_bearing(fields_5070_file, tmp_path):
    """The reprojection genuinely changes the cells for a projected input.

    Compares the CRS-aware output against the pre-fix behaviour — feeding the
    raw projected centroid (metres) straight into ``a5_lonlat_to_cell`` as if it
    were lon/lat — and asserts none of the cells agree.
    """
    out = str(tmp_path / "aware.parquet")
    add_a5_column(fields_5070_file, out, a5_resolution=12)
    aware_cells = _column(out, "a5_cell")

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL a5 FROM community; LOAD a5")
    con.execute("SET geometry_always_xy = true")
    blind_cells = [
        r[0]
        for r in con.execute(
            f"""
            SELECT a5_lonlat_to_cell(
                ST_X(ST_Centroid(geometry)), ST_Y(ST_Centroid(geometry)), 12
            )
            FROM read_parquet('{fields_5070_file}')
            """
        ).fetchall()
    ]
    con.close()

    assert _agreement(aware_cells, blind_cells) == 0.0


# --------------------------------------------------------------------------- #
# Grid keying — table (Python API) path
# --------------------------------------------------------------------------- #

_GRID_TABLE_CASES = [
    ("a5", add_a5_table, "a5_cell", {"resolution": 12}),
    ("h3", add_h3_table, "h3_cell", {"resolution": 9}),
    ("s2", add_s2_table, "s2_cell", {"level": 15}),
    ("quadkey", add_quadkey_table, "quadkey", {"resolution": 14, "use_centroid": True}),
]


def _projected_table(crs84_file: str, target: str = "EPSG:5070"):
    """Build an in-memory table in ``target`` CRS with explicit geo metadata."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute("SET geometry_always_xy = true")
    table = (
        con.sql(
            f"""
        SELECT * EXCLUDE(geometry),
               ST_AsWKB(ST_Transform(geometry, 'OGC:CRS84', '{target}')) AS geometry
        FROM read_parquet('{crs84_file}')
        """
        )
        .arrow()
        .read_all()
    )
    con.close()
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "crs": parse_crs_string_to_projjson(target)}},
    }
    return table.replace_schema_metadata({b"geo": json.dumps(geo_meta).encode("utf-8")})


@pytest.mark.parametrize(("name", "fn", "col", "kwargs"), _GRID_TABLE_CASES)
def test_grid_keying_table_reprojects(name, fn, col, kwargs, fields_geom_type_only_file):
    """The table API detects a projected CRS in geo metadata and reprojects."""
    table84 = pq.read_table(fields_geom_type_only_file)
    table_proj = _projected_table(fields_geom_type_only_file)
    assert extract_crs_from_table(table_proj, "geometry") is not None

    cells84 = fn(table84, **kwargs).column(col).to_pylist()
    cells_proj = fn(table_proj, **kwargs).column(col).to_pylist()

    assert all(c is not None for c in cells_proj)
    assert _agreement(cells84, cells_proj) >= 0.97


def test_quadkey_no_longer_errors_on_projected_input(fields_5070_file, tmp_path):
    """Quadkey used to raise on a projected CRS; it now reprojects instead."""
    out = str(tmp_path / "qk.parquet")
    add_quadkey_column(fields_5070_file, out, resolution=12)
    cells = _column(out, "quadkey")
    assert len(cells) == 100
    assert all(isinstance(c, str) and len(c) == 12 for c in cells)


# --------------------------------------------------------------------------- #
# Class A — admin joins must reproject input to the admin (CRS84) before join
# --------------------------------------------------------------------------- #


class TestSpatialJoinConditionCrs:
    """``build_spatial_join_condition`` honours a reprojected input geom."""

    def test_default_uses_plain_column(self):
        cond = build_spatial_join_condition("geom", "geometry", "bbox", "bbox")
        assert "ST_Transform" not in cond
        assert 'a."geom"' in cond
        # bbox pre-filter present when both sides have bbox.
        assert ".xmin" in cond

    def test_reprojected_input_skips_bbox_prefilter(self):
        transformed = "ST_Transform(a.\"geom\", 'EPSG:5070', 'OGC:CRS84')"
        cond = build_spatial_join_condition(
            "geom", "geometry", "bbox", "bbox", input_geom_sql=transformed
        )
        assert transformed in cond
        # The source-CRS bbox cannot be compared cross-CRS, so it is dropped.
        assert ".xmin" not in cond
        assert cond.startswith("ST_Intersects(")


@pytest.fixture
def local_admin_dataset(tmp_path):
    """A single-polygon CRS84 admin dataset covering the test fields' extent."""
    admin = str(tmp_path / "admin.parquet")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute(
        f"""
        COPY (
            SELECT 'XX' AS country,
                   ST_GeomFromText('POLYGON((16 45, 20 45, 20 49, 16 49, 16 45))') AS geometry
        ) TO '{admin}' (FORMAT PARQUET)
        """
    )
    con.close()
    # Add the bbox covering column + GeoParquet metadata the join expects.
    add_bbox(admin, "bbox", False)
    return admin


def test_admin_divisions_reprojects_projected_input(
    fields_5070_file, local_admin_dataset, tmp_path
):
    """``add admin-divisions`` joins a projected input against CRS84 boundaries.

    Without reprojection DuckDB 1.5 raises a CRS-mismatch Binder error; with it
    every feature falls inside the covering admin polygon.
    """
    out = str(tmp_path / "enriched.parquet")
    add_admin_divisions_multi(
        input_parquet=fields_5070_file,
        output_parquet=out,
        dataset_name="current",
        levels=["country"],
        dataset_source=local_admin_dataset,
        verbose=False,
    )
    countries = _column(out, "current_country")
    assert len(countries) == 100
    assert all(c == "XX" for c in countries)


def test_partition_admin_reprojects_projected_input_single_source(
    fields_5070_file, local_admin_dataset, tmp_path, monkeypatch
):
    """``partition admin`` joins a projected input against CRS84 boundaries.

    Exercises the single-source join (one ``run_admin_join`` step):
    without reprojecting the projected (EPSG:5070) input to the admin CRS the
    join raises a CRS-mismatch error; with it every feature falls inside the
    covering ``XX`` polygon and lands in a single partition.
    """
    from geoparquet_io.core.admin_datasets import CurrentAdminDataset
    from geoparquet_io.core.partition.admin_hierarchical import (
        partition_by_admin_hierarchical,
    )

    def fake_create(dataset_name, source_path=None, verbose=False):
        return CurrentAdminDataset(source_path=local_admin_dataset, verbose=verbose)

    monkeypatch.setattr(
        "geoparquet_io.core.admin_datasets.AdminDatasetFactory.create",
        staticmethod(fake_create),
    )

    out_dir = str(tmp_path / "out_single")
    count = partition_by_admin_hierarchical(
        fields_5070_file,
        out_dir,
        dataset_name="current",
        levels=["country"],
        hive=True,
    )

    assert count == 1
    files = list(Path(out_dir).rglob("*.parquet"))
    assert len(files) == 1
    assert "country=XX" in str(files[0])
    assert pq.ParquetFile(str(files[0])).metadata.num_rows == 100


def test_partition_admin_reprojects_projected_input_per_level(
    fields_5070_file, tmp_path, monkeypatch
):
    """``partition admin`` reprojects a projected input on the per-level path.

    Exercises the chained temp-table enrichment path
    (``run_admin_join``'s per-level chain) used by Overture-shaped datasets:
    each level's join must see the input reprojected to the admin (CRS84) CRS.
    The fields fixture lies within lon 16-20 / lat 45-49 (in CRS84), split here
    into two regions; every feature must be attributed to the covering country
    and one of the two regions.
    """
    import duckdb

    from geoparquet_io.core.admin_datasets import OvertureAdminDataset
    from geoparquet_io.core.partition.admin_hierarchical import (
        partition_by_admin_hierarchical,
    )

    admin_file = str(tmp_path / "overture_admin.parquet")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        f"""
        COPY (
            SELECT subtype, country, region, geometry,
                {{'xmin': ST_XMin(geometry), 'xmax': ST_XMax(geometry),
                  'ymin': ST_YMin(geometry), 'ymax': ST_YMax(geometry)}} AS bbox
            FROM (VALUES
                ('country', 'XX', NULL,
                 ST_GeomFromText('POLYGON((16 45, 20 45, 20 49, 16 49, 16 45))')),
                ('region', 'XX', 'XX-W',
                 ST_GeomFromText('POLYGON((16 45, 18 45, 18 49, 16 49, 16 45))')),
                ('region', 'XX', 'XX-E',
                 ST_GeomFromText('POLYGON((18 45, 20 45, 20 49, 18 49, 18 45))'))
            ) AS t(subtype, country, region, geometry)
        ) TO '{admin_file}' (FORMAT PARQUET)
        """
    )
    con.close()

    def fake_create(dataset_name, source_path=None, verbose=False):
        return OvertureAdminDataset(source_path=admin_file, verbose=verbose)

    monkeypatch.setattr(
        "geoparquet_io.core.admin_datasets.AdminDatasetFactory.create",
        staticmethod(fake_create),
    )

    out_dir = str(tmp_path / "out_per_level")
    count = partition_by_admin_hierarchical(
        fields_5070_file,
        out_dir,
        dataset_name="overture",
        levels=["country", "region"],
        hive=True,
    )

    files = list(Path(out_dir).rglob("*.parquet"))
    assert files, "no partitions were created"
    assert count == len(files)
    # No row multiplication: per-level chaining emits one row per input feature.
    assert sum(pq.ParquetFile(str(f)).metadata.num_rows for f in files) == 100
    # Every feature attributed to the covering country and a known region.
    assert all("country=XX" in str(f) for f in files)
    assert {p.name for f in files for p in Path(f).parents if p.name.startswith("region=")} <= {
        "region=XX-W",
        "region=XX-E",
    }
