"""SQL identifier quoting in the admin spatial joins (one threat model).

Geometry and bbox column names are read verbatim from an input file's own
GeoParquet metadata (`geo.primary_column`, the bbox covering), so the attacker
is whoever wrote the file, not the person running the command. Interpolated
without quoting, a hostile name injects SQL into the user's DuckDB session, and
a legitimate name with a space or quote crashes with a ParserException.

- PR #461 regression (todo 008): `add admin-divisions` / `add country-codes`
  with a hostile primary geometry column name.
- #926: `partition admin` with a hostile or space-containing bbox column name.
  The struct-field subtlety: only the column segment is quoted
  (``a."col".xmin``); the ``a.`` alias and the ``.xmin`` accessor stay bare.

Merged from test_partition_admin_bbox_quoting.py (#942): both joins are built by
``duckdb_utils.build_spatial_join_condition`` now.
"""

import json

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add
from geoparquet_io.core.partition.admin_hierarchical import (
    _build_enrichment_query,
    partition_by_admin_hierarchical,
)

# A column name that breaks out of an unquoted ST_Intersects(...) / ST_XMin(...)
# call if it is not properly quoted.
MALICIOUS_GEOM_NAME = 'geom") AS pwn, (SELECT 1) AS x, ST_GeomFromText("'


@pytest.fixture
def malicious_geom_column_file(buildings_test_file, tmp_path):
    """A valid GeoParquet whose primary geometry column has a dangerous name.

    Built from buildings_test.parquet (WKB geometry, no bbox column) by renaming
    the geometry column and rewriting the `geo` metadata to point at the new name.
    """
    table = pq.read_table(buildings_test_file)
    new_names = [MALICIOUS_GEOM_NAME if n == "geometry" else n for n in table.column_names]
    table = table.rename_columns(new_names)

    geo = {
        "version": "1.0.0",
        "primary_column": MALICIOUS_GEOM_NAME,
        "columns": {MALICIOUS_GEOM_NAME: {"encoding": "WKB", "geometry_types": ["Polygon"]}},
    }
    existing = table.schema.metadata or {}
    md = {k: v for k, v in existing.items() if k != b"geo"}
    md[b"geo"] = json.dumps(geo).encode()
    table = table.replace_schema_metadata(md)

    out = str(tmp_path / "malicious_geom.parquet")
    pq.write_table(table, out)
    return out


def test_admin_divisions_quotes_malicious_geom_column(malicious_geom_column_file):
    """admin-divisions does not crash / inject on a hostile geometry column name."""
    runner = CliRunner()
    result = runner.invoke(
        add,
        [
            "admin-divisions",
            malicious_geom_column_file,
            "output.parquet",
            "--dataset",
            "gaul",
            "--levels",
            "continent",
            "--dry-run",
            "--no-cache",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "DRY RUN MODE" in result.output
    # The embedded double-quote must be doubled inside a quoted identifier; the
    # raw `AS pwn` breakout must never appear as executable SQL.
    assert 'geom"") AS pwn' in result.output


def test_country_codes_join_quotes_a_malicious_geom_column(malicious_geom_column_file, tmp_path):
    """country_codes joins a hostile geometry column name without injecting.

    The country_codes module is exercised by benchmarks/legacy paths rather than
    a CLI command, so it is driven through its core function, against a local
    countries file covering the input. Its ON clause comes from
    ``build_spatial_join_condition``; a breakout would add a ``pwn`` column or
    fail to parse.
    """
    from geoparquet_io.core.add.country_codes import add_country_codes

    countries = tmp_path / "countries.parquet"
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute(
            "COPY (SELECT 'ZZ' AS country, "
            "ST_GeomFromText('POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))') "
            f"AS geometry) TO '{countries}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    out = tmp_path / "out.parquet"

    add_country_codes(malicious_geom_column_file, str(countries), str(out), False, False, False)

    table = pq.read_table(out)
    assert "pwn" not in table.column_names
    assert table.num_rows == pq.read_metadata(malicious_geom_column_file).num_rows
    assert set(table.column("admin:country_code").to_pylist()) == {"ZZ"}


# --------------------------------------------------------------------------- #
# partition admin: the input bbox column (#926)
# --------------------------------------------------------------------------- #

# A bbox column name that breaks out of an unquoted struct-field access. Doubling
# the embedded double-quote (quote_identifier) neutralises it; leaving it bare
# lets the `AS pwn` and the scalar subquery execute.
MALICIOUS_BBOX_NAME = 'bb") AS pwn, (SELECT 1) AS injected, struct_pack(xmin := 0.0'


def _write_bbox_input(con, path, bbox_name):
    """Write a 2-point GeoParquet whose bbox struct column is named ``bbox_name``.

    The covering metadata points at that column so ``check_bbox_structure``
    returns it as ``bbox_column_name`` -- exactly the value that reaches the
    bare interpolation sites.
    """
    con.execute(
        f"""
        COPY (
            SELECT id, geometry,
                   struct_pack(xmin := ST_XMin(geometry), xmax := ST_XMax(geometry),
                               ymin := ST_YMin(geometry), ymax := ST_YMax(geometry)) AS bbox_tmp
            FROM (VALUES
                (1, ST_GeomFromText('POINT(17 46)')),
                (2, ST_GeomFromText('POINT(19 47)'))
            ) AS t(id, geometry)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    table = pq.read_table(path)
    table = table.rename_columns([bbox_name if n == "bbox_tmp" else n for n in table.column_names])
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "covering": {
                    "bbox": {
                        "xmin": [bbox_name, "xmin"],
                        "ymin": [bbox_name, "ymin"],
                        "xmax": [bbox_name, "xmax"],
                        "ymax": [bbox_name, "ymax"],
                    }
                },
            }
        },
    }
    md = dict(table.schema.metadata or {})
    md[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(md), path)


@pytest.fixture
def _covering_admin(tmp_path):
    """A single CRS84 polygon (with bbox covering) over the test points."""
    from geoparquet_io.core.common import add_bbox

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
    add_bbox(admin, "bbox", False)
    return admin


def _patch_dataset(monkeypatch, admin_path):
    from geoparquet_io.core.admin_datasets import CurrentAdminDataset

    def fake_create(dataset_name, source_path=None, verbose=False):
        return CurrentAdminDataset(source_path=admin_path, verbose=verbose)

    monkeypatch.setattr(
        "geoparquet_io.core.partition.admin_hierarchical.AdminDatasetFactory.create",
        staticmethod(fake_create),
    )


# --------------------------------------------------------------------------- #
# Unit: query builder must quote the bbox column segment only
# --------------------------------------------------------------------------- #


def test_enrichment_query_quotes_hostile_bbox_column():
    """`_build_enrichment_query` quotes the input bbox column in the pre-filter."""
    query = _build_enrichment_query(
        "input.parquet",
        "'admin.parquet'",
        "",
        "b.iso as _admin_c",
        "geometry",
        "bbox",
        ["iso"],
        "geometry",
        MALICIOUS_BBOX_NAME,
        "_enriched",
    )
    # Embedded double-quote doubled inside a quoted identifier; the raw breakout
    # never appears as executable SQL.
    assert 'bb"") AS pwn' in query
    assert 'bb") AS pwn' not in query
    # Struct-field subtlety: the alias and accessor stay bare around the quoted
    # column segment.
    assert 'a."' + MALICIOUS_BBOX_NAME.replace('"', '""') + '".xmin' in query


# --------------------------------------------------------------------------- #
# End-to-end: a legitimate name round-trips; a hostile name cannot inject
# --------------------------------------------------------------------------- #


@pytest.mark.integration
def test_partition_admin_bbox_name_with_space_round_trips(tmp_path, monkeypatch, _covering_admin):
    """A bbox column named ``my bbox`` must partition, not raise a ParserException."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    src = str(tmp_path / "in_space.parquet")
    _write_bbox_input(con, src, "my bbox")
    con.close()

    _patch_dataset(monkeypatch, _covering_admin)
    out_dir = str(tmp_path / "out_space")
    count = partition_by_admin_hierarchical(
        src, out_dir, dataset_name="current", levels=["country"], hive=True
    )
    assert count == 1
    from pathlib import Path

    files = list(Path(out_dir).rglob("*.parquet"))
    assert len(files) == 1
    assert sum(pq.ParquetFile(str(f)).metadata.num_rows for f in files) == 2


@pytest.mark.integration
def test_partition_admin_hostile_bbox_name_does_not_inject(tmp_path, monkeypatch, _covering_admin):
    """A hostile bbox column name is treated as a literal identifier, not SQL.

    The attacker names their (real, numeric) bbox struct column with a
    ``struct_pack(...)`` payload whose first field is an ``error()`` over a file
    the user never named. Before the fix the bare interpolation re-parses the
    name as SQL and the payload runs; after the fix the name is a quoted
    identifier referencing the real (benign) struct, so the partition just
    succeeds.
    """
    secret = tmp_path / "victim_secret.txt"
    secret.write_text("aws_secret_access_key=hunter2\n")
    payload = (
        "struct_pack(xmin := (SELECT error('INJECTED926 ' || any_value(l)) "
        f"FROM read_csv('{secret}', header=false, columns={{'l': 'VARCHAR'}})), "
        "xmax := 0.0, ymin := 0.0, ymax := 0.0)"
    )

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    src = str(tmp_path / "in_hostile.parquet")
    _write_bbox_input(con, src, payload)
    con.close()

    _patch_dataset(monkeypatch, _covering_admin)
    out_dir = str(tmp_path / "out_hostile")
    # Must not raise the injected error, and must partition the two real points.
    count = partition_by_admin_hierarchical(
        src, out_dir, dataset_name="current", levels=["country"], hive=True
    )
    assert count == 1
    from pathlib import Path

    files = list(Path(out_dir).rglob("*.parquet"))
    assert sum(pq.ParquetFile(str(f)).metadata.num_rows for f in files) == 2
