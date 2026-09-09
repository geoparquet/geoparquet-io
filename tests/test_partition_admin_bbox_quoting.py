"""Regression guard for #926: bbox column identifiers from the input file's own
schema are interpolated bare into the ``partition admin`` enrichment join.

``input_bbox_col`` is read from the input file's GeoParquet schema
(``check_bbox_structure`` -> ``bbox_column_name``), so the attacker is whoever
wrote the file (the #918 / #923 threat model), not the person running the
command. A hostile bbox column name breaks out of the unquoted
``MIN({col}.xmin)`` / ``a.{col}.xmin`` struct-field accesses and runs
attacker-chosen SQL in the user's DuckDB session; a legitimate name that merely
needs quoting (a space) raises a ParserException instead of partitioning.

The struct-field subtlety: the fix quotes only the column segment
(``a."col".xmin``); the ``a.`` alias and the ``.xmin`` accessor stay bare.
"""

import json

import duckdb
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.partition.admin_hierarchical import (
    _build_enrichment_query,
    partition_by_admin_hierarchical,
)

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
