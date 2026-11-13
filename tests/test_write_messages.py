"""CLI write completions describe published files, including nested repairs.

Use the byte-identity command matrix so every existing write variant is exercised
through a fresh CLI process. Whole-command journeys belong in the slow lane.
"""

import json
import re
from dataclasses import replace
from pathlib import Path

import pytest

from scripts import byte_identity as matrix

ROOT = Path(__file__).resolve().parents[1]
CASES = [case for case in matrix._matrix() if case.kind == "cli"]
CASES += [
    matrix.Case(
        "check_bbox_missing_column",
        ("check", "bbox", "{in}", "--fix", "--fix-output", "{out}"),
        ("buildings_test.parquet",),
        "out.parquet",
    ),
]
CASES += [
    matrix.Case(
        "sort_str", ("sort", "str", "{in}", "{out}"), ("places_test.parquet",), "out.parquet"
    ),
    matrix.Case(
        "to_shapefile",
        ("convert", "shapefile", "{in}", "{out}"),
        ("buildings_test.parquet",),
        "out.shp",
    ),
    matrix.Case(
        "add_kdtree",
        ("add", "kdtree", "{in}", "{out}", "--partitions", "2"),
        ("places_test.parquet",),
        "out.parquet",
    ),
    matrix.Case(
        "partition_kdtree",
        ("partition", "kdtree", "{in}", "{out}", "--partitions", "2", "--force", "--skip-analysis"),
        ("places_test.parquet",),
        "out_dir",
    ),
    matrix.Case(
        "partition_s2",
        ("partition", "s2", "{in}", "{out}", "--level", "1", "--force", "--skip-analysis"),
        ("places_test.parquet",),
        "out_dir",
    ),
    matrix.Case(
        "partition_a5",
        ("partition", "a5", "{in}", "{out}", "--resolution", "1", "--force", "--skip-analysis"),
        ("places_test.parquet",),
        "out_dir",
    ),
    matrix.Case(
        "partition_string",
        (
            "partition",
            "string",
            "{in}",
            "{out}",
            "--column",
            "category",
            "--force",
            "--skip-analysis",
        ),
        ("messages.parquet",),
        "out_dir",
    ),
    matrix.Case(
        "aggregate_a5",
        ("process", "aggregate", "a5", "{in}", "{out}", "--resolution", "1"),
        ("places_test.parquet",),
        "out.parquet",
    ),
    matrix.Case(
        "reproject_inplace",
        ("convert", "reproject", "{in}", "{out}", "--overwrite", "--dst-crs", "EPSG:3857"),
        ("places_test.parquet",),
        "out.parquet",
        inplace=True,
    ),
    matrix.Case(
        "polygonize", ("process", "polygonize", "{in}", "{out}"), ("raster.tif",), "out.parquet"
    ),
    matrix.Case(
        "contour",
        ("process", "contour", "{in}", "{out}", "--interval", "3"),
        ("raster.tif",),
        "out.parquet",
    ),
]

CASES += [
    matrix.Case(
        "add_admin",
        ("add", "admin-divisions", "{in}", "{out}", "--levels", "country"),
        ("buildings_test.parquet",),
        "out.parquet",
        kind="offline",
    ),
    matrix.Case(
        "partition_admin",
        (
            "partition",
            "admin",
            "{in}",
            "{out}",
            "--levels",
            "country",
            "--force",
            "--skip-analysis",
        ),
        ("buildings_test.parquet",),
        "out_dir",
        kind="offline",
    ),
    matrix.Case(
        "aggregate_admin",
        ("process", "aggregate", "admin", "{in}", "{out}"),
        ("buildings_test.parquet",),
        "out.parquet",
        kind="offline",
    ),
    matrix.Case(
        "extract_arcgis",
        (
            "extract",
            "arcgis",
            "https://services.example.com/arcgis/rest/services/Cities/FeatureServer/0",
            "{out}",
        ),
        ("buildings_test.parquet",),
        "out.parquet",
        kind="offline",
    ),
    matrix.Case(
        "extract_wfs",
        (
            "extract",
            "wfs",
            "https://geo.example.org/geoserver/wfs",
            "test:cities",
            "{out}",
            "--wfs-version",
            "1.1.0",
        ),
        ("buildings_test.parquet",),
        "out.parquet",
        kind="offline",
    ),
    matrix.Case(
        "extract_carto",
        ("extract", "carto", "https://example.carto.com", "cities", "{out}", "--geometry"),
        ("buildings_test.parquet",),
        "out.parquet",
        kind="offline",
    ),
    matrix.Case(
        "extract_bigquery",
        ("extract", "bigquery", "project.dataset.cities", "{out}"),
        ("buildings_test.parquet",),
        "out.parquet",
        kind="offline",
    ),
]
for inplace in (False, True):
    CASES.append(
        matrix.Case(
            "stac_" + str(inplace),
            ("add", "stac-metadata", "{in}")
            + (() if inplace else ("{out}",))
            + ("--collection", "{in2}"),
            ("places_test.parquet", "collection.json"),
            "out.parquet",
            inplace=inplace,
        )
    )

for name in ("bbox", "all", "compression", "row-group", "spatial"):
    for inplace in (False, True):
        CASES.append(
            matrix.Case(
                f"check_{name}_{'inplace' if inplace else 'output'}",
                ("check", name, "{in}", "--fix") + (() if inplace else ("--fix-output", "{out}")),
                ("buildings_test.parquet",),
                "out.parquet",
                inplace=inplace,
            )
        )


@pytest.fixture(scope="module")
def write_message_side(tmp_path_factory):
    root = tmp_path_factory.mktemp("write_messages")
    side = matrix.Side("branch", ROOT, root / "outputs", root / "inputs")
    failures = matrix.prepare_inputs(ROOT / "tests/data", side, side.inputs, 120)
    assert not failures, failures
    import numpy as np

    from tests.test_process_raster_reader import _write_tif

    _write_tif(
        side.inputs / "raster.tif", np.arange(16, dtype="float32").reshape(4, 4), crs="EPSG:32633"
    )
    from tests.test_add_stac_metadata import COLLECTION

    (side.inputs / "collection.json").write_text(json.dumps(COLLECTION), encoding="utf-8")
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(side.inputs / "places_test.parquet")
    pq.write_table(
        table.append_column("category", pa.array(["one"] * table.num_rows)),
        side.inputs / "messages.parquet",
    )
    return side


def _offline_source(case, side, monkeypatch):
    """Replace external reads, keeping command orchestration and writers real."""
    import pyarrow.parquet as pq

    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    if "admin" in case.name:
        from geoparquet_io.core import admin_datasets as admin

        boundary = side.inputs / "boundary.parquet"
        with get_duckdb_connection() as con:
            con.execute(
                f"COPY (SELECT 'fixture' gaul0_name, 'AA' country, ST_MakeEnvelope(-180, -90, 180, 90) geometry) TO {sql_path(str(boundary))} (FORMAT PARQUET)"
            )
        monkeypatch.setattr(
            admin.GAULAdminDataset, "get_default_source", lambda self: str(boundary)
        )
        monkeypatch.setattr(admin.GAULAdminDataset, "get_bbox_column", lambda self: None)
        monkeypatch.setattr(admin.GAULAdminDataset, "get_s3_config", lambda self: {})
        monkeypatch.setattr(admin.GAULAdminDataset, "configure_s3", lambda self, con: None)
        monkeypatch.setattr(
            admin.AdminDatasetFactory,
            "create",
            lambda *a, **k: admin.GAULAdminDataset(source_path=str(boundary)),
        )
        monkeypatch.setattr(admin, "get_or_cache_dataset", lambda *a, **k: str(boundary))
    elif case.name == "extract_arcgis":
        from tests.http_transport import FakeTransport
        from tests.test_arcgis_transport import stub_service

        stub_service(FakeTransport.install(monkeypatch), total=3)
    elif case.name == "extract_wfs":
        from tests.http_transport import FakeTransport, wfs_capabilities
        from tests.test_wfs_transport import CAPABILITIES, SCHEMA, stub_service

        wfs_capabilities(monkeypatch, CAPABILITIES, SCHEMA)
        stub_service(FakeTransport.install(monkeypatch), total=3)
    elif case.name == "extract_carto":
        from geoparquet_io.core import carto
        from geoparquet_io.core.crs_utils import projjson_for_label

        table = pq.read_table(side.inputs / case.inputs[0])
        monkeypatch.setattr(carto, "_get_row_count", lambda *a, **k: table.num_rows)
        monkeypatch.setattr(carto, "_fetch_with_retry", lambda *a, **k: table)
        monkeypatch.setattr(
            carto, "_resolve_table_crs", lambda *a, **k: projjson_for_label("OGC:CRS84")
        )
    elif case.name == "extract_bigquery":
        from geoparquet_io.core import extract_bigquery

        def connect():
            con = get_duckdb_connection(isolated=True)
            con.execute(
                f"CREATE MACRO bigquery_scan(table_id) AS TABLE SELECT * FROM {sql_path(str(side.inputs / case.inputs[0]))}"
            )
            return con

        monkeypatch.setattr(extract_bigquery, "_setup_bigquery_connection", connect)


@pytest.mark.slow
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_write_messages_name_published_destinations(case, write_message_side, monkeypatch):
    run = matrix.subprocess.run
    captured = []

    def capture(*args, **kwargs):
        result = run(*args, **kwargs)
        captured.append((result.stdout or "") + (result.stderr or ""))
        return result

    monkeypatch.setattr(matrix.subprocess, "run", capture)
    if case.kind == "offline":
        _offline_source(case, write_message_side, monkeypatch)
    for verbose in (False, True):
        command = replace(
            case,
            name=case.name + ("_verbose" if verbose else "_normal"),
            argv=case.argv + (("--verbose",) if verbose else ()),
        )
        if case.kind == "offline":
            from click.testing import CliRunner

            from geoparquet_io.cli.main import cli

            directory = write_message_side.out_root / command.name
            directory.mkdir(parents=True, exist_ok=True)
            output = directory / case.output
            values = {"in": str(write_message_side.inputs / case.inputs[0]), "out": str(output)}
            result = CliRunner().invoke(cli, [word.format(**values) for word in command.argv])
            captured.append(result.output)
            assert result.exit_code == 0, result.output
            assert output.exists()
        else:
            result = write_message_side.run(command, 120)
            assert result.status == "ok", result.detail
    output = "\n".join(captured)
    assert not re.search(
        r"[/\\](?:\.gpio-stage|\.gpio_partition_staging_|gpio_|(?:h3|a5|s2|quadkey|kdtree)_enriched_)",
        output,
    ), output
