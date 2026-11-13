"""``scripts/byte_identity.py``: the per-track byte-identity harness (#1367).

Two properties make the harness evidence: a tree compared with itself is
all-identical (so a difference it reports is not noise), and a real change on
one side is reported as differing, with a footer diff that names it. The second
is shown with the script's own test hook, ``GPIO_BYTE_IDENTITY_FAULT``, which
adds ``--compression-level 9`` on the branch side and is honoured only under
pytest.

Both run this checkout against itself, through its own ``.venv``, so they need
no second worktree. Slow: every case is two ``uv run`` subprocesses.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from scripts import byte_identity as matrix

pytestmark = pytest.mark.slow

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "byte_identity.py"
SELF_CASES = "convert_parquet_default,sort_hilbert,table_write_in-memory_places"


def _run(tmp_path: Path, only: str, **env) -> tuple[subprocess.CompletedProcess, str]:
    if not (ROOT / ".venv").is_dir():
        pytest.skip(f"{ROOT} has no .venv for the harness to run gpio from")
    report = tmp_path / "report.md"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--base",
            str(ROOT),
            "--branch",
            str(ROOT),
            "--out",
            str(report),
            "--only",
            only,
            "--jobs",
            "3",
        ],
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    assert report.exists(), proc.stdout + proc.stderr
    return proc, report.read_text(encoding="utf-8")


def _verdicts(report: str) -> dict[str, str]:
    rows = [line.split("|") for line in report.splitlines() if line.startswith("| `")]
    return {cells[1].strip().strip("`"): cells[2].strip() for cells in rows}


def test_a_tree_against_itself_is_identical(tmp_path, monkeypatch):
    monkeypatch.delenv("GPIO_BYTE_IDENTITY_FAULT", raising=False)
    proc, report = _run(tmp_path, SELF_CASES)
    assert _verdicts(report) == dict.fromkeys(SELF_CASES.split(","), "identical"), report
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_an_injected_compression_level_is_reported_with_its_footer_diff(tmp_path):
    proc, report = _run(
        tmp_path,
        "convert_parquet_default,table_write_in-memory_places,to_geojson_places",
        # 9, not 3: an unset level on the DuckDB COPY paths already *is* 3
        # (#1335), so a level-3 fault leaves `convert` byte-identical.
        GPIO_BYTE_IDENTITY_FAULT="compression_level:9",
    )
    verdicts = _verdicts(report)
    assert verdicts["convert_parquet_default"] == "differs", report
    assert verdicts["table_write_in-memory_places"] == "differs", report
    # A case that takes no compression level is untouched by the fault.
    assert verdicts["to_geojson_places"] == "identical", report
    assert proc.returncode == 1

    detail = report.split("## `table_write_in-memory_places`", 1)[1].split("\n## ", 1)[0]
    assert "compressed size `geometry`" in detail, detail
    assert "data tables equal: yes" in detail, detail
    assert "codec" not in detail, "a level change must not read as a codec change"
    assert "injected fault (branch side)" in report


def test_the_fault_hook_is_ignored_outside_pytest(tmp_path):
    env = {k: v for k, v in os.environ.items() if k != "PYTEST_CURRENT_TEST"}
    env["GPIO_BYTE_IDENTITY_FAULT"] = "compression_level:9"
    report = tmp_path / "report.md"
    if not (ROOT / ".venv").is_dir():
        pytest.skip(f"{ROOT} has no .venv")
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--base",
            str(ROOT),
            "--branch",
            str(ROOT),
            "--out",
            str(report),
            "--only",
            "convert_parquet_default",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _verdicts(report.read_text()) == {"convert_parquet_default": "identical"}


# Full CLI matrix: nested writers report only published destinations.
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
