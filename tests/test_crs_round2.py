"""Audit regressions through the public table and CLI interfaces."""

import json
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from geoparquet_io.api import Table
from geoparquet_io.cli.main import cli
from tests.test_crs_owner import _file_with_block_and_logical_type, _table


def test_null_block_wins_over_native_type_in_extract(tmp_path):
    path = _file_with_block_and_logical_type(tmp_path, None)
    result = CliRunner().invoke(
        cli,
        [
            "extract",
            "geoparquet",
            str(path),
            str(tmp_path / "out.parquet"),
            "--bbox",
            "-123,37,-121,38",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "uses a projected CRS" not in result.output


def test_null_table_block_with_text_metadata_key():
    # The carried metadata protocol accepts text keys as well as bytes keys.
    source = SimpleNamespace(
        schema=SimpleNamespace(
            metadata={
                "geo": json.dumps(
                    {
                        "version": "1.1.0",
                        "primary_column": "geometry",
                        "columns": {"geometry": {"encoding": "WKB", "crs": None}},
                    }
                )
            }
        ),
        column_names=["geometry"],
    )
    assert Table(source, geometry_column="geometry").crs is None


@pytest.mark.parametrize(
    ("crs", "message"),
    [
        ("EPSG:3857", "datum ensemble"),
        ("EPSG:7789", "has valid epoch: 2020.0"),
        ("EPSG:4283", "on a static crs"),
    ],
)
def test_validate_epoch_reads_string_crs(tmp_path, crs, message):
    import pyarrow.parquet as pq

    table = _table("geometry", crs)
    block = json.loads(table.schema.metadata[b"geo"])
    block["columns"]["geometry"]["epoch"] = 2020.0
    path = tmp_path / "epoch.parquet"
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(block).encode()}), path)
    result = CliRunner().invoke(cli, ["check", "spec", str(path)])
    assert message in result.output.lower(), result.output


@pytest.mark.parametrize("funnel", ["table", "overview"])
def test_field_only_crs_reaches_metadata_funnel(tmp_path, funnel):
    import pyarrow.parquet as pq

    from geoparquet_io.core.crs_utils import CRS_ABSENT, crs_of
    from geoparquet_io.core.process.overview.overview_file import LevelInput, write_overview_file
    from geoparquet_io.core.write_funnels import write_geoparquet_table

    path = _file_with_block_and_logical_type(tmp_path, CRS_ABSENT)
    table = pq.read_table(path)
    expected = crs_of(table)
    assert expected is not None
    output = tmp_path / "out.parquet"

    if funnel == "table":
        write_geoparquet_table(table, str(output), geoparquet_version="1.1")
    else:
        write_overview_file(
            [LevelInput(str(path), 1000), LevelInput(str(path), 100)],
            str(output),
            geoparquet_version="1.1",
        )
    block = json.loads(pq.read_schema(output).metadata[b"geo"])
    assert block["columns"]["geometry"]["crs"] == expected


def test_schema_crs_uses_same_carriers(tmp_path):
    import pyarrow.parquet as pq

    from geoparquet_io.core.crs_utils import CRS_ABSENT, crs_of

    for block in (CRS_ABSENT, None, "EPSG:3857"):
        table = pq.read_table(_file_with_block_and_logical_type(tmp_path, block))
        assert crs_of(table.schema, quiet=True) == crs_of(table, quiet=True)
