"""Every command group's failure reaches the user as one sanitized line (#1034, #1367).

Each case makes the command's core call raise the exception type the command
catches, with a message that carries a presigned-URL signature and an
``api_key=``. The CLI's whole output must be a single ``Error:`` line with
neither secret, and no traceback. Reverting any one file's ``click_error_for``
routing back to ``click.ClickException(str(e))`` fails its case.
"""

from __future__ import annotations

import importlib
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.carto import CartoError
from geoparquet_io.core.streaming import StreamingError
from geoparquet_io.core.wfs import WFSError

SECRET = "SUPERSECRETKEY123"
LEAK = f"boom reading https://h.example/x.parquet?X-Amz-Signature={SECRET} api_key={SECRET}"

CMD = "geoparquet_io.cli.commands"

# (id, argv with {src}/{out}/{dir}, patch target, exception raised)
CASES = [
    (
        "add-bbox",
        ["add", "bbox", "{src}", "{out}"],
        f"{CMD}.add.add_bbox_column_impl",
        StreamingError(LEAK),
    ),
    (
        "add-h3",
        ["add", "h3", "{src}", "{out}"],
        f"{CMD}.add.add_h3_column_impl",
        StreamingError(LEAK),
    ),
    (
        "add-a5",
        ["add", "a5", "{src}", "{out}"],
        f"{CMD}.add.add_a5_column_impl",
        StreamingError(LEAK),
    ),
    (
        "add-s2",
        ["add", "s2", "{src}", "{out}"],
        f"{CMD}.add.add_s2_column_impl",
        StreamingError(LEAK),
    ),
    (
        "add-quadkey",
        ["add", "quadkey", "{src}", "{out}"],
        f"{CMD}.add.add_quadkey_column_impl",
        StreamingError(LEAK),
    ),
    (
        "convert-reproject",
        ["convert", "reproject", "{src}", "{out}", "--dst-crs", "EPSG:3857"],
        f"{CMD}.convert.reproject_core",
        ValueError(LEAK),
    ),
    (
        "partition-string",
        ["partition", "string", "{src}", "{dir}", "--column", "name"],
        f"{CMD}.partition.partition_by_string_impl",
        StreamingError(LEAK),
    ),
    (
        "pmtiles-create",
        ["pmtiles", "create", "{src}", "{out}"],
        "geoparquet_io.core.pmtiles.create_pmtiles_from_geoparquet",
        RuntimeError(LEAK),
    ),
    (
        "pmtiles-pyramid",
        ["pmtiles", "pyramid", "{src}", "{dir}"],
        "geoparquet_io.core.pmtiles_pyramid.create_pmtiles_pyramid",
        RuntimeError(LEAK),
    ),
    (
        "process-simplify",
        ["process", "simplify", "{src}", "{out}", "--tolerance", "0.1"],
        f"{CMD}.process.simplify_file_impl",
        ValueError(LEAK),
    ),
    (
        "process-aggregate-h3",
        ["process", "aggregate", "h3", "{src}", "{out}", "--resolution", "5"],
        f"{CMD}.process.aggregate_by_h3_impl",
        duckdb.IOException(LEAK),
    ),
    (
        "inspect-summary",
        ["inspect", "summary", "{src}"],
        f"{CMD}.inspect._inspect_summary_core",
        RuntimeError(LEAK),
    ),
    (
        "inspect-head",
        ["inspect", "head", "{src}"],
        f"{CMD}.inspect._inspect_preview_core",
        RuntimeError(LEAK),
    ),
    (
        "inspect-stats",
        ["inspect", "stats", "{src}"],
        f"{CMD}.inspect._inspect_stats_core",
        RuntimeError(LEAK),
    ),
    (
        "inspect-layers",
        ["inspect", "layers", "{src}"],
        "geoparquet_io.core.layers.list_layers",
        RuntimeError(LEAK),
    ),
    (
        "extract-wfs",
        ["extract", "wfs", "https://h.example/wfs", "ns:layer", "{out}"],
        "geoparquet_io.core.wfs.convert_wfs_to_geoparquet",
        WFSError(LEAK),
    ),
    (
        "extract-carto",
        ["extract", "carto", "https://h.example/api/v2/sql", "cities", "{out}"],
        "geoparquet_io.core.carto.convert_carto_to_geoparquet",
        CartoError(LEAK),
    ),
]


@pytest.fixture
def paths(tmp_path):
    src = tmp_path / "in.parquet"
    pq.write_table(pa.table({"id": [1], "name": ["a"]}), src)
    return {"src": str(src), "out": str(tmp_path / "out.parquet"), "dir": str(tmp_path / "parts")}


@pytest.mark.parametrize(
    ("argv", "target", "exc"), [c[1:] for c in CASES], ids=[c[0] for c in CASES]
)
def test_a_command_failure_is_one_sanitized_error_line(monkeypatch, paths, argv, target, exc):
    def boom(*_args, **_kwargs):
        raise exc

    module_name, _, attr = target.rpartition(".")
    # sys.modules, not getattr: `cli.commands.add` the attribute is the click group.
    importlib.import_module(module_name)
    monkeypatch.setattr(sys.modules[module_name], attr, boom)

    result = CliRunner().invoke(cli, [a.format(**paths) for a in argv])

    assert result.exit_code != 0, result.output
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert SECRET not in result.output, result.output
    assert "Traceback" not in result.output
    assert "boom reading" in result.output, result.output
    assert result.output.count("Error:") == 1, result.output


def test_the_skills_command_failure_is_an_error_line(monkeypatch):
    def boom(*_args, **_kwargs):
        raise FileNotFoundError(LEAK)

    monkeypatch.setattr("geoparquet_io.skills.get_skill_content", boom)

    result = CliRunner().invoke(cli, ["skills", "--show"])

    assert result.exit_code != 0
    assert SECRET not in result.output
    assert "Traceback" not in result.output
    assert result.output.count("Error:") == 1, result.output


def test_a_rejected_output_destination_is_an_error_line(monkeypatch, paths):
    """``prepare_output`` in cli/_shared.py."""

    def boom(*_args, **_kwargs):
        raise StreamingError(LEAK)

    monkeypatch.setattr("geoparquet_io.core.streaming.validate_output", boom)

    result = CliRunner().invoke(cli, ["sort", "hilbert", paths["src"], paths["out"]])

    assert result.exit_code != 0
    assert SECRET not in result.output
    assert "Traceback" not in result.output
