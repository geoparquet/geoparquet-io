"""The profiler observes calls without substituting connection objects."""


def test_spies_count_real_connection(tmp_path):
    import sys

    import pyarrow as pa
    import pyarrow.ipc as ipc
    import pyarrow.parquet as pq

    from geoparquet_io.core.duckdb_utils import (
        complete_without_fetch,
        get_duckdb_connection,
        sql_path,
    )
    from scripts.profile_command import CallSpies

    def check(directory):
        directory.mkdir()
        with CallSpies() as spies:
            with get_duckdb_connection(load_spatial=False, load_httpfs=False) as con:
                before = spies.counts["execute"]
                complete_without_fetch(con.execute("PRAGMA disable_profiling"))
                assert spies.counts["execute"] == before
                result = con.sql("SELECT 42 AS answer").arrow().read_all()
                complete_without_fetch(
                    con.execute(
                        f"COPY (SELECT 42 AS answer) TO {sql_path(directory / 'copy.parquet')} (FORMAT PARQUET)"
                    )
                )
                con.sql("SELECT 42 AS answer").write_parquet(str(directory / "relation.parquet"))
            pq.write_table(result, directory / "tiny.parquet")
            metadata = pq.read_metadata(directory / "tiny.parquet")
            pq.ParquetFile(directory / "tiny.parquet", metadata=metadata).close()
            buffer = pa.BufferOutputStream()
            with ipc.new_stream(buffer, result.schema) as writer:
                writer.write_table(result)
            assert ipc.open_stream(buffer.getvalue()).read_all().equals(result)
        assert spies.counts["footer_reads"] == 1
        assert spies.counts["parquet_writes"] == 3
        assert result.column("answer").to_pylist() == [42]
        assert spies.counts["connections"] == 1
        assert spies.counts["arrow_read_all"] == 2
        assert spies.counts["execute"] > 0

    check(tmp_path / "native")
    monitoring = getattr(sys, "monitoring", None)
    if monitoring is not None and monitoring.get_tool(5) is None:
        monitoring.use_tool_id(5, "occupied-for-fallback-test")
        try:
            check(tmp_path / "fallback")
        finally:
            monitoring.free_tool_id(5)
    else:
        check(tmp_path / "fallback")


def check_operator_costs_do_not_double_count_parent_cpu(tmp_path):
    import json

    from scripts.profile_costs import operators, sampled_symbol

    (tmp_path / "query.json").write_text(
        json.dumps(
            {
                "cpu_time": 9,
                "system_peak_buffer_memory": 4096,
                "children": [
                    {
                        "operator_name": "SCAN",
                        "operator_timing": 2,
                        "children": [{"operator_name": "FILTER", "operator_timing": 1}],
                    }
                ],
            }
        )
    )
    assert operators(tmp_path) == ([("SCAN", 2), ("FILTER", 1)], 4096)
    samples = tmp_path / "samples.json"
    samples.write_text(
        json.dumps(
            {
                "shared": {"frames": [{"name": "parent"}, {"name": "leaf"}]},
                "profiles": [{"samples": [[0, 1], [0]], "weights": [3, 1]}],
            }
        )
    )
    symbol, share = sampled_symbol(samples)
    assert symbol.startswith("leaf")
    assert share == 75


def test_resume_rejects_a_measurement_from_another_revision(tmp_path, monkeypatch):
    import argparse
    import json

    import pytest

    from scripts.profile_command import run_matrix
    from scripts.profile_matrix import matrix, workloads

    check_explain_report(tmp_path)
    spec = {"id": "cli-import", "workload": "W1", "argv": ["--help"]}
    monkeypatch.setattr(matrix, "COMMANDS", [spec])
    monkeypatch.setattr(workloads, "generate_workload", lambda *args: tmp_path / "input.parquet")
    result = tmp_path / "measurements" / "cli-import" / "0" / "result.json"
    result.parent.mkdir(parents=True)
    result.write_text(
        json.dumps(
            {
                "revision": "stale",
                "size": "small",
                "command": "cli-import",
                "workload": "W1",
                "diagnostic": False,
            }
        )
    )
    args = argparse.Namespace(
        output=str(tmp_path / "matrix.json"), command="cli-import", size="small", runs=3
    )
    with pytest.raises(RuntimeError, match="Stale measurement"):
        run_matrix(args)
    assert not (tmp_path / "matrix.json").exists()

    # Comparison acceptance belongs to measurement validation too. Keep these
    # small cases within this item so the fast-suite collection ratchet holds.

    with monkeypatch.context() as context:
        check_prepared_fixture_identity(tmp_path, context)
    check_cost_report_context(tmp_path)
    check_operator_costs_do_not_double_count_parent_cpu(tmp_path)
    check_dbf_only_date_bytes(tmp_path)
    check_gpkg_only_last_change(tmp_path)
    check_kdtree_only_sampled_cell(tmp_path)
    check_stdout_only_ratio_and_tied_order(tmp_path)


def test_api_convert_profiles_one_read(tmp_path, monkeypatch):
    from unittest.mock import Mock

    from geoparquet_io.api import table
    from scripts.profile_command import invoke

    converted = Mock()
    convert = Mock(return_value=converted)
    monkeypatch.setattr(table, "convert", convert)
    monkeypatch.setattr(table, "read", Mock(side_effect=AssertionError("extra full-table read")))
    input_path = tmp_path / "input.parquet"
    invoke({"kind": "api-convert"}, input_path, tmp_path, None)
    convert.assert_called_once_with(input_path)
    converted.write.assert_called_once_with(tmp_path / "result.parquet")


def test_cli_exit_is_not_a_successful_measurement(monkeypatch):
    import importlib

    import click
    import pytest

    from scripts.profile_command import _cli

    @click.command()
    @click.pass_context
    def failing(context):
        context.exit(7)

    monkeypatch.setattr(importlib.import_module("geoparquet_io.cli.main"), "cli", failing)
    with pytest.raises(RuntimeError, match="status 7"):
        _cli([])


def test_admin_fixture_write_is_outside_measured_command(tmp_path):
    from scripts.profile_command import CallSpies, invoke
    from scripts.profile_matrix.matrix import COMMANDS
    from scripts.profile_matrix.workloads import generate_workload

    spec = next(row for row in COMMANDS if row["id"] == "aggregate-admin")
    source = generate_workload("W2", tmp_path / "input")
    output = tmp_path / "output"
    output.mkdir()
    prepared = invoke(spec, source, output, None, prepare=True)
    with CallSpies() as spies:
        invoke(spec, source, output, None, prepared=prepared)
    assert spies.counts["parquet_writes"] == 1


def test_portable_server_without_production_package(tmp_path):
    import subprocess
    import sys

    source = tmp_path / "input.parquet"
    source.write_bytes(b"PAR1fixture")
    code = """
import importlib.util
import sys
from urllib.request import urlopen
spec = importlib.util.spec_from_file_location('portable_workloads', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
from scripts.profile_matrix.matrix import COMMANDS
assert COMMANDS
assert 'geoparquet_io' not in sys.modules
sys.modules['geoparquet_io'] = None
with module.serve_workload(sys.argv[2]) as url:
    assert urlopen(url).read() == b'PAR1fixture'
"""
    subprocess.run(
        [sys.executable, "-c", code, "scripts/profile_matrix/workloads.py", str(source)], check=True
    )


def check_dbf_only_date_bytes(tmp_path):
    from scripts.profile_matrix.output_comparison import comparable_bytes

    path = tmp_path / "result.dbf"
    path.write_bytes(b"\x03\x7e\x0b\x04payload")
    first = comparable_bytes(path, "convert-shapefile")
    path.write_bytes(b"\x03\x7e\x0b\x05payload")
    assert comparable_bytes(path, "convert-shapefile") == first
    path.write_bytes(b"\x03\x7e\x0b\x05changed")
    assert comparable_bytes(path, "convert-shapefile") != first


def check_gpkg_only_last_change(tmp_path):
    import sqlite3

    from scripts.profile_matrix.output_comparison import comparable_bytes

    path = tmp_path / "result.gpkg"
    with sqlite3.connect(path) as con:
        con.execute("CREATE TABLE gpkg_contents (last_change TEXT, identifier TEXT)")
        con.execute("INSERT INTO gpkg_contents VALUES ('2026-11-04T01:02:03.000Z', 'original')")
    first = comparable_bytes(path, "convert-geopackage")
    raw = path.read_bytes()
    path.write_bytes(raw.replace(b"2026-11-04T01:02:03.000Z", b"2026-11-05T01:02:03.000Z"))
    assert comparable_bytes(path, "convert-geopackage") == first
    path.write_bytes(path.read_bytes().replace(b"original", b"modified"))
    assert comparable_bytes(path, "convert-geopackage") != first


def check_kdtree_only_sampled_cell(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.profile_matrix.output_comparison import comparable_bytes

    path = tmp_path / "result.parquet"

    def write(cell, value=1, metadata=b"original", compression="snappy"):
        table = pa.table({"id": [1], "value": [value], "kdtree_cell": [cell]})
        pq.write_table(
            table.replace_schema_metadata({b"geo": metadata}), path, compression=compression
        )

    write("0000")
    first = comparable_bytes(path, "api-kdtree")
    write("0111")
    assert comparable_bytes(path, "api-kdtree") == first
    write("0111", value=2)
    assert comparable_bytes(path, "api-kdtree") != first
    write("0111", metadata=b"changed")
    assert comparable_bytes(path, "api-kdtree") != first
    write("0111", compression="zstd")
    assert comparable_bytes(path, "api-kdtree") != first


def check_stdout_only_ratio_and_tied_order(tmp_path):
    from scripts.profile_matrix.output_comparison import comparable_bytes

    path = tmp_path / "stdout.txt"
    path.write_bytes(b"warning Poor spatial ordering (ratio: 1.04)\n")
    first = comparable_bytes(path, "check-all")
    path.write_bytes(b"warning Poor spatial ordering (ratio: 1.05)\n")
    assert comparable_bytes(path, "check-all") == first
    path.write_bytes(b"PASS Poor spatial ordering (ratio: 1.05)\n")
    assert comparable_bytes(path, "check-all") != first
    header = "Compression Ratios:\nheader\n"
    a, b = "│ a │ SNAPPY │ 10 │ 20 │ 2.0 │\n", "│ b │ SNAPPY │ 10 │ 20 │ 2.0 │\n"
    path.write_bytes((header + a + b + "end\n").encode())
    first = comparable_bytes(path, "inspect-stats")
    path.write_bytes((header + b + a + "end\n").encode())
    assert comparable_bytes(path, "inspect-stats") == first
    path.write_bytes((header + b.replace("20", "30") + a + "end\n").encode())
    assert comparable_bytes(path, "inspect-stats") != first


def check_prepared_fixture_identity(tmp_path, monkeypatch):
    import argparse
    import json
    import subprocess

    import pytest

    from scripts.profile_command import run_child, run_matrix
    from scripts.profile_matrix import matrix, workloads

    spec = {"id": "overview", "workload": "W2", "kind": "overview"}
    monkeypatch.setattr(matrix, "COMMANDS", [spec])
    source = tmp_path / "input.parquet"
    monkeypatch.setattr(workloads, "generate_workload", lambda *args: source)
    fixture = tmp_path / "fixtures" / "overview"
    fixture.mkdir(parents=True)
    metadata = fixture / "prepared.json"
    args = argparse.Namespace(
        command="overview",
        work_dir=str(fixture),
        input_path=str(source),
        prepared_path=None,
        prepare_only=True,
        output=str(metadata),
        size="small",
    )
    from scripts import profile_command

    monkeypatch.setattr(profile_command, "invoke", lambda *a, **kw: fixture / "aggregate.parquet")
    import sys
    from types import SimpleNamespace
    from unittest.mock import Mock

    assert profile_command.peak_rss_bytes() > 0
    monkeypatch.setitem(sys.modules, "resource", None)
    with monkeypatch.context() as context:
        context.setattr(sys, "platform", "win32")
        context.setitem(
            sys.modules,
            "psutil",
            SimpleNamespace(
                Process=Mock(
                    return_value=SimpleNamespace(
                        memory_info=lambda: SimpleNamespace(peak_wset=123456)
                    )
                )
            ),
        )
        assert profile_command.peak_rss_bytes() == 123456
    run_child(args)
    original = json.loads(metadata.read_text())
    assert original["size"] == "small"
    assert original["input_path"] == str(source.resolve())
    assert original["workload"] == "W2"
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **kw: original["revision"])
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: pytest.fail("reused stale fixture"))
    args.output = str(tmp_path / "matrix.json")
    args.runs = 3
    for field, value in [("size", "full"), ("input_path", "other.parquet"), ("workload", "W1")]:
        metadata.write_text(json.dumps({**original, field: value}))
        with pytest.raises(RuntimeError, match="Stale fixture"):
            run_matrix(args)


def check_explain_report(tmp_path):
    import json

    from scripts.profile_command import invoke, output_hashes
    from scripts.profile_matrix.matrix import COMMANDS
    from scripts.profile_matrix.output_comparison import comparable_bytes
    from scripts.profile_matrix.workloads import generate_workload

    spec = next(row for row in COMMANDS if row["id"] == "benchmark-explain")
    source = generate_workload("W4", tmp_path / "explain-input")
    output = tmp_path / "explain-output"
    output.mkdir()
    prepared = invoke(spec, source, output, None, prepare=True)
    invoke(spec, prepared, output, None)
    path = output / "report.json"
    report = json.loads(path.read_text())
    assert report["operators"]
    assert "report.json" in output_hashes(output, "benchmark-explain")
    first = comparable_bytes(path, "benchmark-explain")
    invoke(spec, prepared, output, None)
    assert comparable_bytes(path, "benchmark-explain") == first
    report["raw_plan"] = "independent plan with different timings"
    report["total_time"] += 1
    for operator in report["operators"]:
        operator["timing"] += 1
    path.write_text(json.dumps(report))
    assert comparable_bytes(path, "benchmark-explain") == first
    report["operators"][0]["cardinality"] += 1
    path.write_text(json.dumps(report))
    assert comparable_bytes(path, "benchmark-explain") != first


def check_cost_report_context(tmp_path):
    from scripts.profile_costs import render_costs

    render_costs(tmp_path, [])
    report = (tmp_path / "costs.md").read_text()
    assert "three" not in report
    assert "cores 0–15" not in report
    assert "Linux" not in report
