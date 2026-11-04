"""The profiler observes calls without substituting connection objects."""


def test_spies_count_real_connection(tmp_path):
    import pyarrow as pa
    import pyarrow.ipc as ipc
    import pyarrow.parquet as pq

    from geoparquet_io.core.duckdb_utils import (
        complete_without_fetch,
        get_duckdb_connection,
        sql_path,
    )
    from scripts.profile_command import CallSpies

    with CallSpies() as spies:
        with get_duckdb_connection(load_spatial=False, load_httpfs=False) as con:
            result = con.sql("SELECT 42 AS answer").arrow().read_all()
            complete_without_fetch(
                con.execute(
                    f"COPY (SELECT 42 AS answer) TO {sql_path(tmp_path / 'copy.parquet')} (FORMAT PARQUET)"
                )
            )
            con.sql("SELECT 42 AS answer").write_parquet(str(tmp_path / "relation.parquet"))
        pq.write_table(result, tmp_path / "tiny.parquet")
        pq.read_metadata(tmp_path / "tiny.parquet")
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


def test_operator_costs_do_not_double_count_parent_cpu(tmp_path):
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

    from geoparquet_io.benchmarks import matrix, workloads
    from scripts.profile_command import run_matrix

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
    from geoparquet_io.benchmarks.matrix import COMMANDS
    from geoparquet_io.benchmarks.workloads import generate_workload
    from scripts.profile_command import CallSpies, invoke

    spec = next(row for row in COMMANDS if row["id"] == "aggregate-admin")
    source = generate_workload("W2", tmp_path / "input")
    output = tmp_path / "output"
    output.mkdir()
    prepared = invoke(spec, source, output, None, prepare=True)
    with CallSpies() as spies:
        invoke(spec, source, output, None, prepared=prepared)
    assert spies.counts["parquet_writes"] == 1
