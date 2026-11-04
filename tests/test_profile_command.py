"""The profiler observes calls without substituting connection objects."""


def test_spies_count_real_connection(tmp_path):
    import pyarrow.parquet as pq

    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from scripts.profile_command import CallSpies

    with CallSpies() as spies:
        with get_duckdb_connection(load_spatial=False, load_httpfs=False) as con:
            result = con.sql("SELECT 42 AS answer").arrow().read_all()
        pq.write_table(result, tmp_path / "tiny.parquet")
        pq.read_metadata(tmp_path / "tiny.parquet")
    assert spies.counts["footer_reads"] == 1
    assert spies.counts["parquet_writes"] == 1
    assert result.column("answer").to_pylist() == [42]
    assert spies.counts["connections"] == 1
    assert spies.counts["arrow_read_all"] == 1
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
