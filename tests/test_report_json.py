"""Every benchmark/inspect --json report goes through json_output.dumps_json (#1367, concept 19).

The reports built their JSON with bare ``json.dumps``: a NaN statistic came out
as the bare token ``NaN`` (not JSON), a date or Decimal in a result's details
raised, and ``gpio benchmark report --format json`` read ``__dict__`` off a
slots dataclass and crashed on every input.
"""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal

from click.testing import CliRunner

from geoparquet_io.cli.main import cli


def _strict(text: str):
    def refuse(token):
        raise AssertionError(f"non-JSON token {token} in the report")

    return json.loads(text, parse_constant=refuse)


def test_benchmark_json_output_is_strict_json():
    from geoparquet_io.core.benchmark import format_json_output

    text = format_json_output(
        {"duckdb": {"mean_time": float("nan"), "std_time": float("inf")}},
        {"name": "f.parquet", "size_mb": Decimal("1.5")},
        {"date": dt.date(2026, 1, 2)},
        [],
        {"iterations": 1},
    )
    report = _strict(text)
    assert report["statistics"]["duckdb"] == {"mean_time": None, "std_time": None}
    assert report["file_info"]["size_mb"] == "1.5"
    assert report["environment"]["date"] == "2026-01-02"
    assert text.startswith('{\n  "environment"')  # still indent=2


def test_benchmark_explain_json_is_strict_json():
    from geoparquet_io.core.benchmark import format_explain_output

    report = _strict(format_explain_output({"cost": float("nan"), "ops": ["SCAN"]}, "json"))
    assert report == {"cost": None, "ops": ["SCAN"]}


def test_suite_result_json_survives_non_json_details():
    from geoparquet_io.core.benchmark_suite import BenchmarkResult, SuiteResult

    result = BenchmarkResult(
        operation="read",
        file="f.parquet",
        time_seconds=float("nan"),
        peak_rss_memory_mb=1.0,
        success=True,
        details={"when": dt.datetime(2026, 1, 2, 3, 4, 5), "bytes": b"\x01\x02"},
    )
    suite = SuiteResult(version="1", timestamp="t", environment={}, results=[result])
    report = _strict(suite.to_json())
    assert report["results"][0]["time_seconds"] is None
    assert report["results"][0]["details"] == {"when": "2026-01-02T03:04:05", "bytes": "0102"}


def test_benchmark_report_json_prints_the_results(tmp_path):
    results = {
        "results": [
            {
                "operation": "read",
                "file": "a.parquet",
                "time_seconds": 1.25,
                "peak_rss_memory_mb": 2.0,
                "success": True,
            }
        ]
    }
    path = tmp_path / "r.json"
    path.write_text(json.dumps(results))
    out = CliRunner().invoke(cli, ["benchmark", "report", str(path), "--format", "json"])
    assert out.exit_code == 0, out.output
    rows = _strict(out.output)
    assert rows == [
        {
            "operation": "read",
            "file": "a.parquet",
            "time_seconds": 1.25,
            "peak_rss_memory_mb": 2.0,
            "success": True,
            "error": None,
            "details": {},
            "memory_limit_mb": None,
            "iteration": 1,
        }
    ]
