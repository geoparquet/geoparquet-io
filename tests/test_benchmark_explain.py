"""
Tests for benchmark explain (EXPLAIN ANALYZE) functionality.
"""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.benchmark import (
    explain_analyze,
    format_explain_output,
    parse_query_plan,
)

# Test data
TEST_DATA_DIR = Path(__file__).parent / "data"
PARQUET_FILE = TEST_DATA_DIR / "buildings_test.parquet"


# Sample DuckDB EXPLAIN ANALYZE JSON output for mocking
SAMPLE_PLAN_JSON = {
    "children": [
        {
            "name": "PARQUET_SCAN",
            "timing": 0.001,
            "cardinality": 100,
            "extra_info": "File: test.parquet\nFilters: id>10\nRow Groups: 1/3",
            "children": [],
        }
    ],
    "name": "PROJECTION",
    "timing": 0.0005,
    "cardinality": 100,
    "extra_info": "",
}


# DuckDB 2.0 nests the operator tree under "operator" (a list of roots), names
# an operator by "type" and its output rows by "intermediate_rows", keeps
# extra_info a dict, and reports row groups as fields of the scan itself.
SAMPLE_PLAN_JSON_V2 = {
    "query": {"sql": "SELECT ...", "total_time": 0.0012},
    "operator": [
        {
            "type": "PROJECTION",
            "timing": 0.0005,
            "intermediate_rows": 100,
            "extra_info": {},
            "children": [
                {
                    "type": "TABLE_SCAN",
                    "timing": 0.001,
                    "intermediate_rows": 100,
                    "extra_info": {"Function": "READ_PARQUET", "Filters": "id>10"},
                    "row_groups_scanned": 1,
                    "total_row_groups_to_scan": 3,
                    "children": [],
                }
            ],
        }
    ],
}


class TestParseQueryPlan:
    """Tests for parsing DuckDB EXPLAIN ANALYZE output."""

    def test_parse_plan_extracts_operators(self):
        """Test that parse_query_plan extracts operator names."""
        result = parse_query_plan(SAMPLE_PLAN_JSON)

        operator_names = [op["name"] for op in result["operators"]]
        assert "PROJECTION" in operator_names
        assert "PARQUET_SCAN" in operator_names

    def test_parse_plan_extracts_timing(self):
        """Test that parse_query_plan extracts timing info."""
        result = parse_query_plan(SAMPLE_PLAN_JSON)

        scan_op = next(op for op in result["operators"] if op["name"] == "PARQUET_SCAN")
        assert scan_op["timing"] == 0.001
        assert scan_op["cardinality"] == 100

    def test_parse_plan_detects_filters(self):
        """Test that parse_query_plan detects filter pushdown."""
        result = parse_query_plan(SAMPLE_PLAN_JSON)

        assert result["has_filter_pushdown"] is True

    def test_parse_plan_detects_row_group_skip(self):
        """Test that parse_query_plan detects row group skipping."""
        result = parse_query_plan(SAMPLE_PLAN_JSON)

        assert result["row_groups_skipped"] is True

    def test_parse_plan_no_filters(self):
        """Test parse with no filter pushdown."""
        plan = {
            "name": "PROJECTION",
            "timing": 0.001,
            "cardinality": 100,
            "extra_info": "",
            "children": [
                {
                    "name": "PARQUET_SCAN",
                    "timing": 0.002,
                    "cardinality": 100,
                    "extra_info": "File: test.parquet",
                    "children": [],
                }
            ],
        }

        result = parse_query_plan(plan)

        assert result["has_filter_pushdown"] is False
        assert result["row_groups_skipped"] is False

    def test_parse_plan_empty(self):
        """Test parse with minimal plan."""
        plan = {
            "name": "RESULT",
            "timing": 0.0,
            "cardinality": 0,
            "extra_info": "",
            "children": [],
        }

        result = parse_query_plan(plan)
        assert len(result["operators"]) == 1
        assert result["operators"][0]["name"] == "RESULT"

    def test_parse_plan_nested_children(self):
        """Test parsing deeply nested operator tree."""
        plan = {
            "name": "TOP_N",
            "timing": 0.001,
            "cardinality": 10,
            "extra_info": "",
            "children": [
                {
                    "name": "FILTER",
                    "timing": 0.002,
                    "cardinality": 50,
                    "extra_info": "(id > 5)",
                    "children": [
                        {
                            "name": "PARQUET_SCAN",
                            "timing": 0.003,
                            "cardinality": 100,
                            "extra_info": "File: data.parquet",
                            "children": [],
                        }
                    ],
                }
            ],
        }

        result = parse_query_plan(plan)

        assert len(result["operators"]) == 3
        # Operators should be in tree order (root first)
        assert result["operators"][0]["name"] == "TOP_N"
        assert result["operators"][0]["depth"] == 0
        assert result["operators"][1]["name"] == "FILTER"
        assert result["operators"][1]["depth"] == 1
        assert result["operators"][2]["name"] == "PARQUET_SCAN"
        assert result["operators"][2]["depth"] == 2


class TestParseQueryPlanDuckDB2:
    """The DuckDB 2.0 profile layout parses to the same result shape."""

    def test_operators_come_from_the_operator_tree(self):
        result = parse_query_plan(SAMPLE_PLAN_JSON_V2)
        assert [op["name"] for op in result["operators"]] == ["PROJECTION", "TABLE_SCAN"]
        assert [op["depth"] for op in result["operators"]] == [0, 1]

    def test_timing_and_rows_are_read(self):
        scan = parse_query_plan(SAMPLE_PLAN_JSON_V2)["operators"][1]
        assert scan["timing"] == 0.001
        assert scan["cardinality"] == 100
        assert parse_query_plan(SAMPLE_PLAN_JSON_V2)["total_time"] == pytest.approx(0.0015)

    def test_filter_pushdown_is_detected(self):
        assert parse_query_plan(SAMPLE_PLAN_JSON_V2)["has_filter_pushdown"] is True

    def test_row_group_skip_is_read_from_the_scan_fields(self):
        assert parse_query_plan(SAMPLE_PLAN_JSON_V2)["row_groups_skipped"] is True

    def test_scanning_every_row_group_is_no_skip(self):
        plan = json.loads(json.dumps(SAMPLE_PLAN_JSON_V2))
        plan["operator"][0]["children"][0]["row_groups_scanned"] = 3
        assert parse_query_plan(plan)["row_groups_skipped"] is False


class TestExplainAnalyzeReadsTheRealPlan:
    """Against the installed DuckDB: the scan is found with its rows and time.

    A layout change in DuckDB's profile (2.0 moved the tree under "operator")
    once parsed to a single UNKNOWN node with no timing, which the shape-only
    tests above still accepted.
    """

    def test_the_scan_reports_rows_and_time(self):
        result = explain_analyze(file_path=str(PARQUET_FILE))
        rows = pq_rows(PARQUET_FILE)
        assert any(op["cardinality"] == rows for op in result["operators"]), result["operators"]
        assert result["total_time"] > 0

    def test_a_pushed_down_filter_is_detected(self):
        result = explain_analyze(
            file_path=str(PARQUET_FILE),
            # A predicate neither optimizer can drop: 2.0 elides id > '0'.
            query="SELECT * FROM read_parquet('{file}') WHERE id LIKE '%a%'",
        )
        assert result["has_filter_pushdown"] is True


def pq_rows(path) -> int:
    import pyarrow.parquet as pq

    return pq.ParquetFile(path).metadata.num_rows


class TestFormatExplainOutput:
    """Tests for formatting explain output."""

    def test_format_table_output(self):
        """Test table formatting of explain results."""
        parsed = {
            "operators": [
                {
                    "name": "PROJECTION",
                    "timing": 0.0005,
                    "cardinality": 100,
                    "extra_info": "",
                    "depth": 0,
                },
                {
                    "name": "PARQUET_SCAN",
                    "timing": 0.001,
                    "cardinality": 100,
                    "extra_info": "File: test.parquet\nFilters: id>10",
                    "depth": 1,
                },
            ],
            "has_filter_pushdown": True,
            "row_groups_skipped": True,
            "total_time": 0.0015,
        }

        output = format_explain_output(parsed, output_format="table")

        assert "QUERY PLAN ANALYSIS" in output
        assert "PROJECTION" in output
        assert "PARQUET_SCAN" in output
        assert "Filter pushdown" in output

    def test_format_json_output(self):
        """Test JSON formatting of explain results."""
        parsed = {
            "operators": [
                {
                    "name": "PARQUET_SCAN",
                    "timing": 0.001,
                    "cardinality": 100,
                    "extra_info": "",
                    "depth": 0,
                },
            ],
            "has_filter_pushdown": False,
            "row_groups_skipped": False,
            "total_time": 0.001,
        }

        output = format_explain_output(parsed, output_format="json")

        data = json.loads(output)
        assert "operators" in data
        assert "has_filter_pushdown" in data
        assert data["has_filter_pushdown"] is False


@pytest.mark.integration
class TestExplainAnalyze:
    """Tests for the explain_analyze function."""

    def test_explain_analyze_with_default_query(self):
        """Test explain_analyze runs with default SELECT * query."""
        result = explain_analyze(
            file_path=str(PARQUET_FILE),
        )

        assert "operators" in result
        assert "total_time" in result
        assert len(result["operators"]) > 0

    def test_explain_analyze_with_custom_query(self):
        """Test explain_analyze with a custom SQL query."""
        result = explain_analyze(
            file_path=str(PARQUET_FILE),
            query="SELECT * FROM read_parquet('{file}') LIMIT 10",
        )

        assert "operators" in result
        assert len(result["operators"]) > 0

    def test_explain_analyze_with_filter_query(self):
        """Test explain_analyze with a WHERE clause to trigger filter pushdown."""
        result = explain_analyze(
            file_path=str(PARQUET_FILE),
            query="SELECT * FROM read_parquet('{file}') WHERE id IS NOT NULL",
        )

        assert "operators" in result

    def test_explain_analyze_nonexistent_file(self):
        """Test explain_analyze with nonexistent file raises error."""
        with pytest.raises(ValueError, match="File not found"):
            explain_analyze(
                file_path="/nonexistent/file.parquet",
            )

    def test_explain_analyze_returns_raw_plan(self):
        """Test explain_analyze includes raw plan text."""
        result = explain_analyze(
            file_path=str(PARQUET_FILE),
        )

        assert "raw_plan" in result


@pytest.mark.integration
class TestExplainCLI:
    """Tests for benchmark explain CLI command."""

    def test_explain_basic(self):
        """Test basic explain invocation."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "benchmark",
                "explain",
                str(PARQUET_FILE),
            ],
        )

        assert result.exit_code == 0
        assert "QUERY PLAN ANALYSIS" in result.output

    def test_explain_json_format(self):
        """Test explain with JSON output."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "benchmark",
                "explain",
                str(PARQUET_FILE),
                "--format",
                "json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "operators" in data

    def test_explain_custom_query(self):
        """Test explain with custom query."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "benchmark",
                "explain",
                str(PARQUET_FILE),
                "--query",
                "SELECT * FROM read_parquet('{file}') LIMIT 5",
            ],
        )

        assert result.exit_code == 0

    def test_explain_save_output(self, temp_output_dir):
        """Test explain saving output to file."""
        output_path = str(Path(temp_output_dir) / "explain.json")
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "benchmark",
                "explain",
                str(PARQUET_FILE),
                "--output",
                output_path,
            ],
        )

        assert result.exit_code == 0
        assert Path(output_path).exists()

        with open(output_path) as f:
            data = json.load(f)
        assert "operators" in data

    def test_explain_nonexistent_file(self):
        """Test explain with missing file."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "benchmark",
                "explain",
                "/nonexistent/file.parquet",
            ],
        )

        assert result.exit_code != 0


class TestExplainFilePlaceholder:
    """``{file}`` is replaced by the whole ``sql_path`` literal (#1367 T1e audit 2, finding 4).

    It used to take ``sql_path(...)[1:-1]`` -- the escape with its quotes cut
    off -- and rely on the user's template to put them back. The quoted
    spelling ``'{file}'`` that the docs used keeps working: it is the same
    literal.
    """

    def test_bare_placeholder_takes_the_quoted_literal(self):
        from geoparquet_io.core.benchmark import _build_explain_query

        sql = _build_explain_query("o'brien.parquet", "SELECT * FROM read_parquet({file})")
        assert sql == "EXPLAIN ANALYZE SELECT * FROM read_parquet('o''brien.parquet')"

    def test_quoted_placeholder_is_the_same_literal(self):
        from geoparquet_io.core.benchmark import _build_explain_query

        quoted = _build_explain_query("o'brien.parquet", "SELECT * FROM read_parquet('{file}')")
        bare = _build_explain_query("o'brien.parquet", "SELECT * FROM read_parquet({file})")
        assert quoted == bare

    def test_a_nul_byte_stays_one_expression(self):
        from geoparquet_io.core.benchmark import _build_explain_query
        from geoparquet_io.core.duckdb_utils import sql_path

        sql = _build_explain_query("a\x00b.parquet", "SELECT * FROM read_parquet('{file}')")
        assert (
            sql
            == f"EXPLAIN ANALYZE SELECT * FROM read_parquet({sql_path('a' + chr(0) + 'b.parquet')})"
        )
