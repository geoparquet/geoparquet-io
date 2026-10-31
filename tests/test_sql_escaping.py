"""SQL escaping has one owner: ``core/duckdb_utils.py`` (#1367 concept 10).

``quote_identifier`` (identifiers), ``_escape_sql_string`` (string-literal
bodies), ``sql_path`` (file paths) and ``sql_literal`` (a Python value as a
DuckDB literal) live there. BigQuery SQL is a different dialect and has its
own two helpers in ``extract_bigquery``'s owner section. This module checks:

* a column named ``it's "odd"`` whose values carry quotes and a NUL byte
  through ``gpio process aggregate h3 --breakdown``;
* a CSV with such a header through ``gpio convert geoparquet``, and that the
  WKT-column probe quotes the name it found;
* a hostile BigQuery table name through the ``__TABLES__`` row-count query;
* a guard: no quote-doubling ``replace`` outside the owner.
"""

from __future__ import annotations

import ast
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import duckdb_utils as du
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path

ODD = 'it\'s "odd"'


def _invoke(args):
    result = CliRunner().invoke(cli, args, catch_exceptions=False)
    assert result.exit_code == 0, result.output
    return result


# --- sql_literal, the one value-literal builder ---------------------------------


def test_sql_literal_round_trips_through_duckdb():
    con = duckdb.connect()
    values = [ODD, "nul\x00byte", "back\\slash", "", 7, 2.5, True, False]
    for value in values:
        (got,) = con.execute(f"SELECT {du.sql_literal(value)}").fetchall()[0]
        assert got == value, value


def test_aggregate_breakdown_on_an_odd_column_and_values(tmp_path):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    src = tmp_path / "in.parquet"
    con.execute(
        f"""COPY (SELECT * FROM (VALUES
                (ST_Point(0.1, 0.1), 'O''Brien'),
                (ST_Point(0.1, 0.1), 'nul' || chr(0) || 'x'),
                (ST_Point(0.1, 0.1), 'O''Brien')) AS t(geometry, {quote_identifier(ODD)}))
            TO {sql_path(str(src))} (FORMAT PARQUET)"""
    )
    con.close()
    out = tmp_path / "out.parquet"
    _invoke(
        [
            "process",
            "aggregate",
            "h3",
            str(src),
            str(out),
            "--resolution",
            "3",
            "--breakdown",
            ODD,
            "--out-geometry",
            "none",
        ]
    )
    (row,) = pq.read_table(out).to_pylist()
    counts = {k: v for k, v in row.items() if k.startswith("count_")}
    assert row["count"] == 3
    assert sorted(counts.values()) == [1, 2], row


# --- convert: the CSV WKT probe ------------------------------------------------


def test_convert_csv_with_an_odd_header(tmp_path):
    csv = tmp_path / "in.csv"
    csv.write_text(f'id,"{ODD.replace(chr(34), chr(34) * 2)}",WKT\n1,a\'b,POINT(1 2)\n')
    out = tmp_path / "out.parquet"
    _invoke(["convert", "geoparquet", str(csv), str(out)])
    table = pq.read_table(out)
    assert table.column(ODD).to_pylist() == ["a'b"]


def test_wkt_probe_quotes_the_column_it_found():
    """The probe interpolates the header it matched; it must be a quoted identifier."""
    from geoparquet_io.core.convert import _try_detect_wkt_column

    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    con.execute("CREATE TABLE t AS SELECT 'POINT(1 2)' AS \"Wkt\"")
    seen = []

    class Recording:
        def execute(self, sql, *args):
            seen.append(sql)
            return con.execute(sql, *args)

    assert _try_detect_wkt_column(Recording(), "t", ["Wkt"]) == "Wkt"
    assert 'SELECT "Wkt" FROM t WHERE "Wkt" IS NOT NULL' in seen[0]


# --- BigQuery: the __TABLES__ row-count query -----------------------------------


class _RecordingBigQuery:
    """Stands in for the network: records the DuckDB SQL instead of calling BigQuery."""

    def __init__(self):
        self.sql = []

    def execute(self, sql, *args):
        self.sql.append(sql)
        return duckdb.connect().execute("SELECT 42")


def _row_count_sql(table_id):
    from geoparquet_io.core.extract_bigquery import _get_table_row_count

    con = _RecordingBigQuery()
    assert _get_table_row_count(con, table_id) == 42
    (sql,) = con.sql
    return sql


def _string_args(sql):
    """The DuckDB string literals in ``sql``, decoded, via DuckDB's own lexer."""
    out = []
    for position, kind in duckdb.tokenize(sql):
        if kind == duckdb.token_type.string_const:
            end = position + 1
            while True:
                end = sql.index("'", end)
                if sql.startswith("''", end):
                    end += 2
                    continue
                break
            out.append(sql[position + 1 : end].replace("''", "'"))
    return out


@pytest.mark.parametrize(
    ("raw", "literal"),
    [
        ("my_table", "'my_table'"),
        ("it's", "'it\\'s'"),
        ("a\\b", "'a\\\\b'"),
        ("\\'", "'\\\\\\''"),
    ],
)
def test_googlesql_string_backslash_escapes(raw, literal):
    """GoogleSQL escapes with a backslash, not DuckDB's doubling: ``it's`` is
    ``'it\\'s'``, and a backslash is escaped first so ``\\'`` cannot end the
    string."""
    from geoparquet_io.core.extract_bigquery import _googlesql_string

    assert _googlesql_string(raw) == literal


def test_googlesql_string_rejects_a_nul_byte():
    from geoparquet_io.core.extract_bigquery import _googlesql_string

    with pytest.raises(ValueError, match="NUL"):
        _googlesql_string("a\x00b")


def test_row_count_query_contains_a_hostile_table_name():
    sql = _row_count_sql("proj.ds.t'), ('x\"`")
    # One DuckDB statement whose bigquery_query call gets exactly two strings.
    (_statement,) = duckdb.extract_statements(sql)
    project, googlesql = _string_args(sql)
    assert project == "proj"
    # In the GoogleSQL, the dataset path is a backtick identifier and the table
    # a single-quoted string literal with its quote backslash-escaped.
    assert googlesql == (
        "SELECT row_count FROM `proj.ds.__TABLES__` WHERE table_id = 't\\'), (\\'x\"`'"
    )


def test_row_count_query_for_a_plain_table():
    sql = _row_count_sql("my-project.my_ds.my_table")
    assert _string_args(sql) == [
        "my-project",
        "SELECT row_count FROM `my-project.my_ds.__TABLES__` WHERE table_id = 'my_table'",
    ]


# --- guard ----------------------------------------------------------------------

PACKAGE = Path(__file__).resolve().parent.parent / "geoparquet_io"
QUOTE_DOUBLINGS = {("'", "''"), ('"', '""')}


def quote_doublings(source: str) -> int:
    """Count ``.replace("'", "''")`` / ``.replace('"', '""')`` calls."""
    return sum(
        1
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "replace"
        and len(node.args) == 2
        and all(isinstance(a, ast.Constant) for a in node.args)
        and (node.args[0].value, node.args[1].value) in QUOTE_DOUBLINGS
    )


def test_quote_doubling_lives_only_in_duckdb_utils():
    found = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        rel = path.relative_to(PACKAGE).as_posix()
        count = quote_doublings(path.read_text(encoding="utf-8"))
        if count:
            found[rel] = count
    assert found == {"core/duckdb_utils.py": 2}


def test_guard_sees_a_hand_rolled_escape():
    assert quote_doublings("x = name.replace('\"', '\"\"') + v.replace(\"'\", \"''\")") == 2
    assert quote_doublings("x = name.replace('a', 'b')") == 0
