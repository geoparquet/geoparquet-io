"""One byte measurement, independent of the writer and Arrow chunk boundaries."""

import pyarrow as pa
import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection, register_arrow
from geoparquet_io.core.parquet_writer import (
    resolve_row_group_rows_for_query,
    resolve_row_group_rows_for_sample,
    resolve_row_group_rows_for_table,
)


@pytest.mark.parametrize("length", [0, 1000, 20000, 30000])
def test_every_resolver_measures_the_same_leading_rows(length):
    # The tail deliberately weighs more; sampling the whole table is observably wrong.
    table = pa.table({"payload": ["x" * (80 if i < 20000 else 800) for i in range(length)]})
    chunked = pa.Table.from_batches(table.to_batches(max_chunksize=1000), schema=table.schema)
    variants = [table, chunked, table.slice(0, min(length, 20000))]
    answers = [
        resolver(value, None, 1)
        for resolver in (resolve_row_group_rows_for_table, resolve_row_group_rows_for_sample)
        for value in variants
    ]
    con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
    try:
        register_arrow(con, "measurement", table)
        answers.append(resolve_row_group_rows_for_query(con, "SELECT * FROM measurement", None, 1))
    finally:
        con.close()
    assert len(set(answers)) == 1, answers


def test_query_measurement_uses_the_output_order():
    table = pa.table({"payload": ["x" * 80] * 20000 + ["x" * 800] * 10000})
    con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
    try:
        register_arrow(con, "measurement", table)
        query = "SELECT * FROM measurement ORDER BY length(payload) DESC"
        ordered = con.sql(query).arrow().read_all()
        assert resolve_row_group_rows_for_query(con, query, None, 1) == (
            resolve_row_group_rows_for_table(ordered, None, 1)
        )
    finally:
        con.close()
