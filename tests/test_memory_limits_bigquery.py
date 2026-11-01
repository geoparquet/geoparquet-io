"""``extract bigquery`` applies ``--write-memory`` only through the memory-limits API.

It used to run its own ``SET memory_limit``, which skipped the thread cap the
limit affords: a 1GB limit spread over every core leaves each thread too little
to spill with, and DuckDB raises instead (#1367 concept 9). The BigQuery
service is the only thing replaced here -- by a local DuckDB connection -- and
the extraction stops at its first schema read, after the limit is in place.
"""

from __future__ import annotations

import os

import duckdb
import pytest

from geoparquet_io.core import extract_bigquery
from geoparquet_io.core.duckdb_utils import fetch_one
from geoparquet_io.core.parquet_writer import WriteOptions


class _StopAfterLimits(Exception):
    pass


@pytest.fixture
def local_bigquery(monkeypatch):
    """BigQueryConnection replaced by a plain local connection with every thread."""
    con = duckdb.connect()
    con.execute(f"SET threads = {max(4, os.cpu_count() or 4)}")

    class FakeBigQueryConnection:
        def __init__(self, **_kwargs):
            pass

        def __enter__(self):
            return con

        def __exit__(self, *exc):
            return False

    def stop(*_args, **_kwargs):
        raise _StopAfterLimits

    monkeypatch.setattr(extract_bigquery, "BigQueryConnection", FakeBigQueryConnection)
    monkeypatch.setattr(extract_bigquery, "_schema_rows", stop)
    yield con
    con.close()


def _run(memory_limit):
    with pytest.raises(_StopAfterLimits):
        extract_bigquery._execute_bigquery_extraction(
            validated_table_id="p.d.t",
            project="p",
            credentials_file=None,
            geography_column=None,
            include_list=None,
            exclude_list=None,
            bbox=None,
            bbox_mode="auto",
            bbox_threshold=0,
            where=None,
            limit=None,
            show_sql=False,
            output_parquet=None,
            write=WriteOptions(compression="ZSTD", memory_limit=memory_limit),
            verbose=False,
        )


def _settings(con):
    return fetch_one(
        con.execute("SELECT current_setting('memory_limit'), current_setting('threads')")
    )


def test_write_memory_caps_threads_with_the_limit(local_bigquery):
    _run("1GB")
    # 1GB is 1e9 bytes (953.6 MiB); at 512 MiB a thread that affords one thread.
    assert _settings(local_bigquery) == ("953.6 MiB", 1)


def test_bare_number_is_megabytes(local_bigquery, monkeypatch):
    # Fix the CPU count so 2000 MB affords fewer threads on every host.
    monkeypatch.setattr(os, "cpu_count", lambda: 4)
    _run("2000")
    assert _settings(local_bigquery) == ("1.8 GiB", 3)


def test_no_write_memory_leaves_the_connection_alone(local_bigquery):
    before = _settings(local_bigquery)
    _run(None)
    assert _settings(local_bigquery) == before
