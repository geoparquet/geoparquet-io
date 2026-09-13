"""The post-conversion success message must not claim validation that never ran (todo 044).

`_report_conversion_results` now runs a metadata-level validation of the final
output and only claims a pass when it actually passed.
"""

import json
import time

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core import convert as convert_mod
from geoparquet_io.core.convert import _report_conversion_results, convert_to_geoparquet
from geoparquet_io.core.duckdb_utils import get_duckdb_connection


@pytest.fixture
def valid_output(tmp_path):
    src = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('POINT (1 2)'))
          ) t(id, geometry)
        ) TO '{src.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V1')
    """)
    con.close()
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(src), str(out), skip_hilbert=True, geoparquet_version="1.1")
    return out


@pytest.fixture
def captured_messages(monkeypatch):
    calls = {"success": [], "warn": [], "progress": []}
    monkeypatch.setattr(convert_mod, "success", lambda m: calls["success"].append(m))
    monkeypatch.setattr(convert_mod, "warn", lambda m: calls["warn"].append(m))
    monkeypatch.setattr(convert_mod, "progress", lambda m: calls["progress"].append(m))
    return calls


def test_valid_output_gets_validation_success(valid_output, captured_messages):
    _report_conversion_results(str(valid_output), time.time(), is_geo=True)
    assert any("passes GeoParquet validation" in m for m in captured_messages["success"]), (
        captured_messages
    )
    assert not any("check spec" in m for m in captured_messages["warn"])


def test_invalid_output_gets_warning_not_success(valid_output, captured_messages):
    # Corrupt the geo metadata (invalid encoding) so metadata validation fails.
    import geoarrow.pyarrow  # noqa: F401

    table = pq.read_table(str(valid_output))
    meta = dict(table.schema.metadata)
    geo = json.loads(meta[b"geo"])
    for col in geo["columns"].values():
        col["encoding"] = "bogus"
    meta[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(meta), str(valid_output))

    _report_conversion_results(str(valid_output), time.time(), is_geo=True)
    assert not any("passes GeoParquet validation" in m for m in captured_messages["success"]), (
        captured_messages
    )
    assert any("check spec" in m for m in captured_messages["warn"]), captured_messages
