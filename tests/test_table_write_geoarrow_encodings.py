"""``Table.write`` on a GeoArrow-native table agrees with ``gpio convert geoparquet``.

The duckdb-kv table path wrapped the primary column in ``ST_GeomFromWKB``
whatever it held, so every native encoding (a coordinate struct, or a list of
them) died with a DuckDB binder error, while the CLI wrote the same file.
"""

from pathlib import Path

import geoarrow.pyarrow as ga
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.geo_metadata import sanitized_carried_geo

DATA = Path(__file__).parent / "data"
ENCODINGS = ["point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon"]


def _fixture(encoding: str) -> Path:
    return DATA / f"data-{encoding}-encoding_native.parquet"


def _wkb(path: Path) -> list:
    """The primary geometry as WKB, whatever encoding the file stores it in.

    Sorted: ``convert`` Hilbert-orders its output and ``Table.write`` keeps the
    table's order, so the two agree on the rows, not on their order.
    """
    table = pq.read_table(path)
    column = table.column("geometry").combine_chunks()
    encoding = sanitized_carried_geo(table.schema.metadata)["columns"]["geometry"]["encoding"]
    wkb = ga.as_wkb(getattr(ga, encoding.lower())().wrap_array(column))
    return sorted(wkb.storage.to_pylist(), key=lambda value: value or b"")


def _encoding_semantics(path: Path) -> tuple:
    column = sanitized_carried_geo(pq.read_schema(path).metadata)["columns"]["geometry"]
    return column["encoding"].lower(), column.get("crs")


@pytest.mark.parametrize("encoding", ENCODINGS)
def test_table_write_matches_convert(encoding, tmp_path):
    source = _fixture(encoding)
    api_out = tmp_path / "api.parquet"
    cli_out = tmp_path / "cli.parquet"

    gpio.read(source).write(api_out)
    result = CliRunner().invoke(cli, ["convert", "geoparquet", str(source), str(cli_out)])
    assert result.exit_code == 0, result.output

    rows = pq.read_metadata(source).num_rows
    assert pq.read_metadata(api_out).num_rows == rows == pq.read_metadata(cli_out).num_rows
    assert gpio.validate(str(api_out)).verdict() != "failure"
    assert _wkb(api_out) == _wkb(cli_out) == _wkb(source)
    assert _encoding_semantics(api_out) == _encoding_semantics(cli_out) == (encoding, None)


def test_a_head_of_a_native_table_writes(tmp_path):
    out = tmp_path / "head.parquet"
    gpio.read(_fixture("point")).head(3).write(out)
    assert pq.read_metadata(out).num_rows == 3
    assert isinstance(pq.read_schema(out).field("geometry").type, pa.StructType)
