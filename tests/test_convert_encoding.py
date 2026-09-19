"""Source text encoding on ``gpio convert geoparquet`` (``--encoding``).

The Estonian Land Board's EHAK shapefiles (2026-09-19) are Windows-ANSI DBFs
without a ``.cpg``: GDAL hands the bytes through, DuckDB rejects "Lääne maakond"
as invalid UTF-8, and the conversion failed with no knob to name the encoding.
"""

from __future__ import annotations

import json
import shutil
import struct
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.convert import (
    _build_st_read_expr,
    convert_to_geoparquet,
    read_spatial_to_arrow,
    source_open_options,
)
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError

DATA = Path(__file__).parent / "data"
LATIN1_NAME = "Lääne maakond"


def _write_dbf(path: Path, values: list[str], encoding: str) -> None:
    """A minimal dBASE III table with one character field and no .cpg.

    The language driver byte is 0x03 (Windows ANSI), exactly what the Estonian
    EHAK files carry: GDAL then tries to recode from CP1252, which DuckDB's GDAL
    build cannot, and the bytes reach DuckDB unchanged and invalid.
    """
    field_len = 40
    header_len = 32 + 32 + 1
    record_len = 1 + field_len
    header = bytearray(32)
    header[0] = 0x03
    header[1:4] = bytes([126, 9, 19])
    header[4:8] = struct.pack("<I", len(values))
    header[8:10] = struct.pack("<H", header_len)
    header[10:12] = struct.pack("<H", record_len)
    header[29] = 0x03
    descriptor = bytearray(32)
    descriptor[0:4] = b"NAME"
    descriptor[11] = ord("C")
    descriptor[16] = field_len
    body = bytearray()
    for value in values:
        body += b" " + value.encode(encoding).ljust(field_len, b" ")
    path.write_bytes(bytes(header) + bytes(descriptor) + b"\r" + bytes(body) + b"\x1a")


@pytest.fixture
def latin1_shapefile(tmp_path: Path) -> Path:
    """buildings_test geometry with a Windows-ANSI attribute table and no .cpg."""
    for suffix in (".shp", ".shx", ".prj"):
        shutil.copy(DATA / f"buildings_test{suffix}", tmp_path / f"buildings{suffix}")
    count = struct.unpack("<I", (DATA / "buildings_test.dbf").read_bytes()[4:8])[0]
    _write_dbf(tmp_path / "buildings.dbf", [LATIN1_NAME] * count, "latin-1")
    return tmp_path / "buildings.shp"


def _geom_col(parquet: Path) -> str:
    geo = json.loads(pq.read_metadata(parquet).metadata[b"geo"])
    return geo["primary_column"]


class TestSourceEncoding:
    def test_open_options_reach_st_read(self):
        expr = _build_st_read_expr("a.shp", open_options=["ENCODING=ISO-8859-1"])
        assert expr.endswith("open_options := ['ENCODING=ISO-8859-1'])")

    def test_open_option_must_be_key_value(self):
        with pytest.raises(InvalidParameterError):
            _build_st_read_expr("a.shp", open_options=["ENCODING='x'; DROP TABLE t"])

    def test_source_open_options_validates_encoding_name(self):
        assert source_open_options(None) is None
        assert source_open_options("ISO-8859-1") == ["ENCODING=ISO-8859-1"]
        with pytest.raises(InvalidParameterError):
            source_open_options("latin 1; drop")

    def test_latin1_dbf_without_encoding_fails_or_is_recoded(self, latin1_shapefile, tmp_path):
        """Whether the bare read fails depends on the GDAL build behind DuckDB.

        The pipeline's DuckDB 1.5.5 build raised "Invalid unicode" on the real
        EHAK files (2026-09-19); a build that recodes LDID 0x03 itself must at
        least not corrupt the value. Either outcome leaves --encoding as the
        explicit, build-independent knob.
        """
        out = tmp_path / "out.parquet"
        try:
            convert_to_geoparquet(str(latin1_shapefile), str(out))
            names = set(pq.read_table(out, columns=["NAME"]).column("NAME").to_pylist())
        except Exception as exc:  # noqa: BLE001 - the failure mode is the point
            assert "unicode" in str(exc).lower() or "utf" in str(exc).lower()
            return
        assert names == {LATIN1_NAME}

    def test_encoding_recodes_latin1_dbf(self, latin1_shapefile, tmp_path):
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(latin1_shapefile), str(out), encoding="ISO-8859-1")
        names = pq.read_table(out, columns=["NAME"]).column("NAME").to_pylist()
        assert names and set(names) == {LATIN1_NAME}

    def test_encoding_on_read_api(self, latin1_shapefile):
        table, _crs, geom = read_spatial_to_arrow(str(latin1_shapefile), encoding="ISO-8859-1")
        assert geom == "geometry"
        assert table.column("NAME")[0].as_py() == LATIN1_NAME

    def test_encoding_is_refused_for_parquet(self, tmp_path):
        with pytest.raises(GeoParquetError, match="Parquet"):
            convert_to_geoparquet(
                str(DATA / "buildings_test.parquet"), str(tmp_path / "o.parquet"), encoding="UTF-8"
            )


class TestCli:
    def test_cli_encoding_option(self, latin1_shapefile, tmp_path):
        out = tmp_path / "cli.parquet"
        result = CliRunner().invoke(
            cli,
            ["convert", "geoparquet", str(latin1_shapefile), str(out), "--encoding", "ISO-8859-1"],
        )
        assert result.exit_code == 0, result.output
        assert set(pq.read_table(out, columns=["NAME"]).column("NAME").to_pylist()) == {LATIN1_NAME}
