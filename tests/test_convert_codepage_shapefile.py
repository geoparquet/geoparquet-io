"""Shapefiles whose DBF is in a code page DuckDB's GDAL cannot recode (#1242).

DuckDB spatial's bundled GDAL has no iconv. Its ``ENCODING`` open option, and a
shapefile's own ``.cpg`` or DBF language driver id, recode ISO-8859-1 and
nothing else: a Shift_JIS (CP932), GBK, CP1251 or even CP1252 attribute table
reaches DuckDB byte for byte. ``COPY ... TO parquet`` then wrote those bytes
into STRING columns, and ``gpio convert`` reported a valid file that no reader
could open.
"""

from __future__ import annotations

import shutil
import struct
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core.convert import (
    _build_plain_select_query,
    convert_to_geoparquet,
    read_spatial_to_arrow,
)
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.source_text import (
    SourceText,
    check_arrow_text,
    dbf_encoding,
    plan_source_text,
)

DATA = Path(__file__).parent / "data"
ROWS = struct.unpack("<I", (DATA / "buildings_test.dbf").read_bytes()[4:8])[0]

TOKYO = ["東京都庁", "新宿区西新宿二丁目", "①丸の内"]  # ① is a CP932 extension
BEIJING = ["北京市", "海淀区中关村大街"]
MOSCOW = ["Москва", "Тверская улица"]
EURO = ["Lääne maakond", "5 € café"]


def _write_dbf(
    path: Path, field: str, values: list[str], encoding: str, ldid: int = 0, width: int = 40
) -> None:
    """A dBASE III table with one character field, the values cycled over every row."""
    rows = [values[i % len(values)] for i in range(ROWS)]
    header = bytearray(32)
    header[0] = 0x03
    header[1:4] = bytes([126, 9, 19])
    header[4:8] = struct.pack("<I", len(rows))
    header[8:10] = struct.pack("<H", 32 + 32 + 1)
    header[10:12] = struct.pack("<H", 1 + width)
    header[29] = ldid
    descriptor = bytearray(32)
    descriptor[0:11] = field.encode(encoding).ljust(11, b"\x00")[:11]
    descriptor[11] = ord("C")
    descriptor[16] = width
    body = b"".join(b" " + value.encode(encoding)[:width].ljust(width, b" ") for value in rows)
    path.write_bytes(bytes(header) + bytes(descriptor) + b"\r" + body + b"\x1a")


def _shapefile(
    directory: Path,
    values: list[str],
    encoding: str,
    *,
    field: str = "NAME",
    cpg: str | None = None,
    ldid: int = 0,
    width: int = 40,
) -> Path:
    for suffix in (".shp", ".shx", ".prj"):
        shutil.copy(DATA / f"buildings_test{suffix}", directory / f"src{suffix}")
    _write_dbf(directory / "src.dbf", field, values, encoding, ldid=ldid, width=width)
    if cpg is not None:
        (directory / "src.cpg").write_text(cpg)
    return directory / "src.shp"


def _expected(values: list[str]) -> list[str]:
    return sorted(values[i % len(values)] for i in range(ROWS))


def _read_back(parquet: Path, column: str) -> list[str]:
    """DuckDB is the reader the issue failed on: it refuses invalid UTF-8 outright.

    Sorted, since Hilbert ordering moves the rows.
    """
    rows = duckdb.sql(f"SELECT \"{column}\" FROM read_parquet('{parquet}')").fetchall()
    return sorted(row[0] for row in rows)


def _convert(source: Path, out: Path, *args: str):
    return CliRunner().invoke(cli, ["convert", "geoparquet", str(source), str(out), *args])


class TestCodePageShapefilesDecode:
    @pytest.mark.parametrize(
        ("cpg", "ldid", "args"),
        [
            pytest.param("CP932", 0, [], id="cpg-cp932"),
            pytest.param("SHIFT_JIS", 0, [], id="cpg-shift-jis-name"),
            pytest.param(None, 0x13, [], id="ldid-0x13"),
            pytest.param(None, 0x7B, [], id="ldid-0x7b"),
            pytest.param(None, 0, ["--encoding", "CP932"], id="encoding-option"),
            pytest.param("CP932", 0, ["--encoding", "CP932"], id="cpg-and-option"),
            pytest.param("GBK", 0, ["--encoding", "CP932"], id="option-beats-cpg"),
            pytest.param("CP932", 0x4D, [], id="cpg-beats-ldid"),
        ],
    )
    def test_cp932(self, tmp_path, cpg, ldid, args):
        values = [v for v in TOKYO if cpg != "SHIFT_JIS" or v.isascii() or "①" not in v]
        source = _shapefile(tmp_path, values, "cp932", field="名称", cpg=cpg, ldid=ldid)
        out = tmp_path / "out.parquet"
        result = _convert(source, out, *args)
        assert result.exit_code == 0, result.output
        assert _read_back(out, "名称") == _expected(values)

    @pytest.mark.parametrize(
        ("cpg", "ldid", "args"),
        [
            pytest.param("GBK", 0, [], id="cpg-gbk"),
            pytest.param("936", 0, [], id="cpg-numeric-936"),
            pytest.param(None, 0x7A, [], id="ldid-0x7a"),
            pytest.param(None, 0, ["--encoding", "CP936"], id="encoding-option"),
        ],
    )
    def test_gbk(self, tmp_path, cpg, ldid, args):
        source = _shapefile(tmp_path, BEIJING, "gbk", cpg=cpg, ldid=ldid)
        out = tmp_path / "out.parquet"
        result = _convert(source, out, *args)
        assert result.exit_code == 0, result.output
        assert _read_back(out, "NAME") == _expected(BEIJING)

    def test_cp1251_from_ldid(self, tmp_path):
        source = _shapefile(tmp_path, MOSCOW, "cp1251", ldid=0xC9)
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(source), str(out))
        assert _read_back(out, "NAME") == _expected(MOSCOW)

    def test_cp1252_from_numeric_cpg(self, tmp_path):
        """ArcGIS writes ``1252`` into the .cpg; GDAL maps it to CP1252, which it cannot recode."""
        source = _shapefile(tmp_path, EURO, "cp1252", cpg="1252")
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(source), str(out))
        assert _read_back(out, "NAME") == _expected(EURO)

    @pytest.mark.parametrize("version", ["1.0", "2.0", "parquet-geo-only"])
    def test_every_output_version(self, tmp_path, version):
        source = _shapefile(tmp_path, TOKYO, "cp932", cpg="CP932")
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(source), str(out), geoparquet_version=version)
        assert _read_back(out, "NAME") == _expected(TOKYO)

    def test_skip_hilbert(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932", cpg="CP932")
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(source), str(out), skip_hilbert=True)
        assert _read_back(out, "NAME") == _expected(TOKYO)

    def test_force_2d(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932", cpg="CP932")
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(source), str(out), force_2d=True)
        assert _read_back(out, "NAME") == _expected(TOKYO)

    def test_truncated_multibyte_character_is_replaced_and_warned(self, tmp_path, caplog):
        """ArcGIS cuts a field at its byte width, sometimes through a double-byte character."""
        source = _shapefile(tmp_path, ["東京都庁"], "cp932", cpg="CP932", width=7)
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(source), str(out))
        assert set(_read_back(out, "NAME")) == {"東京都�"}
        assert "U+FFFD" in caplog.text


class TestLatin1StillDecodes:
    """GDAL recodes ISO-8859-1 natively; that path has to keep working."""

    def test_ldid_0x57_is_iso_8859_1(self, tmp_path):
        source = _shapefile(tmp_path, ["Lääne maakond"], "latin-1", ldid=0x57)
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(source), str(out))
        assert set(_read_back(out, "NAME")) == {"Lääne maakond"}

    @pytest.mark.parametrize("name", ["ISO-8859-1", "latin1", "LATIN-1", "iso8859_1"])
    def test_any_spelling_of_latin1(self, tmp_path, name):
        source = _shapefile(tmp_path, ["Lääne maakond"], "latin-1")
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(source), str(out), encoding=name)
        assert set(_read_back(out, "NAME")) == {"Lääne maakond"}

    def test_utf8_cpg_is_left_to_gdal(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "utf-8", cpg="UTF-8", width=60)
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(source), str(out))
        assert _read_back(out, "NAME") == _expected(TOKYO)


class TestFailsClosed:
    """Bytes nothing names an encoding for must never reach the output as STRING."""

    def test_cli_refuses_undeclared_cp932(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932")
        out = tmp_path / "out.parquet"
        result = _convert(source, out)
        assert result.exit_code != 0
        assert "passes GeoParquet validation" not in result.output
        assert "NAME" in result.output
        assert "--encoding" in result.output

    def test_core_refuses_undeclared_cp932(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932")
        with pytest.raises(GeoParquetError, match=r"(?s)NAME.*not valid UTF-8.*--encoding"):
            convert_to_geoparquet(str(source), str(tmp_path / "out.parquet"))

    def test_skip_hilbert_refuses_too(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932")
        with pytest.raises(GeoParquetError, match="not valid UTF-8"):
            convert_to_geoparquet(str(source), str(tmp_path / "out.parquet"), skip_hilbert=True)

    def test_refusal_leaves_no_output_behind(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932")
        out = tmp_path / "out.parquet"
        with pytest.raises(GeoParquetError):
            convert_to_geoparquet(str(source), str(out))
        assert not out.exists()

    def test_undeclared_field_name_is_refused_by_name(self, tmp_path):
        """A Japanese field name with no hint breaks the bind itself, before any value."""
        source = _shapefile(tmp_path, ["abc"], "cp932", field="名称")
        with pytest.raises(GeoParquetError, match=r"(?s)column name.*not valid UTF-8.*--encoding"):
            convert_to_geoparquet(str(source), str(tmp_path / "out.parquet"))
        with pytest.raises(GeoParquetError, match="not valid UTF-8"):
            read_spatial_to_arrow(str(source))

    def test_api_refuses_undeclared_cp932(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932")
        with pytest.raises(GeoParquetError, match="not valid UTF-8"):
            gpio.convert(str(source))

    def test_arrow_text_check_names_the_column(self):
        bad = pa.Array.from_buffers(
            pa.string(), 1, [None, pa.py_buffer(struct.pack("<2i", 0, 2)), pa.py_buffer(b"\x93s")]
        )
        batch = pa.RecordBatch.from_arrays([pa.array([1]), bad], names=["id", "label"])
        with pytest.raises(GeoParquetError, match='"label".*not valid UTF-8'):
            check_arrow_text(batch)
        check_arrow_text(pa.RecordBatch.from_arrays([pa.array(["ok"])], names=["label"]))


class TestPythonApi:
    def test_convert_decodes_from_cpg(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932", field="名称", cpg="CP932")
        table = gpio.convert(str(source))
        assert sorted(table.to_arrow().column("名称").to_pylist()) == _expected(TOKYO)
        out = tmp_path / "api.parquet"
        table.write(str(out))
        assert _read_back(out, "名称") == _expected(TOKYO)

    def test_convert_encoding_argument(self, tmp_path):
        source = _shapefile(tmp_path, BEIJING, "gbk")
        table = gpio.convert(str(source), encoding="GBK")
        assert sorted(table.to_arrow().column("NAME").to_pylist()) == _expected(BEIJING)

    def test_read_spatial_to_arrow(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932", ldid=0x13)
        table, _crs, geom = read_spatial_to_arrow(str(source))
        assert geom == "geometry"
        assert sorted(table.column("NAME").to_pylist()) == _expected(TOKYO)


def _dbf_only(source: Path) -> Path:
    for suffix in (".shp", ".shx", ".prj"):
        source.with_suffix(suffix).unlink()
    return source.with_suffix(".dbf")


class TestAttributeTableOnly:
    """A source with no geometry reads as a plain table; its text still has to decode."""

    def _rows(self, dbf: Path, encoding: str | None, column: str) -> list[str]:
        con = duckdb.connect()
        con.execute("LOAD spatial")
        try:
            query = _build_plain_select_query(str(dbf), encoding=encoding, con=con)
            return sorted(
                row[0] for row in con.execute(f"SELECT {column} FROM ({query})").fetchall()
            )
        finally:
            con.close()

    def test_declared_code_page(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932", field="名称", cpg="CP932")
        assert self._rows(_dbf_only(source), None, '"名称"') == _expected(TOKYO)

    def test_encoding_option(self, tmp_path):
        source = _shapefile(tmp_path, BEIJING, "gbk")
        assert self._rows(_dbf_only(source), "GBK", "NAME") == _expected(BEIJING)

    def test_undeclared_code_page_is_refused(self, tmp_path):
        source = _shapefile(tmp_path, TOKYO, "cp932")
        with pytest.raises(duckdb.InvalidInputException, match="NAME.*not valid UTF-8"):
            self._rows(_dbf_only(source), None, "NAME")


class TestEncodingResolution:
    """--encoding, then the .cpg, then the DBF language driver id: GDAL's own order."""

    def test_cpg_then_ldid(self, tmp_path):
        source = _shapefile(tmp_path, ["x"], "ascii", cpg="CP932", ldid=0x4D)
        assert dbf_encoding(str(source)) == "CP932"
        (tmp_path / "src.cpg").unlink()
        assert dbf_encoding(str(source)) == "CP936"

    def test_cpg_spellings_follow_gdal(self, tmp_path):
        source = _shapefile(tmp_path, ["x"], "ascii")
        for cpg, expected in [
            ("1252", "CP1252"),
            ("8859-5", "ISO-8859-5"),
            ("88591", "ISO-8859-1"),
            ("UTF-8\n", "UTF-8"),
            ("Big5", "Big5"),
        ]:
            (tmp_path / "src.cpg").write_text(cpg)
            assert dbf_encoding(str(source)) == expected, cpg

    def test_no_hint_and_unknown_ldid(self, tmp_path):
        source = _shapefile(tmp_path, ["x"], "ascii")
        assert dbf_encoding(str(source)) is None
        _write_dbf(tmp_path / "src.dbf", "NAME", ["x"], "ascii", ldid=0xEE)
        assert dbf_encoding(str(source)) is None

    def test_remote_sources_are_not_probed(self):
        assert dbf_encoding("https://example.com/a.shp") is None

    def test_plan_leaves_gdal_native_encodings_to_gdal(self, tmp_path):
        source = str(_shapefile(tmp_path, ["x"], "ascii"))
        assert plan_source_text(source, None) == SourceText()
        assert plan_source_text(source, "UTF-8") == SourceText(["ENCODING=UTF-8"])
        assert plan_source_text(source, "latin1") == SourceText(["ENCODING=ISO-8859-1"])

    def test_plan_decodes_other_code_pages_in_python(self, tmp_path):
        source = str(_shapefile(tmp_path, ["x"], "ascii", cpg="CP932"))
        plan = plan_source_text(source, None)
        assert plan.open_options == ["ENCODING=ISO-8859-1"]
        assert plan.codec == "cp932"
        assert plan_source_text(source, "GBK").codec == "gbk"
        assert plan_source_text("https://example.com/a.shp", "CP932").codec == "cp932"

    def test_plan_ignores_a_declared_name_python_does_not_know(self, tmp_path):
        """GDAL reads the .cpg itself; only a name the user typed is passed on."""
        source = str(_shapefile(tmp_path, ["x"], "ascii", cpg="ANSI 1252x"))
        assert plan_source_text(source, None) == SourceText()
        assert plan_source_text(source, "x-unknown") == SourceText(["ENCODING=x-unknown"])

    def test_plan_leaves_other_drivers_alone(self):
        """Only the shapefile driver is known to honour ENCODING; elsewhere GDAL decides."""
        plan = plan_source_text("a.geojson", "CP932")
        assert plan == SourceText(["ENCODING=CP932"])


class TestCrsFromReadTypeFallback:
    """The ``ST_Read`` type fallback never aborts CRS detection (#1242)."""

    @staticmethod
    def _con_with_type(type_string):
        class _Result:
            description = [("id", "INTEGER"), ("geom", type_string)]

        class _Con:
            def execute(self, _sql):
                return _Result()

        return _Con()

    def test_malformed_projjson_in_the_type_returns_none(self):
        from geoparquet_io.core.crs_utils import _crs_from_read_type

        con = self._con_with_type("GEOMETRY('{not json')")
        assert _crs_from_read_type("in.shp", con) is None

    def test_well_formed_projjson_in_the_type_is_returned(self):
        from geoparquet_io.core.crs_utils import _crs_from_read_type

        con = self._con_with_type('GEOMETRY(\'{"id": {"authority": "EPSG", "code": 3857}}\')')
        assert _crs_from_read_type("in.shp", con) == {"id": {"authority": "EPSG", "code": 3857}}
