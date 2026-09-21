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

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import geoparquet_io as gpio
from geoparquet_io.cli.main import cli
from geoparquet_io.core import convert as convert_mod
from geoparquet_io.core.convert import (
    _bounds_with_curve_fallback,
    _build_st_read_expr,
    _read_spatial_to_arrow,
    convert_to_geoparquet,
    csv_encoding,
    read_spatial_to_arrow,
    source_open_options,
)
from geoparquet_io.core.exceptions import InvalidParameterError

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

    def test_encoding_on_public_api(self, latin1_shapefile):
        table = gpio.convert(str(latin1_shapefile), encoding="ISO-8859-1")
        assert table.to_arrow().column("NAME")[0].as_py() == LATIN1_NAME

    def test_encoding_is_refused_for_parquet_before_any_work(self, tmp_path):
        """The refusal keeps its type and fires before the output path is touched."""
        out = tmp_path / "missing-dir" / "o.parquet"
        with pytest.raises(InvalidParameterError, match="Parquet"):
            convert_to_geoparquet(str(DATA / "buildings_test.parquet"), str(out), encoding="UTF-8")
        with pytest.raises(InvalidParameterError, match="Parquet"):
            read_spatial_to_arrow(str(DATA / "buildings_test.parquet"), encoding="UTF-8")
        assert not out.parent.exists()

    def test_bad_encoding_name_keeps_its_type(self, latin1_shapefile, tmp_path):
        with pytest.raises(InvalidParameterError, match="encoding"):
            convert_to_geoparquet(
                str(latin1_shapefile), str(tmp_path / "o.parquet"), encoding="latin 1; drop"
            )


class TestFallbacksKeepTheEncoding:
    """Every re-read of the source must carry the open options of the first read."""

    CURVE_ERROR = "Unsupported geometry type in WKB"

    def test_bounds_fallback_linearizes_an_encoded_read(self, monkeypatch):
        calls = []

        def fake_bounds(con, input_file, geom_column, verbose, **kwargs):
            if kwargs["table_expr"] == "ST_Read(...)":
                raise duckdb.InvalidInputException(self.CURVE_ERROR)
            return (0, 0, 1, 1)

        def fake_register(con, input_file, layer, geom_column, max_angle_deg, **kwargs):
            calls.append(kwargs)
            return "_gpio_linearized"

        monkeypatch.setattr(convert_mod, "_calculate_bounds", fake_bounds)
        monkeypatch.setattr(convert_mod, "_register_linearized_view", fake_register)
        bounds, table_expr = _bounds_with_curve_fallback(
            None,
            "a.gdb",
            "geom",
            False,
            is_parquet=False,
            encoding="WKB",
            table_expr="ST_Read(...)",
            layer=None,
            linearize_curves=True,
            max_angle_deg=None,
            already_linearized=False,
            open_options=["ENCODING=ISO-8859-1"],
        )
        assert bounds == (0, 0, 1, 1)
        assert table_expr == "_gpio_linearized"
        assert calls == [{"open_options": ["ENCODING=ISO-8859-1"]}]

    def test_bounds_fallback_is_final_once_linearized(self, monkeypatch):
        def fake_bounds(*args, **kwargs):
            raise duckdb.InvalidInputException(self.CURVE_ERROR)

        monkeypatch.setattr(convert_mod, "_calculate_bounds", fake_bounds)
        with pytest.raises(duckdb.Error):
            _bounds_with_curve_fallback(
                None,
                "a.gdb",
                "geom",
                False,
                is_parquet=False,
                encoding="WKB",
                table_expr="_gpio_linearized",
                layer=None,
                linearize_curves=True,
                max_angle_deg=None,
                already_linearized=True,
            )

    def test_arrow_read_fallback_keeps_open_options(self, monkeypatch):
        class Con:
            def execute(self, query):
                raise duckdb.InvalidInputException(TestFallbacksKeepTheEncoding.CURVE_ERROR)

        calls = []

        def fake_linearized(con, input_file, layer, geom_column, max_angle_deg=None, **kwargs):
            calls.append(kwargs)
            return "table"

        monkeypatch.setattr(convert_mod, "_detect_geometry_column", lambda *a, **k: "geom")
        monkeypatch.setattr(convert_mod, "_choose_read_strategy", lambda *a, **k: "normal")
        monkeypatch.setattr(convert_mod, "_read_spatial_linearized", fake_linearized)
        assert _read_spatial_to_arrow(Con(), "a.gdb", False, encoding="ISO-8859-1") == "table"
        assert [c["open_options"] for c in calls] == [["ENCODING=ISO-8859-1"]]


class TestEncodingOnLinearizedReads:
    """The curved fixtures drive the real linearize paths, option attached."""

    def test_encoding_travels_through_the_prescanned_gpkg_linearize(self, tmp_path):
        out = tmp_path / "curved.parquet"
        convert_to_geoparquet(str(DATA / "curved_geometry_test.gpkg"), str(out), encoding="UTF-8")
        assert pq.read_metadata(out).num_rows > 0

    def test_encoding_travels_through_the_filegdb_runtime_fallback(self):
        """FileGDB has no pre-scan: the curve error fires mid-read and the re-read follows."""
        table, _crs, geom = read_spatial_to_arrow(
            str(DATA / "curved_geometry_test.gdb"), encoding="UTF-8"
        )
        assert geom == "geometry"
        assert table.num_rows > 0


class TestCsv:
    @pytest.fixture
    def latin1_csv(self, tmp_path: Path) -> Path:
        path = tmp_path / "points.csv"
        path.write_bytes(b"name,wkt\n" + f"{LATIN1_NAME},POINT(1 2)\n".encode("latin-1"))
        return path

    def test_csv_encoding_maps_the_names_gdal_users_know(self):
        assert csv_encoding(None) is None
        assert csv_encoding("ISO-8859-1") == "latin-1"
        assert csv_encoding("latin1") == "latin-1"
        assert csv_encoding("windows-1252") == "cp1252"
        assert csv_encoding("UTF-8") == "utf-8"
        # Unknown to the map: handed over as typed so DuckDB names what it supports.
        assert csv_encoding("KOI8-R") == "KOI8-R"
        # Unknown to Python's codec registry too: still handed over as typed.
        assert csv_encoding("x-no-such-codec") == "x-no-such-codec"

    def test_encoding_on_csv_without_geometry_reads_a_plain_table(self, tmp_path):
        path = tmp_path / "plain.csv"
        path.write_bytes(b"name,value\n" + f"{LATIN1_NAME},1\n".encode("latin-1"))
        table, crs, geom = read_spatial_to_arrow(str(path), encoding="ISO-8859-1")
        assert (crs, geom) == (None, None)
        assert table.column("name")[0].as_py() == LATIN1_NAME

    def test_latin1_csv_fails_without_encoding(self, latin1_csv, tmp_path):
        with pytest.raises(Exception, match="(?i)unicode|utf"):
            convert_to_geoparquet(str(latin1_csv), str(tmp_path / "o.parquet"))

    def test_encoding_recodes_latin1_csv(self, latin1_csv, tmp_path):
        out = tmp_path / "out.parquet"
        convert_to_geoparquet(str(latin1_csv), str(out), encoding="ISO-8859-1")
        assert pq.read_table(out, columns=["name"]).column("name").to_pylist() == [LATIN1_NAME]

    def test_encoding_on_csv_read_api(self, latin1_csv):
        table, _crs, geom = read_spatial_to_arrow(str(latin1_csv), encoding="ISO-8859-1")
        assert geom == "geometry"
        assert table.column("name")[0].as_py() == LATIN1_NAME

    def test_builtin_csv_encodings_need_no_extension(self, latin1_csv, monkeypatch):
        loads = []
        monkeypatch.setattr(
            convert_mod, "_install_and_load_extension", lambda con, name: loads.append(name)
        )
        read_spatial_to_arrow(str(latin1_csv), encoding="ISO-8859-1")
        assert loads == []

    def test_cp1252_loads_the_encodings_extension(self, latin1_csv, monkeypatch):
        """CP1252 is not built into the CSV reader; DuckDB's `encodings` extension provides it."""
        loads = []

        def fake_load(con, name):
            loads.append(name)
            con.execute("LOAD encodings")  # the real extension, when cached locally

        monkeypatch.setattr(convert_mod, "_install_and_load_extension", fake_load)
        try:
            table, _crs, _geom = read_spatial_to_arrow(str(latin1_csv), encoding="windows-1252")
        except Exception as exc:  # noqa: BLE001 - offline runner without the extension cached
            pytest.skip(f"encodings extension unavailable here: {exc}")
        assert loads == ["encodings"]
        assert table.column("name")[0].as_py() == LATIN1_NAME

    def test_missing_encodings_extension_is_reported_in_terms_of_the_option(
        self, latin1_csv, tmp_path, monkeypatch
    ):
        def fail(con, name):
            raise RuntimeError("no network")

        monkeypatch.setattr(convert_mod, "_install_and_load_extension", fail)
        with pytest.raises(Exception, match="'cp1252' needs DuckDB's 'encodings' extension"):
            convert_to_geoparquet(
                str(latin1_csv), str(tmp_path / "o.parquet"), encoding="windows-1252"
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
