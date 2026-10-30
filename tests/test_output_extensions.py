"""One extension -> output-format table (#1367): `gpio convert`, `Table.write` and the
default output names all read ``format_writers.EXTENSION_TO_FORMAT``/``GDAL_FORMATS``."""

from __future__ import annotations

from pathlib import Path

import pytest

from geoparquet_io.api.table import Table
from geoparquet_io.cli.commands.convert import ConvertDefaultGroup
from geoparquet_io.core.format_writers import (
    EXTENSION_TO_FORMAT,
    GDAL_FORMATS,
    default_output_name,
)

EXPECTED = {
    ".parquet": "geoparquet",
    ".gpkg": "geopackage",
    ".fgb": "flatgeobuf",
    ".csv": "csv",
    ".shp": "shapefile",
    ".geojson": "geojson",
    ".json": "geojson",
}


def test_the_table():
    assert EXTENSION_TO_FORMAT == EXPECTED
    for name, config in GDAL_FORMATS.items():
        assert EXTENSION_TO_FORMAT[config["extension"]] == name


@pytest.mark.parametrize("ext", sorted(EXPECTED))
def test_convert_and_table_write_agree(ext):
    assert ConvertDefaultGroup.EXTENSION_TO_SUBCOMMAND is EXTENSION_TO_FORMAT
    api = Table._detect_format(f"out{ext.upper()}")
    assert api == ("parquet" if EXPECTED[ext] == "geoparquet" else EXPECTED[ext])


def test_unknown_extension_defaults_to_parquet():
    assert Table._detect_format("out.dat") == "parquet"


@pytest.mark.parametrize("name", sorted(GDAL_FORMATS))
def test_default_output_name(name):
    out = default_output_name("data/roads.parquet", name)
    assert out == "roads" + GDAL_FORMATS[name]["extension"]
    assert Path(out).parent == Path(".")
