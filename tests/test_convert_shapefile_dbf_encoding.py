"""A shapefile's DBF is not UTF-8, and ``convert`` must not pretend it is.

DBF attribute data carries no encoding in the format itself; a shapefile
declares it in a ``.cpg`` sidecar, and the GDAL shapefile driver reads that (or
takes ``SHAPE_ENCODING`` / the layer's ``ENCODING`` open option). ``gpio convert
geoparquet`` reads a Windows-1250 DBF and copies its raw bytes into an Arrow
``string`` column without transcoding, so the output Parquet declares UTF-8 and
contains bytes that are not UTF-8.

Measured with 1.5.0, on a three-row shapefile whose DBF holds Czech place names
in CP1250 and whose ``.cpg`` says ``1250``::

    $ gpio convert geoparquet cz.shp cz.parquet
    Converting cz.shp...
    Done in 0.1s
    Output: cz.parquet (1.50 KB)
    ✓ Output passes GeoParquet validation (metadata checks)

    >>> pyarrow.parquet.read_table("cz.parquet").column("nazev").to_pylist()
    UnicodeDecodeError: 'utf-8' codec can't decode byte 0x9a in position 4

So the failure is silent: convert reports success and a validation tick over a
file no conforming Arrow or Parquet reader can decode. The bytes are ordinary
CP1250 -- ``0x9A``->š, ``0xF8``->ř, ``0xF2``->ň, ``0xC8``->Č, ``0xEC``->ě,
``0xED``->í, giving Benešov, Pelhřimov and Kadaň.

Downstream the corruption surfaces far from its cause, as a DuckDB error in a
later pass over the file gpio itself wrote::

    Error: Failed to add bbox: Invalid Input Error: Invalid string encoding found
    in Parquet file "/tmp/tmpXXXX.parquet": value "Bene\\x9Aov" is not valid UTF8!

Nothing on the reading side overrides it. ``--encoding`` exists on ``gpio
convert shapefile``, but that is the GeoParquet-*to*-shapefile writer;
``convert geoparquet`` has no encoding option, and setting ``SHAPE_ENCODING``
in the environment makes no difference. Neither does the sidecar's spelling:
``.cpg`` containing ``CP1250``, ``.cpg`` containing ``1250`` and no ``.cpg`` at
all all produce the same broken output, which is what makes this look like the
declaration is not consulted at all rather than merely misparsed.

Whether the right answer is to honour ``.cpg``, to accept an ``--encoding`` on
the reading side, to default to a GDAL-configured ``SHAPE_ENCODING``, or simply
to refuse the file rather than write an undecodable one, is left to the
maintainers. Failing loudly would already be an improvement on this; the one
outcome that should not survive is a success message over a corrupt file.

Non-UTF-8 CSV is already handled as a known gap (#1102). This is the same class
of problem reached through the shapefile reader.
"""

import struct

import duckdb
import pytest

PLACES = ["Benešov", "Pelhřimov", "Kadaň"]

# Minimal WGS 84; convert refuses a shapefile with no .prj at all.
_PRJ = (
    'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
)


def _write_cp1250_shapefile(directory) -> str:
    """Write a 3-row shapefile whose DBF text is CP1250 and whose .cpg says so.

    Built in two steps rather than shipped as a binary fixture: DuckDB spatial
    writes an ordinary shapefile, then the fixed-width DBF text field is
    re-encoded in place. CP1250 is single-byte, so every name is no longer than
    its UTF-8 form and the field stays space-padded to its declared width.
    """
    base = directory / "cz"
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        rows = " UNION ALL ".join(
            f"SELECT 'ascii{i}' AS nazev, ST_Point({14.0 + i}, {49.0 + i}) AS geom"
            for i in range(len(PLACES))
        )
        con.execute(
            f"COPY ({rows}) TO '{base}.shp' (FORMAT GDAL, DRIVER 'ESRI Shapefile')"
        )
    finally:
        con.close()

    dbf = base.with_suffix(".dbf")
    raw = bytearray(dbf.read_bytes())
    header_len, record_len = struct.unpack_from("<HH", raw, 8)
    field_len = raw[32 + 16]  # first field descriptor's length byte
    for i, name in enumerate(PLACES):
        start = header_len + i * record_len + 1  # +1 skips the deletion flag
        raw[start : start + field_len] = name.encode("cp1250").ljust(field_len, b" ")
    dbf.write_bytes(bytes(raw))

    base.with_suffix(".cpg").write_bytes(b"1250\n")
    base.with_suffix(".prj").write_text(_PRJ, encoding="ascii")
    return f"{base}.shp"


def test_the_fixture_really_holds_cp1250_bytes(tmp_path):
    """Guard the premise: the DBF is non-UTF-8 and the sidecar declares the code page."""
    shp = _write_cp1250_shapefile(tmp_path)
    dbf = (tmp_path / "cz.dbf").read_bytes()
    assert "Benešov".encode("cp1250") in dbf
    assert b"Bene\x9aov" in dbf
    with pytest.raises(UnicodeDecodeError):
        dbf.decode("utf-8")
    assert (tmp_path / "cz.cpg").read_bytes().strip() == b"1250"
    assert shp.endswith(".shp")


@pytest.mark.xfail(
    strict=True,
    reason="gpio gap: convert geoparquet copies non-UTF-8 DBF bytes into an Arrow "
    "string column instead of transcoding from the declared code page, so the "
    "output Parquet cannot be decoded",
)
def test_converted_shapefile_text_is_valid_utf8(tmp_path):
    import pyarrow.parquet as pq
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    shp = _write_cp1250_shapefile(tmp_path)
    output = tmp_path / "cz.parquet"

    result = CliRunner().invoke(cli, ["convert", "geoparquet", shp, str(output)])
    assert result.exit_code == 0, result.output

    # The point of the test: a reader must be able to decode what gpio wrote.
    assert pq.read_table(str(output)).column("nazev").to_pylist() == PLACES
