"""Source text encoding for GDAL reads: decode what GDAL cannot, refuse what nobody named.

DuckDB spatial's bundled GDAL is built without iconv. Its shapefile driver
recodes a DBF to UTF-8 only from ISO-8859-1, whatever the ``.cpg`` file, the
DBF language driver id or the ``ENCODING`` open option say. Every other code
page (Shift_JIS, GBK, CP1251, even CP1252) reaches DuckDB byte for byte as a
VARCHAR that is not UTF-8, and ``COPY ... TO parquet`` writes those bytes into
STRING columns no reader can open (#1242).

Two pieces close that gap.

* :func:`plan_source_text` resolves the shapefile's encoding the way GDAL would
  (``--encoding``, then the ``.cpg``, then the DBF language driver id) and, for
  a code page GDAL cannot recode, asks GDAL for ISO-8859-1 instead. That read is
  lossless, one byte to one code point, so a Python codec can turn the values
  and column names back into the source's real text.
* :func:`text_safe_expr` wraps the read. With a codec it decodes every VARCHAR
  column and every column name. Without one it checks every VARCHAR value is
  UTF-8 and stops the query with an error that names the column and the
  ``--encoding`` option, so invalid strings never reach an output.
"""

from __future__ import annotations

import codecs
import re
from dataclasses import dataclass
from pathlib import Path

from geoparquet_io.core.duckdb_utils import _escape_sql_string, quote_identifier
from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError
from geoparquet_io.core.logging_config import warn
from geoparquet_io.core.remote import is_remote_url

#: GDAL's ``ConvertCodePage`` table (ogr/ogrsf_frmts/shape/ogrshapelayer.cpp):
#: DBF language driver id (header byte 29) to the code page it declares.
_LDID_CODE_PAGES = {
    1: 437, 2: 850, 3: 1252, 4: 10000, 8: 865, 10: 850, 11: 437, 13: 437, 14: 850,
    15: 437, 16: 850, 17: 437, 18: 850, 19: 932, 20: 850, 21: 437, 22: 850, 23: 865,
    24: 437, 25: 437, 26: 850, 27: 437, 28: 863, 29: 850, 31: 852, 34: 852, 35: 852,
    36: 860, 37: 850, 38: 866, 55: 850, 64: 852, 77: 936, 78: 949, 79: 950, 80: 874,
    88: 1252, 89: 1252, 100: 852, 101: 866, 102: 865, 103: 861, 104: 895, 105: 620,
    106: 737, 107: 857, 108: 863, 120: 950, 121: 949, 122: 936, 123: 932, 124: 874,
    134: 737, 135: 852, 136: 857, 150: 10007, 151: 10029, 200: 1250, 201: 1251,
    202: 1254, 203: 1253, 204: 1257,
}  # fmt: skip

#: LDID 87 is the one id GDAL spells as a name rather than a code page.
_LDID_ISO_8859_1 = 87

#: Codec names a DBF declares that Python spells differently, or that are
#: narrower than what the files really hold. Windows writes CP932 and CP936
#: under the names Shift_JIS and GB2312, extensions included.
_CODEC_ALIASES = {
    "cp10000": "mac_roman",
    "cp10007": "mac_cyrillic",
    "cp10029": "mac_latin2",
    "shift_jis": "cp932",
    "gb2312": "gbk",
}

_SHAPEFILE_SUFFIXES = (".shp", ".dbf", ".shz", ".shp.zip")

#: The fail-closed message carries this phrase; :func:`text_error` keys off it.
_NOT_UTF8 = "holds text that is not valid UTF-8"

_ENCODING_HINT = (
    "The source does not say which encoding it uses, or names one gpio cannot read. "
    "Pass --encoding (encoding= in Python) with the code page it was written in, for "
    "example CP932 for Japanese, CP936 for Chinese, CP1251 for Cyrillic or CP1252 for "
    "Western European text."
)


@dataclass(frozen=True)
class SourceText:
    """How one GDAL source's text is read.

    ``open_options`` go to ``ST_Read``. ``codec``, when set, is the Python codec
    that turns the ISO-8859-1 read back into the source's text.
    """

    open_options: list[str] | None = None
    codec: str | None = None


def _is_shapefile(path: str) -> bool:
    return path.lower().split("?", 1)[0].endswith(_SHAPEFILE_SUFFIXES)


def _sibling(path: Path, suffix: str) -> Path | None:
    for candidate in (path.with_suffix(suffix), path.with_suffix(suffix.upper())):
        if candidate.is_file():
            return candidate
    return None


def _cpg_encoding(text: str) -> str | None:
    """A ``.cpg`` first line as GDAL's ``ConvertCodePage`` reads it."""
    value = (text.strip().splitlines() or [""])[0].strip()
    if not value:
        return None
    if value.isdigit():
        number = int(value)
        if 437 <= number <= 950 or 1250 <= number <= 1258:
            return f"CP{number}"
    if value.upper().startswith("8859"):
        return f"ISO-8859-{value[5:] if value[4:5] == '-' else value[4:]}"
    if value.upper().startswith("UTF-8"):
        return "UTF-8"
    return value


def _ldid_encoding(dbf: Path) -> str | None:
    with dbf.open("rb") as handle:
        header = handle.read(32)
    if len(header) < 30:
        return None
    ldid = header[29]
    if ldid == _LDID_ISO_8859_1:
        return "ISO-8859-1"
    code_page = _LDID_CODE_PAGES.get(ldid)
    return f"CP{code_page}" if code_page else None


def dbf_encoding(input_file: str) -> str | None:
    """The encoding a local shapefile declares: its ``.cpg``, else its DBF LDID.

    Remote and zipped shapefiles are not probed (None): fetching side files
    just to look is not worth it, and the UTF-8 check still fails closed.
    """
    if is_remote_url(input_file) or not input_file.lower().endswith((".shp", ".dbf")):
        return None
    path = Path(input_file)
    cpg = _sibling(path, ".cpg")
    if cpg is not None:
        found = _cpg_encoding(cpg.read_text(encoding="latin-1"))
        if found:
            return found
    dbf = _sibling(path, ".dbf")
    return _ldid_encoding(dbf) if dbf is not None else None


def _python_codec(name: str) -> str | None:
    try:
        codec = codecs.lookup(name).name
    except LookupError:
        return None
    return _CODEC_ALIASES.get(codec, codec)


def encoding_name(encoding: str) -> str:
    """Strip a character encoding name and refuse anything that is not one."""
    encoding = encoding.strip()
    if not re.fullmatch(r"[A-Za-z0-9._-]+", encoding):
        raise InvalidParameterError("encoding", f"{encoding!r} is not a character encoding name")
    return encoding


def plan_source_text(input_file: str, encoding: str | None) -> SourceText:
    """Resolve how to read a GDAL source's text: ``encoding``, then what the DBF declares.

    UTF-8 and ISO-8859-1 are left to GDAL, which recodes both itself. Any other
    codec Python knows is read from a shapefile as ISO-8859-1 and decoded by
    :func:`text_safe_expr`, since the shapefile driver is the one whose
    ``ENCODING`` is known to mean that. A name Python does not know, and any
    name for another driver, goes to GDAL as given. A name only the file
    declares is never passed on as an open option, since GDAL reads it there
    itself.
    """
    shapefile = _is_shapefile(input_file)
    name: str | None
    if encoding:
        name = encoding_name(encoding)
    else:
        name = dbf_encoding(input_file) if shapefile else None
    codec = _python_codec(name) if name else None
    if codec is None or codec == "utf-8":
        return SourceText([f"ENCODING={name}"] if encoding else None)
    if codec == "iso8859-1":
        return SourceText(["ENCODING=ISO-8859-1"])
    if shapefile:
        return SourceText(["ENCODING=ISO-8859-1"], codec)
    return SourceText([f"ENCODING={name}"])


def _decoder(codec: str):
    """An Arrow UDF body: ISO-8859-1 text back to bytes, then decoded with ``codec``."""
    import pyarrow as pa

    replaced: list[bool] = []

    def decode(value: str) -> str:
        raw = value.encode("latin-1")
        try:
            return raw.decode(codec)
        except UnicodeDecodeError:
            if not replaced:
                replaced.append(True)
                warn(
                    f"Some text is not valid {codec}, often a multi-byte character cut "
                    "at a DBF field's width. Those bytes were replaced with U+FFFD."
                )
            return raw.decode(codec, "replace")

    def run(values):
        out = [v if v is None or v.isascii() else decode(v) for v in values.to_pylist()]
        return pa.array(out, pa.string())

    return run


def _register_decoder(con, codec: str) -> str:
    from duckdb.sqltypes import VARCHAR

    name = "gpio_decode_" + re.sub(r"[^a-z0-9]", "_", codec.lower())
    exists = con.execute(
        "SELECT 1 FROM duckdb_functions() WHERE function_name = ?", [name]
    ).fetchone()
    if not exists:
        con.create_function(name, _decoder(codec), [VARCHAR], VARCHAR, type="arrow")
    return name


def _decoded_name(name: str, codec: str) -> str:
    try:
        return name.encode("latin-1").decode(codec, "replace")
    except UnicodeEncodeError:
        return name


def decoded_names(names: list[str] | None, plan: SourceText) -> list[str] | None:
    """Column names as :func:`text_safe_expr` exposes them."""
    if names is None or plan.codec is None:
        return names
    return [_decoded_name(name, plan.codec) for name in names]


def _guard(column: str) -> str:
    quoted = quote_identifier(column)
    message = _escape_sql_string(f'Column "{column}" {_NOT_UTF8}. {_ENCODING_HINT}')
    return (
        f"CASE WHEN {quoted} IS NULL OR TRY(decode(encode({quoted}))) IS NOT NULL "
        f"THEN {quoted} ELSE error('{message}') END AS {quoted}"
    )


def _bind(con, table_expr: str) -> list[tuple[str, str]]:
    description = con.execute(f"SELECT * FROM {table_expr} LIMIT 0").description
    return [(col[0], str(col[1])) for col in description]


def _guarded(table_expr: str, text: list[str]) -> str:
    if not text:
        return table_expr
    return f"(SELECT * REPLACE ({', '.join(_guard(name) for name in text)}) FROM {table_expr})"


def _decoded(con, table_expr: str, codec: str, text: list[str], names: list[str]) -> str:
    expr = table_expr
    if text:
        udf = _register_decoder(con, codec)
        decoded = ", ".join(f"{udf}({quote_identifier(n)}) AS {quote_identifier(n)}" for n in text)
        expr = f"(SELECT * REPLACE ({decoded}) FROM {expr})"
    renames = [
        f"{quote_identifier(name)} AS {quote_identifier(_decoded_name(name, codec))}"
        for name in names
        if _decoded_name(name, codec) != name
    ]
    if renames:
        expr = f"(SELECT * RENAME ({', '.join(renames)}) FROM {expr})"
    return expr


def text_safe_expr(con, table_expr: str, plan: SourceText, columns=None) -> str:
    """``table_expr`` with every VARCHAR column decoded by ``plan`` or proven UTF-8.

    ``columns`` is ``[(name, type), ...]`` from a bind the caller already ran,
    so a GeoJSON source is not parsed twice. None binds here. An empty list
    means nothing was captured, and the expression comes back unchanged.
    """
    if columns is not None and not columns:
        return table_expr
    if columns is None:
        columns = _bind(con, table_expr)
    text = [name for name, kind in columns if kind == "VARCHAR"]
    if plan.codec is None:
        return _guarded(table_expr, text)
    return _decoded(con, table_expr, plan.codec, text, [name for name, _ in columns])


def text_error(error: BaseException) -> GeoParquetError | None:
    """The fail-closed refusal as a clean :class:`GeoParquetError`, else None.

    A column *name* that is not UTF-8 never reaches the value check: Python
    fails decoding it from the bind, before any query runs.
    """
    if isinstance(error, UnicodeDecodeError):
        return GeoParquetError(f"A column name in the source {_NOT_UTF8}. {_ENCODING_HINT}")
    message = str(error)
    if _NOT_UTF8 not in message:
        return None
    start = message.find("Column ")
    return GeoParquetError(message[start:] if start >= 0 else message)


def check_arrow_text(batch) -> None:
    """Refuse an Arrow batch whose string columns hold invalid UTF-8.

    Arrow does not check UTF-8 when DuckDB hands a batch over, so a read that
    leaves SQL (the curved-geometry path strokes WKB in Python) checks here.
    """
    import pyarrow as pa

    for field, column in zip(batch.schema, batch.columns, strict=True):
        if not (pa.types.is_string(field.type) or pa.types.is_large_string(field.type)):
            continue
        try:
            column.validate(full=True)
        except pa.ArrowInvalid as e:
            raise GeoParquetError(f'Column "{field.name}" {_NOT_UTF8}. {_ENCODING_HINT}') from e
