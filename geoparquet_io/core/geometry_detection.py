"""
Geometry column detection for GeoParquet files.

This module provides functions to detect geometry columns in Parquet files
by examining GeoParquet metadata and column names.
"""

from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass
from typing import Literal, overload

from geoparquet_io.core.duckdb_utils import (
    _geoarrow_coord_exprs,
    describe_query,
    duckdb_type_is_geometry,
    fetch_one,
    geom_from_wkb_sql,
    is_interrupt,
    parse_duckdb_geo_type,
    quote_identifier,
    register_arrow,
    sql_path,
)
from geoparquet_io.core.exceptions import GeometryError, InvalidParameterError
from geoparquet_io.core.file_utils import resolve_file_url
from geoparquet_io.core.geo_metadata import carried_column_name
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.optional_deps import load_module
from geoparquet_io.core.remote import needs_httpfs

#: The name gpio gives a geometry column it builds itself (a CSV's WKT or
#: lat/lon, an ``ST_Read`` source): an output name, never a guess at an
#: input's column -- that is ``primary_geometry_column``'s question.
BUILT_GEOMETRY_COLUMN = "geometry"

#: The one list of column names that mean "the geometry" for every source, in
#: priority order. ``geo``/``geography`` are not on it: a ``geo`` column is as
#: often a country code, and only a source whose own type is called GEOGRAPHY
#: (BigQuery) can take the name as a geometry; such a source passes
#: :data:`GEOGRAPHY_SOURCE_NAMES` as ``primary_geometry_column(extra_names=...)``,
#: tried after these.
STANDARD_GEOMETRY_NAMES = ["geometry", "geom", "wkb_geometry", "shape", "the_geom"]

#: The extra candidates for a source whose geometry type is GEOGRAPHY
#: (BigQuery): passed as ``extra_names``, so the names live here too.
GEOGRAPHY_SOURCE_NAMES = ("geo", "geography")


def _name_match(column_names, extra_names=()) -> str | None:
    """Step 3 of the rule: the first standard geometry name present, case-insensitive.

    :data:`STANDARD_GEOMETRY_NAMES` in their priority, then ``extra_names``;
    the answer is the schema's own spelling.
    """
    by_lowered: dict[str, str] = {}
    for name in column_names:
        by_lowered.setdefault(str(name).lower(), name)
    for std_name in (*STANDARD_GEOMETRY_NAMES, *(str(n).lower() for n in extra_names)):
        if std_name in by_lowered:
            return by_lowered[std_name]
    return None


def _crs_short(crs) -> str:
    """Short CRS label (e.g. 'EPSG:28992') for verbose output, avoiding full PROJJSON."""
    if crs is None:
        return "none"
    if isinstance(crs, str):
        return crs
    if isinstance(crs, dict):
        cid = crs.get("id")
        if isinstance(cid, dict):
            return f"{cid.get('authority', '?')}:{cid.get('code', '?')}"
        return crs.get("name", "custom")
    return "custom"


def _summarize_geo_metadata(geo_meta) -> str:
    """One-line geo-metadata summary for verbose output (no full CRS dump)."""
    if not isinstance(geo_meta, dict):
        return str(geo_meta)
    columns = geo_meta.get("columns", {})
    cols = (
        "; ".join(
            f"{name}(encoding={col.get('encoding', '?')}, crs={_crs_short(col.get('crs'))}, "
            f"types={col.get('geometry_types', [])})"
            for name, col in columns.items()
        )
        if isinstance(columns, dict)
        else ""
    )
    return (
        f"version={geo_meta.get('version', '?')}, "
        f"primary_column={geo_meta.get('primary_column', '?')}, columns=[{cols}]"
    )


class GeometryColumnNotFoundError(GeometryError):
    """The one error for "this source has no geometry column".

    :func:`primary_geometry_column` answers ``None`` rather than guessing; a
    caller that cannot go on without a column raises this.
    """

    def __init__(self, source=None):
        where = f" in {source}" if source is not None else ""
        names = ", ".join(f"'{n}'" for n in STANDARD_GEOMETRY_NAMES[:-1])
        names += f" or '{STANDARD_GEOMETRY_NAMES[-1]}'"
        super().__init__(
            f"No geometry column found{where}. Expected a 'geo' primary_column, a column "
            f"named {names}, or a native geometry column. Name one with --geometry-column."
        )
        self.source = source


def _block_primary(block, source: str | None) -> str | None:
    """The primary column a ``geo`` block names: ``primary_column``, or the
    legacy list form's ``{"name": ..., "primary": true}`` entry. A non-string
    name is ignored with a warning (#887), never handed on."""
    if isinstance(block, dict):
        return carried_column_name(block.get("primary_column"), source=source)
    if isinstance(block, list):
        for col in block:
            if isinstance(col, dict) and col.get("primary", False):
                name = carried_column_name(col.get("name"), key="name", source=source)
                if name:
                    return name
    return None


def _is_geometry_type(type_name) -> bool:
    """A DuckDB GEOMETRY/GEOGRAPHY type, or a Parquet GEOMETRY/GEOGRAPHY logical type."""
    if isinstance(type_name, bool):
        return type_name
    if not type_name:
        return False
    from geoparquet_io.core.duckdb_metadata import is_geometry_column

    return duckdb_type_is_geometry(type_name) or is_geometry_column(str(type_name))


def _block_declared(block) -> list[str]:
    """The geometry columns a ``geo`` block declares, in its order."""
    if isinstance(block, dict) and isinstance(block.get("columns"), dict):
        return [name for name in block["columns"] if isinstance(name, str)]
    if isinstance(block, list):
        return [c["name"] for c in block if isinstance(c, dict) and isinstance(c.get("name"), str)]
    return []


def _in_schema(name: str, names: list) -> str | None:
    """``name`` as the schema spells it: exactly, else case-insensitively."""
    if name in names:
        return name
    folded = {str(n).lower(): n for n in reversed(names)}
    return folded.get(name.lower())


def _resolve_primary(primary: str | None, declared, columns, extra_names=()) -> str | None:
    """The one rule. ``columns`` is ``[(name, is_geometry_typed)]``, or None when
    the schema could not be read (then the block's word is all there is)."""
    if columns is None:
        return primary
    names = [name for name, _ in columns]
    for candidate in ([primary] if primary is not None else []) + list(declared):
        found = _in_schema(candidate, names)
        if found is not None:
            return found
    by_name = _name_match(names, extra_names)
    if by_name is not None:
        return by_name
    return next((name for name, typed in columns if typed), None)


def _arrow_columns(schema) -> list[tuple[str, bool]]:
    """``[(name, is_geometry_typed)]``: a GeoArrow extension field is typed."""
    return [(field.name, _is_geoarrow_field(field)) for field in schema]


def _is_geoarrow_field(field) -> bool:
    name = getattr(field.type, "extension_name", None)
    return isinstance(name, str) and name.startswith("geoarrow.")


def _file_columns(raw_url: str, con, verbose: bool) -> list[tuple[str, bool]] | None:
    """``[(name, is_geometry_typed)]`` for a Parquet file's columns, None when unreadable.

    The file's described columns, so a file answers as its ``describe_query``
    does (a native ``location`` column with no ``geo`` key is the primary).
    DuckDB's ``read_parquet`` first; when it refuses the file (a ``geo`` block
    it will not open, #982) the footer's root columns and their logical types,
    so a malformed block still reaches the schema rule.
    """
    own = con is None
    try:
        if own:
            from geoparquet_io.core.duckdb_utils import get_duckdb_connection

            con = get_duckdb_connection(load_httpfs=needs_httpfs(raw_url))
        described = describe_query(con, f"SELECT * FROM read_parquet({sql_path(raw_url)})")
        return [(name, _is_geometry_type(type_)) for name, type_ in described]
    except Exception as e:  # noqa: BLE001 - the footer, or the block, still answers
        if is_interrupt(e):
            raise
        if verbose:
            debug(f"Failed to read schema: {e}")
    finally:
        if own and con is not None:
            con.close()
    try:
        from geoparquet_io.core.duckdb_metadata import get_schema_info
        from geoparquet_io.core.parquet_schema import root_schema_columns

        return [
            (col["name"], _is_geometry_type(col.get("logical_type")))
            for col in root_schema_columns(get_schema_info(raw_url))
        ]
    except Exception as e:  # noqa: BLE001 - nothing readable: the block's word stands
        if is_interrupt(e):
            raise
        return None


def _described_columns(source) -> list[tuple[str, bool]]:
    columns = []
    for item in source:
        if isinstance(item, (tuple, list)):
            columns.append((item[0], _is_geometry_type(item[1] if len(item) > 1 else None)))
        else:
            columns.append((item, False))
    return columns


def _source_schema(source, geo, con, verbose):
    """``(block, label, [(name, is_geometry_typed)] or None)`` of any source the
    resolver takes: "typed columns of a source", spelled once."""
    import pyarrow as pa

    if source is None:
        # The block alone (no schema to read): its declared columns stand.
        return geo, None, [(name, False) for name in _block_declared(geo)]
    if isinstance(source, (str, os.PathLike)):
        label = str(source)
        if geo is None:
            from geoparquet_io.core.geo_metadata import raw_geo_of_file

            geo = raw_geo_of_file(label, con).value
            if verbose and geo:
                debug(f"Geo metadata: {_summarize_geo_metadata(geo)}")
        return geo, label, _file_columns(resolve_file_url(label, verbose=False), con, verbose)
    if isinstance(source, (pa.Table, pa.Schema)):
        schema = source.schema if isinstance(source, pa.Table) else source
        if geo is None:
            from geoparquet_io.core.geo_metadata import sanitized_carried_geo

            geo = sanitized_carried_geo(schema.metadata)
        return geo, None, _arrow_columns(schema)
    return geo, None, _described_columns(source)


def primary_geometry_column(
    source,
    *,
    geo=None,
    extra_names=(),
    con=None,
    verbose: bool = False,
) -> str | None:
    """The primary geometry column of ``source``, or ``None`` when it has none.

    The one resolver (#1367 concept 7). ``source`` is any of:

    - a RAW Parquet path or URL (``str`` / ``os.PathLike``): the block is the
      footer's ``geo`` key as the file holds it, the schema the file's
      described columns and their types (both read on ``con`` when given);
    - a ``pa.Table`` or ``pa.Schema``: the block is its sanitized carried
      ``geo``, the schema its fields (a GeoArrow extension field is typed);
    - an iterable of ``(name, type)`` pairs (a ``describe_query`` result, or
      Parquet logical types): no block unless ``geo`` is given. Bare names are
      accepted but carry no types: a caller holding a table, schema, path or
      query hands the owner that instead (#1367);
    - ``None`` with ``geo``: the block alone, for a reader with no schema
      (a carried metadata dict): its primary, else its first declared column.

    ``geo`` is a block the caller already holds; it replaces the source's own.
    ``extra_names`` are a source's own candidate names, tried after the
    standard ones (BigQuery's ``geo``/``geography``); never a second list.

    The rule, one for every source and every caller:

    1. the block's primary (``primary_column``, or the legacy list form's
       primary entry; a non-string one is ignored with a warning, #887), if
       the schema has it -- exactly, else case-insensitively, answered in the
       schema's spelling. A block describes the data it was written with; a
       column a projection or query dropped is not an answer;
    2. else the first column the block declares that the schema has;
    3. else the first standard geometry name (:data:`STANDARD_GEOMETRY_NAMES`,
       in that priority, then ``extra_names``; case-insensitive);
    4. else the first column typed as native geometry;
    5. else ``None``. Never the literal ``"geometry"``: a caller that needs a
       column raises :class:`GeometryColumnNotFoundError` on ``None`` (see
       :func:`require_geometry_column`).

    Only when the schema cannot be read at all does the block's primary stand
    on its own.
    """
    geo, label, columns = _source_schema(source, geo, con, verbose)
    primary = _block_primary(geo, label)
    resolved = _resolve_primary(primary, _block_declared(geo), columns, extra_names)
    if verbose:
        debug(
            f"Primary geometry column: {resolved}"
            if resolved
            else "No geometry column found in the source"
        )
    return resolved


def require_geometry_column(column: str | None, source=None) -> str:
    """``column``, or :class:`GeometryColumnNotFoundError` naming ``source`` when it is None.

    The one hard failure for a caller that cannot go on without a geometry
    column; it takes :func:`primary_geometry_column`'s *answer*, never a source.
    """
    if column is None:
        raise GeometryColumnNotFoundError(source)
    return column


@overload
def resolve_geometry_column(
    source,
    named: str | None = None,
    *,
    geo=None,
    where=None,
    required: Literal[True] = True,
    con=None,
    verbose: bool = False,
) -> str: ...


@overload
def resolve_geometry_column(
    source,
    named: str | None = None,
    *,
    geo=None,
    where=None,
    required: bool,
    con=None,
    verbose: bool = False,
) -> str | None: ...


def resolve_geometry_column(
    source,
    named: str | None = None,
    *,
    geo=None,
    where=None,
    required: bool = True,
    con=None,
    verbose: bool = False,
) -> str | None:
    """The geometry column a command runs on: the one front door (#1367).

    ``source`` is any source :func:`primary_geometry_column` takes (a RAW
    path, an Arrow table or schema, ``(name, type)`` pairs). ``named`` is the
    caller's explicit column: it is the answer when ``source`` has it, else an
    :class:`InvalidParameterError` -- never silently swapped for another. With
    no ``named``, the resolver's answer, or :class:`GeometryColumnNotFoundError`.
    Only a *block's* primary falls through to the schema. ``where`` names the
    source in the error (default: the path, for a file). ``required=False`` is
    for a path that runs without a geometry column (an attribute table, the
    aggregate bbox and point-column modes): no column is then ``None``, but a
    named one that is missing still raises.
    """
    if named is None:
        found = primary_geometry_column(source, geo=geo, con=con, verbose=verbose)
        if not required:
            return found
        label = where if where is not None else _label_of(source)
        return require_geometry_column(found, label)
    _, label, columns = _source_schema(source, {} if geo is None else geo, con, verbose)
    if columns is None or any(name == named for name, _ in columns):
        return named
    label = where if where is not None else label
    in_where = f" in {label}" if label is not None else ""
    # The columns it does have, so a typo or a stale name is fixable.
    available = ", ".join(repr(name) for name, _ in columns) or "none"
    raise InvalidParameterError(
        "geometry_column",
        f"geometry column '{named}' not found{in_where}; its columns are: {available}",
    )


def _label_of(source) -> str | None:
    return str(source) if isinstance(source, (str, os.PathLike)) else None


def native_geometry_types_from_query(con, query: str, verbose: bool = False) -> dict[str, str]:
    """``{column: logical type string}`` for every geometry column the query emits.

    The output query's own ``DESCRIBE`` already names them and the CRS each one
    carries -- ``GEOMETRY``, ``GEOMETRY('EPSG:3857')`` -- so it answers "which
    geometry columns does this write emit, and in which CRS" with **no input-file
    witness at all**. That is what lets ``partition`` staging, whose per-partition
    write passes none, describe a secondary native geometry column (#1175).

    Rendered in the ``GeometryType(crs=EPSG:3857)`` spelling
    :func:`~geoparquet_io.core.duckdb_metadata.parse_geometry_logical_type` reads,
    so one derivation (``derive_secondary_geometry_info``) serves both this and an
    input's own footer. The CRS arrives as a bare authority reference rather than
    the inline PROJJSON a logical type carries, which is why a caller that *has* a
    witness prefers it.

    DuckDB has no ``GEOGRAPHY`` type, so this source can never report ``edges``;
    those travel separately, through ``write_funnels.apply_nonplanar_edges``.

    Best-effort: a ``DESCRIBE`` that fails answers ``{}`` rather than failing the
    write.
    """

    try:
        described = describe_query(con, query)
    except Exception as e:  # noqa: BLE001 - a probe, never the write's failure
        if is_interrupt(e):
            raise
        if verbose:
            debug(f"Could not read geometry types off the output query: {e}")
        return {}

    native: dict[str, str] = {}
    for name, col_type in described:
        parsed = parse_duckdb_geo_type(col_type)
        if not name or parsed is None:
            continue
        family = "Geography" if parsed[0] == "GEOGRAPHY" else "Geometry"
        crs = parsed[1]
        native[name] = f"{family}Type(crs={crs})" if crs else f"{family}Type()"
    return native


def _detect_geometry_from_query(
    con, query: str, original_metadata: dict | None = None, verbose: bool = False
) -> str | None:
    """:func:`primary_geometry_column` of a query's output, ``original_metadata``
    (the input's block) as the block. ``None`` when the query does not bind."""
    try:
        described = describe_query(con, query)
    except Exception as e:
        if is_interrupt(e):
            raise
        if verbose:
            debug(f"Error detecting geometry from query: {e}")
        return None
    return primary_geometry_column(described, geo=original_metadata, verbose=verbose)


# =============================================================================
# How a geometry column is read (#624, #1082)
# =============================================================================

# A GeoArrow coordinate struct, bare (a point) or wrapped in one list level per
# nesting step, so `STRUCT(x DOUBLE, y DOUBLE)[][]` is a polygon's rings.
_GEOARROW_STRUCT_TYPE = re.compile(r"^STRUCT\((.*)\)((?:\[\])*)$", re.IGNORECASE | re.DOTALL)

# List depth of a GeoArrow column -> an encoding whose coordinate path it
# exposes. Depth alone cannot tell linestring from multipoint, nor polygon from
# multilinestring, but both twins share a coordinate path, which is all a reader
# of coordinates needs.
_GEOARROW_ENCODING_BY_DEPTH = {0: "point", 1: "linestring", 2: "polygon", 3: "multipolygon"}

_READABLE_KINDS = frozenset({"native", "wkb", "geoarrow"})


def _geoarrow_struct_depth(duckdb_type: str) -> int | None:
    """List depth of a GeoArrow coordinate struct type, None when it is not one."""
    match = _GEOARROW_STRUCT_TYPE.match(duckdb_type.strip())
    if not match:
        return None
    fields = {f.strip().split(" ")[0].strip('"').lower() for f in match.group(1).split(",")}
    if not {"x", "y"} <= fields:
        return None
    return len(match.group(2)) // 2


def _read_kind(duckdb_type: str | None) -> str:
    if duckdb_type is None:
        return "missing"

    text = duckdb_type.strip()
    if parse_duckdb_geo_type(text) is not None:
        return "native"
    if text.upper() == "BLOB":
        return "wkb"
    if _geoarrow_struct_depth(text) in _GEOARROW_ENCODING_BY_DEPTH:
        return "geoarrow"
    return "unsupported"


@dataclass(frozen=True)
class GeometryRead:
    """The one answer to "how does SQL read this geometry column?".

    Every check that reads geometry needs the same facts, and each used to
    guess them from ``"GEOMETRY" in col_type`` (#624). The expression that
    yields a GEOMETRY, the predicate for "this row holds a value" and, when the
    column cannot be read at all, a sentence saying why. ``kind`` is one of
    ``native`` (a GEOMETRY column), ``wkb`` (a BLOB of WKB), ``geoarrow`` (a
    coordinate struct), ``missing`` or ``unsupported``.

    A zero-length BLOB is not WKB (#1082), and neither are bytes that do not
    parse (#1064). ``expr`` maps both to NULL and ``present_sql`` excludes them,
    so a check reads the other rows instead of dying on them. Only an encoding
    check goes looking for them, through ``empty_sql`` and ``unparsable_sql``.

    ``flat`` says the column reaches SQL as flat vectors, as a registered
    non-dictionary Arrow column does. The ``list_extract`` guard against
    dictionary vectors (see ``parseable_sql``) copies every value, which over
    a whole geometry column is a second copy of it, so a flat read skips it.
    """

    column: str
    kind: str
    duckdb_type: str = ""
    flat: bool = False
    encoding: str | None = None

    @classmethod
    def for_type(cls, column: str, duckdb_type: str | None, *, flat: bool = False) -> GeometryRead:
        return cls(column, _read_kind(duckdb_type), duckdb_type or "", flat)

    @classmethod
    def for_geoarrow(cls, column: str, encoding: str) -> GeometryRead:
        """A GeoArrow column whose native encoding the ``geo`` block already names
        (``point`` ... ``multipolygon``), so no DuckDB type has to be probed."""
        return cls(column, "geoarrow", encoding=encoding.lower())

    @property
    def readable(self) -> bool:
        return self.kind in _READABLE_KINDS

    @property
    def native(self) -> bool:
        """The column already reaches SQL as GEOMETRY (or GEOGRAPHY): no decode."""
        return self.kind == "native"

    @property
    def quoted(self) -> str:
        return quote_identifier(self.column)

    @property
    def expr(self) -> str:
        """SQL for the value as GEOMETRY (NULL for a NULL or zero-length value).

        For a GeoArrow column this is a representative point, the centre of the
        value's coordinate extent, which is what a distance-based reader needs.
        """
        if self.kind == "wkb" and self.flat:
            q = self.quoted
            return f"(CASE WHEN octet_length({q}) > 0 THEN TRY({geom_from_wkb_sql(q)}) END)"
        if self.kind == "wkb":
            # CASE, not a WHERE conjunct, so no evaluation order can hand a bad
            # value to ST_GeomFromWKB. The THEN branch reads a flat vector as
            # well. On a dictionary vector DuckDB evaluates a bare call over the
            # whole dictionary, ignoring the CASE, and raises on its bad entry.
            flat = f"list_extract([{self.quoted}], 1)"
            return f"(CASE WHEN {self.parseable_sql} THEN {geom_from_wkb_sql(flat)} END)"
        if self.kind == "geoarrow":
            return self._geoarrow_point_expr()
        return self.quoted

    @property
    def shape_expr(self) -> str:
        """SQL for the whole value as GEOMETRY, never a representative point.

        ``expr`` for a native or WKB column. Any other column is handed over
        bare, so a function that needs a real shape (a bbox, a sort key, a
        validity check) fails on it as it always did rather than reading the
        GeoArrow centre point ``expr`` gives.
        """
        return self.expr if self.kind in ("native", "wkb") else self.quoted

    @property
    def present_sql(self) -> str:
        """Predicate for a row that holds a geometry value to read."""
        return f"{self.quoted} IS NOT NULL AND {self.parseable_sql}"

    @property
    def parseable_sql(self) -> str:
        """Predicate for a value SQL can read as a geometry.

        For WKB, non-empty bytes that ``ST_GeomFromWKB`` parses. The length test
        comes first because ``TRY(ST_GeomFromWKB(''))`` is not NULL, it is a
        value every later function rejects. ``list_extract([v], 1)`` hands
        ``TRY`` a flat vector. On a dictionary-encoded column, pyarrow's
        default, one failing row otherwise nulls every row of the vector
        (measured on DuckDB 1.5.6, see tests/test_validate_malformed_wkb.py).
        """
        if self.kind != "wkb":
            return "TRUE"
        q = self.quoted
        if self.flat:
            return f"(octet_length({q}) > 0 AND TRY({geom_from_wkb_sql(q)}) IS NOT NULL)"
        flat = geom_from_wkb_sql(f"list_extract([{q}], 1)")
        return f"(octet_length({q}) > 0 AND TRY({flat}) IS NOT NULL)"

    @property
    def empty_sql(self) -> str:
        """Predicate for a value that is present but zero-length (never valid WKB)."""
        if self.kind == "wkb":
            return f"octet_length({self.quoted}) = 0"
        return "FALSE"

    @property
    def unparsable_sql(self) -> str:
        """Predicate for a non-empty value that is not valid WKB."""
        if self.kind == "wkb":
            return f"(octet_length({self.quoted}) > 0 AND NOT {self.parseable_sql})"
        return "FALSE"

    @property
    def problem(self) -> str | None:
        """Why this column cannot be read, or None when it can."""
        if self.kind == "missing":
            return f'geometry column "{self.column}" not found in the file'
        if self.kind == "unsupported":
            return (
                f'geometry column "{self.column}" has type {self.duckdb_type}, '
                "which is not WKB, a GEOMETRY type or a GeoArrow coordinate struct"
            )
        return None

    @property
    def geoarrow_encoding(self) -> str | None:
        """The GeoArrow native encoding (``point`` ... ``multipolygon``) of a
        ``geoarrow`` column, from its coordinate struct's list depth; else None."""
        if self.kind != "geoarrow":
            return None
        if self.encoding is not None:
            return self.encoding
        return _GEOARROW_ENCODING_BY_DEPTH[_geoarrow_struct_depth(self.duckdb_type) or 0]

    def _geoarrow_point_expr(self) -> str:
        encoding = self.geoarrow_encoding or "point"
        _, _, _, _, cx, cy = _geoarrow_coord_exprs(self.quoted, encoding)
        return f"ST_Point({cx}, {cy})"


@dataclass(frozen=True)
class GeometryPresence:
    """Row counts for a geometry column, and how many hold no readable value.

    The ``first_*_row`` fields are 0-based row indexes in the file, so a user
    can find the value.
    """

    total: int
    nulls: int
    empties: int
    first_empty_row: int | None
    unparsable: int = 0
    first_unparsable_row: int | None = None


def _find_described_type(described, column: str) -> str | None:
    by_name = {row[0]: str(row[1]) for row in described if row and row[0]}
    if column in by_name:
        return by_name[column]
    folded = {name.lower(): col_type for name, col_type in by_name.items()}
    return folded.get(column.lower())


def resolve_geometry_read(
    con, raw_url: str, column: str, *, relation: bool = False
) -> GeometryRead:
    """How ``column`` of the Parquet file at RAW ``raw_url`` is read on ``con``.

    The one producer of a geometry read expression (#1367 concept 7): every
    path that has to turn a column into GEOMETRY asks here and uses the
    answer's ``shape_expr`` / ``expr``, never ``"GEOMETRY" in col_type``.

    With ``relation=True``, ``raw_url`` is instead SQL usable in a ``FROM``
    clause -- a registered relation's name, ``read_parquet(...)``, a
    parenthesized subquery -- and the column's type is read off it.

    The answer depends on the connection. With GeoParquet conversion on, DuckDB
    presents a WKB column as GEOMETRY and decodes it lazily, so a caller that
    must see the stored bytes calls ``disable_geoparquet_conversion`` first.
    """
    source = raw_url if relation else f"read_parquet({sql_path(raw_url)})"
    described = describe_query(con, f"SELECT * FROM {source}")
    return GeometryRead.for_type(column, _find_described_type(described, column))


def create_geometry_view(con, relation: str, column: str, view: str | None = None) -> str:
    """The relation to read ``column`` from as GEOMETRY: ``relation`` or a new view.

    The one view builder (#1367 concept 7). A registered Arrow table, an IPC
    stream or a file with no ``geo`` key reaches DuckDB with its WKB column as
    ``BLOB``; this decodes it in place -- ``CREATE OR REPLACE TEMP VIEW view AS
    SELECT * REPLACE (ST_GeomFromWKB(column) AS column) FROM relation`` -- so
    every column keeps its position. A column that is not WKB (already native,
    a GeoArrow struct, absent) needs no view and ``relation`` comes back
    unchanged. ``view`` defaults to a fresh name. Malformed WKB raises when
    the view is read, as every copy of this did.
    """
    read = resolve_geometry_read(con, relation, column, relation=True)
    if read.kind != "wkb":
        return relation
    view = view or f"__gpio_geometry_{uuid.uuid4().hex}"
    quoted = read.quoted
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {quote_identifier(view)} AS "
        f"SELECT * REPLACE ({geom_from_wkb_sql(quoted)} AS {quoted}) FROM {relation}"
    )
    return view


def register_geometry_column(
    con, name: str, table, column: str, *, row_index: str | None = None
) -> GeometryRead:
    """Register only ``column`` of the Arrow ``table`` on ``con`` as ``name``.

    The one way a table-centric helper hands geometry to DuckDB (#1268): the
    other columns never cross, so nothing but the geometry is scanned and the
    caller's table is never rebuilt from a DuckDB result. Registration is
    zero-copy. With ``row_index``, an int64 column of that name numbering the
    rows from 0 is registered beside it, for a caller that wants positions
    back. A dictionary-typed column is decoded first, so the reader can use
    the flat-vector WKB parse (see ``GeometryRead.flat``).

    Returns how SQL reads the registered column.
    """
    np = load_module("numpy")
    import pyarrow as pa

    geometry = table.column(column)
    if pa.types.is_dictionary(geometry.type):
        geometry = geometry.cast(geometry.type.value_type)
    columns = {}
    if row_index is not None:
        columns[row_index] = pa.array(np.arange(table.num_rows, dtype=np.int64))
    columns[column] = geometry
    register_arrow(con, name, pa.table(columns))
    described = describe_query(con, f"SELECT * FROM {quote_identifier(name)}")
    return GeometryRead.for_type(column, _find_described_type(described, column), flat=True)


def count_geometry_presence(
    con, raw_url: str, read: GeometryRead, limit_clause: str
) -> GeometryPresence:
    """Count rows, NULL, zero-length and unparsable values over the sampled rows."""
    q = read.quoted
    empty, bad = read.empty_sql, read.unparsable_sql
    row = fetch_one(
        con.execute(f"""
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE {q} IS NULL),
               COUNT(*) FILTER (WHERE {empty}),
               MIN(file_row_number) FILTER (WHERE {empty}),
               COUNT(*) FILTER (WHERE {bad}),
               MIN(file_row_number) FILTER (WHERE {bad})
        FROM (
            SELECT {q}, file_row_number
            FROM read_parquet({sql_path(raw_url)}, file_row_number = true)
            {limit_clause}
        )
    """)
    )
    return GeometryPresence(*row) if row else GeometryPresence(0, 0, 0, None)
