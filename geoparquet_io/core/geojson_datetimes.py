"""GeoJSON dates and times parsed by DuckDB, not by the GDAL bundled with DuckDB spatial.

GDAL 3.8.5, which DuckDB spatial 1.5 bundles, drops the UTC offset of a GeoJSON
datetime when every value in a column has the same offset. The Arrow stream
labels the column with that offset but stores the local clock time as UTC, so
``2026-01-01T00:00:00+01:00`` comes back as ``2026-01-01 00:00:00`` UTC (#1152).
A column with mixed offsets is read correctly. GDAL 3.10 fixed it.

The workaround reads the source with the GeoJSON driver's ``DATE_AS_STRING=YES``
open option, which hands back the raw ISO text, and casts each column back to
the type GDAL gave it. DuckDB's own cast applies the offset.

Remove this module once gpio requires DuckDB 2.0, whose spatial extension
bundles GDAL 3.12 (https://github.com/duckdb/duckdb-spatial/pull/838).
"""

from __future__ import annotations

from dataclasses import dataclass

from geoparquet_io.core.duckdb_utils import quote_identifier

#: The GeoJSON driver's open option that reads dates as their source text.
DATE_AS_STRING = "DATE_AS_STRING=YES"

#: The types GDAL's date, time and datetime fields bind to in DuckDB.
_TEMPORAL_TYPES = frozenset(
    {
        "DATE",
        "TIME",
        "TIME WITH TIME ZONE",
        "TIMESTAMP",
        "TIMESTAMP_S",
        "TIMESTAMP_MS",
        "TIMESTAMP_NS",
        "TIMESTAMP WITH TIME ZONE",
    }
)


@dataclass(frozen=True)
class DatetimeRead:
    """How to read one source's dates.

    ``open_options`` go to ``ST_Read``. ``casts`` is ``(name, type)`` for every
    column the read hands back as text and :meth:`wrap` turns back into ``type``.
    """

    open_options: list[str] | None = None
    casts: tuple[tuple[str, str], ...] = ()

    def wrap(self, table_expr: str) -> str:
        """``table_expr`` with each text date cast back to the type GDAL gave it."""
        if not self.casts:
            return table_expr
        replaced = ", ".join(
            f"CAST({quote_identifier(name)} AS {kind}) AS {quote_identifier(name)}"
            for name, kind in self.casts
        )
        return f"(SELECT * REPLACE ({replaced}) FROM {table_expr})"


def _sets_date_as_string(open_options: list[str] | None) -> bool:
    return any(option.upper().startswith("DATE_AS_STRING=") for option in open_options or ())


def plan_datetime_read(
    is_geojson: bool, open_options: list[str] | None, columns: list[tuple[str, str]] | None
) -> DatetimeRead:
    """Plan a GeoJSON read whose dates DuckDB parses, from a bind already run.

    ``columns`` is ``[(name, type), ...]`` from the caller's ``LIMIT 0`` bind of
    the plain read, so this costs no extra pass over the file. Anything that is
    not GeoJSON, has no date column, or already sets ``DATE_AS_STRING`` is read
    as it was.
    """
    if not is_geojson or _sets_date_as_string(open_options):
        return DatetimeRead(open_options)
    casts = tuple((name, kind) for name, kind in columns or () if kind in _TEMPORAL_TYPES)
    if not casts:
        return DatetimeRead(open_options)
    return DatetimeRead([*(open_options or []), DATE_AS_STRING], casts)
