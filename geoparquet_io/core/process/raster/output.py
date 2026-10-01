"""Shape a contourrs Arrow table into a gpio-owned GeoParquet table.

contourrs ``*_arrow()`` output carries its own ``geo`` schema metadata and a
``geoarrow.wkb`` extension annotation on the geometry field. gpio owns the
output metadata (ADR-0007): the foreign block and extension annotation are
stripped and replaced with a minimal carried ``geo`` block — encoding and CRS
only — so the write funnel computes ``geometry_types`` and ``bbox`` from the
data, exactly as for any other table.

Docs: docs/guide/process-raster.md
"""

from __future__ import annotations

import json

import pyarrow as pa


def finalize_raster_table(
    table: pa.Table,
    *,
    crs: dict | None,
    rename: dict[str, str] | None = None,
) -> pa.Table:
    """Strip foreign metadata, rename columns, attach the minimal geo block.

    ``crs`` is a PROJJSON dict (or ``None`` when the raster declares none —
    the ``geo`` block then omits the key rather than guessing).
    """
    rename = rename or {}
    table = table.rename_columns(
        [rename.get(name, name) for name in table.column_names]
    )
    geo_column: dict = {"encoding": "WKB"}
    if crs is not None:
        geo_column["crs"] = crs
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": geo_column},
    }
    target = pa.schema(
        [pa.field(f.name, f.type, nullable=f.nullable) for f in table.schema],
        metadata={b"geo": json.dumps(geo).encode("utf-8")},
    )
    return table.cast(target)
