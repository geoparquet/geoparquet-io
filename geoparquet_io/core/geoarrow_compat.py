"""Keep a ``srid:N`` type crs readable once ``geoarrow.pyarrow`` is loaded.

PyArrow maps a Parquet GEOMETRY typed ``srid:5070`` (or the ``srid:0`` gpio
writes for an unknown CRS, #1276) to geoarrow extension metadata
``{"crs": "5070", "crs_type": "srid"}``. geoarrow-types (0.3.0) ignores the
``crs_type`` and holds the bare number as ``StringCrs("5070")``, whose
``__geoarrow_crs_json_values__`` JSON-decodes the text into the integer
``5070``. Re-parsing that metadata calls ``crs.create(5070)``, which raises,
and PyArrow does not survive an exception out of ``__arrow_ext_deserialize__``.
``pq.read_table`` segfaults.

:func:`install` patches three places so the SRID keeps its meaning and stays text.

* ``TypeSpec.from_extension_metadata`` reads ``crs_type: "srid"`` as the
  Parquet spelling ``srid:<N>``, the same text DuckDB reports for the type.
* ``StringCrs.__geoarrow_crs_json_values__`` embeds only a decoded JSON
  *object* (PROJJSON) as an object, which is what the upstream comment means
  ("avoid escaping valid JSON into a JSON string"). A scalar stays text.
* ``crs.create`` accepts a number by wrapping its text, for a file a buggy
  writer already stored as ``{"crs": 5070}``.

Every value geoarrow-types already handles goes through unchanged.
``geoparquet_io.core`` installs it on import, so every gpio read path is
covered whichever module imports ``geoarrow.pyarrow`` first.
"""

from __future__ import annotations

import json
import sys

_INSTALLED_FLAG = "_gpio_srid_compat"


def srid_crs_as_text(metadata: dict) -> dict:
    """Rewrite ``{"crs": "5070", "crs_type": "srid"}`` as ``{"crs": "srid:5070"}``.

    Anything else is returned as given.
    """
    if metadata.get("crs_type") != "srid" or metadata.get("crs") is None:
        return metadata
    out = {k: v for k, v in metadata.items() if k != "crs_type"}
    out["crs"] = f"srid:{metadata['crs']}"
    return out


def _json_values(self) -> dict:
    try:
        parsed = json.loads(self._crs)
    except ValueError:
        return {"crs": self._crs}
    return {"crs": parsed if isinstance(parsed, dict) else self._crs}


def _patch_type_spec() -> None:
    import geoarrow.types.type_spec  # noqa: F401  -- the name is shadowed by a function

    type_spec = sys.modules["geoarrow.types.type_spec"]
    original = type_spec.TypeSpec.from_extension_metadata

    def from_extension_metadata(extension_metadata):
        try:
            metadata = json.loads(extension_metadata) if extension_metadata else None
        except (TypeError, ValueError):
            metadata = None
        if isinstance(metadata, dict) and metadata.get("crs_type") == "srid":
            extension_metadata = json.dumps(srid_crs_as_text(metadata))
        return original(extension_metadata)

    from_extension_metadata.__doc__ = original.__doc__
    type_spec.TypeSpec.from_extension_metadata = staticmethod(from_extension_metadata)


def _patch_crs(gcrs) -> None:
    original_create = gcrs.create

    def create(obj):
        if isinstance(obj, int | float) and not isinstance(obj, bool):
            return gcrs.StringCrs(str(obj))
        return original_create(obj)

    create.__doc__ = original_create.__doc__
    gcrs.StringCrs.__geoarrow_crs_json_values__ = _json_values
    gcrs.create = create


def install() -> None:
    """Patch geoarrow-types so a ``srid`` crs never crashes a PyArrow read."""
    try:
        from geoarrow.types import crs as gcrs
    except ImportError:  # pragma: no cover - geoarrow-pyarrow is a hard dependency
        return
    if getattr(gcrs, _INSTALLED_FLAG, False):
        return
    _patch_type_spec()
    _patch_crs(gcrs)
    setattr(gcrs, _INSTALLED_FLAG, True)
