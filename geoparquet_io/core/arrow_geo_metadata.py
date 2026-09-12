"""GeoParquet metadata for an Arrow table: work it out, attach it, write it.

Everything here takes a ``pyarrow.Table`` and answers one question about the
``geo`` block that describes it -- which version the table already declares,
which column is its bbox covering, which geometry types and bounds its data
actually holds, which encoding each geometry column must be re-shaped into for
the version being written -- and then builds the block and hangs it on the
table's schema metadata.

It is the layer *under* the write funnels rather than part of them: nothing here
knows about DuckDB, queries, remote outputs or the ``COPY`` path, and nothing
here calls back out into ``core/common.py``. Split out of ``common.py``
unchanged; every name is still importable from there.
"""

import json

import pyarrow.parquet as pq

from geoparquet_io.core.arrow_types import _rebuild_array_with_type
from geoparquet_io.core.bbox_structure import _bbox_column_from_covering
from geoparquet_io.core.crs_utils import (
    _format_crs_display,
    apply_output_crs,
    is_default_crs,
    parse_geo_metadata_from_schema,
)
from geoparquet_io.core.geo_metadata import (
    DEFAULT_GEOPARQUET_VERSION,
    GEOPARQUET_VERSIONS,
    carried_version,
    create_geo_metadata,
    detect_bbox_column_from_schema,
    geoarrow_wkb_codes,
    sanitize_geo_metadata,
)
from geoparquet_io.core.geoarrow_encoding import (
    is_geoarrow_extension_field,
    is_wkb_extension_field,
)
from geoparquet_io.core.logging_config import debug, success
from geoparquet_io.core.parquet_writer import ParquetWriteSettings


def _detect_version_from_table(table, verbose: bool = False) -> str | None:
    """
    Detect GeoParquet version from table's schema metadata.

    Checks the table's schema metadata for existing geo metadata and extracts
    the version. This allows preserving v2.0 or parquet-geo-only formats when
    writing a table that was read from such a source.

    Also checks for native geoarrow extension types which indicate v2.0 or
    parquet-geo-only format.

    Args:
        table: PyArrow Table to check
        verbose: Whether to print verbose output

    Returns:
        Version string (e.g., "1.1", "2.0", "parquet-geo-only") or None if not detected
    """
    import json

    # Check for native geoarrow extension types (indicates v2.0 or parquet-geo-only).
    # `is_geoarrow_extension_field` reads the field, not just the type, so a
    # metadata-declared geoarrow column is detected too -- without that, the
    # same table resolved to "parquet-geo-only" or to the 1.1 default purely
    # according to whether geoarrow.pyarrow had been imported (#792).
    has_native_geo = any(is_geoarrow_extension_field(field) for field in table.schema)

    # Check schema metadata for geo version
    metadata = table.schema.metadata
    if not metadata:
        if has_native_geo:
            # Native geo types but no metadata suggests parquet-geo-only
            if verbose:
                debug("Detected parquet-geo-only format from native geo types")
            return "parquet-geo-only"
        return None

    if b"geo" not in metadata:
        if has_native_geo:
            # Native geo types but no geo metadata = parquet-geo-only
            if verbose:
                debug("Detected parquet-geo-only format (native types, no geo metadata)")
            return "parquet-geo-only"
        return None

    try:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        if isinstance(geo_meta, dict):
            # Guarded, not truthiness-checked: a non-string version crashed
            # `.split` here on the write path too -- `write_geoparquet_table`
            # resolves auto-mode through this function (#979).
            version = carried_version(geo_meta.get("version"))
            if version:
                parts = version.split(".")
                if len(parts) >= 2:
                    major = parts[0]
                    if major == "2":
                        if verbose:
                            debug("Detected GeoParquet version 2.0 from table metadata")
                        return "2.0"
                    # Upgrade all 1.x versions to 1.1 (backwards compatible)
                    if major == "1":
                        if verbose:
                            debug("Detected GeoParquet version 1.x from table metadata")
                        return "1.1"
        return None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _table_bbox_struct_ok(table, column_name: str) -> bool:
    """True when ``column_name`` is a struct column with the bbox fields."""
    import pyarrow as pa

    try:
        field = table.schema.field(column_name)
    except KeyError:
        return False
    return pa.types.is_struct(field.type) and {"xmin", "ymin", "xmax", "ymax"}.issubset(
        {f.name for f in field.type}
    )


def _detect_bbox_column_from_table(table, verbose: bool = False) -> str | None:
    """
    Detect bbox struct column from Arrow table schema.

    Consults the GeoParquet ``covering.bbox`` metadata first (the authoritative
    pointer, which may use a non-conventional name), then falls back to columns
    with conventional names (bbox, bounds, extent) that have the required
    struct fields (xmin, ymin, xmax, ymax).

    Args:
        table: PyArrow Table to check
        verbose: Whether to print verbose output

    Returns:
        str: Name of bbox column if found, None otherwise
    """
    geo_meta = parse_geo_metadata_from_schema(table.schema.metadata)
    covering_column = _bbox_column_from_covering(geo_meta)
    if covering_column and _table_bbox_struct_ok(table, covering_column):
        if verbose:
            debug(f"Found bbox column from covering metadata: {covering_column}")
        return covering_column

    return detect_bbox_column_from_schema(table.schema, verbose)


# WKB geometry type codes to GeoParquet base names (2D types)
_GEOMETRY_TYPE_CODES = {
    0: "Unknown",
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}

# Dimensional suffixes based on WKB type code modifier
_DIMENSION_SUFFIXES = {
    0: "",  # 2D (no suffix)
    1: " Z",  # Z dimension (codes 1001-1007)
    2: " M",  # M dimension (codes 2001-2007)
    3: " ZM",  # ZM dimensions (codes 3001-3007)
}


def _get_geometry_type_name(code: int) -> str:
    """
    Convert WKB geometry type code to GeoParquet geometry type name.

    Handles 2D types (0-7) and Z/M/ZM variants (1001-1007, 2001-2007, 3001-3007).

    Args:
        code: WKB geometry type code

    Returns:
        GeoParquet geometry type name (e.g., "Point", "Point Z", "Polygon ZM")
    """
    # Extract base type (0-7) and dimensional modifier (0, 1, 2, or 3)
    base_type = code % 1000
    dimension = code // 1000

    base_name = _GEOMETRY_TYPE_CODES.get(base_type, "Unknown")
    if base_name == "Unknown":
        return "Unknown"

    suffix = _DIMENSION_SUFFIXES.get(dimension, "")
    return base_name + suffix


def _strip_geoarrow_to_plain_wkb(table, geometry_column: str, verbose: bool):
    """
    Convert geoarrow extension type back to plain binary WKB.

    Used for GeoParquet 1.x output which uses plain binary geometry
    with CRS only in metadata (not in schema).
    """
    import pyarrow as pa

    geom_col = table.column(geometry_column)

    # Check if it's a WKB extension type, in EITHER carrier shape: the resolved
    # Arrow type, or a plain binary type whose field metadata declares
    # `ARROW:extension:name`. Reading only the type left the metadata-declared
    # shape in place, so the same column got a different carrier depending on
    # whether the process had imported geoarrow.pyarrow (#792).
    #
    # WKB names only, not any geoarrow.* name: this rewrites the column to plain
    # `binary`, which is a lossless re-carrier for WKB bytes and destructive for
    # anything else -- `geoarrow.point` over `struct<x, y>` cannot be cast at all,
    # and `geoarrow.wkt` over `string` casts into a binary column still declared
    # `encoding: WKT`. A native carrier is simply not this function's business.
    if not is_wkb_extension_field(table.schema.field(geometry_column)):
        return table  # Already plain binary, or a non-WKB carrier to leave alone

    if verbose:
        debug("v1.x: stripping geoarrow extension type to plain binary WKB")

    # geoarrow.wkb's storage is `large_binary`, so building a `binary`
    # chunked_array straight from the storage chunks raises -- and the raise used
    # to be swallowed here, silently leaving the extension type in place. That is
    # invisible on the primary column (it reaches the writer as plain
    # `large_binary` via the WKB query wrapper and is narrowed downstream) and
    # was the reason a SECONDARY geometry column kept a native Parquet GEOMETRY
    # type inside a 1.x file (#706). `_to_plain_wkb_array` unwraps the extension
    # and narrows the offsets, and is the same helper the streaming strategy uses.
    from geoparquet_io.core.write_strategies.arrow_streaming import _to_plain_wkb_array

    try:
        plain_col = _to_plain_wkb_array(geom_col)
        col_index = table.schema.get_field_index(geometry_column)
        # Passing the bare name rebuilds the field from scratch, which is what
        # drops a stale `ARROW:extension:name` on the metadata-declared shape --
        # left behind, DuckDB reads it back off `register()` and the COPY writes
        # a native Parquet GEOMETRY type into a 1.x file (#706, #727).
        return table.set_column(col_index, geometry_column, plain_col)

    except (TypeError, ValueError, AttributeError, pa.ArrowInvalid) as e:
        if verbose:
            debug(f"Could not strip geoarrow type: {e}")
        return table


def _crs_as_projjson(crs):
    """Normalize a geoarrow CRS object to a PROJJSON dict, or return it as-is."""
    if crs is None:
        return None
    to_json_dict = getattr(crs, "to_json_dict", None)
    if to_json_dict is None:
        return crs
    try:
        return to_json_dict()
    except (TypeError, ValueError):
        return None


def _process_geometry_column_for_version(
    table,
    geometry_column: str,
    geoparquet_version: str | None,
    input_crs: dict | None,
    verbose: bool,
    *,
    crs_resolved: bool = False,
):
    """
    Process geometry column based on GeoParquet version.

    Handles different GeoParquet versions:
    - v1.x: Plain binary WKB (no extension type), CRS only in metadata
    - v2.0/parquet-geo-only: geoarrow extension type with CRS in schema

    When streaming data enters with geoarrow extension type:
    - v1.x output: strips geoarrow to plain binary WKB
    - v2.0 output: preserves/enhances geoarrow extension type

    Args:
        table: PyArrow Table to modify
        geometry_column: Name of the geometry column
        geoparquet_version: GeoParquet version
        input_crs: PROJJSON dict with CRS
        verbose: Whether to print verbose output
        crs_resolved: True when ``input_crs`` is the geo block's already-resolved
            answer for this column, so ``None`` means "spec default, declared by
            omission" and must not be second-guessed from the Arrow field's type

    Returns:
        pa.Table: Table with geometry column processed
    """
    import geoarrow.pyarrow as ga

    try:
        geom_col = table.column(geometry_column)

        if geoparquet_version in ("2.0", "parquet-geo-only"):
            # For v2.0/parquet-geo-only: use geoarrow extension type with CRS
            wkb_arr = ga.as_wkb(geom_col)

            # Set the CRS explicitly in BOTH directions. Applying it only when
            # non-default left whatever the reader had attached in place, so the
            # same input produced a different schema type depending on whether
            # the process had imported geoarrow.pyarrow — DuckDB hands over a
            # GEOMETRY column carrying OGC:CRS84 when it is registered and a bare
            # one when it is not (#706). A default CRS is the spec default and is
            # declared by the geo block, so the schema type carries none.
            # The field-CRS fallback is only for columns nothing has resolved:
            # there a missing CRS does not mean "no CRS" -- clearing it
            # unconditionally relabelled projected data as the CRS84 default,
            # silent corruption nothing validates. But when `crs_resolved` says
            # the geo block just resolved this column (`apply_output_crs` on the
            # write path), `input_crs=None` IS the answer -- the spec default,
            # declared by omission -- and falling back to the field's CRS would
            # resurrect exactly what the block dropped, e.g. an explicit CRS84
            # `input_crs` over a field carrying EPSG:3857: the type would say
            # 3857 beside a block claiming the default, and gpio's own
            # `v2_crs_consistency_geometry` check fails on the file it wrote.
            #
            # geoarrow hands the field CRS back as a CRS object, which
            # `is_default_crs` does not recognize; normalizing to PROJJSON is
            # what keeps the reader's incidental OGC:CRS84 classified as the
            # default and thus cleared, so the output stays independent of
            # import state (#706).
            resolved_crs = input_crs
            if not crs_resolved:
                resolved_crs = input_crs or _crs_as_projjson(getattr(wkb_arr.type, "crs", None))
            if resolved_crs and not is_default_crs(resolved_crs):
                if verbose:
                    debug(
                        "Applying CRS to geometry schema type: "
                        f"{_format_crs_display(input_crs) if input_crs else resolved_crs}"
                    )
                new_type = wkb_arr.type.with_crs(resolved_crs)
            else:
                new_type = ga.wkb()
            # Always rebuild: geoarrow's WkbType.__eq__ ignores the CRS, so a
            # "types are equal" shortcut would skip precisely the case this is
            # here to normalize.
            wkb_arr = _rebuild_array_with_type(wkb_arr, new_type)

            # Replace geometry column in table
            col_index = table.schema.get_field_index(geometry_column)
            table = table.set_column(col_index, geometry_column, wkb_arr)
        else:
            # For v1.x: ensure plain binary WKB (strip a WKB extension carrier if
            # present). CRS goes only in metadata, not in schema.
            # A non-WKB geoarrow carrier is left alone: casting it here is either
            # impossible (`geoarrow.point` over `struct<x, y>`) or silently wrong
            # (`geoarrow.wkt` over `string`), so the broad geoarrow predicate is
            # the wrong gate for a rewrite (#792).
            if is_wkb_extension_field(table.schema.field(geometry_column)):
                table = _strip_geoarrow_to_plain_wkb(table, geometry_column, verbose)
            elif verbose:
                debug("v1.x: geometry is already plain binary WKB (CRS in metadata only)")

    except (TypeError, ValueError, AttributeError) as e:
        if verbose:
            debug(f"Could not process geometry column: {e}")
        # Continue without conversion - geometry is already WKB

    return table


def _compute_geometry_types(table, geometry_column: str, verbose: bool) -> list[str]:
    """
    Compute geometry types from a geometry column using geoarrow.

    Args:
        table: PyArrow Table containing the geometry column
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output

    Returns:
        list: List of GeoParquet geometry type names (e.g., ["Point", "Polygon"])
    """
    import geoarrow.pyarrow as ga
    import pyarrow.compute as pc

    # Skip for empty tables (geoarrow crashes on empty arrays)
    if table.num_rows == 0:
        return []

    try:
        geom_col = table.column(geometry_column)

        # Filter out NULL values to avoid geoarrow errors on invalid geometries
        # This handles cases where BigQuery returns NULL or empty geometries
        non_null_mask = pc.is_valid(geom_col)
        if pc.any(non_null_mask).as_py():
            geom_col = pc.filter(geom_col, non_null_mask)
        else:
            # All values are NULL
            return []

        # Skip if no valid geometries remain after filtering
        if len(geom_col) == 0:
            return []

        wkb_arr = ga.as_wkb(geom_col)
        types_struct = ga.unique_geometry_types(wkb_arr)

        # Extract geometry type codes from struct array. geoarrow reports the
        # base type and the dimensions separately, so the two are recombined
        # into a WKB code -- reading `geometry_type` alone dropped the " Z" /
        # " M" / " ZM" the spec makes part of the type name (#892).
        type_codes = geoarrow_wkb_codes(types_struct)

        # Map codes to GeoParquet standard names (avoid duplicates)
        type_names = []
        for code in type_codes:
            name = _get_geometry_type_name(code)
            if name not in type_names:
                type_names.append(name)

        if verbose:
            debug(f"Computed geometry_types from data: {type_names}")
        return type_names

    except Exception as e:
        # Catch all exceptions including geoarrow C++ errors
        # (e.g., "Expected valid geometry type code but found 0")
        if verbose:
            debug(f"Could not compute geometry_types: {e}")
        # Return empty list as fallback (allowed by spec - means any type)
        return []


def _compute_bbox_from_data(table, geometry_column: str, verbose: bool) -> list[float] | None:
    """
    Compute bounding box from geometry column data.

    Args:
        table: PyArrow Table containing the geometry column
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output

    Returns:
        list: [xmin, ymin, xmax, ymax] or None if computation fails
    """
    import geoarrow.pyarrow as ga
    import pyarrow.compute as pc

    # Skip for empty tables
    if table.num_rows == 0:
        return None

    try:
        geom_col = table.column(geometry_column)

        # Filter out NULL values to avoid geoarrow errors on invalid geometries
        non_null_mask = pc.is_valid(geom_col)
        if pc.any(non_null_mask).as_py():
            geom_col = pc.filter(geom_col, non_null_mask)
        else:
            # All values are NULL
            return None

        # Skip if no valid geometries remain after filtering
        if len(geom_col) == 0:
            return None

        wkb_arr = ga.as_wkb(geom_col)
        box_arr = ga.box(wkb_arr)

        # Combine chunks and get storage (underlying struct array)
        combined = box_arr.combine_chunks()
        storage = combined.storage

        # Extract struct fields and compute min/max
        xmin = pc.min(pc.struct_field(storage, "xmin")).as_py()
        ymin = pc.min(pc.struct_field(storage, "ymin")).as_py()
        xmax = pc.max(pc.struct_field(storage, "xmax")).as_py()
        ymax = pc.max(pc.struct_field(storage, "ymax")).as_py()

        if all(v is not None for v in [xmin, ymin, xmax, ymax]):
            if verbose:
                debug(f"Computed bbox from data: [{xmin:.6f}, {ymin:.6f}, {xmax:.6f}, {ymax:.6f}]")
            return [xmin, ymin, xmax, ymax]

    except Exception as e:
        # Catch all exceptions including geoarrow C++ errors
        if verbose:
            debug(f"Could not compute bbox: {e}")

    return None


def _assemble_and_apply_geo_metadata(
    table,
    geo_meta: dict,
    metadata_version: str,
    verbose: bool,
):
    """
    Apply the finished geo metadata to the table's schema.

    The geometry column's ``crs`` is already resolved by the caller, which has to
    do it before this point: the native Parquet GEOMETRY logical types are built
    from what the block declares, so the block is finished first (#848).

    Args:
        table: PyArrow Table to modify
        geo_meta: Geo metadata dict to apply
        metadata_version: GeoParquet metadata version string
        verbose: Whether to print verbose output

    Returns:
        pa.Table: Table with geo metadata applied
    """
    # Apply metadata to table
    existing_metadata = dict(table.schema.metadata) if table.schema.metadata else {}
    new_metadata = {}

    # Copy non-geo metadata from existing
    for k, v in existing_metadata.items():
        key_str = k.decode("utf-8") if isinstance(k, bytes) else k
        if not key_str.startswith("geo"):
            new_metadata[k] = v

    # Add geo metadata
    new_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    table = table.replace_schema_metadata(new_metadata)

    if verbose:
        debug(f"Applied geo metadata with version {metadata_version}")

    return table


# Schema-metadata keys that describe the *input's* schema rather than the
# output's, so they must never ride along into a write.
#
# `geo` would declare the input's GeoParquet version over output the writer has
# just given a different shape; `ARROW:schema` and `pandas` are serialized
# descriptors that pyarrow writes through verbatim rather than regenerating,
# leaving the output naming the input's columns and CRS.
#
# One definition, used by both places that need it -- the parquet-geo-only path
# below (which sees `bytes` keys, straight off a pyarrow schema) and
# write_parquet_with_metadata's preserved-keys loop (which sees them decoded).
# They were separate literals; a key added to one and not the other is a silent
# metadata leak, so the bytes form is derived rather than written out again.
_CARRIED_SCHEMA_METADATA_KEYS = frozenset({"geo", "ARROW:schema", "pandas"})
_CARRIED_SCHEMA_METADATA_KEYS_BYTES = frozenset(
    key.encode("utf-8") for key in _CARRIED_SCHEMA_METADATA_KEYS
)


def _strip_geo_metadata_key(table, verbose: bool = False):
    """Drop the input's ``geo``/``ARROW:schema``/``pandas`` keys, keeping other KV.

    Used for parquet-geo-only output, which carries its geometry typing in the
    Parquet schema and must not also declare a GeoParquet version.

    The exclusion set matches ``write_parquet_with_metadata``'s (see the
    preserved-keys loop in that function): besides ``geo``, a carried
    ``ARROW:schema``/``pandas`` describes the *input's* schema and is written
    through verbatim, leaving the output with a serialized descriptor naming
    columns and a CRS the file does not have.

    Args:
        table: PyArrow Table whose schema metadata to clean
        verbose: Whether to print verbose output

    Returns:
        pa.Table: Table without carried geo/schema-descriptor metadata keys
    """
    existing_metadata = table.schema.metadata
    if not existing_metadata:
        return table

    dropped = [k for k in existing_metadata if k in _CARRIED_SCHEMA_METADATA_KEYS_BYTES]
    if not dropped:
        return table

    new_metadata = {k: v for k, v in existing_metadata.items() if k not in dropped}
    if verbose:
        names = ", ".join(sorted(k.decode("utf-8") for k in dropped))
        debug(f"parquet-geo-only: dropped the input's {names} metadata key(s)")
    return table.replace_schema_metadata(new_metadata)


def _canonicalize_wkb_columns(table, geometry_columns, verbose: bool = False, native_columns=None):
    """Give every 1.x geometry column the canonical plain-``binary`` carrier.

    ``_process_geometry_column_for_version`` handles the case where PyArrow
    resolved the geoarrow extension type. It cannot see the other shape DuckDB
    emits: plain ``large_binary`` carrying a raw ``ARROW:extension:name`` field
    metadata key, which appears when nothing in the process registered
    ``geoarrow.pyarrow``. Left in place, that key rides into the output's
    ``ARROW:schema``, so the same input produced different bytes depending on
    what the process had imported (#688's shape, on a secondary column — #706).

    Reuses the streaming strategy's ``canonicalize_wkb_fields`` so both paths
    agree on what "canonical" means -- including its ``_WKB_EXTENSION_NAMES``
    guard, which is what keeps a native nested carrier (``geoarrow.point`` over
    ``struct<x, y>``) out of a binary cast that PyArrow cannot perform.

    Args:
        table: PyArrow Table to rewrite.
        geometry_columns: Names of the declared geometry columns.
        verbose: Whether to log a skipped column.
        native_columns: Columns to leave exactly as they are, because the target
            version writes them as a native geo type or because the caller
            converts them itself. Passed straight through to
            ``canonicalize_wkb_fields``.
    """
    import pyarrow as pa

    from geoparquet_io.core.write_strategies.arrow_streaming import (
        _to_plain_wkb_array,
        canonicalize_wkb_fields,
    )

    target = canonicalize_wkb_fields(
        table.schema, set(geometry_columns), native_columns=set(native_columns or ())
    )
    # check_metadata=True matters: the leak this exists to stop is a stale
    # ARROW:extension:name on a field whose *type* is already plain binary, and
    # the default comparison ignores metadata entirely.
    if target.equals(table.schema, check_metadata=True):
        return table

    for index, field in enumerate(target):
        current = table.schema.field(index)
        if field.equals(current, check_metadata=True):
            continue
        try:
            table = table.set_column(index, field, _to_plain_wkb_array(table.column(index)))
        except (TypeError, ValueError, pa.ArrowInvalid) as e:
            if verbose:
                debug(f"Could not canonicalize WKB carrier for '{field.name}': {e}")
    return table


def _parse_geo_metadata_quietly(original_metadata) -> dict:
    """Best-effort parse of a carried ``geo`` key; ``{}`` when absent or unreadable."""
    if not original_metadata:
        return {}
    raw = original_metadata.get(b"geo") or original_metadata.get("geo")
    if not raw:
        return {}
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        parsed = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return {}
    # Callers read ``columns`` as a mapping of column name to a dict of that
    # column's metadata. A carried block is arbitrary JSON from the input file,
    # so anything else is dropped rather than allowed to raise a TypeError three
    # frames away, in the middle of a write. The check lives in one place --
    # ``sanitize_geo_metadata`` -- that every write-path reader goes through
    # (#771); this function is one of them.
    return sanitize_geo_metadata(parsed) or {}


def _apply_geoparquet_metadata(
    table,
    geometry_column: str,
    geoparquet_version: str | None,
    original_metadata: dict | None = None,
    input_crs: dict | None = None,
    custom_metadata: dict | None = None,
    verbose: bool = False,
    edges: str | None = None,
    geometry_info: dict | None = None,
    geo_bbox: list[float] | None = None,
):
    """
    Apply GeoParquet metadata to an Arrow Table based on version.

    Handles different GeoParquet versions:
    - v1.x: Apply geo metadata to schema, CRS via geoarrow type
    - v2.0: Apply CRS to schema type AND geo metadata
    - parquet-geo-only: Apply CRS to schema type only, no geo metadata

    When geoparquet_version is None, the function will detect the version from
    the table's existing schema metadata, preserving v2.0 or parquet-geo-only
    formats when present.

    Args:
        table: PyArrow Table to modify
        geometry_column: Name of the geometry column
        geoparquet_version: GeoParquet version (1.0, 1.1, 2.0, parquet-geo-only),
            or None to auto-detect from existing table metadata
        original_metadata: Original metadata to preserve
        input_crs: PROJJSON dict with CRS
        custom_metadata: Custom metadata (e.g., H3 covering info)
        verbose: Whether to print verbose output
        edges: Edge interpretation, "spherical" or "planar" (default None = planar).
               Use "spherical" for data from BigQuery or other S2-based sources.
        geometry_info: Dict containing multi-geometry column info with keys:
            - "primary": primary geometry column name
            - "secondary": list of secondary geometry column names
            - "metadata": dict mapping column names to their metadata
        geo_bbox: Pre-computed [xmin, ymin, xmax, ymax] for the primary column,
            used instead of the plain min/max taken off the data. A caller that
            knows the data crosses the antimeridian passes the RFC 7946 5.2
            wrap form (xmin > xmax) here, which cannot be recovered from
            per-geometry extents alone.

    Returns:
        pa.Table: Table with GeoParquet metadata applied
    """
    # Auto-detect version from table schema metadata if not specified
    effective_version = geoparquet_version
    if effective_version is None:
        effective_version = _detect_version_from_table(table, verbose)

    version_config = GEOPARQUET_VERSIONS.get(
        effective_version, GEOPARQUET_VERSIONS[DEFAULT_GEOPARQUET_VERSION]
    )
    metadata_version = version_config["metadata_version"]
    should_add_geo_metadata = effective_version != "parquet-geo-only"

    if verbose:
        debug(f"Applying GeoParquet metadata for version: {effective_version or 'default (1.1)'}")

    # Check if geometry column exists in table
    if geometry_column not in table.column_names:
        # An explicit parquet-geo-only request still has to drop a carried geo
        # key, even with nothing to convert: the key names a primary_column the
        # file does not contain (issue #701). Restricted to the explicit request
        # so auto mode (geoparquet_version=None) keeps whatever it resolves to,
        # which is issue #600's territory.
        #
        # The three strategies behind Table.write() had the same-shaped guard
        # and leaked the key (#773); they now route their file-level metadata
        # through parquet_writer.apply_output_kv_metadata, which applies this
        # same rule before any such guard can be reached. This branch stays
        # because write_geoparquet_table does not go through a strategy.
        if geoparquet_version == "parquet-geo-only":
            if verbose:
                debug(
                    f"Geometry column '{geometry_column}' not found in table; "
                    "dropping carried geo metadata for parquet-geo-only"
                )
            return _strip_geo_metadata_key(table, verbose)
        if verbose:
            debug(f"Geometry column '{geometry_column}' not found in table, skipping metadata")
        return table

    from geoparquet_io.core.write_strategies.base import (
        native_geometry_crs,
        resolve_geometry_columns,
    )

    # The table entry points (`write_geoparquet_table`, the strategies'
    # `write_from_table`) get no `geometry_info`, so fall back to naming the
    # secondaries from the carried geo metadata -- otherwise they would silently
    # keep the single-column behaviour the loop below exists to replace.
    # `original_metadata` is deliberately None on the table entry points (passing
    # it there would smuggle the input's stale geo block into the output), so the
    # secondary names come from the table's own carried key instead.
    carried_geo = _parse_geo_metadata_quietly(original_metadata) or _parse_geo_metadata_quietly(
        table.schema.metadata
    )

    # Step 1: Build the geo metadata, BEFORE the geometry columns are retyped.
    #
    # The 2.0 / parquet-geo-only native Parquet GEOMETRY types take each column's
    # CRS from the entry that declares it, so that entry has to exist first --
    # otherwise the primary is typed from `input_crs`, which only a reprojection
    # supplies, and an ordinary write leaves the type bare beside a block still
    # declaring the source's EPSG:3857 (#848).
    #
    # Built for parquet-geo-only too, which drops it again below: there the
    # logical type is the column's only geometry identity, so losing the CRS is
    # not a disagreement but a silent relabelling of projected coordinates.
    geo_meta = _build_geo_block(
        table,
        geometry_column,
        original_metadata,
        input_crs,
        custom_metadata,
        metadata_version,
        edges,
        geometry_info,
        verbose,
    )

    # A column the block does not name falls back to what the input declared for
    # it: on the table entry points the block is built from `original_metadata`,
    # which is None there, while the carrier decision still has to see the
    # secondaries the table's own carried key names.
    declared_crs = dict(carried_geo.get("columns") or {})
    declared_crs.update((geometry_info or {}).get("metadata", {}))
    declared_crs.update(geo_meta.get("columns") or {})
    native_crs = native_geometry_crs(
        effective_version, {"columns": declared_crs}, geometry_column, geometry_info
    )

    # Which columns' CRS the block-building actually RESOLVED, as opposed to
    # merely giving an entry. Only two things count as a source: an explicit
    # `crs` value in a column's entry, and -- for the primary alone, the one
    # column `apply_output_crs` runs on -- a caller-supplied `input_crs`
    # (a requested spec default is dropped from the block, so that entry is
    # crs-less yet still resolved). A crs-less entry produced without
    # consulting either is "unknown", NOT "default": at 2.0 the schema type is
    # authoritative and a block may legitimately omit the key, so the field-CRS
    # fallback in `_process_geometry_column_for_version` must stay live there.
    crs_resolved_columns = {name for name, meta in declared_crs.items() if (meta or {}).get("crs")}
    if input_crs is not None:
        crs_resolved_columns.add(geometry_column)

    # Step 2: Handle geometry columns based on version.
    #
    # EVERY geometry column, not just the primary: validation applies the same
    # per-version requirements to each column in geo["columns"], and a secondary
    # left with whatever carrier the reader produced is both wrong for the target
    # version and dependent on whether anything imported geoarrow.pyarrow (#706).
    # Each column carries its OWN crs -- giving a secondary the primary's fails
    # v2_crs_consistency.
    for column in sorted(resolve_geometry_columns(geometry_column, geometry_info, carried_geo)):
        if column not in table.column_names:
            continue
        table = _process_geometry_column_for_version(
            table,
            column,
            effective_version,
            native_crs.get(column),
            verbose,
            # For a column whose CRS the block-building genuinely resolved, a
            # None from `native_crs` is "default by omission" and the field-CRS
            # fallback must not resurrect what the block dropped.
            crs_resolved=column in crs_resolved_columns,
        )

    if effective_version in ("1.0", "1.1"):
        table = _canonicalize_wkb_columns(
            table, resolve_geometry_columns(geometry_column, geometry_info, carried_geo), verbose
        )

    # Step 3: Apply the geo metadata (unless parquet-geo-only)
    if not should_add_geo_metadata:
        # parquet-geo-only means no GeoParquet metadata at all. Step 2 has just
        # given the column a native Parquet GEOMETRY logical type, so a 'geo'
        # key carried in from the input would declare a version whose spec
        # forbids that type (issue #687). Drop it, keeping unrelated KV
        # metadata, to match the other write paths.
        return _strip_geo_metadata_key(table, verbose)

    # Stats are read off the written data, so they are filled in after step 2.
    col_meta = geo_meta["columns"][geometry_column]
    if "geometry_types" not in col_meta:
        col_meta["geometry_types"] = _compute_geometry_types(table, geometry_column, verbose)

    computed_bbox = geo_bbox or _compute_bbox_from_data(table, geometry_column, verbose)
    if computed_bbox:
        col_meta["bbox"] = computed_bbox

    return _assemble_and_apply_geo_metadata(table, geo_meta, metadata_version, verbose)


def _build_geo_block(
    table,
    geometry_column: str,
    original_metadata: dict | None,
    input_crs: dict | None,
    custom_metadata: dict | None,
    metadata_version: str,
    edges: str | None,
    geometry_info: dict | None,
    verbose: bool,
) -> dict:
    """Build the output's ``geo`` block, with the primary column's CRS resolved.

    Everything here reads the table's *schema* only -- the bbox-covering probe --
    so it can run before the geometry columns are retyped, which is what lets the
    native Parquet GEOMETRY types be built from the CRS this declares (#848).
    The per-column stats that need the data (``geometry_types``, ``bbox``) are
    filled in by the caller afterwards.
    """
    bbox_column = _detect_bbox_column_from_table(table, verbose)
    geo_meta = create_geo_metadata(
        original_metadata,
        geometry_column,
        {"has_bbox_column": bbox_column is not None, "bbox_column_name": bbox_column},
        custom_metadata,
        verbose,
        version=metadata_version,
        edges=edges,
    )

    # Set/clear the geometry column's crs per the GeoParquet null-vs-default rule
    # (shared helper is the single source of truth across all write paths).
    columns = geo_meta.setdefault("columns", {})
    apply_output_crs(columns.setdefault(geometry_column, {}), input_crs)
    if verbose and input_crs and not is_default_crs(input_crs):
        debug(f"Added CRS to geo metadata: {_format_crs_display(input_crs)}")

    # Secondary entries are merged in here rather than after the stats pass, so
    # each one's own `crs` is available to the native-type decision.
    from geoparquet_io.core.write_strategies.base import merge_secondary_geometry_metadata

    merge_secondary_geometry_metadata(geo_meta, geometry_info)
    return geo_meta


def _estimate_row_size(table) -> int:
    """
    Estimate bytes per row from PyArrow table memory usage.

    Uses table.get_total_buffer_size() if available (PyArrow >= 0.17),
    falls back to table.nbytes, and uses a default of 100 bytes if
    neither is available or returns 0.

    Args:
        table: PyArrow Table

    Returns:
        int: Estimated bytes per row (minimum 1)
    """
    default_row_size = 100
    num_rows = max(1, table.num_rows)

    # Try get_total_buffer_size() first (more accurate, includes all buffers)
    if hasattr(table, "get_total_buffer_size"):
        try:
            total_bytes = table.get_total_buffer_size()
            if total_bytes > 0:
                return max(1, total_bytes // num_rows)
        except Exception:
            pass

    # Fall back to nbytes property
    if hasattr(table, "nbytes"):
        try:
            total_bytes = table.nbytes
            if total_bytes > 0:
                return max(1, total_bytes // num_rows)
        except Exception:
            pass

    return default_row_size


def _write_table_with_settings(
    table,
    output_path: str,
    compression: str,
    compression_level: int | None,
    row_group_rows: int | None,
    row_group_size_mb: int | None,
    geoparquet_version: str | None,
    geometry_column: str,
    verbose: bool = False,
) -> None:
    """
    Write Arrow table to Parquet with proper settings.

    Uses pq.write_table directly since we've already applied all GeoParquet
    metadata to the table. This preserves the metadata we set (including version,
    geometry_types, CRS, etc.) without geoarrow overwriting it.

    Args:
        table: PyArrow Table to write
        output_path: Output file path
        compression: Compression type (ZSTD, GZIP, etc.)
        compression_level: Compression level
        row_group_rows: Exact number of rows per row group
        row_group_size_mb: Target row group size in MB
        geoparquet_version: GeoParquet version
        geometry_column: Name of the geometry column
        verbose: Whether to print verbose output
    """
    # Calculate row group size
    rows_per_group = row_group_rows
    if not rows_per_group and row_group_size_mb and table.num_rows > 0:
        # Estimate bytes per row from actual table memory usage
        estimated_row_size = _estimate_row_size(table)
        target_bytes = row_group_size_mb * 1024 * 1024
        rows_per_group = max(1, int(target_bytes // estimated_row_size))
        rows_per_group = min(rows_per_group, table.num_rows)

    # Use central configuration for write settings
    settings = ParquetWriteSettings(
        compression=compression,
        compression_level=compression_level,
        row_group_rows=row_group_rows,
        row_group_size_mb=row_group_size_mb,
    )
    write_kwargs = settings.get_pyarrow_kwargs(calculated_row_group_size=rows_per_group)

    if verbose:
        compression_desc = (
            f"{compression}:{compression_level}" if compression_level else compression
        )
        debug(f"Writing with {compression_desc} compression")
        if rows_per_group:
            debug(f"Row group size: {rows_per_group:,} rows")

    # Use pq.write_table for all versions - we've already applied all metadata
    # Using geoarrow's write_geoparquet_table would overwrite our carefully constructed metadata
    pq.write_table(table, output_path, **write_kwargs)

    if verbose:
        success(f"Wrote {table.num_rows:,} rows to {output_path}")


def _normalize_arrow_large_types(table):
    """
    Convert large Arrow types to standard types for Parquet compatibility.

    DuckDB with arrow_large_buffer_size=true exports strings as large_string
    (LargeUtf8) and binaries as large_binary (LargeBinary). While this allows
    handling >2GB buffers in memory, it causes compatibility issues when:
    - Reading Hive-partitioned datasets (partition columns inferred as string)
    - Merging schemas from different sources

    This function casts large types back to standard types before writing to
    Parquet. The Parquet format itself handles large values fine - the 2GB
    limit is only for Arrow's in-memory representation.

    Args:
        table: PyArrow table potentially containing large types

    Returns:
        Table with large_string → string and large_binary → binary
    """
    import pyarrow as pa

    new_fields = []
    needs_cast = False

    for field in table.schema:
        if pa.types.is_large_string(field.type):
            new_fields.append(pa.field(field.name, pa.string(), field.nullable, field.metadata))
            needs_cast = True
        elif pa.types.is_large_binary(field.type):
            new_fields.append(pa.field(field.name, pa.binary(), field.nullable, field.metadata))
            needs_cast = True
        else:
            new_fields.append(field)

    if not needs_cast:
        return table

    new_schema = pa.schema(new_fields, metadata=table.schema.metadata)
    return table.cast(new_schema)
