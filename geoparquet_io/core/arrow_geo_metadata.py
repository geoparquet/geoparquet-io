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
unchanged; every name a caller imports is still importable from there (the
module-private helpers -- ``_build_geo_block``, ``_crs_as_projjson`` and the
like -- are not re-exported, and nothing outside this module reached them).
"""

from geoparquet_io.core.arrow_types import _rebuild_array_with_type
from geoparquet_io.core.bbox_structure import _bbox_column_from_covering
from geoparquet_io.core.crs_utils import (
    PARQUET_UNKNOWN_CRS,
    _format_crs_display,
    crs_of,
    field_type_crs_is_unknown,
    is_default_crs,
    is_empty_crs_value,
    table_crs_is_unknown,
)
from geoparquet_io.core.duckdb_utils import geoarrow_extension, planar_view
from geoparquet_io.core.edges import (
    declares_edges,
    geoarrow_edge_type,
    geoarrow_type_edges,
    is_nonplanar,
    set_edges,
)
from geoparquet_io.core.geo_metadata import (
    DETECT_BBOX_COLUMN,
    GEOPARQUET_VERSIONS,
    GeoBlockOptions,
    SchemaSource,
    _bbox_claimed_by_another_column,
    _self_evident_bbox_column,
    compose_geo_block,
    geometry_stats_of,
    geometry_types_of,
    is_bbox_struct_field,
    sanitized_carried_geo,
    with_geo_key,
)
from geoparquet_io.core.geoarrow_encoding import (
    is_geoarrow_extension_field,
    is_wkb_extension_field,
)
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.parquet_writer import (
    _CARRIED_SCHEMA_METADATA_KEYS_BYTES,
    output_schema_metadata,
    resolve_output_geoparquet_version,
)
from geoparquet_io.core.write_strategies.base import (
    native_geometry_crs,
    resolve_geometry_columns,
)


def _table_bbox_struct_ok(table, column_name: str) -> bool:
    """True when ``column_name`` is a struct column with the bbox fields."""
    try:
        field = table.schema.field(column_name)
    except KeyError:
        return False
    return is_bbox_struct_field(field)


def _detect_bbox_column_from_table(table, verbose: bool = False) -> str | None:
    """The PRIMARY geometry's bbox struct column in an Arrow table, or None.

    The table-side twin of
    :func:`~geoparquet_io.core.bbox_structure.check_bbox_structure`, and it
    applies the same two rules: the PRIMARY column's own ``covering.bbox`` is
    the authoritative pointer (its name need not follow any convention), and
    with no such provenance only the one self-evident conventional name
    (:data:`~geoparquet_io.core.geo_metadata.SELF_EVIDENT_BBOX_COLUMN`) may be
    read as the primary's envelope -- not even that when a non-primary column's
    own ``covering`` already claims it. The broader read-side names
    (``bounds``, ``extent``, any ``*_bbox`` suffix) used to match here, so
    ``process aggregate --bucket-point bbox`` keyed a multi-geometry table's
    Point cells from the SECONDARY ``boundary`` column's extents in memory
    while the identical file was refused on the file path (#1171/#953).

    Deliberately NOT routed through
    :func:`~geoparquet_io.core.geo_metadata.bbox_column_to_declare`: this is a
    read-side probe, and reading ``bbox.xmin`` by name works in any field
    order, so the struct-order gate that decides what may be *declared* does
    not apply.

    Args:
        table: PyArrow Table to check
        verbose: Whether to print verbose output

    Returns:
        str: Name of the primary's bbox column if found, None otherwise
    """
    geo_meta = sanitized_carried_geo(table.schema.metadata)
    covering_column = _bbox_column_from_covering(geo_meta)
    if covering_column and _table_bbox_struct_ok(table, covering_column):
        if verbose:
            debug(f"Found bbox column from covering metadata: {covering_column}")
        return covering_column

    name = _self_evident_bbox_column(table.schema, verbose)
    if name is None:
        return None
    if _bbox_claimed_by_another_column(geo_meta, name):
        if verbose:
            debug(f"Not reading '{name}' as the primary's bbox: another column's covering names it")
        return None
    return name


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


def _secondary_crs_from_field(field) -> dict:
    """The ``crs`` key a secondary's GeoArrow type gives its ``geo`` entry.

    ``{"crs": None}`` for an unknown CRS, the ``srid:0`` type or an empty
    value, which PROJ cannot parse and the default must not swallow (#1277
    review). ``{}`` for the default, which the spec states by omitting the key
    (``apply_output_crs`` is where that rule lives for the primary). Otherwise
    the PROJJSON the type names.
    """
    if field_type_crs_is_unknown(field):
        return {"crs": None}
    crs = _crs_as_projjson(getattr(field.type, "crs", None))
    if is_empty_crs_value(crs):
        return {"crs": None}
    if isinstance(crs, dict) and not is_default_crs(crs):
        return {"crs": crs}
    return {}


def nonplanar_edges_from_schema(schema) -> dict[str, str]:
    """Map each GeoArrow field with non-planar edges to its edges value.

    A table read from a native GEOGRAPHY file says its edges in the field type
    (or, when ``geoarrow.pyarrow`` is not imported, in the field's extension
    metadata), not in a ``geo`` block. The write paths that rebuild the block
    need this to carry the edges over (#1269, #1253).

    The type carrier only. Only a GeoParquet edge algorithm
    (``edges.is_nonplanar``) counts (#1288). What a table declares once
    its ``geo`` block is weighed too is ``write_funnels.table_nonplanar_edges``.
    """
    edges_by_col: dict[str, str] = {}
    for field in schema:
        if not is_geoarrow_extension_field(field):
            continue
        edges = geoarrow_type_edges(field.type, geoarrow_extension(field)[1])
        if is_nonplanar(edges):
            edges_by_col[field.name] = edges
    return edges_by_col


def with_planar_edge_types(table, columns=None):
    """The table with each named GeoArrow column's edge type set to planar.

    DuckDB refuses to register a GeoArrow column with non-planar edges ("Can't
    import non-planar edges"), so a write that goes through DuckDB clears the
    edge type first and restores it on the written file (#1253). The CRS and the
    bytes are kept as they are. ``columns=None`` clears every GeoArrow column
    whose type says anything but planar, a value gpio does not know included,
    because DuckDB refuses those too. Which edges come back is the restore
    side's question (``write_funnels.table_nonplanar_edges``). The relabel is
    ``duckdb_utils.planar_view``, the same one ``register_arrow`` does.
    """
    return planar_view(table, columns=columns)[0]


# What a secondary geometry column's carried entry says about the column ITSELF,
# as opposed to about the rows a particular write happens to put in it. Only these
# travel into a derived `geometry_info`; `bbox` and `covering` are the derived
# facts a transform can invalidate (#934).
_DESCRIPTIVE_SECONDARY_KEYS = frozenset(
    {"encoding", "geometry_types", "crs", "edges", "orientation"}
)


def table_geometry_info(
    table, primary_column: str | None, carried_geo: dict | None = None
) -> dict | None:
    """``geometry_info`` for an in-memory table whose caller supplied none.

    The table entry points -- ``write_geoparquet_table``, each strategy's
    ``write_from_table``, and the Python API's ``Table.write`` behind them -- get
    no ``geometry_info``, and the only secondaries they learned about were the ones
    the table's own carried ``geo`` key happened to name. A table read from a
    native-geo-only file (``gpio.read(f).write(out)``) carries no such key, so its
    secondary geometry column was left out of ``geo.columns`` -- which 2.0 requires
    for every geometry column -- and written as a native Parquet GEOMETRY logical
    type inside a 1.1 file (#1175).

    The table's *schema* is the witness those entry points do have: a GeoArrow
    extension field is a geometry column whether or not a ``geo`` key says so, and
    it carries its OWN CRS, never the primary's (#993/#1000). Each secondary gets
    the spec's "not known" ``geometry_types: []`` -- every 1.x entry needs the key
    present, and the write paths that can recompute a real list do.

    This is the sibling of
    :func:`~geoparquet_io.core.derive_geo_from_file.derive_secondary_geometry_info`:
    same answer, read off an in-memory table rather than a written file, which is
    the line between this module and that one.

    ``primary_column=None`` includes every geometry field, for an input boundary
    that must carry its complete description before DuckDB drops field types.
    Returns None when the table has no secondary geometry column.
    """
    declared = (carried_geo or {}).get("columns") or {}
    metadata: dict[str, dict] = {}
    for field in table.schema:
        if field.name == primary_column:
            continue
        # A column the carried key declares is a geometry column even as plain
        # WKB bytes with no extension name -- dropping it here would un-describe
        # a secondary the input itself described.
        if not is_geoarrow_extension_field(field) and field.name not in declared:
            continue
        declared_meta = declared.get(field.name) or {}
        # The descriptive keys the input stated for this column, which the entry
        # points do not otherwise carry over. Deliberately NOT `bbox` or
        # `covering`: those are derived facts a transform can invalidate, and the
        # write paths that can recompute them do.
        col_meta = {
            key: value for key, value in declared_meta.items() if key in _DESCRIPTIVE_SECONDARY_KEYS
        }
        # Every 1.x entry needs the key present -- DuckDB refuses to open a file
        # whose described column has no geometry_types -- so an undescribed
        # column gets the spec's "not known" sentinel, which the paths that can
        # measure the real list treat as a gap (#952).
        col_meta.setdefault("geometry_types", [])
        if "crs" not in col_meta:
            col_meta.update(_secondary_crs_from_field(field))
        # The schema carries the edge interpretation as well as the CRS: left
        # behind, a spherical secondary came out planar with no warning.
        type_edges = geoarrow_type_edges(field.type, geoarrow_extension(field)[1])
        if not declares_edges(col_meta) and is_nonplanar(type_edges):
            set_edges(col_meta, type_edges)
        metadata[field.name] = col_meta

    if not metadata:
        return None
    return {"primary": primary_column, "secondary": list(metadata), "metadata": metadata}


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
            # The CRS is normalized, never the edges: a spherical column rebuilt
            # as a bare `ga.wkb()` came out a planar GEOMETRY (#1175).
            type_edges = geoarrow_type_edges(wkb_arr.type)
            if is_nonplanar(type_edges):
                new_type = new_type.with_edge_type(geoarrow_edge_type(type_edges))
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
    # Keep the table's other keys under the facade's rule: the input's `pandas`,
    # `ARROW:schema` and `geo:overviews` go, while a sidecar whose name only
    # starts with "geo" stays. The `geo` key itself is replaced just below.
    new_metadata = output_schema_metadata(table.schema.metadata, metadata_version)
    table = table.replace_schema_metadata(with_geo_key(new_metadata, geo_meta))

    if verbose:
        debug(f"Applied geo metadata with version {metadata_version}")

    return table


# The carried-key set (`_CARRIED_SCHEMA_METADATA_KEYS` and its bytes form) is
# the write facade's now (`parquet_writer`), which applies it to every
# Arrow-side write. Import it from there.


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
    geo_types: list[str] | None = None,
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
        edges: Edge interpretation, one of ``edges.EDGES`` (default None = planar).
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
        geo_types: Pre-computed ``geometry_types`` for the primary column (a
            caller that measured rows this table does not hold, such as the
            union over an overview's levels), declared by the composer.

    Returns:
        pa.Table: Table with GeoParquet metadata applied
    """
    # Auto mode: the facade decides from the table's own schema.
    effective_version = resolve_output_geoparquet_version(
        geoparquet_version, table=table, verbose=verbose
    )

    version_config = GEOPARQUET_VERSIONS[effective_version]
    metadata_version = version_config["metadata_version"]
    should_add_geo_metadata = effective_version != "parquet-geo-only"

    if verbose:
        debug(f"Applying GeoParquet metadata for version: {effective_version}")

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

    # The table entry points (`write_geoparquet_table`, the strategies'
    # `write_from_table`) get no `geometry_info`, so fall back to naming the
    # secondaries from the carried geo metadata -- otherwise they would silently
    # keep the single-column behaviour the loop below exists to replace.
    # `original_metadata` is deliberately None on the table entry points (passing
    # it there would smuggle the input's stale geo block into the output), so the
    # secondary names come from the table's own carried key instead.
    carried_geo = sanitized_carried_geo(original_metadata) or sanitized_carried_geo(
        table.schema.metadata
    )

    # A table with no carried key at all still names its geometry columns, in its
    # schema: a GeoArrow extension field is one whether or not a `geo` key says so.
    # Without this, `gpio.read(native_geo_only).write(out)` left its secondary out
    # of `geo.columns` and wrote it as a native Parquet GEOMETRY inside a 1.1 file
    # (#1175). A caller that supplied `geometry_info` has already answered.
    if geometry_info is None:
        geometry_info = table_geometry_info(
            table, geometry_column, sanitized_carried_geo(table.schema.metadata) or carried_geo
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
    block_args = (
        geometry_column,
        original_metadata,
        input_crs,
        custom_metadata,
        effective_version,
        edges,
        geometry_info,
        verbose,
    )
    geo_meta = _build_geo_block(table, *block_args)

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

    # Stats are read off the written data, so the block is composed again once
    # step 2 has retyped it. One owner call measures whatever is missing, in one
    # pass when both are.
    need_types = "geometry_types" not in geo_meta["columns"][geometry_column]
    measured: dict = {} if geo_types is None else {"geometry_types": geo_types}
    need_types = need_types and geo_types is None
    if geo_bbox:
        measured["bbox"] = geo_bbox
        if need_types:
            measured["geometry_types"] = geometry_types_of(table, geometry_column, verbose=verbose)
    else:
        stats = geometry_stats_of(table, geometry_column, verbose=verbose)
        if stats.bbox:
            measured["bbox"] = stats.bbox
        if need_types:
            measured["geometry_types"] = stats.geometry_types
    geo_meta = _build_geo_block(table, *block_args, stats={geometry_column: measured})

    return _assemble_and_apply_geo_metadata(table, geo_meta, metadata_version, verbose)


def _build_geo_block(
    table,
    geometry_column: str,
    original_metadata: dict | None,
    input_crs: dict | None,
    custom_metadata: dict | None,
    effective_version: str,
    edges: str | None,
    geometry_info: dict | None,
    verbose: bool,
    stats: dict | None = None,
) -> dict:
    """The output's ``geo`` block, composed over the table's schema.

    Composed before the geometry columns are retyped, which is what lets the
    native Parquet GEOMETRY types be built from the CRS it declares (#848), and
    again with the ``stats`` measured off the retyped data.

    Covering provenance is the input's own block when the caller has one, the
    table's otherwise: a table DuckDB built from a query carries no schema
    metadata at all, so reading only that would forget which column the
    primary declared (#953); a table entry point passes no
    ``original_metadata``, so its own block is the provenance.
    """
    if verbose and input_crs and not is_default_crs(input_crs):
        debug(f"Added CRS to geo metadata: {_format_crs_display(input_crs)}")
    # Resolve CRS data from the intact table before composing stripped declarations.
    column_crs = {
        name: (
            PARQUET_UNKNOWN_CRS
            if table_crs_is_unknown(table, name)
            else crs_of(table, geometry_column=name, quiet=True)
        )
        for name in (
            table_geometry_info(table, None, sanitized_carried_geo(table.schema.metadata)) or {}
        ).get("metadata", {})
        if name != geometry_column
    }
    return compose_geo_block(
        SchemaSource(table.schema, original_metadata),
        GeoBlockOptions(
            geometry_column=geometry_column,
            version=effective_version,
            covering=DETECT_BBOX_COLUMN,
            covering_provenance=table.schema.metadata,
            custom_metadata=custom_metadata,
            edges=edges,
            crs=input_crs,
            column_crs=column_crs,
            stats=stats,
            geometry_info=geometry_info,
            verbose=verbose,
        ),
    )


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
