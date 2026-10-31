"""
GeoJSON conversion for GeoParquet files.

Outputs GeoJSON using DuckDB's ST_AsGeoJSON function. Supports:
1. Streaming to stdout: Newline-delimited GeoJSON (GeoJSONSeq) with RFC 8142
   record separators for piping to tippecanoe.
2. File output: Standard GeoJSON FeatureCollection written to a file.
3. Stdin input: Read Arrow IPC streams from stdin for pipeline use.

Examples:
    # Stream to tippecanoe for PMTiles
    gpio convert geojson input.parquet | tippecanoe -P -o out.pmtiles

    # Pipeline with filtering
    gpio extract in.parquet --bbox ... | gpio convert geojson - | tippecanoe -P -o out.pmtiles

    # Write to file
    gpio convert geojson input.parquet output.geojson
"""

from __future__ import annotations

import codecs
import contextlib
import hashlib
import io
import sys
from typing import TYPE_CHECKING

from geoparquet_io.core.column_selection import same_column
from geoparquet_io.core.geometry_detection import (
    create_geometry_view,
    primary_geometry_column,
    require_geometry_column,
    resolve_geometry_column,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import TextIO

    import duckdb

from geoparquet_io.core.crs_utils import (
    crs_is_explicitly_null,
    crs_string_from_table,
    source_crs_string,
    transform_source_label,
    warn_null_crs_once,
)
from geoparquet_io.core.duckdb_utils import (
    describe_query,
    duckdb_capabilities,
    duckdb_type_contains_geometry,
    fetch_one,
    quote_identifier,
    register_arrow,
    sql_literal,
    sql_path,
    uses_aws_profile,
)
from geoparquet_io.core.edges import is_nonplanar
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.file_utils import (
    get_all_parquet_files,
    glob_matches,
    has_glob_pattern,
    resolve_file_url,
)
from geoparquet_io.core.geo_metadata import sanitized_carried_geo
from geoparquet_io.core.geometry_repair import repair_geometry_sql
from geoparquet_io.core.logging_config import configure_verbose, debug, info, success
from geoparquet_io.core.memory_limits import scoped_write_memory_limit
from geoparquet_io.core.remote import needs_httpfs
from geoparquet_io.core.streaming import is_stdin
from geoparquet_io.core.write_funnels import collect_nonplanar_edges, table_nonplanar_edges

# RFC 8142 record separator character
RS = "\x1e"


# WGS84 CRS identifier for RFC 7946 compliance
WGS84_CRS = "EPSG:4326"


def _require_planar_geojson_edges(column: str, edges: str | None) -> None:
    """Refuse an unsupported input contract before any planar export operation.

    RFC 7946 section 3.1.1 gives GeoJSON Cartesian segments, not geodesics.
    Skipping MakeValid still leaves planar precision reduction and, even with
    both skipped, would silently change the meaning of the same coordinates.
    The declaration decides this contract, including point-only or empty data.
    """
    if is_nonplanar(edges):
        raise GeoParquetError(
            f"GeoJSON export does not support non-planar edges. Column {column!r} "
            f"declares {edges!r} edges. GeoJSON uses straight Cartesian segments. "
            "Keep the data in GeoParquet, or use a geodesic-aware tool to densify "
            "and cut the shapes into a planar representation before exporting. "
            "Disabling geometry repair does not make this conversion safe."
        )


@uses_aws_profile
def require_planar_geojson_file(
    input_path: str,
    geometry_column: str | None = None,
    profile: str | None = None,
) -> None:
    """Check file edge declarations before GeoJSON or PMTiles preprocessing.

    The column is the file's primary (the owner's answer, block included) for
    direct GeoJSON and PMTiles alike, resolved under the same scoped profile
    as the footer reads.
    """
    edges = {
        column: algorithm
        for path in _geojson_input_files(input_path)
        for column, algorithm in collect_nonplanar_edges(path).items()
    }
    if not edges:
        return
    if geometry_column is None:
        # The file's primary, block and typed columns both (#1367): the direct
        # GeoJSON path and PMTiles ask the same question, of the first footer
        # for a glob or a directory.
        files = _geojson_input_files(input_path)
        geometry_column = resolve_geometry_column(files[0] if files else input_path)
    _require_planar_geojson_edges(geometry_column, edges.get(geometry_column))


def _geojson_input_files(input_path: str) -> list[str]:
    """Enumerate every footer so a planar-first glob cannot hide non-planar rows."""
    if has_glob_pattern(input_path):
        # Every match, not only `.parquet`: `*.pq` is a GeoParquet glob too.
        return glob_matches(input_path)
    return get_all_parquet_files(input_path)


def _encodes_utf8(stream: TextIO) -> bool:
    """Whether ``stream`` already writes UTF-8, under any spelling of the name."""
    name = getattr(stream, "encoding", None)
    if not name:
        return False
    try:
        return codecs.lookup(name).name == "utf-8"
    except LookupError:
        return False


@contextlib.contextmanager
def _utf8_stdout() -> Iterator[TextIO]:
    """Yield a stdout stream that can encode any GeoJSON we produce.

    GeoJSON is UTF-8 by definition (RFC 7946 §11), but Python hands
    ``sys.stdout`` the console's codec - cp1252 on a default Windows console -
    and the first place name outside Latin-1 then raises UnicodeEncodeError
    mid-stream, after some features are already written. The CLI reconfigures
    its own streams in the group callback, so only Python API callers were
    exposed; reconfiguring theirs would leave a process-wide side effect behind
    for code that merely called ``to_geojson()``. So wrap the underlying binary
    buffer for the duration and detach - never close - it afterwards, leaving
    ``sys.stdout`` exactly as we found it.
    """
    stream = sys.stdout
    buffer = getattr(stream, "buffer", None)
    if buffer is None or _encodes_utf8(stream):
        # Already UTF-8, or a stream with no binary layer to wrap (a StringIO
        # test double takes str directly and cannot raise an encoding error).
        yield stream
        return

    # Anything the caller already buffered belongs in front of our output, and
    # must go out under the codec it was written for.
    stream.flush()
    wrapper = io.TextIOWrapper(buffer, encoding="utf-8", newline="", write_through=True)
    try:
        yield wrapper
    finally:
        # A broken pipe is handled by the caller and must not resurface here;
        # detaching a wrapper whose buffer already went away raises ValueError.
        with contextlib.suppress(OSError, ValueError):
            wrapper.flush()
        with contextlib.suppress(ValueError):
            wrapper.detach()


def _get_source_crs(input_path: str) -> str | None:
    """The primary geometry's ``ST_Transform`` source, or ``None`` for lon/lat.

    Read through :func:`crs_utils.source_crs_string`, the resolver every other
    command uses, so a native-geo-only input (CRS only in the Parquet logical
    type, no ``geo`` block) is reprojected too (#1248), and so is a CRS with no
    authority code (#1258). A CRS that cannot be resolved raises
    :class:`crs_utils.UnresolvableCRSError` rather than being written as
    RFC 7946 lon/lat.

    Args:
        input_path: RAW path to input GeoParquet file

    Returns:
        ``"AUTH:CODE"``, canonical PROJJSON, or ``None`` for lon/lat input
    """
    return source_crs_string(input_path)


def _needs_reprojection(source_crs: str | None) -> bool:
    """
    Check if reprojection to WGS84 is needed for RFC 7946 compliance.

    Args:
        source_crs: Source CRS string or None

    Returns:
        True if data needs reprojection to WGS84
    """
    if source_crs is None:
        # No CRS info - assume WGS84
        return False

    # Normalize CRS string for comparison
    crs_upper = source_crs.upper().replace(" ", "")

    # Common WGS84 representations
    wgs84_variants = {
        "EPSG:4326",
        "OGC:CRS84",
        "CRS84",
        "WGS84",
    }

    return crs_upper not in wgs84_variants


def _is_geometry_or_bbox(column: str, geometry_column: str) -> bool:
    """Whether ``column`` is the geometry or the bbox, which are not GeoJSON properties."""
    return same_column(column, geometry_column) or same_column(column, "bbox")


def _get_property_columns(
    con: duckdb.DuckDBPyConnection,
    source_ref: str,
    geometry_column: str,
) -> list[str]:
    """
    Get list of property columns (all columns except geometry and bbox).

    Args:
        con: DuckDB connection
        source_ref: Table reference for SQL query
        geometry_column: Name of geometry column to exclude

    Returns:
        List of column names to include as properties
    """
    columns = [name for name, _ in describe_query(con, f"SELECT * FROM {source_ref}")]

    # Exclude geometry column and bbox
    return [col for col in columns if not _is_geometry_or_bbox(col, geometry_column)]


def _reprojected_source_ref(source_ref: str, quoted_geom: str, source_crs: str) -> str:
    """Wrap ``source_ref`` so every reader below sees WGS84 geometry.

    Reproject first, in a subquery, so everything downstream sees the geometry
    in the CRS it will be written in. Reprojection is not topology-preserving: a
    polygon ST_MakeValid has just made valid in the source CRS can come out of
    ST_Transform invalid in WGS84, and ST_ReducePrecision - a GEOS topology
    operation - then raises TopologyException on it. Repairing after the
    transform is also what the explicit --src-crs route already does, since
    core/pmtiles.py reprojects in a separate process before piping into this
    query; only the auto-detected CRS path had the two in the wrong order.

    The transform lives in a subquery rather than inline because
    repair_geometry_sql repeats its argument, and ST_Transform is far too
    expensive to evaluate once per repetition per row.
    """
    # source_crs comes straight out of a file's own geo metadata, so it can
    # be any free-form string -- escape it like every other SQL literal.
    source_crs_literal = sql_literal(source_crs)
    return (
        f"(SELECT * REPLACE ("
        f"ST_Transform({quoted_geom}, {source_crs_literal}, {sql_literal(WGS84_CRS)}) AS {quoted_geom}"
        f") FROM {source_ref} WHERE {quoted_geom} IS NOT NULL)"
    )


def _output_geometry_exprs(
    quoted_geom: str, precision: int, repair_geometry: bool
) -> tuple[str, str]:
    """The repaired geometry and its precision-reduced form, as SQL expressions.

    Both GeoJSON writers must serialize exactly the same geometry, so the chain
    lives here: repair (issue #506) happens before precision reduction so
    downstream consumers (e.g. tippecanoe in the PMTiles pipeline) never see a
    self-intersection, and ST_ReducePrecision applies the RFC 7946 coordinate
    grid of 10^-precision (e.g. precision=7 -> grid=0.0000001). Very low
    precision values may collapse small geometries to empty.
    """
    geom_for_output = repair_geometry_sql(quoted_geom) if repair_geometry else quoted_geom
    geom_with_precision = f"ST_ReducePrecision({geom_for_output}, pow(10, -{precision}))"
    return geom_for_output, geom_with_precision


def _build_feature_query(
    source_ref: str,
    geometry_column: str,
    property_columns: list[str],
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    source_crs: str | None = None,
    repair_geometry: bool = True,
) -> str:
    """
    Build SQL query that outputs GeoJSON Feature strings.

    Args:
        source_ref: Table reference for SQL query
        geometry_column: Name of geometry column
        property_columns: List of property column names
        precision: Coordinate decimal precision
        write_bbox: Whether to include bbox property
        id_field: Optional field to use as feature id
        source_crs: If provided, reproject from this CRS to WGS84

    Returns:
        SQL query string
    """
    # Interpolated into SQL below. The CLI declares --precision as an int, but
    # the Python API takes it from a free-form format_options dict, so coerce
    # rather than trust it.
    precision = int(precision)
    quoted_geom = quote_identifier(geometry_column)

    if source_crs:
        source_ref = _reprojected_source_ref(source_ref, quoted_geom, source_crs)

    # The repair/precision chain is shared with the native fast path so both
    # writers serialize exactly the same geometry. The NULL filter in the WHERE
    # clause stays on the raw column. ST_AsGeoJSON requires native GEOMETRY,
    # which this already is.
    geom_for_output, geom_with_precision = _output_geometry_exprs(
        quoted_geom, precision, repair_geometry
    )
    geom_expr = f"ST_AsGeoJSON({geom_with_precision})"

    # Build properties expression
    if property_columns:
        prop_pairs = ", ".join(
            f"{quote_identifier(col)} := {quote_identifier(col)}" for col in property_columns
        )
        props_expr = f"to_json(struct_pack({prop_pairs}))"
    else:
        props_expr = "'{}'"

    # Optional members are whole expressions, joined into the `||` chain below
    # rather than carrying their own trailing separator. A fragment that ended
    # in a bare comma is what split the SELECT into several columns and
    # truncated every Feature (#726).
    fragments = ['\'{"type":"Feature",\'']

    # DuckDB's `||` yields NULL if any operand is NULL, which would null the
    # whole Feature string and abort the conversion when the writer tried to
    # parse it. Both optional members can go NULL on perfectly ordinary input,
    # so each is wrapped to degrade to "member omitted" instead.
    if id_field:
        quoted_id = quote_identifier(id_field)
        # to_json() of a NULL id is NULL. Omitting the member is also what
        # RFC 7946 wants -- `id` must be a string or a number, never null.
        fragments.append(f"""COALESCE('"id":' || to_json({quoted_id}) || ',', '')""")

    # Bbox coordinates honor the precision parameter via ROUND().
    if write_bbox:
        # ST_XMin of an EMPTY geometry is NULL, and EMPTY passes the
        # `IS NOT NULL` filter below -- ST_ReducePrecision and ST_MakeValid can
        # both produce it from valid input, so this is reachable without a
        # malformed file.
        fragments.append(
            f"""COALESCE('"bbox":[' || """
            f"ROUND(ST_XMin({geom_for_output}), {precision}) || ',' || "
            f"ROUND(ST_YMin({geom_for_output}), {precision}) || ',' || "
            f"ROUND(ST_XMax({geom_for_output}), {precision}) || ',' || "
            f"""ROUND(ST_YMax({geom_for_output}), {precision}) || '],', '')"""
        )

    fragments += [
        "'\"geometry\":'",
        f"COALESCE({geom_expr}, 'null')",
        "',\"properties\":'",
        props_expr,
        "'}'",
    ]

    # Build complete Feature JSON using string concatenation
    joined = " ||\n            ".join(fragments)
    query = f"""
        SELECT
            {joined} AS feature
        FROM {source_ref}
        WHERE {quoted_geom} IS NOT NULL
    """

    return query


def _geometry_typed_property_columns(
    con: duckdb.DuckDBPyConnection,
    source_ref: str,
    property_columns: list[str],
) -> list[str]:
    """Property columns whose type is GEOMETRY, which the two writers disagree on.

    ``to_json`` renders a GEOMETRY property as a WKT string, while the native
    writer nests it as GeoJSON. Any such column keeps the conversion on the
    hand-built writer so the output does not change.
    """
    # Any depth, not only a top-level GEOMETRY: a STRUCT or LIST holding a
    # geometry is rendered differently by the two writers too.
    types = dict(describe_query(con, f"SELECT * FROM {source_ref}"))
    return [col for col in property_columns if duckdb_type_contains_geometry(types.get(col))]


def _native_feature_collection_to_file(
    con: duckdb.DuckDBPyConnection,
    source_ref: str,
    geometry_column: str,
    property_columns: list[str],
    output_path: str,
    precision: int,
    source_crs: str | None,
    repair_geometry: bool,
    *,
    verbose: bool = False,
) -> int | None:
    """Write a FeatureCollection file with DuckDB 2.0's native GeoJSON writer.

    Semantic equivalence with the hand-built writer is the contract
    (tests/test_geojson_native_writer.py compares both parsed outputs):

    - The geometry chain (reprojection subquery, repair, ST_ReducePrecision)
      is the same SQL both writers run, and the doubles both serializers emit
      parse back to equal values. Parsed equality, not byte equality, is the
      enforced contract: the hand-built output passes through a Python ``json``
      re-serialization, so the two files need not match byte for byte.
    - ``ID_COLUMN ''`` disables the writer's default promotion of a column
      named ``id``, keeping it in ``properties`` like the hand-built writer.
    - The same ``WHERE geometry IS NOT NULL`` filter drops NULL-geometry rows.

    The caller guarantees eligibility; options the writer cannot reproduce
    (pretty, description, id_field, write_bbox, geometry-typed properties)
    never reach this function. ``ST_ReducePrecision`` is a GEOS topology
    operation, so precision was already coerced to int by the caller.

    The COPY is a DuckDB write, so it runs inside
    :func:`scoped_write_memory_limit` like the GDAL and CSV export COPY sites
    (#1174): no ``--write-memory`` reaches this path, so the ceiling-based
    default applies and threads are capped to keep headroom per thread.

    Returns:
        Number of features written (the COPY row count after the NULL filter),
        or ``None`` when the engine refuses ``FORMAT GEOJSON`` as unknown at
        bind time. The version gate only proves the library is 2.x, not that
        this build ships the copy format, so that refusal sends the caller to
        the hand-built writer. A mid-write IO, conversion or interrupt error
        still propagates.
    """
    import duckdb

    quoted_geom = quote_identifier(geometry_column)
    if source_crs:
        source_ref = _reprojected_source_ref(source_ref, quoted_geom, source_crs)
    _, geom_with_precision = _output_geometry_exprs(quoted_geom, precision, repair_geometry)
    select_list = ", ".join(
        [f"{geom_with_precision} AS {quoted_geom}"]
        + [quote_identifier(col) for col in property_columns]
    )
    copy_query = (
        f"COPY (SELECT {select_list} FROM {source_ref} WHERE {quoted_geom} IS NOT NULL) "
        f"TO {sql_path(output_path)} (FORMAT GEOJSON, ID_COLUMN '', "
        f"GEOMETRY_COLUMN {sql_literal(geometry_column)})"
    )
    debug(f"Native GeoJSON COPY: {copy_query}")
    with scoped_write_memory_limit(con, None, verbose):
        try:
            row = fetch_one(con.execute(copy_query))
        except (duckdb.CatalogException, duckdb.BinderException) as exc:
            # A bind-time refusal: this engine has no GEOJSON copy format, so
            # nothing was written and the hand-built writer still can.
            debug(f"Native GeoJSON COPY not taken ({exc}); using the hand-built writer")
            return None
    return int(row[0]) if row else 0


def _maybe_native_feature_collection(
    con: duckdb.DuckDBPyConnection,
    source_ref: str,
    geometry_column: str,
    property_columns: list[str],
    *,
    output_path: str | None,
    precision: int,
    write_bbox: bool,
    id_field: str | None,
    description: str | None,
    pretty: bool,
    source_crs: str | None,
    repair_geometry: bool,
    verbose: bool = False,
) -> int | None:
    """Take the native fast path when its output would be semantically identical.

    Returns the feature count on success, or ``None`` when the conversion must
    stay on the hand-built writer:

    - no ``output_path``: the stdout paths stream (RS-delimited GeoJSONSeq for
      tippecanoe, or a FeatureCollection) and COPY only writes files;
    - ``pretty``/``description``: the native writer has no such options;
    - ``id_field``: the writer's ID_COLUMN removes the column from properties
      and emits ``"id": null`` for NULL values, where gpio keeps the property
      and omits the member per RFC 7946;
    - ``write_bbox``: the writer's BBOX option computes the box from the
      precision-reduced geometry and omits it for EMPTY, where gpio rounds the
      pre-reduction extent;
    - a GEOMETRY-typed property column (WKT string vs nested GeoJSON);
    - DuckDB 1.x, which has no native GeoJSON writer;
    - a 2.x engine that refuses ``FORMAT GEOJSON`` as unknown at bind time,
      which :func:`_native_feature_collection_to_file` reports as ``None``.
    """
    if not output_path or pretty or description or id_field or write_bbox:
        return None
    # FORMAT GEOJSON arrived with DuckDB 2.0's spatial extension; on 1.x the
    # statement is a binder error (#1349).
    if not duckdb_capabilities().native_geojson_writer:
        return None
    if _geometry_typed_property_columns(con, source_ref, property_columns):
        return None
    return _native_feature_collection_to_file(
        con,
        source_ref,
        geometry_column,
        property_columns,
        output_path,
        int(precision),
        source_crs,
        repair_geometry,
        verbose=verbose,
    )


def _stream_to_stdout(
    con: duckdb.DuckDBPyConnection,
    query: str,
    rs: bool = True,
    pretty: bool = False,
) -> int:
    """
    Stream GeoJSON features to stdout line by line (GeoJSONSeq format).

    Args:
        con: DuckDB connection
        query: SQL query that returns feature JSON strings
        rs: Whether to include RFC 8142 record separators
        pretty: Whether to pretty-print each feature

    Returns:
        Number of features written
    """
    import json
    import os

    result = con.execute(query)
    count = 0

    # The `with` is inside the `try`: switching stdout to UTF-8 flushes whatever
    # the caller buffered under the old codec, and if the reader is already gone
    # that flush raises where only this handler should see it.
    try:
        with _utf8_stdout() as output:
            while True:
                row = result.fetchone()  # drains to None; guard: open-result ok
                if row is None:
                    break

                if rs:
                    output.write(RS)

                if pretty:
                    # Parse and re-serialize with indentation
                    feature = json.loads(row[0])
                    output.write(json.dumps(feature, indent=2))
                else:
                    output.write(row[0])

                output.write("\n")
                count += 1

            output.flush()
    except BrokenPipeError:
        # Downstream consumer closed the pipe early (e.g. `| head`, tippecanoe
        # finished reading). Redirect stdout to devnull so interpreter-shutdown
        # flush does not re-raise. Standard Unix pipe-tool behavior.
        try:
            stdout_fd = sys.stdout.fileno()
        except (OSError, ValueError):
            return count
        devnull_fd = os.open(os.devnull, os.O_WRONLY)
        try:
            os.dup2(devnull_fd, stdout_fd)
        finally:
            os.close(devnull_fd)

    return count


def _stream_feature_collection(
    con: duckdb.DuckDBPyConnection,
    query: str,
    description: str | None = None,
    pretty: bool = False,
    output_path: str | None = None,
) -> int:
    """
    Output GeoJSON as a complete FeatureCollection to stdout or file.

    Args:
        con: DuckDB connection
        query: SQL query that returns feature JSON strings
        description: Optional description for the FeatureCollection
        pretty: Whether to pretty-print the output
        output_path: If provided, write to this file; otherwise write to stdout

    Returns:
        Number of features written
    """
    import json

    result = con.execute(query)
    features = []
    count = 0

    # Collect all features
    while True:
        row = result.fetchone()  # drains to None; guard: open-result ok
        if row is None:
            break
        features.append(json.loads(row[0]))
        count += 1

    # Build FeatureCollection
    fc: dict = {
        "type": "FeatureCollection",
    }

    if description:
        fc["description"] = description

    fc["features"] = features

    # Output to file or stdout
    if output_path:
        # newline="\n" keeps the output byte-identical across platforms. Text mode
        # would translate every "\n" to os.linesep, so Windows produced CRLF
        # GeoJSON - a different file for the same input, which breaks checksum
        # comparisons and any consumer reading the stream as bytes.
        with open(output_path, "w", encoding="utf-8", newline="\n") as f:
            if pretty:
                json.dump(fc, f, indent=2)
            else:
                json.dump(fc, f)
            f.write("\n")
    else:
        with _utf8_stdout() as output:
            if pretty:
                output.write(json.dumps(fc, indent=2))
            else:
                output.write(json.dumps(fc))
            output.write("\n")
            output.flush()

    return count


@uses_aws_profile
def convert_to_geojson_stream(
    input_path: str,
    output_path: str | None = None,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    description: str | None = None,
    seq: bool = True,
    pretty: bool = False,
    verbose: bool = False,
    profile: str | None = None,
    keep_crs: bool = False,
    repair_geometry: bool = True,
) -> int:
    """
    Convert GeoParquet to GeoJSON.

    By default (no output_path), outputs RFC 8142 GeoJSONSeq format (newline-delimited
    features) to stdout, suitable for piping to tippecanoe with the -P (parallel) flag.

    When output_path is provided, writes a FeatureCollection to the file.

    Automatically reprojects to WGS84 (EPSG:4326) for RFC 7946 compliance unless
    keep_crs is True.

    Args:
        input_path: Path to input file, or "-" to read Arrow IPC from stdin
        output_path: Output file path (writes FeatureCollection), or None to stream
        rs: Include RFC 8142 record separators (streaming only, default True)
        precision: Coordinate decimal precision (default 7 per RFC 7946)
        write_bbox: Include bbox property for each feature
        id_field: Field to use as feature 'id' member
        description: Description for FeatureCollection
        seq: If True and no output_path, output GeoJSONSeq; if False, FeatureCollection
        pretty: Pretty-print the JSON output
        verbose: Enable verbose output (to stderr)
        profile: AWS profile name for S3 files
        keep_crs: If True, keep original CRS instead of reprojecting to WGS84

    Returns:
        Number of features written

    Raises:
        GeoParquetError: If the exported column declares non-planar edges.
            GeoJSON cannot preserve geodesic edges. Refused before output,
            including when geometry repair is disabled or keep_crs is True.
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    configure_verbose(verbose)

    # Handle stdin input
    if is_stdin(input_path):
        return _convert_from_stream(
            output_path=output_path,
            rs=rs,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            description=description,
            seq=seq,
            pretty=pretty,
            verbose=verbose,
            keep_crs=keep_crs,
            repair_geometry=repair_geometry,
        )

    # Get safe URL for input
    input_url = resolve_file_url(input_path, verbose)

    # Check if reprojection is needed
    # --keep-crs writes the input coordinates, so the CRS is never resolved.
    source_crs = None if keep_crs else _get_source_crs(input_path)
    needs_reproject = _needs_reprojection(source_crs)
    if needs_reproject:
        info(f"Reprojecting from {transform_source_label(source_crs)} to WGS84 (RFC 7946)")
        debug(f"Source CRS: {source_crs}, reprojecting to {WGS84_CRS}")

    # Create DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))

    try:
        source_ref = f"read_parquet({sql_path(input_url)})"

        # The file's primary, with its `geo` block: a declared `footprint`
        # is the geometry even beside a `geometry` secondary (#1367).
        geometry_column = resolve_geometry_column(input_path, con=con)
        require_planar_geojson_file(input_path, geometry_column)
        debug(f"Using geometry column: {geometry_column}")

        # Get property columns
        property_columns = _get_property_columns(con, source_ref, geometry_column)
        debug(f"Property columns: {', '.join(property_columns)}")

        # A WKB column DuckDB does not read as GEOMETRY -- a file with no `geo`
        # key, such as the one DuckDB writes for zero rows -- is decoded once,
        # here, so the repair and serialization chain below sees GEOMETRY (#1359).
        source_ref = create_geometry_view(con, source_ref, geometry_column, "__gpio_geojson_source")

        count = _maybe_native_feature_collection(
            con,
            source_ref,
            geometry_column,
            property_columns,
            output_path=output_path,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            description=description,
            pretty=pretty,
            source_crs=source_crs if needs_reproject else None,
            repair_geometry=repair_geometry,
            verbose=verbose,
        )
        if count is not None:
            success(f"Wrote {count:,} features to {output_path}")
            return count

        # Build and execute query (pass source_crs if reprojection needed)
        query = _build_feature_query(
            source_ref,
            geometry_column,
            property_columns,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            source_crs=source_crs if needs_reproject else None,
            repair_geometry=repair_geometry,
        )
        debug(f"Query: {query}")

        # File output: always write FeatureCollection
        if output_path:
            count = _stream_feature_collection(
                con, query, description, pretty, output_path=output_path
            )
            success(f"Wrote {count:,} features to {output_path}")
            return count

        # Streaming output: GeoJSONSeq or FeatureCollection to stdout
        if seq:
            return _stream_to_stdout(con, query, rs, pretty)
        else:
            return _stream_feature_collection(con, query, description, pretty)

    finally:
        con.close()


def _warn_on_explicit_null_crs(table, geometry_column: str) -> None:
    """Warn once per stream ``geo`` block whose column says ``"crs": null`` (unknown)."""
    metadata = table.schema.metadata or {}
    geo_meta = sanitized_carried_geo(metadata)
    columns = geo_meta.get("columns") if isinstance(geo_meta, dict) else None
    col_meta = columns.get(geometry_column) if isinstance(columns, dict) else None
    if col_meta is None or not crs_is_explicitly_null(col_meta):
        return
    digest = hashlib.sha1(metadata[b"geo"], usedforsecurity=False).hexdigest()
    warn_null_crs_once(f"table:{digest}")


def _convert_from_stream(
    output_path: str | None = None,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    description: str | None = None,
    seq: bool = True,
    pretty: bool = False,
    verbose: bool = False,
    keep_crs: bool = False,
    repair_geometry: bool = True,
) -> int:
    """
    Convert Arrow IPC stream from stdin to GeoJSON.

    Args:
        output_path: Output file path, or None to stream to stdout
        rs: Whether to include RFC 8142 record separators (streaming only)
        precision: Coordinate decimal precision
        write_bbox: Include bbox property for each feature
        id_field: Field to use as feature 'id' member
        description: Description for FeatureCollection
        seq: If True and no output_path, output GeoJSONSeq; if False, FeatureCollection
        pretty: Pretty-print the JSON output
        verbose: Enable verbose output
        keep_crs: If True, keep original CRS instead of reprojecting to WGS84

    Returns:
        Number of features written

    Note:
        The stream's CRS is read from its ``geo`` block, then from the geometry
        field's GeoArrow CRS, through the same resolver a file input uses (#1261).
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.streaming import (
        read_arrow_stream,
    )

    debug("Reading Arrow IPC stream from stdin...")

    # Read Arrow IPC from stdin
    table = read_arrow_stream()

    debug(f"Read {table.num_rows} rows from stream")

    # Find geometry column
    geometry_column = require_geometry_column(
        primary_geometry_column(table),
        f"the Arrow stream (columns: [{', '.join(table.column_names)}])",
    )

    debug(f"Using geometry column: {geometry_column}")

    edges = table_nonplanar_edges(table, geometry_column, None).get(geometry_column)
    _require_planar_geojson_edges(geometry_column, edges)
    _warn_on_explicit_null_crs(table, geometry_column)
    # --keep-crs writes the input coordinates, so the CRS is never resolved.
    source_crs = None if keep_crs else crs_string_from_table(table, geometry_column)
    needs_reproject = _needs_reprojection(source_crs)
    if needs_reproject:
        info(f"Reprojecting from {transform_source_label(source_crs)} to WGS84 (RFC 7946)")

    # Get property columns
    property_columns = [
        col for col in table.column_names if not _is_geometry_or_bbox(col, geometry_column)
    ]

    debug(f"Property columns: {', '.join(property_columns)}")

    # Register table with DuckDB
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)

    try:
        register_arrow(con, "input_stream", table)

        # Create view with proper geometry type
        source_ref = (
            create_geometry_view(con, "input_stream", geometry_column, "input_stream_geom")
            if geometry_column
            else "input_stream"
        )

        count = _maybe_native_feature_collection(
            con,
            source_ref,
            geometry_column,
            property_columns,
            output_path=output_path,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            description=description,
            pretty=pretty,
            source_crs=source_crs if needs_reproject else None,
            repair_geometry=repair_geometry,
            verbose=verbose,
        )
        if count is not None:
            success(f"Wrote {count:,} features to {output_path}")
            return count

        # Build and execute query
        query = _build_feature_query(
            source_ref,
            geometry_column,
            property_columns,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            source_crs=source_crs if needs_reproject else None,
            repair_geometry=repair_geometry,
        )
        debug(f"Query: {query}")

        # File output: always write FeatureCollection
        if output_path:
            count = _stream_feature_collection(
                con, query, description, pretty, output_path=output_path
            )
            success(f"Wrote {count:,} features to {output_path}")
            return count

        # Streaming output: GeoJSONSeq or FeatureCollection to stdout
        if seq:
            return _stream_to_stdout(con, query, rs, pretty)
        else:
            return _stream_feature_collection(con, query, description, pretty)

    finally:
        con.close()


def convert_to_geojson(
    input_path: str,
    output_path: str | None = None,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
    description: str | None = None,
    seq: bool = True,
    pretty: bool = False,
    verbose: bool = False,
    profile: str | None = None,
    keep_crs: bool = False,
    repair_geometry: bool = True,
) -> int:
    """
    Convert GeoParquet to GeoJSON.

    When output_path is provided, writes a FeatureCollection to the file.
    Otherwise, streams to stdout (GeoJSONSeq by default, or FeatureCollection with seq=False).

    Automatically reprojects to WGS84 (EPSG:4326) for RFC 7946 compliance
    unless keep_crs is True.

    Args:
        input_path: Path to input file, or "-" to read Arrow IPC from stdin
        output_path: Output file path, or None to stream to stdout
        rs: Include RFC 8142 record separators (streaming only, seq mode)
        precision: Coordinate decimal precision (default 7 per RFC 7946)
        write_bbox: Include bbox property for features
        id_field: Field to use as feature 'id' member
        description: Description for FeatureCollection
        seq: If True and streaming, output GeoJSONSeq; if False, FeatureCollection
        pretty: Pretty-print the output
        verbose: Enable verbose output
        profile: AWS profile name for S3 files
        keep_crs: If True, keep original CRS instead of reprojecting to WGS84

    Returns:
        Number of features written

    Raises:
        GeoParquetError: If the exported column declares non-planar edges.
            Densify and cut the geometry into a planar representation with
            a geodesic-aware tool before exporting, or keep GeoParquet.
    """
    return convert_to_geojson_stream(
        input_path=input_path,
        output_path=output_path,
        rs=rs,
        precision=precision,
        write_bbox=write_bbox,
        id_field=id_field,
        description=description,
        seq=seq,
        pretty=pretty,
        verbose=verbose,
        profile=profile,
        keep_crs=keep_crs,
        repair_geometry=repair_geometry,
    )
