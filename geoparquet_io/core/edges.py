"""The ``edges`` field: one owner for its vocabulary, version gate and readers.

GeoParquet's per-column ``edges`` says how to interpolate between two vertices:
``planar`` (straight lines, the spec default, also spelled by leaving the key
out), ``spherical`` (great circles), or -- in 2.0 and in a Parquet GEOGRAPHY
type -- one of four ellipsoidal algorithms. Before #1367 the names were spelled
out in five places, two readers recognised only ``spherical`` while two took all
five, the 1.x pair was written twice, and the carried-metadata fallback counted a
typo as non-planar where the file reader ignored it.

This module owns:

* the vocabulary (:data:`PLANAR`, :data:`SPHERICAL`, the ellipsoidal names, and
  the tuples built from them, in parquet.thrift's enum order);
* the version gate, :func:`edges_allowed` / :func:`edges_for_version` (what
  a 1.x block may say; whether a version *is* 1.x is
  ``geo_metadata.is_geoparquet_1x``, the version owners' answer);
* the GeoArrow carrier: one reader for either shape of the extension
  (:func:`geoarrow_type_edges`) and one writer (:func:`geoarrow_edge_type`);
* the one precedence, :func:`detect_edges` -- an explicit request, then the
  carried ``geo`` block, then the native type -- and its cross-file form,
  :func:`agree_across_files`, for a glob that must describe one column;
* the key's readers and writers on a column entry (:func:`edges_of`,
  :func:`declared_edges`, :func:`declares_edges`, :func:`set_edges`, which
  omits planar, :func:`state_edges` for a report, :func:`without_edges`) and the
  strip/restore pair a rewrite runs on a block (:func:`strip_edges`,
  :func:`restore_edges`);
* the point a planar-only operation (a cell key) takes for non-planar
  geometry (:func:`representative_point_sql`).

It is a leaf: it imports nothing from gpio but the exceptions and the logger,
so every layer can ask it, from ``duckdb_utils`` up. Reading a file or a table
to *gather* what each carrier says is the callers' job (``write_funnels``
``collect_nonplanar_edges`` / ``table_nonplanar_edges``); deciding what the
answers mean is this module's.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, MutableMapping
from typing import TypeGuard

from geoparquet_io.core.exceptions import GeoParquetError, InvalidParameterError
from geoparquet_io.core.logging_config import warn

PLANAR = "planar"
SPHERICAL = "spherical"
#: The one ``orientation`` value the spec defines (the winding a spherical
#: edges declaration implies). Its owner, so no module spells it again.
COUNTERCLOCKWISE = "counterclockwise"
VINCENTY = "vincenty"
THOMAS = "thomas"
ANDOYER = "andoyer"
KARNEY = "karney"

#: The ellipsoidal algorithms, in parquet.thrift's order.
ELLIPSOIDAL_ALGORITHMS = (VINCENTY, THOMAS, ANDOYER, KARNEY)

#: The non-planar edge algorithms a Parquet GEOGRAPHY can name, in the order of
#: parquet.thrift's ``EdgeInterpolationAlgorithm`` (the position is the enum
#: value). GeoParquet 2.0's ``edges`` spells them the same, lowercase.
GEOGRAPHY_EDGE_ALGORITHMS = (SPHERICAL, *ELLIPSOIDAL_ALGORITHMS)

#: What a GeoParquet 1.x ``edges`` may say.
GEOPARQUET_1X_EDGES = (PLANAR, SPHERICAL)

#: Every ``edges`` value gpio knows.
EDGES = (PLANAR, *GEOGRAPHY_EDGE_ALGORITHMS)

_KEY = "edges"


class EdgesConflictError(GeoParquetError):
    """The files of one input declare different edges for the same column."""


def is_nonplanar(edges: object) -> TypeGuard[str]:
    """Whether ``edges`` is a GeoParquet non-planar algorithm (a typo is not)."""
    return isinstance(edges, str) and edges in GEOGRAPHY_EDGE_ALGORITHMS


def is_ellipsoidal(edges: object) -> bool:
    """Whether ``edges`` names one of the four ellipsoidal algorithms."""
    return isinstance(edges, str) and edges in ELLIPSOIDAL_ALGORITHMS


def is_geodesic(edges: object) -> bool:
    """Whether ``edges`` says anything but planar (absent, ``None`` and ``""`` are planar).

    Wider than :func:`is_nonplanar`: an unknown value counts. For the checks
    that compare a bbox with a planar envelope, which must not claim a
    planar comparison for a column that does not say it is planar."""
    return declared_edges({_KEY: edges}) != PLANAR


def requests_planar(edges: object) -> bool:
    """Whether an ``edges=`` request is an explicit ``planar`` (``None`` asks
    for nothing): a planar request also clears a native GEOGRAPHY type."""
    return edges == PLANAR


def implied_orientation(edges: object) -> str | None:
    """The ``orientation`` an ``edges=`` request implies: ``counterclockwise``
    for spherical, per the GeoParquet spec; nothing for any other value. Keyed
    off the request, so an ellipsoidal algorithm a 1.x block degrades to
    spherical does not gain an orientation it never stated."""
    return COUNTERCLOCKWISE if edges == SPHERICAL else None


def normalized_edges(edges: object) -> object:
    """``edges`` in the vocabulary's case: a string lowercased, anything else as is.

    The one case fold, for carriers that spell the value in another case (a
    GeoArrow ``EdgeType`` name, a hand-written block)."""
    return edges.lower() if isinstance(edges, str) else edges


def requested_edges(edges: str | None) -> str | None:
    """An explicit ``edges=`` request, validated once: a known value or ``None``.

    Raises :class:`InvalidParameterError` naming the accepted values.
    """
    if edges is None or edges in EDGES:
        return edges
    raise InvalidParameterError(
        "edges", f"Unknown edges {edges!r}; expected one of: {', '.join(EDGES)}."
    )


def edges_allowed(geoparquet_1x: bool) -> tuple[str, ...]:
    """The edges values a block may state: the 1.x pair, or all of :data:`EDGES`.

    ``geoparquet_1x`` is ``geo_metadata.is_geoparquet_1x(version)``: a missing
    or unparsable version is not 1.x, so it gates nothing."""
    return GEOPARQUET_1X_EDGES if geoparquet_1x else EDGES


def edges_for_version(edges: str, geoparquet_1x: bool) -> str:
    """Map an edges value to one the output's spec version can say.

    GeoParquet 1.x allows only ``planar`` and ``spherical``; an ellipsoidal
    algorithm (from a 2.0 or native GEOGRAPHY input) degrades to ``spherical``
    there, with a warning. 2.0, and a write with no block (no version), keep
    the algorithm. ``geoparquet_1x`` is ``geo_metadata.is_geoparquet_1x`` of the
    block's version. A value outside :data:`EDGES` is refused here, once.
    """
    requested_edges(edges)
    if edges in edges_allowed(geoparquet_1x):
        return edges
    warn(f"edges algorithm '{edges}' is not representable in GeoParquet 1.x; writing 'spherical'")
    return SPHERICAL


#: A Parquet GEOGRAPHY logical type's edge-algorithm parameter, in any case.
_ALGORITHM_PARAM_RE = re.compile(rf"algorithm=({'|'.join(EDGES)})", re.IGNORECASE)


def logical_type_algorithm(params: str) -> str | None:
    """The edge algorithm a GEOGRAPHY logical type's parameter text names, lowercase.

    The full parquet.thrift vocabulary, not just the 1.x pair: an ellipsoidal
    algorithm read as "no algorithm" degraded to spherical in every caller's
    fallback (#1272). ``None`` when the text names none."""
    match = _ALGORITHM_PARAM_RE.search(params)
    return match.group(1).lower() if match else None


def logical_type_edges(parsed: Mapping | None) -> str:
    """The edges a parsed Parquet GEOGRAPHY logical type declares.

    The type's algorithm, or ``spherical``, the Parquet default when a
    GEOGRAPHY names none. ``parsed`` is ``duckdb_metadata.parse_geometry_logical_type``'s
    answer for a GEOGRAPHY column.
    """
    return (parsed or {}).get("algorithm") or SPHERICAL


def geoarrow_type_edges(arrow_type, extension_metadata: Mapping | None = None) -> str | None:
    """The edges a GeoArrow column declares, lowercase, in either carrier.

    pyarrow resolves the type when ``geoarrow.pyarrow`` is imported: its
    ``edge_type`` answers. Otherwise the field holds storage plus the
    extension's JSON, whose parsed ``extension_metadata`` answers; planar is
    GeoArrow's default there, so a planar column says ``None``. ``None`` too
    for a field that is not GeoArrow.
    """
    name = getattr(getattr(arrow_type, "edge_type", None), "name", "")
    if name:
        return name.lower()
    edges = normalized_edges(edges_of(extension_metadata))
    return edges if isinstance(edges, str) else None


def geoarrow_edge_type(edges: object):
    """The ``geoarrow.pyarrow.EdgeType`` for an edges value, the one writer.

    Every GeoParquet algorithm has its own member; planar, ``None`` and a
    value outside the vocabulary (a typo says nothing) are ``PLANAR``.
    """
    import geoarrow.pyarrow as ga

    edges = normalized_edges(edges)
    return ga.EdgeType[edges.upper()] if is_nonplanar(edges) else ga.EdgeType.PLANAR


# ---------------------------------------------------------------------------
# The key on a column entry (a geo block column, a GeoArrow extension's JSON)
# ---------------------------------------------------------------------------


def edges_of(entry: object) -> object:
    """The ``edges`` value ``entry`` states, as written; ``None`` when it states none.

    ``entry`` is a ``geo`` column entry, a GeoArrow extension's metadata, or
    any mapping that carries the key; anything else states nothing.
    """
    return entry.get(_KEY) if isinstance(entry, Mapping) else None


def declared_edges(entry: object) -> object:
    """What ``entry`` states, ``planar`` when it states nothing (absent, null, empty).

    A value outside the vocabulary is returned as written, for the checks that
    report it."""
    return edges_of(entry) or PLANAR


def declares_edges(entry: object) -> bool:
    """Whether ``entry`` has an ``edges`` key at all (absent means planar)."""
    return isinstance(entry, Mapping) and _KEY in entry


def set_edges(entry: MutableMapping, edges: object) -> None:
    """Set ``entry``'s ``edges`` to ``edges``, omitting planar.

    The one spell-or-omit rule for a written column entry (a ``geo`` block
    column, a GeoArrow extension's JSON): planar is the spec default, so it is
    said by leaving the key out, and ``None`` or ``planar`` removes it."""
    if edges is None or edges == PLANAR:
        entry.pop(_KEY, None)
    else:
        entry[_KEY] = edges


def state_edges(report: MutableMapping, edges: object) -> None:
    """Put ``edges`` on a report (an inspect or check result), as given.

    A report always carries the key, ``None`` and ``planar`` included: it
    says what was read, not what a file should store."""
    report[_KEY] = edges


def without_edges(entry: Mapping) -> dict:
    """A copy of ``entry`` without its ``edges`` key."""
    return {key: value for key, value in entry.items() if key != _KEY}


def block_edges(geo: Mapping | None) -> dict[str, object]:
    """``{column: edges}`` for every column a decoded ``geo`` block gives ``edges``.

    A column whose entry has no key is left out: it says nothing. ``geo`` is a
    block already decoded and sanitized (``geo_metadata.sanitized_carried_geo``).
    """
    columns = (geo or {}).get("columns") or {}
    return {name: edges_of(entry) for name, entry in columns.items() if declares_edges(entry)}


def strip_edges(geo: dict, columns: Collection[str] | None = None) -> dict:
    """Remove every non-planar ``edges`` from the decoded block ``geo``, in place.

    A rewrite that invalidates the edge interpretation (reprojecting into a
    projected CRS, #601) must not carry a declaration it cannot vouch for: the
    key's absence -- planar, the spec default -- is then the truthful answer.
    An explicit ``planar`` is left as it is. ``columns`` limits the strip to
    those column entries (``None``: every column). Returns ``geo``.
    """
    for name, entry in (geo.get("columns") or {}).items():
        if columns is not None and name not in columns:
            continue
        if declares_edges(entry) and edges_of(entry) != PLANAR:
            del entry[_KEY]
    return geo


def carry_edges(
    geo: Mapping | None, edges_by_col: Mapping[str, object] | None = None
) -> Mapping | None:
    """Rewrite the decoded block ``geo``'s ``edges`` as a writer says them, in place.

    The carry rule for a block a writer passes through: a non-planar value
    stays; ``planar`` is said by leaving the key out (:func:`set_edges`); a
    value outside the vocabulary says nothing (#1288), so it is not carried.
    Then each ``edges_by_col`` value (already mapped to the block's version) is
    set on the column of that name the block describes. Returns ``geo``.
    """
    columns = (geo or {}).get("columns") or {}
    for entry in columns.values():
        if declares_edges(entry):
            carried = edges_of(entry)
            set_edges(entry, carried if is_nonplanar(carried) else None)
    for name, edges in (edges_by_col or {}).items():
        entry = columns.get(name)
        if isinstance(entry, dict):
            set_edges(entry, edges)
    return geo


def restore_edges(geo: Mapping | None, edges_by_col: Mapping[str, str]) -> bool:
    """Set each column's ``edges`` in the decoded block ``geo``, in place.

    The restore half of a rewrite through DuckDB, which regenerates a block
    without ``edges`` (#588). Only columns the block describes are touched.
    ``edges_by_col`` is already mapped to the block's version. Returns whether
    anything changed.
    """
    changed = False
    columns = (geo or {}).get("columns") or {}
    for name, edges in edges_by_col.items():
        entry = columns.get(name)
        if isinstance(entry, dict) and edges_of(entry) != edges:
            set_edges(entry, edges)
            changed = True
    return changed


# ---------------------------------------------------------------------------
# The one precedence
# ---------------------------------------------------------------------------


def detect_edges(
    native: Mapping[str, object] | None = None,
    block: Mapping[str, object] | None = None,
    explicit: str | None = None,
    primary: str | None = None,
) -> dict[str, str]:
    """``{column: edges}`` for every column with non-planar edges, from three carriers.

    One precedence, lowest first:

    1. ``native`` -- what each column's type says (a Parquet GEOGRAPHY's
       algorithm, a GeoArrow field's edge type). Only a non-planar algorithm
       counts;
    2. ``block`` -- what the carried ``geo`` block states
       (:func:`block_edges`). ``planar`` wins over the type; a non-planar
       algorithm replaces it; any other value (a typo) says nothing, so the
       type decides (#1288);
    3. ``explicit`` -- the caller's request for ``primary``. ``planar`` drops
       the column; any other known value sets it; an unknown one is refused.

    A planar column is left out: the answer is what a write must carry.
    """
    edges_by_col: dict[str, str] = {
        name: edges for name, edges in (native or {}).items() if is_nonplanar(edges)
    }
    for name, declared in (block or {}).items():
        if declared == PLANAR:
            edges_by_col.pop(name, None)
        elif is_nonplanar(declared):
            edges_by_col[name] = declared
    explicit = requested_edges(explicit or None)
    if explicit is not None and primary is not None:
        if explicit == PLANAR:
            edges_by_col.pop(primary, None)
        else:
            edges_by_col[primary] = explicit
    return edges_by_col


def agree_across_files(per_file: Mapping[str, Mapping[str, str]]) -> dict[str, str]:
    """The one ``{column: edges}`` the files of a dataset declare, or an error.

    ``per_file`` maps each file to its :func:`detect_edges` answer (planar
    columns absent). A glob or directory is read as one column per name, so
    its files must agree: reading the first file only repaired a spherical
    file's rings as planar, or relabelled a planar file spherical (#1289).
    Raises :class:`EdgesConflictError` naming a file for each value.
    """
    answers = list(per_file.items())
    if not answers:
        return {}
    columns = {name for _, edges_by_col in answers for name in edges_by_col}
    for name in sorted(columns):
        seen: dict[str, str] = {}
        for path, edges_by_col in answers:
            seen.setdefault(edges_by_col.get(name, PLANAR), path)
        if len(seen) > 1:
            listed = "; ".join(f"{edges} in {path}" for edges, path in seen.items())
            raise EdgesConflictError(
                f"The input files disagree on the edges of column '{name}' ({listed}). "
                "One column cannot be both; convert the files separately, or rewrite "
                "them to one edges declaration first."
            )
    return dict(answers[0][1])


# ---------------------------------------------------------------------------
# A representative point for planar-only operations
# ---------------------------------------------------------------------------


def _unit_vector_sql(point: str) -> str:
    lon, lat = f"radians(ST_X({point}))", f"radians(ST_Y({point}))"
    return f"[cos({lat}) * cos({lon}), cos({lat}) * sin({lon}), sin({lat})]"


def _weighted_midpoints_sql() -> str:
    """Each segment of the vertex-vector list ``v`` as its normalised midpoint
    scaled by its arc length (the angle between its ends)."""
    chord = "sqrt(list_sum(list_transform(range(1, 4), lambda k: pow(v[i][k] - v[i + 1][k], 2))))"
    angle = f"2 * asin(least(1.0, {chord} / 2))"
    norm = "greatest(sqrt(list_sum(list_transform(s, lambda y: y * y))), 1e-300)"
    mid = "list_transform(range(1, 4), lambda k: v[i][k] + v[i + 1][k])"
    return (
        f"list_transform(range(1, len(v)), lambda i: list_transform([{mid}], "
        f"lambda s: list_transform(s, lambda x: x * {angle} / {norm}))[1])"
    )


def representative_point_sql(geom: str, edges: object) -> str:
    """The SQL point a planar-only operation (a cell key) takes for ``geom``.

    Planar edges: ``ST_Centroid``, as before. Non-planar edges: a point on the
    sphere (#1293) -- the direction of the sum of unit vectors, each segment
    contributing its great-circle midpoint weighted by its length; a polygon
    contributes its exterior rings, points themselves. So a line's point is
    its great-circle midpoint, a polar cap's is the pole and a line across the
    antimeridian stays on it, none of which a planar centroid gets. Ellipsoidal
    algorithms take the same spherical point (a cell is far coarser than the
    difference). Empty geometry gives NULL. ``geom`` is a SQL expression for a
    lon/lat GEOMETRY, evaluated once.
    """
    if not is_nonplanar(edges):
        return f"ST_Centroid({geom})"
    vertices = f"list_transform(ST_Dump(ST_Points(c)), lambda d: {_unit_vector_sql('d.geom')})"
    segments = f"list_transform([{vertices}], lambda v: {_weighted_midpoints_sql()})[1]"
    lines = (
        "CASE ST_Dimension(g) WHEN 2 THEN list_transform(ST_Dump(g), lambda c: "
        "ST_ExteriorRing(c.geom)) ELSE list_transform(ST_Dump(g), lambda c: c.geom) END"
    )
    points = f"list_transform(ST_Dump(ST_Points(g)), lambda d: {_unit_vector_sql('d.geom')})"
    vectors = (
        f"CASE WHEN ST_Dimension(g) = 0 THEN {points} "
        f"ELSE flatten(list_transform({lines}, lambda c: {segments})) END"
    )
    total = f"list_transform(range(1, 4), lambda k: list_sum(list_transform({vectors}, lambda w: w[k])))"
    point = (
        f"list_transform([{total}], lambda s: ST_Point(degrees(atan2(s[2], s[1])), "
        "degrees(atan2(s[3], sqrt(s[1] * s[1] + s[2] * s[2])))))[1]"
    )
    return f"list_transform([{geom}], lambda g: {point})[1]"
