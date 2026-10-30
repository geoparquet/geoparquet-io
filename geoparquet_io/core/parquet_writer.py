"""The write facade: the decisions every gpio write shares, owned in one place.

gpio has many write *paths* -- a DuckDB ``COPY`` behind
``common.write_parquet_with_metadata``, four Arrow/DuckDB strategies behind
``Table.write()``, a partition staging rewrite -- and for a long time each one
decided the same handful of facts about its output independently. They
disagreed, and the disagreements shipped one bug at a time: ``gpio sort``
writing 50,000-row groups that landed at 51,200 (#961), ``check --fix`` writing
100,000 (#974), ``Table.write(row_group_rows=50000)`` writing 51,200 while the
identical CLI request wrote 49,152 (#971), ``convert geoparquet`` documenting
100k row groups while nothing set any (#981), and every auto-version path but
``convert`` downgrading a native-geo-only input to 1.1 WKB (#600).

This module is the single owner of those shared decisions. It owns exactly
four, because each one is a fact about the *output file* that no individual
write path is entitled to answer for itself:

1. **How many rows go in a row group** -- :func:`resolve_row_group_rows`, and
   the constants it derives the default from.
2. **Which GeoParquet version the output declares** when the caller did not ask
   for one -- :func:`resolve_output_geoparquet_version`.
3. **Which CRS the output declares** when the caller did not name one --
   :func:`resolve_input_crs`.
4. **Which file-level keys the output carries** -- whether a ``geo`` key
   appears at all, and that the input's ``pandas``, ``ARROW:schema`` and
   ``geo:overviews`` never do -- :func:`output_schema_metadata` and
   :func:`apply_output_kv_metadata`, and for a DuckDB ``COPY`` that carries its
   own ``geo`` block, :func:`geoparquet_copy_options`.

The third exists because it is the *same question as the second*, asked of the
same witness. Both are "what did the input declare", and a rewrite that answers
one from the input file and the other from the carried ``geo`` key produces a
file that contradicts itself: native 2.0 with EPSG:5070 in the Parquet
``GEOMETRY`` logical type and no ``crs`` key in the ``geo`` block, which
GeoParquet resolves as ``OGC:CRS84`` (#993). Splitting the two answers between
two owners is what made that possible, so they are answered together.

It deliberately does **not** own the rest of the write. The codec and level
(``compression.resolve_codec``), the contents of the ``geo`` block
(``write_strategies.base.build_geo_metadata``), the null-vs-default CRS rule
(``crs_utils.apply_output_crs``), bbox coverings and geometry-type computation
all already have a single owner each, and pulling them in here would turn a
facade into a rewrite. The line is: this module decides what the paths were
*disagreeing* about; the existing owners keep what they already own. In
particular :func:`resolve_input_crs` only *finds* the input's CRS; what a write
does with it -- omit it when it is the default, strip a stale one, refuse to
overwrite an explicit null -- stays with ``apply_output_crs``.

"Only finds" is not "only informs one place", though: a non-``None`` answer
where the write previously had ``None`` moves three things at once.
``apply_output_crs`` writes an explicit ``crs`` into the ``geo`` block,
``crs_utils._wrap_query_with_crs`` wraps the query in ``ST_SetCRS`` so DuckDB
stamps the CRS into the Parquet ``GEOMETRY`` logical type (``common._plain_copy_to``
and both ``write_strategies/duckdb_kv`` writers), and
``geoarrow_encoding`` attaches it to the Arrow extension type it emits. The
``ST_SetCRS`` wrap is the load-bearing one: without it DuckDB restates whatever
the column it read carried, which is how ``gpio sort quadkey`` stamped
``OGC:CRS84`` onto EPSG:5070 data.
"""

from __future__ import annotations

import math
from collections.abc import Collection, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast, overload

from geoparquet_io.core.compression import resolve_codec
from geoparquet_io.core.crs_utils import (
    PARQUET_UNKNOWN_CRS,
    extract_crs_from_parquet,
    source_crs_is_unknown,
)
from geoparquet_io.core.duckdb_utils import _strip_trailing_order_by, build_kv_metadata_clause
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_type import detect_geoparquet_file_type
from geoparquet_io.core.geo_metadata import (
    DEFAULT_GEOPARQUET_VERSION,
    GEOPARQUET_VERSIONS,
    OVERVIEWS_KEY,
    carried_version,
    geoarrow_primary_encoding,
    sanitized_carried_geo,
    sanitized_carried_geo_of_file,
)
from geoparquet_io.core.geoarrow_encoding import is_geoarrow_extension_field
from geoparquet_io.core.logging_config import debug, info, success
from geoparquet_io.core.sizing import mb_to_bytes

if TYPE_CHECKING:
    import duckdb
    import pyarrow as pa

# The unit DuckDB's Parquet writer emits row groups in.
#
# A ``ROW_GROUP_SIZE`` request is not honoured literally: the writer flushes
# whole vectors, and it rounds the request *up* to a multiple of this -- a
# strict ceiling, never a nearest. Measured, not assumed
# (tests/test_sort_row_group_default.py writes files and reads the footers
# back): a request of 3,000 rows lands at 4,096 rather than the nearer 2,048,
# 9,000 lands at 10,240 rather than the nearer 8,192, 50,000 lands at 51,200,
# and 100,000 at 100,352. The same suite asserts this equals DuckDB's own
# published ``duckdb.__standard_vector_size__``.
WRITER_VECTOR_ROWS = 2_048

# The top of the spatial row-count band gpio's own advice quotes: the
# 10,000-50,000 rows per group that ``gpio check`` prints and that
# ``check_optimization`` scores.
#
# This is a copy of ``check_parquet_structure.SPATIAL_ROW_COUNT_RANGE[1]``, and
# a deliberate one: that module imports this one for
# DEFAULT_ROW_GROUP_ROWS, so importing the band back the other way is an
# import cycle (``lint-imports`` would refuse it).
# ``TestWriterVectorAlignment.test_the_local_band_top_matches_the_band_check_uses``
# asserts the two agree, so the copy cannot drift.
SPATIAL_BAND_TOP_ROWS = 50_000


def align_to_writer_vector(rows: int, band_top: int = SPATIAL_BAND_TOP_ROWS) -> int:
    """Snap a row-group row count to a whole writer vector, as the writer would.

    The writer quantises the request anyway; doing it here means gpio knows
    what will land, and can say so. The rule is therefore the writer's own --
    round *up* to a whole vector -- with a single exception, so that gpio's own
    advice does not contradict itself:

    * A request whose ceiling stays at or below ``band_top`` gets that ceiling,
      which is exactly the file the writer would have produced unaided.
    * A request that is itself inside the band but whose ceiling would leave it
      (anything from 49,153 to 50,000) snaps *down* to 49,152 instead -- the
      largest whole vector the band allows. This is #961: ``gpio sort`` asked
      for the band's top of 50,000, the writer wrote 51,200, and
      ``gpio check optimization`` then failed the file gpio had just written.

    Rounding to the *nearest* vector was tried and reverted. Nearest differs
    from ceiling wherever the fractional part is under a half, and in that
    window it rounds down: ``--row-group-size 9000`` became 8,192, below the
    band's 10,000-row floor, which is the same defect moved to the other end of
    the band.

    The residual behaviour, stated out loud: 49,153-50,000 is the only input
    range where gpio writes *fewer* rows than were asked for, and above the
    band gpio rounds up just like the writer, so ``--row-group-size 100000``
    yields 100,352 rather than being dragged back into the band. The result is
    monotonic and never below one vector -- the writer's own minimum row-group
    size.
    """
    ceiling = max(1, math.ceil(rows / WRITER_VECTOR_ROWS)) * WRITER_VECTOR_ROWS
    if rows <= band_top < ceiling:
        return max(WRITER_VECTOR_ROWS, ceiling - WRITER_VECTOR_ROWS)
    return ceiling


# The row-group size every gpio write uses when neither --row-group-size nor
# --row-group-size-mb is given (#775, #971, #981).
#
# gpio's own advice -- printed by ``gpio check``, scored by
# ``check_optimization``, repeated in the guide -- is 10,000-50,000 rows per
# group for spatial workloads. This is the top of that band as the writer can
# actually express it: 24 whole vectors, the largest groups the band allows, so
# bounding boxes stay tight enough to prune without multiplying per-group footer
# overhead.
#
# It was 50,000 until #961. That is the band's literal top, but no file can have
# it: the writer rounded the request up to 51,200, so ``gpio sort`` produced
# files that ``gpio check optimization`` scored ``[fail]`` on its row-group
# factor and told the user to re-partition. 49,152 is a whole number of vectors,
# so the writer passes it through unchanged and the number gpio asks for is the
# number that lands. It is derived rather than typed, so the default is by
# construction whatever the alignment rule makes of the band's top.
#
# Until this module became the facade it was named DEFAULT_SORT_ROW_GROUP_ROWS
# and applied to ``gpio sort`` alone, on the reasoning that only sorted files are
# read by spatial predicates. Measured, that reasoning did not survive: every
# other command fell through to whatever its writer happened to default to --
# DuckDB's 122,880 on the COPY paths (#981), pyarrow's 100,000 on the Arrow ones
# -- so gpio shipped three different answers and ``gpio check optimization``
# failed files gpio had just written (#972). One number, one owner.
DEFAULT_ROW_GROUP_ROWS = align_to_writer_vector(SPATIAL_BAND_TOP_ROWS)  # 49,152


@overload
def resolve_row_group_rows(
    row_group_rows: None,
    row_group_size_mb: None,
    param_name: str = ...,
    mb_param_name: str = ...,
) -> int: ...


@overload
def resolve_row_group_rows(
    row_group_rows: int | None,
    row_group_size_mb: float | None,
    param_name: str = ...,
    mb_param_name: str = ...,
) -> int | None: ...


def resolve_row_group_rows(
    row_group_rows: int | None,
    row_group_size_mb: float | None,
    param_name: str = "--row-group-size",
    mb_param_name: str = "--row-group-size-mb",
) -> int | None:
    """Decide how many rows a row group gets. The facade's first decision.

    This is the one place a row-group count is decided: an explicit row count,
    else an explicit MB target (measured from the data by
    :func:`resolve_row_group_rows_for_table` or
    :func:`resolve_row_group_rows_for_query`, which call this first), else
    :data:`DEFAULT_ROW_GROUP_ROWS`. A write path never picks a number of its
    own, and never leaves the choice to whichever writer it reaches.

    An explicit row count wins, and an explicit ``--row-group-size-mb`` target
    is left alone (it sizes groups by bytes, and forcing a row count here would
    override the option the user actually passed). Only when neither is given
    does :data:`DEFAULT_ROW_GROUP_ROWS` apply -- previously that case fell
    through as ``None`` and whichever writer the path happened to reach picked
    for itself.

    An explicit row count is snapped to a whole writer vector, because the
    writer would otherwise round it up itself and land somewhere the caller did
    not ask for (#961). The adjustment is announced rather than made silently:
    a request that is already a whole number of vectors passes through without
    a word, and one that is not gets a one-line note naming both the value
    asked for and the value that will be written. Calling this twice on one
    write is therefore harmless and silent -- the sort commands resolve early so
    they can reject a bad value before doing any work, and the write funnel
    resolves again for the paths that do not.

    A request of one whole vector or less is passed through untouched, because
    below that threshold alignment can only do harm. Measured on both writers
    with a 120,000-row input: DuckDB's ``COPY`` quantises a sub-vector request
    up to 2,048 whatever gpio does (1, 10, 100, 1,000 and 2,047 all produce
    2,048-row groups), so rounding first changes nothing there -- while
    ``pq.write_table`` honours the request exactly (10 gives 12,000 groups of
    10). Rounding a sub-vector request would therefore be gpio changing the
    output on the only writer that could have obeyed it, which is the inverse of
    what the alignment is for. ``align_to_writer_vector`` itself keeps its
    ceiling-everywhere rule: it answers "what would the DuckDB writer do", and
    that answer is still 2,048.

    Passing the request through is not the same as pretending it lands. A
    DuckDB-backed write still produces 2,048-row groups for a request of 249,
    and used to do it without a word, so the user read 249 in the command and
    2,048 in the footer (#986). :func:`note_duckdb_copy_rounding` says it, at
    the funnels that know the write reaches DuckDB's ``COPY``.

    A row count below 1 is rejected, not clamped: ``sort str`` raised on
    ``--row-group-size -5`` while the other three quietly wrote 2,048-row groups.
    ``param_name`` is what that rejection blames. One function now serves both
    front ends, so the default names the CLI flag and ``Table.write`` passes its
    own keyword instead -- a Python caller never typed ``--row-group-size`` and
    should not be sent looking for it in their code.

    A byte target of zero or less is rejected on the same footing. ``0MB``
    parses, and used to mean two different things on two branches of one
    command: "a target was given" here (so no default), and "no target" at the
    writer (``0.0`` is falsy), which handed ``pq.write_table`` a ``None`` and
    got one row group for the whole file -- the #1021 shape, surviving the fix
    for #1021 through the one value nobody had typed.
    """
    if row_group_rows is not None and row_group_rows < 1:
        raise InvalidParameterError(param_name, "must be at least 1")
    if row_group_size_mb is not None and row_group_size_mb <= 0:
        raise InvalidParameterError(mb_param_name, "must be greater than 0")
    if row_group_rows is None and row_group_size_mb is None:
        return DEFAULT_ROW_GROUP_ROWS
    if row_group_rows is None:
        return None
    if row_group_rows <= WRITER_VECTOR_ROWS:
        return row_group_rows

    aligned = align_to_writer_vector(row_group_rows)
    if aligned != row_group_rows:
        info(_rounding_note(row_group_rows, aligned))
    return aligned


def estimate_row_size(table) -> int:
    """Bytes per row, as well as an in-memory Arrow table can say.

    ``get_total_buffer_size()`` first (it counts every buffer), then
    ``nbytes``, then 100 bytes for a table-like object that answers neither.
    The floor is 1: a zero would make the division below a ``ZeroDivisionError``
    rather than a big row group.

    A top-level ``large_string``/``large_binary``/``large_list`` column is
    counted at 32-bit offsets. DuckDB hands its results back in the large
    types and pyarrow reads a file in the small ones, so the same rows weighed
    12 bytes more per row through ``gpio convert`` (a DuckDB sample) than
    through ``Table.write`` (a pyarrow table) with three variable-width
    columns, and one ``--row-group-size-mb`` landed on different groups (#1215).
    """
    default_row_size = 100
    num_rows = max(1, table.num_rows)

    for accessor in ("get_total_buffer_size", "nbytes"):
        try:
            value = getattr(table, accessor)
            total_bytes = value() if callable(value) else value
            if total_bytes > 0:
                return max(1, total_bytes // num_rows - _large_offset_surplus(table))
        except Exception:
            continue

    return default_row_size


def _large_offset_surplus(table) -> int:
    """Bytes per row a table's 64-bit offsets add over the 32-bit ones."""
    import pyarrow as pa

    schema = getattr(table, "schema", None)
    if not isinstance(schema, pa.Schema):
        return 0
    large = 0
    for field in schema:
        arrow_type = field.type
        while isinstance(arrow_type, pa.BaseExtensionType):
            arrow_type = arrow_type.storage_type
        if (
            pa.types.is_large_string(arrow_type)
            or pa.types.is_large_binary(arrow_type)
            or pa.types.is_large_list(arrow_type)
        ):
            large += 1
    return 4 * large


#: Rows sampled from a query to measure bytes per row for an MB target. Large
#: enough to be representative, small enough to stay cheap.
_MB_ESTIMATE_SAMPLE_ROWS = 20_000


def _rows_for_byte_target(sample, row_group_size_mb: float, verbose: bool = False) -> int:
    """Turn an MB target into a row count from ``sample``'s bytes per row.

    The count is snapped to a whole writer vector the way an explicit row count
    is (:func:`align_to_writer_vector`), so every writer lands on the same
    groups: DuckDB's ``COPY`` rounds a request up to a whole vector by itself,
    and pyarrow honours one exactly, and they used to answer one MB target with
    two different files (#1215). It floors at one vector: a target below what
    one vector of these rows weighs (``1KB`` against 110-byte rows) would size
    groups of a handful of rows -- 8,572 seven-row groups from 60,000 rows --
    which no reader wants and DuckDB's ``COPY`` would not write anyway.
    """
    bytes_per_row = estimate_row_size(sample)
    rows = max(WRITER_VECTOR_ROWS, int(mb_to_bytes(row_group_size_mb) // bytes_per_row))
    rows = align_to_writer_vector(rows)
    if verbose:
        debug(
            f"Resolved --row-group-size-mb {row_group_size_mb} to {rows:,} rows/group "
            f"(~{bytes_per_row} bytes/row)"
        )
    return rows


def resolve_row_group_rows_for_table(
    table,
    row_group_rows: int | None,
    row_group_size_mb: float | None,
    param_name: str = "--row-group-size",
    mb_param_name: str = "--row-group-size-mb",
    verbose: bool = False,
) -> int:
    """:func:`resolve_row_group_rows`, with an MB target turned into rows.

    ``resolve_row_group_rows`` deliberately answers ``None`` for an explicit
    ``--row-group-size-mb``, because a *byte* target can only become a row count
    once there is a table to measure. This is that second step, and it lives
    here rather than at each write because a path that does it for itself
    invents a second answer to the facade's first question -- which is exactly
    how ``gpio extract bigquery`` came to hand ``pq.write_table`` a byte count
    where a row count goes, asking for 134,217,728 rows per group and writing
    one row group for the whole file (#1021).

    The answer is always a whole writer vector (or the caller's own count). An
    MB target against an empty table has nothing to measure and no rows to
    group, so it gets the default. It is not capped at the table's row count:
    a bigger group is still one group, and a capped count that is not a whole
    vector would make the next resolve along the write announce a rounding
    nobody asked for.
    """
    resolved = resolve_row_group_rows(row_group_rows, row_group_size_mb, param_name, mb_param_name)
    if resolved is not None:
        return resolved
    if table.num_rows <= 0:
        return DEFAULT_ROW_GROUP_ROWS
    # resolve answers None only for an MB target, so there is one here.
    return _rows_for_byte_target(table, cast(float, row_group_size_mb), verbose)


def resolve_row_group_rows_for_query(
    con: duckdb.DuckDBPyConnection,
    query: str,
    row_group_rows: int | None,
    row_group_size_mb: float | None,
    verbose: bool = False,
) -> int:
    """:func:`resolve_row_group_rows_for_table` for a write that has a query, not a table.

    DuckDB's ``ROW_GROUP_SIZE`` is a row count (``ROW_GROUP_SIZE_BYTES`` needs
    insertion order off, which would undo a spatial sort), so an MB target is
    measured on a sample of the query's first rows. The sample drops a trailing
    ``ORDER BY`` so the ``LIMIT`` streams instead of sorting (and, for a remote
    input, re-reading) the whole source just to size row groups.

    Every DuckDB write path asks here: the plain-``COPY`` fast path a 2.0 input
    takes used to skip the measurement and write DuckDB's own 122,880-row
    default for any MB target (#1215). A query that cannot be sampled, or that
    returns no rows, gets the default.
    """
    resolved = resolve_row_group_rows(row_group_rows, row_group_size_mb)
    if resolved is not None:
        return resolved
    sample_query = _strip_trailing_order_by(query)
    try:
        sample = (
            con.sql(f"SELECT * FROM ({sample_query}) LIMIT {_MB_ESTIMATE_SAMPLE_ROWS}")
            .arrow()
            .read_all()
        )
    except Exception as exc:  # pragma: no cover - defensive, fall back to the default
        debug(f"Could not sample rows for the --row-group-size-mb estimate: {exc}")
        return DEFAULT_ROW_GROUP_ROWS
    if sample.num_rows == 0:
        return DEFAULT_ROW_GROUP_ROWS
    # resolve answers None only for an MB target, so there is one here.
    return _rows_for_byte_target(sample, cast(float, row_group_size_mb), verbose)


def resolve_row_group_rows_for_rewrite(num_rows: int, input_row_groups: int) -> int:
    """Rows per group for a rewrite that must not lose the input's row groups.

    ``check --fix`` rewrites a file so gpio's own checks pass it next, and the
    spatial-order check only judges a file with at least eight row groups. The
    default alone turned a 12-group file of 60,000 rows into two groups, and a
    file the check could fail into one it could not judge (#1098). So the
    default applies whenever it already keeps the input's count; otherwise the
    groups shrink to the largest whole writer vector that keeps it, never below
    one vector (DuckDB's ``COPY`` writes no smaller group).
    """
    if input_row_groups <= 1 or math.ceil(num_rows / DEFAULT_ROW_GROUP_ROWS) >= input_row_groups:
        return DEFAULT_ROW_GROUP_ROWS
    per_group = num_rows // input_row_groups
    return max(WRITER_VECTOR_ROWS, per_group // WRITER_VECTOR_ROWS * WRITER_VECTOR_ROWS)


def _rounding_note(requested: int, landed: int) -> str:
    """The one sentence gpio says when a row-group request does not land.

    One string, so the note the sort commands print when *gpio* snaps a value
    and the note the DuckDB funnels print when the *writer* does are the same
    sentence. They are the same fact about the same file.
    """
    return (
        f"Writing {landed:,} rows per row group rather than the requested "
        f"{requested:,}: the Parquet writer emits row groups in whole "
        f"{WRITER_VECTOR_ROWS:,}-row vectors, so {requested:,} is not a "
        "size a row group can have."
    )


def note_duckdb_copy_rounding(row_group_rows: int | None) -> None:
    """Say what DuckDB's ``COPY`` will make of an already-resolved row count.

    :func:`resolve_row_group_rows` announces every value *it* moves, so by the
    time a write funnel calls this the only request still able to surprise
    anyone is a sub-vector one: resolve passes 1-2,047 through untouched
    because ``pq.write_table`` honours it exactly, and DuckDB's ``COPY`` then
    rounds it up to 2,048 regardless. The note therefore fires for exactly that
    window, and calling it after resolve is silent for everything else --
    including a resolve that already spoke, whose value is a whole vector by
    construction.

    Which writer a request reaches is not something the facade can see, so this
    is deliberately a *funnel* decision rather than part of resolve. Measured on
    a 10,000-row input with ``--row-group-size 249``: the plain ``COPY`` fast
    path and the ``duckdb-kv`` strategy both write 2,048-row groups, while
    ``in-memory``, ``streaming`` and ``disk-rewrite`` write 249-row groups --
    ``disk-rewrite`` because its pyarrow pass regroups the ``COPY`` output at
    the requested size afterwards. Call this only where DuckDB's ``COPY`` has
    the last word on the file the user gets.

    ``None`` -- no row count asked for, or only a ``--row-group-size-mb``
    target -- says nothing: naming a row count the user never typed would be
    the inverse of the fix (#986).
    """
    if row_group_rows is None:
        return
    landed = align_to_writer_vector(row_group_rows)
    if landed != row_group_rows:
        info(_rounding_note(row_group_rows, landed))


def resolve_output_geoparquet_version(
    requested: str | None,
    *,
    input_file: str | None = None,
    original_metadata: Mapping | None = None,
    table: pa.Table | None = None,
    source_version: str | None = None,
    verbose: bool = False,
) -> str:
    """Decide which GeoParquet version the output declares. The second decision.

    This is the only place an output version is decided (#1367 concept 1). An
    explicit request always wins. Otherwise this is auto mode, which the
    shared ``--geoparquet-version`` help documents as "preserves 2.0; 1.x
    inputs write 1.1; bare native geo types upgrade to 2.0", asked of the
    witnesses in this order:

    1. ``input_file`` -- the file itself (:func:`_version_of_file`);
    2. ``original_metadata`` -- the carried ``geo`` key
       (:func:`carried_geoparquet_version`);
    3. ``table`` -- an Arrow table's own schema: its carried key, else a
       GeoArrow column with no ``geo`` key is native data and writes 2.0, the
       same answer the file path gives a native-geo-only file (approved
       change 10; the table path used to say parquet-geo-only);
    4. ``source_version`` -- an answer already reached for the file the data
       was read from (``Table.write`` remembers it from ``gpio.read``);
    5. ``DEFAULT_GEOPARQUET_VERSION``.

    The default is part of the decision, so no caller adds an ``or "1.1"`` of
    its own: before #1367 ``extract``, ``sort hilbert``, ``Table.write`` and
    the query funnel each did, and the strategies each re-detected a table's
    version with a copy of step 3 that disagreed with the file path.

    Whether the output may declare a bbox ``covering`` is a separate decision,
    gated on the struct's shape by ``geo_metadata.bbox_column_to_declare``,
    never on the version chosen here.

    The file is the better witness than its ``geo`` key because the
    interesting case is the one with no ``geo`` key: a native-geo-only input
    resolved from its metadata alone returned nothing, the write defaulted to
    1.1, and the geometry was rewritten as plain WKB with its native logical
    type stripped (#600).

    ``input_file`` must be the file whose *rows* the write will read, or a file
    that is lossless with respect to it. Handing this the user's input while
    reading the data from a scratch rewrite that dropped the native type and
    the CRS produced the worst of both: a native 2.0 output declaring the
    default CRS over projected data, which reads as an assertion rather than an
    omission and which ``gpio check spec`` then blesses. ``gpio sort quadkey``
    and the five index-adding ``gpio partition`` drivers did exactly that until
    #993 made their scratch write pass the same witness, so the intermediate is
    now lossless with respect to the user's file. See ``partition/staging.py``.

    The same witness answers :func:`resolve_input_crs`, and it has to: this
    function is what makes the output native, and a native output states its
    CRS in two places that must agree.
    """
    if requested is not None:
        return requested

    detected = (
        (_version_of_file(input_file, verbose) if input_file else None)
        or carried_geoparquet_version(original_metadata)
        or (_version_of_table(table, verbose) if table is not None else None)
        or source_version
    )
    if detected:
        if verbose:
            debug(f"Auto-detected GeoParquet version from input: {detected}")
        return detected
    return DEFAULT_GEOPARQUET_VERSION


def carried_geoparquet_version(metadata: Mapping | None) -> str | None:
    """The version a carried ``geo`` key resolves to: ``"1.1"``, ``"2.0"`` or None.

    Every 1.x reads as 1.1 (backwards compatible, and the version the writer
    knows) and every 2.x as 2.0, the highest 2.x this writer knows -- the same
    two answers :func:`_version_of_file` gives a file. Anything else -- no
    ``geo`` key, no ``version``, a version that is not a string (``2``,
    ``["2.0"]``: guarded by ``carried_version`` rather than crashing
    ``.split``, #979), or a major this writer cannot write -- is None, so the
    caller's next witness answers. (The metadata path used to hand a ``3.x``
    through as ``"3.0"``, a version no writer table knows; the file and table
    paths already read it as no version.)
    """
    return _writable_version(carried_version(sanitized_carried_geo(metadata).get("version")))


def _writable_version(declared: str | None) -> str | None:
    """A declared ``geo.version`` as the version this writer writes for it (1.x -> 1.1, 2.x -> 2.0)."""
    parts = declared.split(".") if declared else []
    if len(parts) < 2:
        return None
    return {"1": "1.1", "2": "2.0"}.get(parts[0])


def auto_version_upgrade(input_file: str) -> str | None:
    """The version auto mode writes for *input_file*, when it differs from the one it declares.

    A verbatim copy of a file keeps the version it declares, so a command that
    may answer with a copy (``add bbox`` over an input that already has its
    bbox column, #728) asks this first: a 1.0 file is written at 1.1 by auto
    mode like every other command (approved change 7), so it cannot be copied.
    A block that declares the version auto mode writes, or no version at all
    (a native-geo-only file has no ``geo`` key), can be: None.
    """
    declared = carried_version(sanitized_carried_geo_of_file(input_file).get("version"))
    target = _writable_version(declared)
    if target is None or GEOPARQUET_VERSIONS[target]["metadata_version"] == declared:
        return None
    return target


def declared_geoparquet_version(geo_meta: Mapping) -> object:
    """The ``version`` a decoded ``geo`` block declares, exactly as declared ("" when absent).

    For a refusal or a piece of advice that names the *input's* version
    (``add bbox-metadata`` refusing a 1.0 file, ``add bbox``'s advice); never
    for choosing the output's, which is :func:`resolve_output_geoparquet_version`.
    """
    return geo_meta.get("version", "")


def preserved_geoparquet_version(file_type: str, declared: str | None) -> str | None:
    """The version a repair of a file writes: the file's own, exactly.

    ``check --fix`` rewrites a file to repair it, not to convert it, so unlike
    auto mode it keeps a 1.0 file at 1.0 and a native-geo-only file without a
    ``geo`` key. ``file_type`` is ``detect_geoparquet_file_type``'s answer and
    ``declared`` the file's declared ``geo.version``. None for a file that is
    not GeoParquet, which the writer then decides by auto mode.
    """
    if file_type == "geoparquet_v2":
        return "2.0"
    if file_type == "parquet_geo_only":
        return "parquet-geo-only"
    if file_type == "geoparquet_v1":
        return "1.0" if (declared or "").startswith("1.0") else "1.1"
    return None


def _version_of_file(parquet_file: str, verbose: bool = False) -> str | None:
    """What a parquet file says about its version: ``2.0``, ``1.1``, ``1.1-geoarrow`` or None.

    1.x normalizes to 1.1, matching :func:`carried_geoparquet_version`, and a
    native-geo-only file upgrades to 2.0. None when the file cannot be
    inspected (a remote without credentials) or says nothing, so the next
    witness answers.
    """
    try:
        file_type = detect_geoparquet_file_type(parquet_file, verbose).get("file_type")
    except Exception as e:
        debug(f"Version auto-detect failed for {parquet_file}: {e}; falling back to default")
        return None

    if file_type == "geoparquet_v1" and _declares_geoarrow_encoding(parquet_file):
        # A GeoArrow-native 1.1 file declares version 1.1.0 just as a WKB one
        # does, so "1.1" is the version but not the *encoding*: writing it meant
        # passing the nested-list geometry through under `encoding: WKB`, a block
        # contradicting the schema beside it (#1176). "1.1-geoarrow" is the one
        # version that preserves what the input actually is.
        return "1.1-geoarrow"
    return {"geoparquet_v2": "2.0", "parquet_geo_only": "2.0", "geoparquet_v1": "1.1"}.get(
        file_type
    )


def _declares_geoarrow_encoding(parquet_file: str) -> bool:
    """Whether the file's ``geo`` block declares native GeoArrow for its primary column."""
    try:
        return geoarrow_primary_encoding(sanitized_carried_geo_of_file(parquet_file)) is not None
    except Exception as e:  # unreadable footer: the version decision stands as made
        debug(f"GeoArrow encoding check failed for {parquet_file}: {e}")
        return False


def _version_of_table(table: pa.Table, verbose: bool = False) -> str | None:
    """What an Arrow table's own schema says about its version.

    A carried ``geo`` key answers through :func:`carried_geoparquet_version`.
    With no key, a GeoArrow column -- read off the field, not just the type,
    so a metadata-declared one counts whether or not ``geoarrow.pyarrow`` was
    imported (#792) -- is native data, which writes 2.0.
    """
    metadata = table.schema.metadata
    if metadata and b"geo" in metadata:
        return carried_geoparquet_version(metadata)
    if any(is_geoarrow_extension_field(field) for field in table.schema):
        if verbose:
            debug("Native geo types with no geo key: writing GeoParquet 2.0")
        return "2.0"
    return None


def resolve_input_crs(
    input_crs: dict | str | None,
    *,
    input_file: str | None = None,
    geometry_column: str | None = None,
    verbose: bool = False,
) -> dict | str | None:
    """Decide which CRS the output describes its geometry with. The third decision.

    An output with no geometry column describes no geometry, so the question has
    no subject and the answer is ``None`` -- before any witness is consulted, and
    whatever the caller named. A column projection can drop the geometry column
    (``gpio extract geoparquet --exclude-cols geometry``), and answering anyway
    handed ``crs_utils._wrap_query_with_crs`` a CRS with no column to
    ``ST_SetCRS``, which raises rather than writes.

    A caller that names a CRS otherwise wins -- ``gpio convert reproject`` passes
    the CRS it is transforming *to*, and ``gpio convert`` the one a non-Parquet
    source's geometry is in (``--crs``, for CSV/TSV input). Both are facts about
    the output that no reading of the input could supply. Everything else is a
    rewrite that keeps
    its input's coordinates, and for those the input file is the witness, the
    same one :func:`resolve_output_geoparquet_version` consults.

    Without this, a rewrite answered the CRS question from whatever ``geo``
    block it happened to carry. A native-geo-only input has none, so nothing
    described the output's CRS at all -- while the version question, answered
    from the file, made that output *native 2.0*. The two answers then landed in
    two different places: EPSG:5070 in the Parquet ``GEOMETRY`` logical type
    (DuckDB writes it there from the column it read) and nothing in the ``geo``
    block, which GeoParquet reads as ``OGC:CRS84``. One file, two CRSs, and
    ``gpio check spec`` printing ``✗ CRS in geo metadata must match CRS in
    Parquet schema`` over it (#993).

    It also repairs a quieter loss on the same inputs: an explicit
    ``--geoparquet-version 1.1`` has nowhere but the ``geo`` block to put a CRS,
    so before this the 1.1 rewrite of a native-geo-only file declared no CRS
    anywhere.

    ``extract_crs_from_parquet`` returns ``None`` for the default CRS and for an
    unknown one. The two are told apart here: an input whose CRS is unknown
    (``crs: null``, an empty value, or a ``srid:0`` type with no block) answers
    :data:`crs_utils.PARQUET_UNKNOWN_CRS`, so the output says "unknown" in the
    ``geo`` block and in its type rather than nothing, which means OGC:CRS84
    (#1276). Any other miss leaves ``input_crs`` ``None``. A file
    that cannot be inspected (remote without credentials, a path that is not
    Parquet) is a miss too, not an error: the write worked before this function
    existed and must keep working.
    """
    if not geometry_column:
        return None
    if input_crs is not None:
        return input_crs
    if not input_file:
        return None

    try:
        # RAW path: extract_crs_from_parquet escapes its own argument (#718).
        # The geometry column says which native column's CRS a block-less
        # (native-geo-only) input means: in schema order the first CRS can be
        # a secondary's.
        crs = extract_crs_from_parquet(input_file, verbose=verbose, geometry_column=geometry_column)
        if crs is None and source_crs_is_unknown(input_file, geometry_column):
            # An unknown CRS is a fact about the output too (#1276).
            return PARQUET_UNKNOWN_CRS
        return crs
    except Exception as exc:  # pragma: no cover - defensive, see docstring
        debug(f"CRS auto-detect failed for {input_file}: {exc}; leaving the CRS unstated")
        return None


# Schema-metadata keys that describe the *input's* schema rather than the
# output's, so they must never ride along into a write.
#
# `geo` would declare the input's GeoParquet version over output the writer has
# just given a different shape; `ARROW:schema` and `pandas` are serialized
# descriptors that pyarrow writes through verbatim rather than regenerating,
# leaving the output naming the input's columns and CRS. A stale `pandas` block
# also relabels rows: after a sort it hands back the input's index labels in the
# input's order, and a stale `datetimetz` entry shifts a naive timestamp by the
# zone's offset (#710, #1268).
#
# One definition, used by every place that needs it -- the facade below, the
# parquet-geo-only path in arrow_geo_metadata (which sees `bytes` keys, straight
# off a pyarrow schema) and write_parquet_with_metadata's preserved-keys loop
# (which sees them decoded). They were separate literals; a key added to one and
# not the other is a silent metadata leak, so the bytes form is derived rather
# than written out again. It lives here because the facade applies it to every
# Arrow-side write.
#
# `geo:overviews` describes the file's own row-group layout (OVERVIEWS_SPEC
# 3.1 calls it authoritative), so a rewrite that regroups or filters rows must
# not carry it: an honest plain file with a `level` column beats a footer that
# lies about where each level ends. The writer of a levelled file mints a fresh
# one; nothing else may forward it.
_CARRIED_SCHEMA_METADATA_KEYS = frozenset({"geo", "ARROW:schema", "pandas", OVERVIEWS_KEY})
_CARRIED_SCHEMA_METADATA_KEYS_BYTES = frozenset(
    key.encode("utf-8") for key in _CARRIED_SCHEMA_METADATA_KEYS
)


def _as_bytes(value: str | bytes) -> bytes:
    return value.encode("utf-8") if isinstance(value, str) else value


def output_schema_metadata(
    metadata: Mapping[str | bytes, str | bytes] | Mapping[bytes, bytes] | None,
    geoparquet_version: str | None,
    extra_kv_metadata: Mapping[str, str] | Mapping[str | bytes, str | bytes] | None = None,
) -> dict[bytes, bytes]:
    """The file-level KV metadata an output gets from its table's metadata.

    * The input's ``pandas``, ``ARROW:schema`` and ``geo:overviews`` are
      dropped: they describe the input's rows, not the output's (#710, #1268).
      pyarrow regenerates ``ARROW:schema`` from what is left.
    * ``geo`` is kept, because by the time a write gets here the table carries
      the block that write built. ``parquet-geo-only`` drops it too, as it
      means "carry no GeoParquet metadata".
    * Every other key (fiboa, vecorel, STAC, provenance) survives (#690), and
      ``extra_kv_metadata`` is merged in last, so the caller's value wins.

    Keys and values come back as ``bytes``, ready for
    ``replace_schema_metadata`` or ``with_metadata``.
    """
    dropped = _CARRIED_SCHEMA_METADATA_KEYS_BYTES
    if geoparquet_version != "parquet-geo-only":
        dropped = dropped - {b"geo"}

    result = {
        _as_bytes(key): _as_bytes(value)
        for key, value in (metadata or {}).items()
        if _as_bytes(key) not in dropped
    }
    for key, value in (extra_kv_metadata or {}).items():
        result[_as_bytes(key)] = _as_bytes(value)
    return result


def apply_output_kv_metadata(
    table: pa.Table,
    geoparquet_version: str | None,
    extra_kv_metadata: dict[str, str] | None = None,
) -> pa.Table:
    """Give ``table`` its output's file-level KV metadata. The fourth decision.

    The rules are :func:`output_schema_metadata`'s; this is the table form. It
    is zero-copy: only the schema metadata is replaced.

    The three Arrow strategies each carried their own copy of the merge half and
    none of them had the drop half: ``write_from_table`` guards its metadata
    work on the geometry column being present, so a table whose geometry had
    been projected away was written with ``table.schema`` verbatim -- carrying a
    ``geo`` key naming a ``primary_column`` the output does not have, and
    declaring a GeoParquet version for a file explicitly asked to declare none
    (#773). The same hole let a parquet-geo-only write *with* geometry keep the
    input's carried block on the streaming path. Copying the rest of the
    metadata verbatim also carried the input's ``pandas`` block into every
    in-memory and streaming output (#1268).

    Applied at the top of each strategy's ``write_from_table``, before any
    early return can skip it, and again, with the same extras, by
    ``write_arrow_table`` just before ``pq.write_table``, so the
    Arrow-table funnel and every helper that writes through it get the same
    answer.
    """
    metadata = output_schema_metadata(table.schema.metadata, geoparquet_version, extra_kv_metadata)
    return table.replace_schema_metadata(metadata)


def geoparquet_copy_options(duckdb_version: str, kv_pairs: Mapping[str, str] | None) -> list[str]:
    """The ``GEOPARQUET_VERSION`` and ``KV_METADATA`` options of one DuckDB ``COPY``.

    The fourth decision, for a write that hands DuckDB a ``geo`` block of its
    own. DuckDB's writer with ``GEOPARQUET_VERSION`` ``'V1'``, ``'V2'`` or
    ``'BOTH'`` does not replace a ``geo`` key supplied in ``KV_METADATA``: it
    writes the supplied entry and then appends the one it generated, so the
    footer carries two ``geo`` keys that disagree (one with the ``covering``,
    one with DuckDB's ``crs``) and each reader picks its own (#1225). Only
    ``'NONE'`` writes the supplied entry alone.

    So whenever ``kv_pairs`` holds ``geo``, a ``'V2'`` request becomes
    ``'NONE'``. That is lossless on a native ``GEOMETRY`` column: ``'NONE'``
    governs the ``geo`` key only, and the column keeps its Parquet ``GEOMETRY``
    logical type, the CRS in it and its geospatial statistics.

    A ``'V1'`` or ``'BOTH'`` request with a ``geo`` key is refused rather than
    rewritten. ``'V1'`` turns a ``GEOMETRY`` column into WKB, and ``'NONE'``
    would write it native instead, which a 1.x file must not carry (#712).
    ``'BOTH'`` keeps the column native, but what it promises is DuckDB's own
    dual-readable block, which a supplied block would silently stand in for.
    No caller does this today; a caller that wants it has to cast to ``BLOB``
    itself and ask for ``'NONE'``.

    Args:
        duckdb_version: The ``GEOPARQUET_VERSION`` the write would otherwise use.
        kv_pairs: RAW file-level key/value pairs for ``KV_METADATA``, or None.

    Returns:
        ``["GEOPARQUET_VERSION '...'"]``, plus the ``KV_METADATA`` clause when
        there is anything to write.
    """
    pairs = dict(kv_pairs or {})
    if "geo" in pairs and duckdb_version != "NONE":
        if duckdb_version != "V2":
            raise ValueError(
                f"a supplied geo block cannot be written beside GEOPARQUET_VERSION "
                f"'{duckdb_version}': DuckDB would append its own geo key, and 'NONE' "
                f"would write the geometry column native"
            )
        duckdb_version = "NONE"
    options = [f"GEOPARQUET_VERSION '{duckdb_version}'"]
    kv_clause = build_kv_metadata_clause(pairs)
    if kv_clause:
        options.append(kv_clause)
    return options


def _leaf_paths(name: str, arrow_type) -> Iterator[str]:
    """The Parquet leaf paths an Arrow column is written as, in schema order.

    pyarrow names a dictionary-encoding request by leaf path, so ``bbox`` alone
    would not reach ``bbox.xmin``. Lists and maps use the compliant nested
    names pyarrow writes by default.
    """
    import pyarrow as pa

    while isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    if pa.types.is_dictionary(arrow_type):
        arrow_type = arrow_type.value_type
    if pa.types.is_struct(arrow_type):
        for i in range(arrow_type.num_fields):
            child = arrow_type.field(i)
            yield from _leaf_paths(f"{name}.{child.name}", child.type)
    elif pa.types.is_map(arrow_type):
        yield from _leaf_paths(f"{name}.key_value.key", arrow_type.key_type)
        yield from _leaf_paths(f"{name}.key_value.value", arrow_type.item_type)
    elif (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        yield from _leaf_paths(f"{name}.list.element", arrow_type.value_type)
    else:
        yield name


def dictionary_columns(schema: pa.Schema, geometry_columns: Collection[str] = ()) -> list[str]:
    """The leaf columns a pyarrow write may dictionary-encode: all but the geometry.

    A WKB value is unique per row, so a dictionary over it is a second copy of
    the column plus an index, and the overviews spec (4.5) forbids one (#1122).
    The geometry columns are the ones named here, the ones the schema's ``geo``
    block describes, and every GeoArrow extension field. Pass the result as
    ``use_dictionary`` so every Arrow writer makes the same choice.
    """
    geometry = set(geometry_columns)
    geometry.update((sanitized_carried_geo(schema.metadata).get("columns") or {}).keys())
    geometry.update(field.name for field in schema if is_geoarrow_extension_field(field))
    return [
        path
        for field in schema
        if field.name not in geometry
        for path in _leaf_paths(field.name, field.type)
    ]


def without_geometry_dictionary(
    writer_kwargs: dict, schema: pa.Schema, geometry_columns: Collection[str] = ()
) -> dict:
    """``writer_kwargs`` with ``use_dictionary`` limited to the non-geometry columns.

    The one place a pyarrow ``ParquetWriter`` or ``write_table`` call gets its
    ``use_dictionary``. pyarrow's default encodes every column, so a write that
    builds its own kwargs re-encodes the geometry (#1122).
    ``tests/test_geometry_not_dictionary_encoded.py`` fails on a new pyarrow
    write that does not ask here.
    """
    return {**writer_kwargs, "use_dictionary": dictionary_columns(schema, geometry_columns)}


@dataclass
class ParquetWriteSettings:
    """
    Central configuration for Parquet write best practices.
    Single source of truth for compression, row groups, and other settings.
    """

    #: ``None`` means the owner's default (``compression.resolve_codec``):
    #: ZSTD, at the codec's documented level.
    compression: str | None = None
    compression_level: int | None = None

    DEFAULT_PARQUET_VERSION = "2.6"

    def get_pyarrow_kwargs(
        self,
        row_group_rows: int,
        schema: pa.Schema | None = None,
        geometry_columns: Collection[str] = (),
    ) -> dict:
        """Get kwargs dict for PyArrow write_table().

        ``row_group_rows`` is the count :func:`resolve_row_group_rows_for_table`
        (or one of its siblings) already decided; these settings decide none of
        their own, so there is no fallback here that could skip the
        validation, the vector snap or an MB target. With ``schema``, dictionary encoding is limited to the non-geometry
        columns (:func:`dictionary_columns`). Without it, pyarrow's default
        applies to every column.
        """
        # The owner validates the codec and level and drops a level the codec
        # does not take; it is the only codec list in the package.
        codec_kwargs = resolve_codec(
            self.compression, self.compression_level, context="write"
        ).pyarrow_kwargs()

        kwargs = {
            "row_group_size": row_group_rows,
            **codec_kwargs,
            "write_statistics": True,
            "use_dictionary": (
                True if schema is None else dictionary_columns(schema, geometry_columns)
            ),
            "version": self.DEFAULT_PARQUET_VERSION,
        }

        return kwargs


def write_arrow_table(
    table: pa.Table,
    output_path: str,
    compression: str | None,
    compression_level: int | None,
    row_group_rows: int | None,
    row_group_size_mb: float | None,
    geoparquet_version: str | None,
    geometry_column: str | None,
    verbose: bool = False,
    extra_kv_metadata: dict[str, str] | None = None,
) -> None:
    """Write an Arrow table as the facade says: the one final pyarrow table write.

    Every Arrow-table write ends here -- ``write_funnels.write_geoparquet_table``
    and its geometry-free sibling, and the ``in-memory`` strategy -- so the
    row-group count (:func:`resolve_row_group_rows_for_table`), the codec and
    level (``compression.resolve_codec``), the dictionary policy
    (:func:`dictionary_columns`) and the file-level keys
    (:func:`apply_output_kv_metadata`) are decided once. It moved here from
    ``arrow_geo_metadata._write_table_with_settings``: a module that builds
    ``geo`` blocks had become a second writer beside the funnels.

    The table's ``geo`` block, when it has one, is the caller's: this writes
    what it is given rather than letting geoarrow regenerate it.

    Args:
        table: PyArrow Table to write
        output_path: Output file path
        compression: Compression type (ZSTD, GZIP, etc.)
        compression_level: Compression level
        row_group_rows: Exact number of rows per row group
        row_group_size_mb: Target row group size in MB
        geoparquet_version: GeoParquet version
        geometry_column: Name of the geometry column, or None for a table
            with no geometry
        verbose: Whether to print verbose output
        extra_kv_metadata: The caller's file-level keys. Passed again here so
            this last pass does not drop a caller key named like a carried one
    """
    # The facade's row-group decision, including the MB -> rows sizing. Calling
    # it again after write_geoparquet_table already resolved is harmless and
    # silent; the paths that reach here without resolving still land on the one
    # number rather than pyarrow's own default (#1021).
    rows_per_group = resolve_row_group_rows_for_table(table, row_group_rows, row_group_size_mb)

    # Use central configuration for write settings
    settings = ParquetWriteSettings(compression=compression, compression_level=compression_level)
    write_kwargs = settings.get_pyarrow_kwargs(
        rows_per_group,
        schema=table.schema,
        geometry_columns=[geometry_column] if geometry_column else (),
    )

    if verbose:
        codec = resolve_codec(compression, compression_level, context="write")
        debug(f"Writing with {codec.description} compression")
        debug(f"Row group size: {rows_per_group:,} rows")

    # The facade has the last word on the file-level keys, here at the one
    # place every Arrow-table write reaches: the table funnel and the helpers
    # that call this directly never pass through a strategy (#1268).
    table = apply_output_kv_metadata(table, geoparquet_version, extra_kv_metadata)

    import pyarrow.parquet as pq

    pq.write_table(table, output_path, **write_kwargs)

    if verbose:
        success(f"Wrote {table.num_rows:,} rows to {output_path}")
