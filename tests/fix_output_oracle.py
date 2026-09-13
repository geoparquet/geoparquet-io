"""The oracle every ``gpio check --fix`` output has to satisfy.

``--fix`` is the one command whose whole promise is that the file it leaves
behind is *better* than the file it found. Before this module the suite's 103
``--fix`` invocations each asserted only the metric they had just repaired --
``current_compression == "ZSTD"``, ``num_row_groups == 1``, ``"bbox" in
schema`` -- so a repair that fixed its metric while corrupting the ``geo``
block, dropping a CRS, losing rows or declaring a covering over a column that
cannot legally be one was invisible. That is the "gpio writes a file gpio
rejects" shape of #890, #954, #972 and #1003.

:func:`assert_fix_output_is_sound` asks four independent questions, because
each has shipped a green test past a real defect:

1. **Does gpio's own validator pass the output?** ``check spec`` with zero
   FAILED checks -- compared as an exact set against ``known_spec_failures``,
   so a repair that silently *starts* failing a check is visible and so is one
   that stops.
2. **Are the rows still there?** A rewrite that drops or duplicates rows is a
   data-loss bug no metric assertion can see.
3. **What CRS does the output claim, in each carrier separately?** The ``geo``
   block and the Parquet ``GEOMETRY`` logical type are read independently
   through :mod:`tests.native_geo_probes`, never through
   ``crs_utils.source_crs_string`` (which falls back from one to the other and
   so cannot see them disagree; #997 shipped green under it). Every carrier
   that speaks has to say the same thing.
4. **Does the covering name a column that exists**, with all four corners as
   floats? #1003 was ``check all --fix`` dropping a covering; the mirror image
   is declaring one that points at nothing.

What it does *not* see, by design: row content (the count is preserved, not
the values), an all-null geometry column, and whether the fix did anything --
a ``check spatial --fix`` that leaves the rows unsorted writes a perfectly
valid file. The matrix in ``test_check_fix_output_is_valid.py`` asks each
fix's own check about the output for that, and
:func:`assert_bbox_column_matches_geometry` covers the one metric the
validator never reads: the bbox *values*.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1018 (WP-1)
"""

from __future__ import annotations

import os
from collections.abc import Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from tests.native_geo_probes import (
    DEFAULTED_CRS,
    SILENT_CRS,
    geo_block,
    geo_block_crs_id,
    geo_version,
    logical_crs_id,
    spec_failures,
)

StrPath = str | os.PathLike[str]
#: A PROJJSON ``id`` mapping (``{"authority": "EPSG", "code": 5070}``) or :data:`CRS84`.
CrsId = Mapping[str, object] | str
NormalisedCrs = str | tuple[object, object] | None

#: What both carriers say when they mean "the GeoParquet default".
CRS84 = "OGC:CRS84"

#: ``expected_version_prefix`` sentinel: the output must carry no ``geo`` key at
#: all -- a native-geo-only input has to stay native-geo-only (#1001).
NO_GEO_BLOCK = "<no geo block>"

#: The two fixtures most ``--fix`` tests are built from.
PLACES_ROWS = 766  # tests/data/places_test.parquet: 1.0, CRS84, has a bbox column
BUILDINGS_ROWS = 42  # tests/data/buildings_test.parquet: 1.0, CRS84, no bbox column

#: ``tests/conftest.py::places_v11_file`` declares 1.1.0 but DuckDB writes its
#: geometry with a native Parquet GEOMETRY logical type, which is 2.0-only, so
#: the fixture fails ``check spec`` before any fix touches it (gpio #1037).
#: Carried as an explicit baseline rather than a weakened oracle: a second
#: failure still fails the test, and so does this one going away once the
#: fixture is repaired.
PLACES_V11_FIXTURE_BASELINE = {
    "version_features_match": (
        "tests/conftest.py::places_v11_file declares 1.1.0 while DuckDB gives it "
        "a native Parquet GEOMETRY logical type -- a fixture defect, not a fix "
        "defect (gpio #1037)"
    )
}


def covering_of(path: StrPath, geometry_column: str = "geometry") -> dict | None:
    """One column's declared ``covering``, read off the ``geo`` block and nothing else."""
    columns = (geo_block(path) or {}).get("columns") or {}
    return (columns.get(geometry_column) or {}).get("covering")


def _normalise_crs(raw: object) -> NormalisedCrs:
    """One carrier's answer as a comparable value, or None when it does not speak."""
    if isinstance(raw, Mapping):
        authority, code = raw.get("authority"), raw.get("code")
        if (authority, code) == ("OGC", "CRS84"):
            return CRS84
        return (authority, code)
    if raw in SILENT_CRS:
        return None
    if raw in DEFAULTED_CRS:
        return CRS84
    # "<crs: null -- unknown>" and anything unexpected compare literally.
    return raw


def _resolve_field(schema: pa.Schema, parts: list[str]) -> pa.DataType | None:
    """Walk a ``covering`` path (``["bbox", "xmin"]``) down the real Arrow schema."""
    if not parts or parts[0] not in schema.names:
        return None
    current = schema.field(parts[0]).type
    for part in parts[1:]:
        if not pa.types.is_struct(current):
            return None
        index = current.get_field_index(part)
        if index < 0:
            return None
        current = current.field(index).type
    return current


def _assert_covering_resolves(path: StrPath, covering: Mapping, schema: pa.Schema) -> None:
    """Every corner the covering names must exist and be a float."""
    bbox = covering.get("bbox") or {}
    assert set(bbox) == {"xmin", "ymin", "xmax", "ymax"}, (
        f"{path}: a covering names all four corners, found {sorted(bbox)!r}"
    )
    for corner, parts in bbox.items():
        resolved = _resolve_field(schema, list(parts))
        assert resolved is not None, (
            f"{path}: covering names {corner} at {list(parts)!r}, which does not "
            f"exist in the output schema {schema.names!r}"
        )
        assert pa.types.is_floating(resolved), (
            f"{path}: covering's {corner} at {list(parts)!r} is {resolved}, "
            "but the spec requires a float"
        )


def assert_fix_output_is_sound(
    path: StrPath,
    *,
    expected_rows: int,
    expected_crs: CrsId,
    geometry_column: str = "geometry",
    expects_covering: bool | None = None,
    expected_version_prefix: str | None = None,
    known_spec_failures: Mapping[str, str] | None = None,
) -> None:
    """Assert a ``--fix`` output is a file gpio would accept.

    Args:
        path: the file the fix wrote.
        expected_rows: the input's row count. A repair never changes it.
        expected_crs: what the output must claim, as a PROJJSON ``id`` mapping
            or :data:`CRS84`. Every carrier that states a CRS at all has to
            state this one, and at least one carrier has to state it.
        geometry_column: the primary column's name.
        expects_covering: ``True`` requires a ``covering`` naming a real column,
            ``False`` requires none, ``None`` does not care. Pass a bool for any
            fix that touches the bbox.
        expected_version_prefix: e.g. ``"1.1"``; :data:`NO_GEO_BLOCK` for an
            output that must carry no ``geo`` key. A fix that silently upgrades
            the file a user asked it to *repair* is a defect of the same family
            as one that loses its CRS, so say which version you expect.
        known_spec_failures: ``{check name: why it is legitimate}`` for failures
            the fix cannot be blamed for -- a fixture whose coordinates are
            outside its CRS's area of use, say. Compared as a set, so a new
            failure and a repaired one both fail the test.
    """
    expected_failures = dict(known_spec_failures or {})
    assert all(expected_failures.values()), (
        "every entry in known_spec_failures needs a reason -- an unexplained "
        "baseline is how an oracle gets quietly weakened"
    )

    # 1. gpio's own validator, compared as a set.
    failures = spec_failures(path)
    unexpected = {name: text for name, text in failures.items() if name not in expected_failures}
    repaired = sorted(set(expected_failures) - set(failures))
    assert set(failures) == set(expected_failures), (
        f"{path}: `gpio check spec` disagrees with the expected baseline.\n"
        f"  unexpected failures: {unexpected}\n"
        f"  expected but absent: {repaired}"
    )

    # 2. The rows.
    actual_rows = pq.read_metadata(str(path)).num_rows
    assert actual_rows == expected_rows, (
        f"{path}: the fix changed the row count -- {expected_rows} in, {actual_rows} out"
    )

    # 3. Both CRS carriers, read separately (never through source_crs_string).
    expected = _normalise_crs(expected_crs)
    stated = {
        "geo block": _normalise_crs(geo_block_crs_id(path, geometry_column)),
        "Parquet logical type": _normalise_crs(logical_crs_id(path, geometry_column)),
    }
    speaking = {carrier: value for carrier, value in stated.items() if value is not None}
    assert speaking, (
        f"{path}: the output states no CRS in either carrier -- neither the "
        f"`geo` block nor the Parquet logical type names {expected_crs!r}"
    )
    wrong = {carrier: value for carrier, value in speaking.items() if value != expected}
    assert not wrong, (
        f"{path}: expected {expected!r} from every carrier that speaks, but "
        f"{wrong!r} disagrees (all carriers: {stated!r})"
    )

    # 4. The covering, when the fix touches the bbox.
    if expects_covering is not None:
        covering = covering_of(path, geometry_column)
        if expects_covering:
            assert covering, (
                f"{path}: no covering on column {geometry_column!r} -- "
                "a bbox column a client cannot find is a bbox column that does nothing"
            )
            _assert_covering_resolves(path, covering, pq.read_schema(str(path)))
        else:
            assert not covering, f"{path}: expected no covering, found {covering!r}"

    # 5. The version the user's file came in as.
    if expected_version_prefix == NO_GEO_BLOCK:
        assert geo_block(path) is None, (
            f"{path}: a native-geo-only input must stay native-geo-only, "
            f"found a geo block: {geo_block(path)!r}"
        )
    elif expected_version_prefix is not None:
        found = geo_version(path)
        assert (found or "").startswith(expected_version_prefix), (
            f"{path}: expected GeoParquet {expected_version_prefix}.x, found {found!r}"
        )


def assert_bbox_column_matches_geometry(
    path: StrPath, *, geometry_column: str = "geometry"
) -> None:
    """Every row's declared bbox is its geometry's envelope.

    The one thing neither ``check spec`` nor question 4 reads: the validator
    checks the file-level ``bbox`` against the data, and the covering check
    only that the corners exist and are floats. A ``fix_bbox_column`` that
    wrote ``ymin`` into ``xmin`` passes both. Compared with a tolerance that
    admits float32 storage of float64 bounds.
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, quote_identifier, sql_path

    covering = covering_of(path, geometry_column)
    assert covering, f"{path}: no covering on {geometry_column!r} to check the values of"
    corners = {corner: list(parts) for corner, parts in (covering.get("bbox") or {}).items()}

    def field(parts: list[str]) -> str:
        return ".".join(quote_identifier(part) for part in parts)

    geom = quote_identifier(geometry_column)
    con = get_duckdb_connection(load_spatial=True)
    try:
        kind = con.execute(f"SELECT typeof({geom}) FROM {sql_path(str(path))} LIMIT 1").fetchone()
        if kind and kind[0] == "BLOB":
            geom = f"ST_GeomFromWKB({geom})"
        checks = " AND ".join(
            f"abs({field(corners[corner])} - {fn}({geom})) <= 1e-6 * greatest(1, abs({fn}({geom})))"
            for corner, fn in (
                ("xmin", "ST_XMin"),
                ("ymin", "ST_YMin"),
                ("xmax", "ST_XMax"),
                ("ymax", "ST_YMax"),
            )
        )
        wrong = con.execute(
            f"SELECT count(*) FROM {sql_path(str(path))} "
            f"WHERE {geom} IS NOT NULL AND NOT ({checks})"
        ).fetchone()[0]
    finally:
        con.close()
    assert wrong == 0, f"{path}: {wrong} rows whose bbox is not their geometry's envelope"


# ---------------------------------------------------------------------------
# The two fixtures most --fix tests share, with their facts written once
# ---------------------------------------------------------------------------


def assert_places_output_is_sound(
    path: StrPath,
    *,
    version: str,
    covering: bool,
    known_spec_failures: Mapping[str, str] | None = None,
) -> None:
    """The oracle for anything derived from ``places_test.parquet``."""
    assert_fix_output_is_sound(
        path,
        expected_rows=PLACES_ROWS,
        expected_crs=CRS84,
        expects_covering=covering,
        expected_version_prefix=version,
        known_spec_failures=known_spec_failures,
    )


def assert_buildings_output_is_sound(
    path: StrPath, *, version: str = "1.1", covering: bool = True
) -> None:
    """The oracle for anything derived from ``buildings_test.parquet``.

    Defaults to what a bbox fix leaves behind: adding a bbox column means
    declaring it in a ``covering``, a 1.1-only key, so the 1.0 input comes out
    as 1.1 (#686).
    """
    assert_fix_output_is_sound(
        path,
        expected_rows=BUILDINGS_ROWS,
        expected_crs=CRS84,
        expects_covering=covering,
        expected_version_prefix=version,
    )
