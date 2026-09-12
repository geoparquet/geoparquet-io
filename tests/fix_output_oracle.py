"""The oracle every ``gpio check --fix`` output has to satisfy.

``--fix`` is the one command whose whole promise is that the file it leaves
behind is *better* than the file it found. Until this module there were 40-odd
``--fix`` invocations in the suite and every one of them asserted only the
metric it had just repaired -- ``current_compression == "ZSTD"``,
``num_row_groups == 1``, ``"bbox" in schema``. A repair that fixes its own
metric while corrupting the ``geo`` block, dropping a CRS, losing rows or
declaring a covering over a column that cannot legally be one was invisible,
and that is exactly the "gpio writes a file gpio rejects" shape of #890, #954,
#972 and #1003.

:func:`assert_fix_output_is_sound` is the shared oracle. Four independent
questions, because each of the four has shipped a green test past a real defect:

1. **Does gpio's own validator pass the output?** ``check spec`` with **zero**
   FAILED checks. Not "no new failures" -- a set, compared exactly, so a repair
   that silently *starts* failing a check is visible and so is one that stops.
2. **Are the rows still there?** A rewrite that drops or duplicates rows is a
   data-loss bug no metric assertion can see.
3. **What CRS does the output claim -- in each carrier separately?** A file
   states its CRS in the ``geo`` block *and* in the Parquet ``GEOMETRY`` logical
   type, and the interesting failure is the two disagreeing.
   ``crs_utils.source_crs_string`` reads one, falls back to the other and
   returns whichever answered, so it reports *an* answer for a file that holds
   two: structurally unable to see the defect. #997 shipped green under exactly
   that oracle. So both carriers are read through
   :mod:`tests.native_geo_probes`, independently, and each one that speaks at
   all has to say the same thing.
4. **Does the covering name a column that exists?** #1003 was ``add bbox`` then
   ``check all --fix`` dropping the covering off a 2.0 file; the mirror-image
   defect is declaring one that points at nothing. Both the column and every
   ``xmin``/``ymin``/``xmax``/``ymax`` path are resolved against the real schema.

Where a fix *legitimately* leaves a failure behind -- a fixture whose
coordinates are outside its CRS's area of use, say -- pass it in
``known_spec_failures`` as ``{check_name: why}``. That keeps the oracle exact
rather than weakening it to "at most N failures": a new failure still fails the
test, and so does a listed one that has since been repaired.

Refs: https://github.com/geoparquet/geoparquet-io/issues/1018 (WP-1)
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.validate import CheckStatus, validate_geoparquet
from tests.native_geo_probes import geo_block, geo_block_crs_id, geo_version, logical_crs_id

#: What both carriers say when they mean "the GeoParquet default".
CRS84 = "OGC:CRS84"

#: Marker strings from ``native_geo_probes`` meaning *this carrier says nothing*.
_SILENT = frozenset(
    {
        "<no geo key>",
        "<column not described>",
        "<no native geo type>",
    }
)

#: Marker strings meaning *this carrier is silent, and silence means CRS84*.
_DEFAULTED = frozenset(
    {
        "<no crs key -- resolves as OGC:CRS84>",
        "<no crs -- resolves as OGC:CRS84>",
    }
)


def spec_failures(path) -> dict[str, str]:
    """``{check name: message}`` for every FAILED check ``gpio check spec`` reports.

    Calls ``validate_geoparquet`` with the arguments ``gpio check spec`` itself
    passes, so the verdict here is the verdict a user gets from the CLI.
    """
    result = validate_geoparquet(str(path))
    return {
        check.name: check.message for check in result.checks if check.status == CheckStatus.FAILED
    }


def covering_of(path, geometry_column: str = "geometry") -> dict | None:
    """One column's declared ``covering``, read off the ``geo`` block and nothing else."""
    columns = (geo_block(path) or {}).get("columns") or {}
    return (columns.get(geometry_column) or {}).get("covering")


def covering_column_name(covering: Mapping | None) -> str | None:
    """The column a ``covering`` points at, or None when it points nowhere."""
    if not covering:
        return None
    xmin = (covering.get("bbox") or {}).get("xmin")
    if not xmin:
        return None
    return xmin[0]


def _normalise_crs(raw):
    """One carrier's answer as a comparable value, or None when it does not speak."""
    if isinstance(raw, Mapping):
        authority, code = raw.get("authority"), raw.get("code")
        if (authority, code) == ("OGC", "CRS84"):
            return CRS84
        return (authority, code)
    if raw in _SILENT:
        return None
    if raw in _DEFAULTED:
        return CRS84
    # "<crs: null -- unknown>" and anything unexpected compare literally.
    return raw


def _resolve_field(schema: pa.Schema, parts) -> pa.DataType | None:
    """Walk a ``covering`` path (``["bbox", "xmin"]``) down the real Arrow schema."""
    if not parts:
        return None
    if parts[0] not in schema.names:
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


def _assert_covering_resolves(path, covering: Mapping, schema: pa.Schema) -> None:
    """Every corner the covering names must exist and be a float."""
    bbox = covering.get("bbox") or {}
    assert bbox, f"{path}: covering has no 'bbox' member: {covering!r}"
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
    path,
    *,
    expected_rows: int,
    expected_crs,
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
            (``{"authority": "EPSG", "code": 5070}``) or :data:`CRS84`. Every
            carrier that states a CRS at all has to state this one, and at
            least one carrier has to state it.
        geometry_column: the primary column's name.
        expects_covering: ``True`` requires a ``covering`` naming a real column,
            ``False`` requires none, ``None`` does not care. Pass a bool for any
            fix that touches the bbox.
        expected_version_prefix: e.g. ``"1.1"``. A fix that silently upgrades
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

    parquet_file = pq.ParquetFile(str(path))
    try:
        actual_rows = parquet_file.metadata.num_rows
    finally:
        # Closed deterministically: on Windows an open handle blocks the
        # os.replace() an in-place fix does next (see MEMORY: #770).
        parquet_file.close()

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
            schema = pq.read_schema(str(path))
            named = covering_column_name(covering)
            assert named in schema.names, (
                f"{path}: covering names column {named!r}, which is not in {schema.names!r}"
            )
            _assert_covering_resolves(path, covering, schema)
        else:
            assert not covering, f"{path}: expected no covering, found {covering!r}"

    # 5. The version the user's file came in as.
    if expected_version_prefix is not None:
        found = geo_version(path)
        assert (found or "").startswith(expected_version_prefix), (
            f"{path}: expected GeoParquet {expected_version_prefix}.x, found {found!r}"
        )


def assert_every_fix_output_is_sound(paths, **kwargs) -> None:
    """:func:`assert_fix_output_is_sound` over a directory's worth of outputs."""
    paths = list(paths)
    assert paths, "no output files to check"
    for path in paths:
        assert_fix_output_is_sound(path, **kwargs)
