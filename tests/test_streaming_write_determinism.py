"""Streaming-write output must not depend on ``geoarrow.pyarrow`` import state (#688).

``geoarrow.pyarrow`` registers its extension types process-globally on import, and
several ordinary gpio code paths import it. That registration changed what DuckDB's
Arrow export handed the arrow-streaming write strategy, so the ON-DISK geometry type
of a 1.1 file depended on unrelated import history:

  clean interpreter    -> ``large_binary`` + raw ``ARROW:extension:name`` field
                          metadata  (fails validate's ``geometry_byte_array``)
  after the import     -> registered ``geoarrow.wkb`` extension, which PyArrow
                          writes as a native Parquet GEOMETRY logical type
                          (fails ``version_features_match`` for a 1.1 file)

Both variants are invalid GeoParquet 1.1 and which one you got was decided by
import order. The canonical 1.x streaming geometry column is plain ``binary``
WKB with no extension metadata — matching what every other write strategy
already produces.
"""

from __future__ import annotations

import hashlib
import json
import struct
import subprocess
import sys
import textwrap

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.validate import validate_geoparquet
from geoparquet_io.core.write_funnels import write_parquet_with_metadata
from geoparquet_io.core.write_strategies.arrow_streaming import ArrowStreamingStrategy

# WKB point (little-endian, type 1) — the smallest valid geometry payload.
POINT_WKB = [
    struct.pack("<BI2d", 1, 1, 1.0, 2.0),
    struct.pack("<BI2d", 1, 1, 3.0, 4.0),
]

GEO_1_1 = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
}


# A second geometry column, which GeoParquet allows and gpio carries through
# ``geometry_info["secondary"]``. Validation treats every column in
# ``geo["columns"]`` alike, so a secondary must satisfy the same per-version
# requirements as the primary.
GEO_MULTI = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {"encoding": "WKB", "geometry_types": ["Point"]},
        "geom2": {"encoding": "WKB", "geometry_types": ["Point"]},
    },
}

MULTI_GEOMETRY_INFO = {
    "primary": "geometry",
    "secondary": ["geom2"],
    "metadata": {
        "geometry": {"encoding": "WKB", "geometry_types": ["Point"]},
        "geom2": {"encoding": "WKB", "geometry_types": ["Point"]},
    },
}


def _write_source(path) -> str:
    """Write a tiny WKB GeoParquet 1.1 input file and return its path."""
    table = pa.table({"id": [1, 2], "geometry": POINT_WKB})
    table = table.replace_schema_metadata({b"geo": json.dumps(GEO_1_1).encode("utf-8")})
    pq.write_table(table, path)
    return str(path)


def _write_multi_source(path) -> str:
    """Write an input file carrying a primary *and* a secondary geometry column."""
    table = pa.table({"id": [1, 2], "geometry": POINT_WKB, "geom2": POINT_WKB})
    table = table.replace_schema_metadata({b"geo": json.dumps(GEO_MULTI).encode("utf-8")})
    pq.write_table(table, path)
    return str(path)


def _stream_write(
    src: str,
    out,
    version: str = "1.1",
    geometry_info: dict | None = None,
    input_crs: dict | None = None,
) -> str:
    """Run the arrow-streaming write strategy over ``src``."""
    con = get_duckdb_connection()
    try:
        original_metadata, _ = get_parquet_metadata(src)
        write_parquet_with_metadata(
            con,
            f"SELECT * FROM read_parquet('{src}')",
            str(out),
            original_metadata=original_metadata,
            geoparquet_version=version,
            write_strategy="streaming",
            input_file=src,
            geometry_info=geometry_info,
            input_crs=input_crs,
        )
    finally:
        con.close()
    return str(out)


def _geometry_field(path: str, column: str = "geometry") -> pa.Field:
    return pq.ParquetFile(path).schema_arrow.field(column)


def _parquet_logical_type(path: str, column: str = "geometry") -> str:
    pf = pq.ParquetFile(path)
    index = pf.schema_arrow.names.index(column)
    return str(pf.metadata.schema.column(index).logical_type)


# A non-default CRS, so the native-type path actually applies `with_crs`.
EPSG_3857 = {
    "type": "ProjectedCRS",
    "name": "WGS 84 / Pseudo-Mercator",
    "id": {"authority": "EPSG", "code": 3857},
}


def _wkb_values(table: pa.Table, column: str) -> list[bytes]:
    """Raw WKB values, whether the column came back as binary or as an extension type."""
    chunked = table.column(column)
    values = []
    for chunk in chunked.chunks:
        storage = chunk.storage if isinstance(chunk, pa.ExtensionArray) else chunk
        values.extend(storage.to_pylist())
    return values


def _failed_checks(path: str) -> list[str]:
    """Every validator failure, on every platform.

    The `native_geo_stats_contains_data_*` checks used to be excused on win32:
    DuckDB <= 1.5.1 misread the geospatial statistics of a native GEOMETRY
    column there (#721). gpio now floors DuckDB at 1.5.5, which reads them
    correctly, so nothing is excused any more.
    """
    result = validate_geoparquet(path)
    return [f"{c.name}: {c.message}" for c in result.checks if c.status.value == "failed"]


class TestCanonicalStreamingSchema:
    """``_build_streaming_schema`` must normalize every WKB carrier to plain binary.

    This is the import-state-independent half of the regression: it feeds the
    strategy each of the three carriers DuckDB can hand back and asserts they all
    collapse to one canonical field, without depending on what this interpreter
    happens to have imported.
    """

    def _geometry_field_for(self, geometry_field: pa.Field) -> pa.Field:
        schema = pa.schema([pa.field("id", pa.int64()), geometry_field])
        built = ArrowStreamingStrategy()._build_streaming_schema(
            schema=schema,
            geometry_column="geometry",
            geo_meta=json.loads(json.dumps(GEO_1_1)),
            use_native_geometry=False,
            native_crs={},
            geom_types=["Point"],
            verbose=False,
        )
        return built.field("geometry")

    def test_plain_binary_stays_plain_binary(self):
        field = self._geometry_field_for(pa.field("geometry", pa.binary()))
        assert field.type == pa.binary()
        assert not (field.metadata or {})

    def test_large_binary_is_narrowed_to_binary(self):
        """The clean-interpreter carrier: DuckDB's 64-bit export."""
        field = self._geometry_field_for(pa.field("geometry", pa.large_binary()))
        assert field.type == pa.binary()

    def test_unregistered_extension_metadata_is_stripped(self):
        """The clean-interpreter carrier as PyArrow actually presents it.

        Without the registration PyArrow leaves ``ARROW:extension:name`` sitting
        on the field metadata; carried through to the writer it lands in the
        file's ARROW:schema and makes the column non-canonical.
        """
        source = pa.field(
            "geometry",
            pa.large_binary(),
            metadata={
                b"ARROW:extension:name": b"geoarrow.wkb",
                b"ARROW:extension:metadata": b'{"crs_type":"projjson","crs":{}}',
            },
        )
        field = self._geometry_field_for(source)
        assert field.type == pa.binary()
        assert b"ARROW:extension:name" not in (field.metadata or {})

    def test_registered_geoarrow_extension_is_unwrapped(self):
        """The polluted-interpreter carrier: a resolved geoarrow.wkb extension type."""
        import geoarrow.pyarrow as ga

        field = self._geometry_field_for(pa.field("geometry", ga.wkb()))
        assert field.type == pa.binary()
        assert b"ARROW:extension:name" not in (field.metadata or {})

    def test_all_carriers_produce_the_same_field(self):
        import geoarrow.pyarrow as ga

        carriers = [
            pa.field("geometry", pa.binary()),
            pa.field("geometry", pa.large_binary()),
            pa.field(
                "geometry",
                pa.large_binary(),
                metadata={b"ARROW:extension:name": b"geoarrow.wkb"},
            ),
            pa.field("geometry", ga.wkb()),
        ]
        fields = [self._geometry_field_for(c) for c in carriers]
        assert len({(f.type, tuple(sorted((f.metadata or {}).items()))) for f in fields}) == 1


class TestStreamingWriteProducesValid11:
    """End-to-end: a 1.1 streaming write is valid GeoParquet whatever is imported."""

    def test_geometry_column_is_plain_byte_array(self, tmp_path):
        # Force the "polluted" process state this test class is about; the point
        # of the fix is that it no longer changes the output.
        import geoarrow.pyarrow  # noqa: F401

        src = _write_source(tmp_path / "src.parquet")
        out = _stream_write(src, tmp_path / "out.parquet")

        field = _geometry_field(out)
        assert field.type == pa.binary(), f"expected plain binary WKB, got {field.type}"
        assert b"ARROW:extension:name" not in (field.metadata or {})

    def test_no_native_geometry_logical_type_in_11_output(self, tmp_path):
        import geoarrow.pyarrow  # noqa: F401

        src = _write_source(tmp_path / "src.parquet")
        out = _stream_write(src, tmp_path / "out.parquet")
        # A native Parquet GEOMETRY logical type in a 1.1 file is what tripped
        # `version_features_match`.
        assert "Geometry" not in _parquet_logical_type(out)

    def test_output_passes_validation(self, tmp_path):
        import geoarrow.pyarrow  # noqa: F401

        src = _write_source(tmp_path / "src.parquet")
        out = _stream_write(src, tmp_path / "out.parquet")
        assert _failed_checks(out) == []

    def test_geometry_values_survive_normalization(self, tmp_path):
        src = _write_source(tmp_path / "src.parquet")
        out = _stream_write(src, tmp_path / "out.parquet")
        written = pq.read_table(out)
        assert written.column("geometry").to_pylist() == POINT_WKB


class TestNativeVersionsUnaffected:
    """The fix must not push the dependence into the versions that want native types."""

    def test_20_still_writes_native_geometry(self, tmp_path):
        src = _write_source(tmp_path / "src.parquet")
        out = _stream_write(src, tmp_path / "out20.parquet", version="2.0")
        assert "Geometry" in _parquet_logical_type(out)

    def test_11_geoarrow_still_writes_native_encoding(self, tmp_path):
        src = _write_source(tmp_path / "src.parquet")
        out = _stream_write(src, tmp_path / "out_ga.parquet", version="1.1-geoarrow")
        field = _geometry_field(out)
        extension_name = (
            getattr(field.type, "extension_name", None)
            or (field.metadata or {}).get(b"ARROW:extension:name", b"").decode()
        )
        assert extension_name.startswith("geoarrow.")
        assert extension_name != "geoarrow.wkb"


# The two writes must happen in one interpreter, in this order, so the first is
# provably before any geoarrow registration. Splitting them across two processes
# would work too, but one subprocess halves the cost and removes the ordering
# assumption. Kept subprocess-based (rather than importing geoarrow in-process)
# because the registration is global and irreversible — see
# tests/test_crs_normalize.py for the same pattern.
_TWO_WRITE_SCRIPT = textwrap.dedent(
    """
    import sys

    src, clean_out, geoarrow_out = sys.argv[1], sys.argv[2], sys.argv[3]

    from geoparquet_io.core.common import (
        get_duckdb_connection,
        get_parquet_metadata,
        write_parquet_with_metadata,
    )

    def write(out):
        con = get_duckdb_connection()
        try:
            meta, _ = get_parquet_metadata(src)
            write_parquet_with_metadata(
                con,
                f"SELECT * FROM read_parquet('{src}')",
                out,
                original_metadata=meta,
                geoparquet_version="1.1",
                write_strategy="streaming",
                input_file=src,
            )
        finally:
            con.close()

    assert "geoarrow.pyarrow" not in sys.modules, "write path imported geoarrow too early"
    write(clean_out)

    import geoarrow.pyarrow  # noqa: F401

    write(geoarrow_out)
    """
)


def test_streaming_output_is_independent_of_geoarrow_import(tmp_path):
    """The headline invariant, end to end: same input, same bytes out.

    Runs in a subprocess because ``geoarrow.pyarrow``'s registration cannot be
    undone once this interpreter has done it, and the fast suite pollutes itself
    nondeterministically under xdist.
    """
    src = _write_source(tmp_path / "src.parquet")
    clean_out = tmp_path / "clean.parquet"
    geoarrow_out = tmp_path / "geoarrow.parquet"

    result = subprocess.run(
        [sys.executable, "-c", _TWO_WRITE_SCRIPT, src, str(clean_out), str(geoarrow_out)],
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        timeout=300,
    )
    assert result.returncode == 0, (
        f"subprocess failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    clean_digest = hashlib.sha256(clean_out.read_bytes()).hexdigest()
    geoarrow_digest = hashlib.sha256(geoarrow_out.read_bytes()).hexdigest()
    assert clean_digest == geoarrow_digest, (
        "streaming write is not deterministic: "
        f"clean interpreter wrote {_geometry_field(str(clean_out)).type}, "
        f"after importing geoarrow.pyarrow it wrote {_geometry_field(str(geoarrow_out)).type}"
    )

    # Deterministic is only half the requirement — both must also be valid 1.1.
    for path in (clean_out, geoarrow_out):
        assert _failed_checks(str(path)) == [], f"{path.name} is not valid GeoParquet 1.1"


@pytest.mark.parametrize("version", ["1.0", "1.1"])
def test_wkb_versions_write_plain_binary(tmp_path, version):
    """Both WKB-encoded versions share the canonical carrier."""
    src = _write_source(tmp_path / "src.parquet")
    out = _stream_write(src, tmp_path / f"out_{version}.parquet", version=version)
    assert _geometry_field(out).type == pa.binary()


class TestSecondaryGeometryColumns:
    """A secondary geometry column must follow the same rules as the primary.

    ``validate_geoparquet`` runs ``native_geo_type_present`` and
    ``v2_uses_native_types`` over *every* column in ``geo["columns"]``, so
    demoting a secondary to plain binary makes a 2.0 file invalid — and on
    parquet-geo-only, where the geo metadata is dropped entirely, the native
    logical type is the column's only remaining geometry identity.
    """

    def test_20_writes_both_columns_native(self, tmp_path):
        src = _write_multi_source(tmp_path / "src.parquet")
        out = _stream_write(
            src, tmp_path / "out.parquet", version="2.0", geometry_info=MULTI_GEOMETRY_INFO
        )
        assert "Geometry" in _parquet_logical_type(out, "geometry")
        assert "Geometry" in _parquet_logical_type(out, "geom2")

    def test_20_multi_geometry_validates_clean(self, tmp_path):
        src = _write_multi_source(tmp_path / "src.parquet")
        out = _stream_write(
            src, tmp_path / "out.parquet", version="2.0", geometry_info=MULTI_GEOMETRY_INFO
        )
        assert _failed_checks(out) == []

    def test_20_multi_geometry_validates_clean_with_geoarrow_imported(self, tmp_path):
        import geoarrow.pyarrow  # noqa: F401

        src = _write_multi_source(tmp_path / "src.parquet")
        out = _stream_write(
            src, tmp_path / "out.parquet", version="2.0", geometry_info=MULTI_GEOMETRY_INFO
        )
        assert _failed_checks(out) == []

    def test_parquet_geo_only_keeps_both_columns_native(self, tmp_path):
        """With no geo metadata, the logical type is the only geometry identity left."""
        src = _write_multi_source(tmp_path / "src.parquet")
        out = _stream_write(
            src,
            tmp_path / "out.parquet",
            version="parquet-geo-only",
            geometry_info=MULTI_GEOMETRY_INFO,
        )
        assert "Geometry" in _parquet_logical_type(out, "geometry")
        assert "Geometry" in _parquet_logical_type(out, "geom2")

    def test_11_demotes_both_columns_to_plain_binary(self, tmp_path):
        src = _write_multi_source(tmp_path / "src.parquet")
        out = _stream_write(
            src, tmp_path / "out.parquet", version="1.1", geometry_info=MULTI_GEOMETRY_INFO
        )
        assert _geometry_field(out, "geometry").type == pa.binary()
        assert _geometry_field(out, "geom2").type == pa.binary()

    def test_11_multi_geometry_validates_clean(self, tmp_path):
        src = _write_multi_source(tmp_path / "src.parquet")
        out = _stream_write(
            src, tmp_path / "out.parquet", version="1.1", geometry_info=MULTI_GEOMETRY_INFO
        )
        assert _failed_checks(out) == []

    def test_multi_geometry_values_survive(self, tmp_path):
        src = _write_multi_source(tmp_path / "src.parquet")
        out = _stream_write(
            src, tmp_path / "out.parquet", version="1.1", geometry_info=MULTI_GEOMETRY_INFO
        )
        written = pq.read_table(out)
        assert written.column("geometry").to_pylist() == POINT_WKB
        assert written.column("geom2").to_pylist() == POINT_WKB


class TestCarrierGuards:
    """The normalization must recognise what is *not* a WKB column."""

    def test_native_nested_geoarrow_is_not_a_wkb_carrier(self):
        """geoarrow.point holds coordinates, not WKB bytes — never rewrite it to binary."""
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.write_strategies.arrow_streaming import _is_wkb_carrier

        assert not _is_wkb_carrier(pa.field("geometry", ga.point()))

    def test_already_plain_binary_is_returned_untouched(self):
        from geoparquet_io.core.write_strategies.arrow_streaming import _to_plain_wkb_array

        array = pa.array(POINT_WKB, pa.binary())
        assert _to_plain_wkb_array(array) is array


class TestOversizedBatchSplit:
    """Narrowing to plain ``binary`` reintroduces Arrow's 2 GB 32-bit offset ceiling.

    A batch above it is split rather than overflowing, so the guard does not
    regress writes of very large geometries that worked (invalidly) before.
    """

    def _batch(self):
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("geometry", pa.large_binary())])
        return pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3, 4], pa.int64()), pa.array(POINT_WKB * 2, pa.large_binary())],
            schema=schema,
        )

    def _target_schema(self):
        return pa.schema([pa.field("id", pa.int64()), pa.field("geometry", pa.binary())])

    def test_normal_batch_is_one_span(self):
        from geoparquet_io.core.write_strategies.arrow_streaming import _binary_cast_spans

        assert _binary_cast_spans(self._batch(), self._target_schema()) == [(0, 4)]

    def test_oversized_batch_splits_into_covering_spans(self, monkeypatch):
        from geoparquet_io.core import streaming
        from geoparquet_io.core.write_strategies.arrow_streaming import _binary_cast_spans

        # One WKB point per span: the smallest budget that still forces a split.
        monkeypatch.setattr(streaming, "_MAX_WKB_CHUNK_BYTES", 1)
        spans = _binary_cast_spans(self._batch(), self._target_schema())

        assert len(spans) > 1
        # Spans must tile the batch exactly — no dropped or duplicated rows.
        assert spans[0][0] == 0
        assert sum(length for _, length in spans) == 4
        for (offset, length), (next_offset, _) in zip(spans, spans[1:], strict=False):
            assert offset + length == next_offset

    def test_split_write_keeps_every_row(self, tmp_path, monkeypatch):
        from geoparquet_io.core import streaming

        src = _write_source(tmp_path / "src.parquet")
        monkeypatch.setattr(streaming, "_MAX_WKB_CHUNK_BYTES", 1)
        out = _stream_write(src, tmp_path / "out.parquet")

        written = pq.read_table(out)
        assert written.column("geometry").to_pylist() == POINT_WKB
        assert _geometry_field(out).type == pa.binary()

    def test_split_measures_registered_extension_arrays(self, monkeypatch):
        """A registered geoarrow.wkb column must be measurable too.

        PyArrow's binary kernels reject extension arrays outright, so the split
        has to measure the storage — otherwise the oversized path blows up only
        in processes that happen to have imported geoarrow.pyarrow, which is the
        very state-dependence this module is about.
        """
        import geoarrow.pyarrow as ga

        from geoparquet_io.core import streaming
        from geoparquet_io.core.write_strategies.arrow_streaming import _binary_cast_spans

        # geoarrow.large_wkb is exactly what DuckDB's 64-bit Arrow export resolves
        # to once the extension types are registered: extension name geoarrow.wkb
        # over large_binary storage. That is the shape that actually gets narrowed.
        geometry = pa.ExtensionArray.from_storage(
            ga.large_wkb(), pa.array(POINT_WKB * 2, pa.large_binary())
        )
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3, 4], pa.int64()), geometry],
            schema=pa.schema([pa.field("id", pa.int64()), pa.field("geometry", geometry.type)]),
        )
        monkeypatch.setattr(streaming, "_MAX_WKB_CHUNK_BYTES", 1)

        spans = _binary_cast_spans(batch, self._target_schema())
        assert len(spans) > 1
        assert sum(length for _, length in spans) == 4

    def test_two_columns_overflowing_at_different_rows_are_merged(self, monkeypatch):
        """Every narrowed column's boundaries must constrain the same span list.

        The other cases here have one oversized column, or two whose rows are
        the same size and so split at identical offsets — neither can tell a
        real merge from an implementation that just keeps the last column's
        boundaries. Here the two columns carry different per-row sizes, so
        their natural split points interleave, and each returned span has to
        satisfy both budgets at once.
        """
        from geoparquet_io.core import streaming
        from geoparquet_io.core.write_strategies.arrow_streaming import _binary_cast_spans

        budget = 10
        # a: 6 bytes/row  -> wants to break after every row (6 + 6 > 10)
        # b: 4 bytes/row  -> wants to break after every 2 rows (4 + 4 + 4 > 10)
        rows = 6
        a = pa.array([b"x" * 6] * rows, pa.large_binary())
        b = pa.array([b"y" * 4] * rows, pa.large_binary())
        batch = pa.RecordBatch.from_arrays(
            [a, b],
            schema=pa.schema([pa.field("a", pa.large_binary()), pa.field("b", pa.large_binary())]),
        )
        target = pa.schema([pa.field("a", pa.binary()), pa.field("b", pa.binary())])
        monkeypatch.setattr(streaming, "_MAX_WKB_CHUNK_BYTES", budget)

        spans = _binary_cast_spans(batch, target)

        # Tiling, as the single-column case already requires.
        assert spans[0][0] == 0
        assert sum(length for _, length in spans) == rows
        for (offset, length), (next_offset, _) in zip(spans, spans[1:], strict=False):
            assert offset + length == next_offset

        # The point of the merge: every span fits *both* columns' budgets.
        for offset, length in spans:
            for name in ("a", "b"):
                nbytes = sum(len(v) for v in batch.column(name).slice(offset, length).to_pylist())
                assert nbytes <= budget or length == 1, (
                    f"span ({offset}, {length}) holds {nbytes} bytes of column {name!r}, "
                    f"over the {budget}-byte budget — the other column's boundaries "
                    f"were not merged in"
                )

    def test_split_write_20_with_crs_and_secondary_column(self, tmp_path, monkeypatch):
        """The hardest combination through the split path.

        Native 2.0 conversion goes through ``pa.ExtensionArray``, which cannot
        carry a slice offset — and the split path hands it *sliced* batches. If
        the offset were dropped, every part after the first would silently repeat
        the parent's leading rows, so pin the row values, not just the types.
        """
        from geoparquet_io.core import streaming

        src = _write_multi_source(tmp_path / "src.parquet")
        monkeypatch.setattr(streaming, "_MAX_WKB_CHUNK_BYTES", 1)
        out = _stream_write(
            src,
            tmp_path / "out.parquet",
            version="2.0",
            geometry_info=MULTI_GEOMETRY_INFO,
            input_crs=EPSG_3857,
        )

        # Guard: the budget must actually have forced a split, or this proves nothing.
        assert pq.ParquetFile(out).num_row_groups > 1

        written = pq.read_table(out)
        assert written.num_rows == 2
        assert _wkb_values(written, "geometry") == POINT_WKB
        assert _wkb_values(written, "geom2") == POINT_WKB
        assert "Geometry" in _parquet_logical_type(out, "geometry")
        assert "Geometry" in _parquet_logical_type(out, "geom2")

    def test_uncastable_column_raises_actionable_error(self):
        """If several columns overflow at once the cast can still fail — say why."""
        from geoparquet_io.core.write_strategies.arrow_streaming import _coerce_column

        class Overflowing:
            type = pa.large_binary()

            def cast(self, _target):
                raise pa.ArrowInvalid("offset overflow while concatenating arrays")

        with pytest.raises(ValueError, match="row-group"):
            _coerce_column(Overflowing(), pa.binary())
