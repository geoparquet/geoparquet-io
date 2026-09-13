"""The geoarrow extension name is carried in two shapes, and every path must agree (#792).

``geoarrow.pyarrow`` registers its extension types process-globally on import, so
the same WKB column reaches gpio either as

1. a resolved ``geoarrow.wkb`` extension type
   (``field.type.extension_name == "geoarrow.wkb"``), or
2. plain ``large_binary`` carrying ``ARROW:extension:name`` in the FIELD METADATA.

Which one arrives depends on whether anything in the process imported
``geoarrow.pyarrow`` -- an unstable thing to key behaviour on. #791 fixed the
write strategies; these tests are the standing guard that the *shared* predicate
and its metadata/streaming callers see both shapes too.

Every test here builds the same column in both shapes and asserts the two paths
agree. Agreement is the property; which branch is taken is secondary.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pytest

# A single POINT (1 2) in little-endian WKB.
WKB_POINT = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"

_EXTENSION_MARKER = {
    b"ARROW:extension:name": b"geoarrow.wkb",
    b"ARROW:extension:metadata": b"{}",
}


def _geo_metadata(version: str = "1.1", encoding: str = "WKB") -> dict:
    return {
        "version": version,
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": encoding, "geometry_types": []}},
    }


def _schema_metadata(with_geo: bool, version: str = "1.1", encoding: str = "WKB"):
    if not with_geo:
        return None
    return {b"geo": json.dumps(_geo_metadata(version, encoding)).encode()}


def resolved_extension_table(with_geo: bool = True) -> pa.Table:
    """Shape (1): PyArrow resolved the geoarrow.wkb extension type."""
    import geoarrow.pyarrow as ga

    array = ga.as_wkb(pa.array([WKB_POINT], pa.binary()))
    schema = pa.schema(
        [pa.field("id", pa.int64()), pa.field("geometry", array.type)],
        metadata=_schema_metadata(with_geo),
    )
    return pa.table({"id": [1], "geometry": array}, schema=schema)


def metadata_only_table(with_geo: bool = True) -> pa.Table:
    """Shape (2): plain large_binary carrying ARROW:extension:name in field metadata."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("geometry", pa.large_binary(), metadata=_EXTENSION_MARKER),
        ],
        metadata=_schema_metadata(with_geo),
    )
    return pa.table({"id": [1], "geometry": [WKB_POINT]}, schema=schema)


def metadata_only_native_point_table(with_geo: bool = True) -> pa.Table:
    """A NON-WKB geoarrow carrier: ``geoarrow.point`` over ``struct<x, y>``.

    Declared in field metadata only, which is the shape gpio sees whenever
    nothing in the process registered ``geoarrow.pyarrow``'s types. There are no
    WKB bytes here at all -- the coordinates live in the struct -- so a cast to
    binary is not a re-carrier, it is impossible (PyArrow raises).
    """
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "geometry",
                pa.struct([("x", pa.float64()), ("y", pa.float64())]),
                metadata={
                    b"ARROW:extension:name": b"geoarrow.point",
                    b"ARROW:extension:metadata": b"{}",
                },
            ),
        ],
        metadata=_schema_metadata(with_geo, encoding="point"),
    )
    return pa.table({"id": [1], "geometry": [{"x": 1.0, "y": 2.0}]}, schema=schema)


def metadata_only_wkt_table(with_geo: bool = True) -> pa.Table:
    """A NON-WKB geoarrow carrier: ``geoarrow.wkt`` over ``string``.

    Casting this to ``binary`` succeeds -- and produces WKT *text* in a column
    the ``geo`` block still declares as ``encoding: WKT``. Nothing downstream
    notices, which is why the cast has to be gated on the WKB names, not on
    "is this geoarrow at all?".
    """
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "geometry",
                pa.string(),
                metadata={
                    b"ARROW:extension:name": b"geoarrow.wkt",
                    b"ARROW:extension:metadata": b"{}",
                },
            ),
        ],
        metadata=_schema_metadata(with_geo, encoding="WKT"),
    )
    return pa.table({"id": [1], "geometry": ["POINT (1 2)"]}, schema=schema)


BOTH_SHAPES = pytest.mark.parametrize("with_geo", [True, False])


def _storage_type(field: pa.Field) -> pa.DataType:
    """The field's underlying storage, whichever carrier shape it arrived in.

    A round trip through Parquet resolves the extension type whenever
    ``geoarrow.pyarrow`` happens to be imported, so assertions about the carrier
    have to look past that -- which is the whole point of #792.
    """
    if isinstance(field.type, pa.ExtensionType):
        return field.type.storage_type
    return field.type


def _carrier_facts(table: pa.Table) -> dict:
    """The observable carrier facts a 1.x reader would see for the geometry field."""
    field = table.schema.field("geometry")
    return {
        "type": str(field.type),
        "extension_name": getattr(field.type, "extension_name", None),
        "field_metadata": dict(field.metadata or {}),
    }


class TestSharedPredicate:
    """One predicate, both shapes."""

    def test_shapes_are_actually_different(self):
        """Guard the fixtures: the two tables really are the two different shapes."""
        resolved = resolved_extension_table().schema.field("geometry")
        meta_only = metadata_only_table().schema.field("geometry")
        assert getattr(resolved.type, "extension_name", None) == "geoarrow.wkb"
        assert getattr(meta_only.type, "extension_name", None) is None
        assert (meta_only.metadata or {}).get(b"ARROW:extension:name") == b"geoarrow.wkb"

    def test_extension_name_agrees(self):
        from geoparquet_io.core.geoarrow_encoding import arrow_extension_name

        assert arrow_extension_name(resolved_extension_table().schema.field("geometry")) == (
            arrow_extension_name(metadata_only_table().schema.field("geometry"))
        )

    def test_predicate_agrees(self):
        from geoparquet_io.core.geoarrow_encoding import is_geoarrow_extension_field

        assert is_geoarrow_extension_field(resolved_extension_table().schema.field("geometry"))
        assert is_geoarrow_extension_field(metadata_only_table().schema.field("geometry"))

    def test_plain_binary_is_not_geoarrow(self):
        from geoparquet_io.core.geoarrow_encoding import (
            arrow_extension_name,
            is_geoarrow_extension_field,
        )

        field = pa.field("geometry", pa.binary())
        assert arrow_extension_name(field) is None
        assert is_geoarrow_extension_field(field) is False

    def test_geoarrow_prefix_needs_the_dot(self):
        """``geoarrowesque.thing`` is a different namespace, not a GeoArrow type."""
        from geoparquet_io.core.geoarrow_encoding import is_geoarrow_extension_field

        field = pa.field(
            "geometry",
            pa.large_binary(),
            metadata={b"ARROW:extension:name": b"geoarrowesque.thing"},
        )
        assert is_geoarrow_extension_field(field) is False

    def test_wkb_predicate_accepts_only_the_wkb_names(self):
        """The strip gate is narrower than the version-detection gate (see #807 review)."""
        from geoparquet_io.core.geoarrow_encoding import is_wkb_extension_field

        def marked(name: bytes, storage=pa.large_binary()):
            return pa.field("geometry", storage, metadata={b"ARROW:extension:name": name})

        assert is_wkb_extension_field(marked(b"geoarrow.wkb")) is True
        assert is_wkb_extension_field(marked(b"ogc.wkb")) is True
        assert is_wkb_extension_field(marked(b"geoarrow.wkt", pa.string())) is False
        assert (
            is_wkb_extension_field(
                marked(b"geoarrow.point", pa.struct([("x", pa.float64()), ("y", pa.float64())]))
            )
            is False
        )
        assert is_wkb_extension_field(pa.field("geometry", pa.binary())) is False

    def test_wkb_predicate_sees_the_resolved_shape_too(self):
        from geoparquet_io.core.geoarrow_encoding import is_wkb_extension_field

        assert is_wkb_extension_field(resolved_extension_table().schema.field("geometry")) is True

    def test_non_geoarrow_extension_is_not_geoarrow(self):
        """A non-geoarrow extension marker in field metadata must not be mistaken for one."""
        from geoparquet_io.core.geoarrow_encoding import (
            arrow_extension_name,
            is_geoarrow_extension_field,
        )

        field = pa.field(
            "payload",
            pa.large_binary(),
            metadata={b"ARROW:extension:name": b"arrow.json"},
        )
        assert arrow_extension_name(field) == "arrow.json"
        assert is_geoarrow_extension_field(field) is False


class TestMetadataPathAgrees:
    """core/common.py's metadata application: the 1.x carrier decision."""

    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_process_geometry_column_agrees(self, version):
        from geoparquet_io.core.common import _process_geometry_column_for_version

        resolved = _process_geometry_column_for_version(
            resolved_extension_table(), "geometry", version, None, False
        )
        meta_only = _process_geometry_column_for_version(
            metadata_only_table(), "geometry", version, None, False
        )
        assert _carrier_facts(resolved) == _carrier_facts(meta_only)

    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_process_geometry_column_leaves_plain_wkb(self, version):
        """Shape (2) must lose the stale ARROW:extension:name for a 1.x output."""
        from geoparquet_io.core.common import _process_geometry_column_for_version

        out = _process_geometry_column_for_version(
            metadata_only_table(), "geometry", version, None, False
        )
        field = out.schema.field("geometry")
        assert field.type == pa.binary()
        assert b"ARROW:extension:name" not in (field.metadata or {})

    def test_strip_to_plain_wkb_agrees(self):
        from geoparquet_io.core.common import _strip_geoarrow_to_plain_wkb

        resolved = _strip_geoarrow_to_plain_wkb(resolved_extension_table(), "geometry", False)
        meta_only = _strip_geoarrow_to_plain_wkb(metadata_only_table(), "geometry", False)
        assert _carrier_facts(resolved) == _carrier_facts(meta_only)
        assert resolved.column("geometry").to_pylist() == meta_only.column("geometry").to_pylist()

    def test_apply_geoparquet_metadata_agrees(self):
        """The whole 1.1 apply path, with _canonicalize_wkb_columns as a second line of defence.

        Not an isolation test of the backstop: _process_geometry_column_for_version
        already strips the carrier, so this fails only when both mechanisms break.
        """
        from geoparquet_io.core.common import _apply_geoparquet_metadata

        resolved = _apply_geoparquet_metadata(
            resolved_extension_table(), "geometry", "1.1", None, None, None, False
        )
        meta_only = _apply_geoparquet_metadata(
            metadata_only_table(), "geometry", "1.1", None, None, None, False
        )
        assert _carrier_facts(resolved) == _carrier_facts(meta_only)
        assert b"ARROW:extension:name" not in (meta_only.schema.field("geometry").metadata or {})


class TestVersionDetectionAgrees:
    """core/common.py: auto-mode version detection."""

    @BOTH_SHAPES
    def test_detect_version_from_table_agrees(self, with_geo):
        from geoparquet_io.core.common import _detect_version_from_table

        assert _detect_version_from_table(resolved_extension_table(with_geo)) == (
            _detect_version_from_table(metadata_only_table(with_geo))
        )

    @BOTH_SHAPES
    def test_resolve_version_from_table_agrees(self, with_geo):
        from geoparquet_io.core.common import resolve_geoparquet_version_from_table

        assert resolve_geoparquet_version_from_table(resolved_extension_table(with_geo)) == (
            resolve_geoparquet_version_from_table(metadata_only_table(with_geo))
        )

    def test_declared_geoarrow_without_geo_metadata_is_native(self):
        """No geo block + a geoarrow column is the in-memory parquet-geo-only shape."""
        from geoparquet_io.core.common import (
            _detect_version_from_table,
            resolve_geoparquet_version_from_table,
        )

        table = metadata_only_table(with_geo=False)
        assert _detect_version_from_table(table) == "parquet-geo-only"
        assert resolve_geoparquet_version_from_table(table) == "2.0"

    def test_carried_geo_version_still_wins(self):
        """A declared version in the geo block is unchanged by the wider detection."""
        from geoparquet_io.core.common import _detect_version_from_table

        assert _detect_version_from_table(metadata_only_table(with_geo=True)) == "1.1"


class TestStreamingPathAgrees:
    """core/streaming.py: table-level geoarrow detection and stripping."""

    @BOTH_SHAPES
    def test_has_geoarrow_extension_in_table_agrees(self, with_geo):
        from geoparquet_io.core.streaming import has_geoarrow_extension_in_table

        assert has_geoarrow_extension_in_table(resolved_extension_table(with_geo)) == (
            has_geoarrow_extension_in_table(metadata_only_table(with_geo))
        )

    def test_has_geoarrow_extension_in_table_sees_metadata_shape(self):
        from geoparquet_io.core.streaming import has_geoarrow_extension_in_table

        assert has_geoarrow_extension_in_table(metadata_only_table()) is True

    def test_plain_wkb_table_is_not_geoarrow(self):
        from geoparquet_io.core.streaming import has_geoarrow_extension_in_table

        table = pa.table({"id": [1], "geometry": pa.array([WKB_POINT], pa.binary())})
        assert has_geoarrow_extension_in_table(table) is False

    @BOTH_SHAPES
    def test_detect_version_for_output_agrees(self, with_geo):
        from geoparquet_io.core.streaming import detect_version_for_output

        resolved = resolved_extension_table(with_geo)
        meta_only = metadata_only_table(with_geo)
        assert detect_version_for_output(resolved.schema.metadata, resolved) == (
            detect_version_for_output(meta_only.schema.metadata, meta_only)
        )


class TestWriteRoundTripAgrees:
    """End to end: the same table in both shapes writes the same 1.1 file."""

    @pytest.mark.parametrize("strategy", ["duckdb-kv", "disk-rewrite", "streaming", "in-memory"])
    def test_write_from_table_agrees(self, strategy, tmp_path):
        import pyarrow.parquet as pq

        from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory

        written = {}
        for label, table in (
            ("resolved", resolved_extension_table()),
            ("meta_only", metadata_only_table()),
        ):
            out = str(tmp_path / f"{strategy}_{label}.parquet")
            WriteStrategyFactory.get_strategy(WriteStrategy(strategy)).write_from_table(
                table=table,
                output_path=out,
                geometry_column="geometry",
                geoparquet_version="1.1",
                compression="ZSTD",
                compression_level=None,
                row_group_size_mb=None,
                row_group_rows=None,
                verbose=False,
            )
            schema = pq.read_schema(out)
            written[label] = {
                "physical": str(pq.ParquetFile(out).schema_arrow.field("geometry").type),
                "logical": str(schema.field("geometry").type),
                "geo": json.loads(schema.metadata[b"geo"].decode()).get("version"),
            }
        assert written["resolved"] == written["meta_only"]
        assert written["meta_only"]["geo"].startswith("1.1")


class TestNonWkbCarriersSurviveTheStrip:
    """A geoarrow name is not a licence to cast to WKB bytes.

    The 1.x "strip geoarrow to plain WKB" paths turn the carrier into
    ``pa.binary()``. That is only meaningful for a column that already holds WKB
    bytes -- ``geoarrow.wkb`` or ``ogc.wkb``. Gating them on "is this geoarrow at
    all?" reaches two carriers it must not touch:

    * ``geoarrow.point`` over ``struct<x, y>`` -- PyArrow cannot cast it, so the
      write dies mid-flight with ``ArrowNotImplementedError``;
    * ``geoarrow.wkt`` over ``string`` -- PyArrow *can* cast it, and the result is
      WKT text sitting in a binary column the ``geo`` block still labels
      ``encoding: WKT``. Nothing downstream notices.

    Both only bite the metadata-declared shape, which is exactly the shape the
    predicate was widened to see (#792, #807 review).
    """

    def test_native_point_is_not_cast_by_strip(self):
        from geoparquet_io.core.common import _strip_geoarrow_to_plain_wkb

        table = metadata_only_native_point_table()
        out = _strip_geoarrow_to_plain_wkb(table, "geometry", False)
        assert out.schema.field("geometry").type == table.schema.field("geometry").type
        assert out.column("geometry").to_pylist() == [{"x": 1.0, "y": 2.0}]

    def test_wkt_is_not_cast_by_strip(self):
        from geoparquet_io.core.common import _strip_geoarrow_to_plain_wkb

        out = _strip_geoarrow_to_plain_wkb(metadata_only_wkt_table(), "geometry", False)
        assert out.schema.field("geometry").type == pa.string()
        assert out.column("geometry").to_pylist() == ["POINT (1 2)"]

    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_native_point_survives_version_processing(self, version):
        from geoparquet_io.core.common import _process_geometry_column_for_version

        table = metadata_only_native_point_table()
        out = _process_geometry_column_for_version(table, "geometry", version, None, False)
        assert out.schema.field("geometry").type == table.schema.field("geometry").type

    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_wkt_survives_version_processing(self, version):
        from geoparquet_io.core.common import _process_geometry_column_for_version

        out = _process_geometry_column_for_version(
            metadata_only_wkt_table(), "geometry", version, None, False
        )
        assert out.schema.field("geometry").type == pa.string()
        assert out.column("geometry").to_pylist() == ["POINT (1 2)"]

    def test_native_point_write_does_not_crash(self, tmp_path):
        """Writing a 1.1 file used to die with ArrowNotImplementedError here."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.geoarrow_encoding import arrow_extension_name
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        out = str(tmp_path / "native_point.parquet")
        write_geoparquet_table(
            metadata_only_native_point_table(), out, "geometry", geoparquet_version="1.1"
        )

        field = pq.read_schema(out).field("geometry")
        assert arrow_extension_name(field) == "geoarrow.point"
        assert pa.types.is_struct(_storage_type(field))

    def test_wkt_write_keeps_the_text_carrier(self, tmp_path):
        """A binary column full of WKT text under ``encoding: WKT`` is silent corruption."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.geoarrow_encoding import arrow_extension_name
        from geoparquet_io.core.write_funnels import write_geoparquet_table

        out = str(tmp_path / "wkt.parquet")
        write_geoparquet_table(metadata_only_wkt_table(), out, "geometry", geoparquet_version="1.1")

        schema = pq.read_schema(out)
        assert json.loads(schema.metadata[b"geo"].decode())["columns"]["geometry"]["encoding"] == (
            "WKT"
        )
        field = schema.field("geometry")
        assert arrow_extension_name(field) == "geoarrow.wkt"
        assert pa.types.is_string(_storage_type(field))

    def test_native_point_still_detects_as_native_geo(self):
        """The version-detection predicate stays broad: any geoarrow name counts."""
        from geoparquet_io.core.common import _detect_version_from_table

        assert _detect_version_from_table(metadata_only_native_point_table(with_geo=False)) == (
            "parquet-geo-only"
        )
