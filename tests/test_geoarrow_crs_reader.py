"""One geoarrow-CRS reader, four entry points, and the no-`geo`-block gap (#863).

Three modules used to carry their own "read the CRS off a geoarrow extension
type" code, each subtly different: ``streaming._geoarrow_crs_to_projjson`` tried
``to_json_dict()`` then ``to_json()`` and warned on failure, while
``crs_utils._crs_from_extension_type`` and ``duckdb_metadata`` tried only
``to_json_dict()`` and swallowed the failure. The same ``Crs`` object therefore
resolved differently depending on which read path touched it.

`TestCrsShapeMatrix` runs one shape table through every entry point that
delegates to the consolidated ``crs_utils.geoarrow_crs_to_projjson`` and demands
the *same* answer from each. `TestNoGeoBlockCrs` covers the second half of the
issue: a GeoArrow file with no ``geo`` key-value metadata, read in a process
that never imported ``geoarrow.pyarrow``, used to lose its CRS entirely and be
written back out labelled with the CRS84 default.
"""

from __future__ import annotations

import json
import logging
import struct
import subprocess
import sys
import textwrap

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.crs_utils import _crs_from_extension_type, geoarrow_crs_to_projjson
from geoparquet_io.core.duckdb_metadata import _get_pyarrow_logical_type
from geoparquet_io.core.streaming import extract_crs_from_table

# ---------------------------------------------------------------------------
# Carrier shapes
# ---------------------------------------------------------------------------


class _OpaqueCrs:
    """A CRS-ish object exposing neither PROJJSON accessor."""

    def __repr__(self) -> str:
        return "Opaque(EPSG:9999)"


class _ExplodingCrs:
    """A geoarrow-shaped ``Crs`` whose PROJJSON accessor raises.

    Real ``StringCrs.to_json_dict()`` calls pyproj, which raises for an
    identifier PROJ does not know.
    """

    def to_json_dict(self):
        raise ValueError("crs not found: EPSG:999999")


class _ToJsonOnlyCrs:
    """A ``Crs`` exposing only the string accessor -- the fallback rung."""

    def to_json(self) -> str:
        return json.dumps({"id": {"authority": "EPSG", "code": 26918}})


def _projjson(identifier: str) -> dict:
    import pyproj

    return pyproj.CRS(identifier).to_json_dict()


class _AuthCode:
    """Expectation marker: "a PROJJSON dict whose ``id`` is this pair"."""

    def __init__(self, authority: str, code):
        self.expected = {"authority": authority, "code": code}

    def __repr__(self) -> str:
        return f"_AuthCode({self.expected})"


def _carrier_shapes():
    """(id, carrier value, expected resolved CRS) for every shape ``.crs`` takes."""
    import pyproj
    from geoarrow.types.crs import ProjJsonCrs, StringCrs

    crs84 = _projjson("OGC:CRS84")
    return [
        ("projjson_crs_object", ProjJsonCrs(crs84), _AuthCode("OGC", "CRS84")),
        ("string_crs_object", StringCrs("EPSG:3857"), _AuthCode("EPSG", 3857)),
        ("pyproj_crs_object", pyproj.CRS("EPSG:4326"), _AuthCode("EPSG", 4326)),
        ("to_json_only_object", _ToJsonOnlyCrs(), _AuthCode("EPSG", 26918)),
        ("projjson_dict", crs84, crs84),
        ("identifier_string", "EPSG:3857", "EPSG:3857"),
        ("utf8_bytes", b"EPSG:3857", "EPSG:3857"),
        ("empty_dict", {}, None),
        ("empty_string", "", None),
        ("undecodable_bytes", b"\xff\xfe not utf-8", None),
        ("accessor_raises", _ExplodingCrs(), None),
        ("no_accessor", _OpaqueCrs(), None),
    ]


SHAPES = _carrier_shapes()
SHAPE_PARAMS = [pytest.param(carrier, expected, id=name) for name, carrier, expected in SHAPES]


def _assert_resolved(resolved, expected):
    if expected is None:
        assert resolved is None, f"expected rejection, got {resolved!r}"
    elif isinstance(expected, _AuthCode):
        assert isinstance(resolved, dict), f"expected PROJJSON dict, got {resolved!r}"
        assert resolved["id"] == expected.expected
    else:
        assert resolved == expected


# ---------------------------------------------------------------------------
# Entry points: each returns the resolved CRS value for one carrier
# ---------------------------------------------------------------------------


class _FakeGeoArrowType:
    """A duck-typed registered GeoArrow extension type holding a ``.crs``."""

    extension_name = "geoarrow.wkb"
    extension_metadata = None

    def __init__(self, crs):
        self.crs = crs

    def __str__(self) -> str:  # `_get_pyarrow_logical_type` falls back to str()
        return "extension<geoarrow.wkb>"


class _FakeField:
    def __init__(self, field_type, metadata=None):
        self.type = field_type
        self.metadata = metadata


# PyArrow rebuilds an extension type from its serialized form while a table is
# assembled, so the payload cannot live on the instance; a module-level registry
# keyed by the serialized token survives any number of round-trips.
_CRS_PAYLOADS: dict[bytes, object] = {}


class _CrsCarrierType(pa.ExtensionType):
    """An Arrow extension type exposing an arbitrary value as ``.crs``."""

    def __init__(self, token: bytes):
        self._token = token
        super().__init__(pa.binary(), "gpio.test.crs_reader_carrier")

    def __arrow_ext_serialize__(self) -> bytes:
        return self._token

    @classmethod
    def __arrow_ext_deserialize__(cls, _storage_type, serialized):
        return cls(serialized)

    @property
    def crs(self):
        return _CRS_PAYLOADS[self._token]


def _wkb_point(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


def _via_helper(carrier):
    return geoarrow_crs_to_projjson(carrier)


def _via_streaming(carrier):
    token = f"crs-{len(_CRS_PAYLOADS)}".encode()
    _CRS_PAYLOADS[token] = carrier
    storage = pa.array([_wkb_point(0.0, 0.0)], type=pa.binary())
    geom = pa.ExtensionArray.from_storage(_CrsCarrierType(token), storage)
    return extract_crs_from_table(pa.table({"id": [1], "geometry": geom}), "geometry")


def _via_crs_utils(carrier):
    return _crs_from_extension_type(_FakeGeoArrowType(carrier))


def _via_duckdb_metadata(carrier):
    """Read back the CRS ``_get_pyarrow_logical_type`` embedded in its type string."""
    logical = _get_pyarrow_logical_type(_FakeField(_FakeGeoArrowType(carrier)))
    if logical == "GeometryType()":
        return None
    assert logical.startswith("GeometryType(crs=") and logical.endswith(")"), logical
    inner = logical[len("GeometryType(crs=") : -1]
    try:
        return json.loads(inner)
    except json.JSONDecodeError:
        return inner


ENTRY_POINTS = [
    pytest.param(_via_helper, id="crs_utils.geoarrow_crs_to_projjson"),
    pytest.param(_via_streaming, id="streaming.extract_crs_from_table"),
    pytest.param(_via_crs_utils, id="crs_utils._crs_from_extension_type"),
    pytest.param(_via_duckdb_metadata, id="duckdb_metadata._get_pyarrow_logical_type"),
]


class TestCrsShapeMatrix:
    """Every carrier shape must resolve identically through every entry point."""

    @pytest.mark.parametrize("entry_point", ENTRY_POINTS)
    @pytest.mark.parametrize("carrier,expected", SHAPE_PARAMS)
    def test_shape_resolves_the_same_everywhere(self, entry_point, carrier, expected):
        _assert_resolved(entry_point(carrier), expected)


class TestGeometryTypeCrsFormatting:
    """Both ``extension_metadata`` rungs render a CRS the way DuckDB does.

    These are the fallbacks ``_get_pyarrow_logical_type`` drops to when
    ``field.type.crs`` yields nothing, and they now share one formatter with the
    ``.crs`` rung -- so a PROJJSON dict is inlined as JSON and a reference string
    (``srid:5070``, ``projjson:key``) stays verbatim, wherever it came from.
    """

    @pytest.mark.parametrize(
        "crs,expected",
        [
            ({"id": {"authority": "EPSG", "code": 5070}}, 'GeometryType(crs={"id": '),
            ("srid:5070", "GeometryType(crs=srid:5070)"),
        ],
        ids=["inline_projjson", "reference_string"],
    )
    def test_registered_type_falls_back_to_extension_metadata(self, crs, expected):
        field_type = _FakeGeoArrowType(None)
        field_type.extension_metadata = json.dumps({"crs": crs})

        logical = _get_pyarrow_logical_type(_FakeField(field_type))

        assert logical.startswith(expected)

    @pytest.mark.parametrize(
        "crs,expected",
        [
            ({"id": {"authority": "EPSG", "code": 5070}}, 'GeometryType(crs={"id": '),
            ("srid:5070", "GeometryType(crs=srid:5070)"),
        ],
        ids=["inline_projjson", "reference_string"],
    )
    def test_unregistered_field_reads_its_raw_metadata(self, crs, expected):
        """No extension type registered: the CRS is on ``field.metadata``."""
        field = _FakeField(
            pa.binary(),
            metadata={
                b"ARROW:extension:name": b"geoarrow.wkb",
                b"ARROW:extension:metadata": json.dumps({"crs": crs}).encode("utf-8"),
            },
        )

        logical = _get_pyarrow_logical_type(field)

        assert logical.startswith(expected)


class TestRejectionsAreReported:
    """Every rejection warns -- silence is how a wrong CRS gets written (#863)."""

    @pytest.mark.parametrize(
        "carrier,fragment",
        [
            (_OpaqueCrs(), "_OpaqueCrs"),
            (_ExplodingCrs(), "crs not found: EPSG:999999"),
            (b"\xff\xfe not utf-8", "bytes"),
        ],
        ids=["no_accessor", "accessor_raises", "undecodable_bytes"],
    )
    def test_rejection_warns(self, caplog, carrier, fragment):
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            assert geoarrow_crs_to_projjson(carrier) is None
        assert fragment in caplog.text

    def test_repr_is_never_offered_as_a_crs(self, caplog):
        """A repr is not a CRS; it must not appear in the warning either."""
        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            geoarrow_crs_to_projjson(_OpaqueCrs())
        assert "Opaque(EPSG:9999)" not in caplog.text


class TestEmptyValuesFallThrough:
    """An empty dict/string is not a CRS and must not shadow the `geo` block."""

    @pytest.mark.parametrize("empty", [{}, ""], ids=["empty_dict", "empty_string"])
    def test_empty_carrier_falls_back_to_geo_metadata(self, empty):
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": [],
                    "crs": _projjson("EPSG:3857"),
                }
            },
        }
        token = f"crs-empty-{len(_CRS_PAYLOADS)}".encode()
        _CRS_PAYLOADS[token] = empty
        storage = pa.array([_wkb_point(0.0, 0.0)], type=pa.binary())
        geom = pa.ExtensionArray.from_storage(_CrsCarrierType(token), storage)
        table = pa.table({"id": [1], "geometry": geom}).replace_schema_metadata(
            {b"geo": json.dumps(geo).encode("utf-8")}
        )

        crs = extract_crs_from_table(table, "geometry")

        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "EPSG", "code": 3857}


# ---------------------------------------------------------------------------
# Issue #863 item 2: GeoArrow file with no `geo` block
# ---------------------------------------------------------------------------


def _write_geoarrow_only_parquet(path, identifier: str = "EPSG:3857") -> dict:
    """A Parquet file carrying its CRS ONLY in ``ARROW:extension:metadata``.

    No ``geo`` key-value metadata at all -- the shape a plain GeoArrow writer
    produces. Coordinates are Web Mercator metres, impossible as lon/lat.
    """
    projjson = _projjson(identifier)
    field = pa.field(
        "geometry",
        pa.binary(),
        metadata={
            b"ARROW:extension:name": b"geoarrow.wkb",
            b"ARROW:extension:metadata": json.dumps({"crs": projjson}).encode("utf-8"),
        },
    )
    schema = pa.schema([pa.field("id", pa.int64()), field])
    table = pa.table(
        [
            pa.array([1, 2]),
            pa.array([_wkb_point(1e6, 5e6), _wkb_point(2e6, 6e6)], type=pa.binary()),
        ],
        schema=schema,
    )
    pq.write_table(table, path)
    written_meta = pq.read_schema(path).metadata or {}
    assert b"geo" not in written_meta, "fixture must have no geo block"
    return projjson


class TestNoGeoBlockCrs:
    """A GeoArrow input with no ``geo`` block must keep its CRS (#863)."""

    def test_table_crs_reads_the_extension_metadata(self, tmp_path):
        """``Table.crs`` used to be None, so the write labelled it CRS84."""
        import geoparquet_io as gpio

        src = tmp_path / "in.parquet"
        _write_geoarrow_only_parquet(src)

        crs = gpio.read(str(src)).crs

        assert crs is not None, "CRS lost: the write would label this data CRS84"
        assert isinstance(crs, dict)
        assert crs["id"] == {"authority": "EPSG", "code": 3857}

    def test_write_preserves_the_projected_crs(self, tmp_path):
        import geoparquet_io as gpio

        src = tmp_path / "in.parquet"
        out = tmp_path / "out.parquet"
        _write_geoarrow_only_parquet(src)

        gpio.read(str(src)).write(str(out))

        geo = json.loads(pq.read_schema(out).metadata[b"geo"].decode("utf-8"))
        written = geo["columns"][geo["primary_column"]].get("crs")
        assert written is not None, "projected data written as the CRS84 default"
        assert written["id"] == {"authority": "EPSG", "code": 3857}

    def test_write_preserves_the_crs_without_geoarrow_pyarrow_imported(self, tmp_path):
        """The mirror-image import state of #816, pinned in a clean subprocess.

        ``geoarrow.pyarrow`` registers its extension types globally, and whether
        some earlier import in the session did so decides *where* PyArrow puts
        the CRS. This test owns its process so the unregistered state -- the one
        that lost the CRS -- is the one actually exercised.

        ``gpio.read`` now registers the extension types itself (#1175), so the
        unregistered state is reached the way a caller still can: wrapping a
        table that plain ``pq.read_table`` produced.
        """
        src = tmp_path / "in.parquet"
        out = tmp_path / "out.parquet"
        _write_geoarrow_only_parquet(src)

        script = textwrap.dedent(
            """
            import json, sys
            import pyarrow.parquet as pq
            import geoparquet_io as gpio

            src, out = sys.argv[1], sys.argv[2]
            assert "geoarrow.pyarrow" not in sys.modules, "geoarrow.pyarrow was imported"
            gpio.Table(pq.read_table(src)).write(out)
            assert "geoarrow.pyarrow" not in sys.modules, "geoarrow.pyarrow was imported"
            geo = json.loads(pq.read_schema(out).metadata[b"geo"].decode("utf-8"))
            print(json.dumps(geo["columns"][geo["primary_column"]].get("crs")))
            """
        )
        proc = subprocess.run(
            [sys.executable, "-c", script, str(src), str(out)],
            capture_output=True,
            text=True,
            timeout=300,
        )

        assert proc.returncode == 0, proc.stderr
        written = json.loads(proc.stdout.strip().splitlines()[-1])
        assert written is not None, "projected data written as the CRS84 default"
        assert written["id"] == {"authority": "EPSG", "code": 3857}
