"""A ``srid:N`` type crs must not crash a read once ``geoarrow.pyarrow`` is loaded.

PyArrow maps a Parquet GEOMETRY whose crs is ``srid:5070`` (or the ``srid:0``
gpio writes for an unknown CRS, #1276) to geoarrow extension metadata
``{"crs": "5070", "crs_type": "srid"}``. geoarrow-types turns that into
``StringCrs("5070")``, whose JSON form decodes the text to the *integer* 5070.
``crs.create(5070)`` then raises inside ``__arrow_ext_deserialize__``, and
PyArrow does not survive that exception: the process segfaults in
``pq.read_table``. That is why the reads run in a subprocess here, a crash
would otherwise take the pytest worker down with it.

The child ends with ``os._exit(0)``. Arrow's thread pool can deadlock in its
own teardown at interpreter exit after any native GEOMETRY read with
``geoarrow.pyarrow`` loaded, srid or not. That is a separate upstream issue
and not what these tests measure.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap

import duckdb
import pyarrow as pa
import pytest

from geoparquet_io.core.duckdb_utils import sql_path


def _duckdb_srid(path, srid: str) -> str:
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(
            f"COPY (SELECT 1 AS id, ST_SetCRS(ST_Point(1, 2), '{srid}') AS geometry) "
            f"TO {sql_path(str(path))} (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')"
        )
    finally:
        con.close()
    return str(path)


def _run(code: str) -> subprocess.CompletedProcess:
    exit_now = "\nimport os\nos._exit(0)\n"
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code) + exit_now],
        capture_output=True,
        text=True,
        timeout=120,
        stdin=subprocess.DEVNULL,
    )


@pytest.mark.parametrize(
    ("srid", "expected_crs"),
    [("srid:0", "None"), ("srid:5070", "5070")],
)
def test_gpio_read_of_srid_type_does_not_crash(srid, expected_crs, tmp_path):
    src = _duckdb_srid(tmp_path / "in.parquet", srid)
    result = _run(
        f"""
        import json
        import geoparquet_io as gpio
        t = gpio.read({src!r})
        print("rows", t.num_rows, flush=True)
        print("crs", json.dumps(t.crs), flush=True)
        """
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert "rows 1" in result.stdout
    crs_line = next(line for line in result.stdout.splitlines() if line.startswith("crs "))
    if expected_crs == "None":
        assert crs_line == "crs null"
    else:
        assert expected_crs in crs_line


def test_pyarrow_written_srid_type_reads_back(tmp_path):
    import geoarrow.pyarrow as ga
    import pyarrow.parquet as pq

    typ = ga.wkb().with_crs("srid:0")
    storage = pa.array([b"\x01\x01\x00\x00\x00" + b"\x00" * 16], pa.binary())
    out = tmp_path / "pa.parquet"
    pq.write_table(pa.table({"geometry": pa.ExtensionArray.from_storage(typ, storage)}), out)
    result = _run(
        f"""
        import geoparquet_io  # noqa: F401  installs the compat shim
        import geoarrow.pyarrow  # noqa: F401
        import pyarrow.parquet as pq
        print("rows", pq.read_table({str(out)!r}).num_rows, flush=True)
        """
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert "rows 1" in result.stdout


def test_string_crs_keeps_a_scalar_as_text():
    from geoarrow.types import crs

    from geoparquet_io.core.geoarrow_compat import install

    install()
    assert crs.StringCrs("0").__geoarrow_crs_json_values__() == {"crs": "0"}
    assert crs.StringCrs("5070").__geoarrow_crs_json_values__() == {"crs": "5070"}
    # A PROJJSON object is still embedded as an object, not escaped into a string.
    assert crs.StringCrs('{"a": 1}').__geoarrow_crs_json_values__() == {"crs": {"a": 1}}
    assert crs.StringCrs("EPSG:4326").__geoarrow_crs_json_values__() == {"crs": "EPSG:4326"}


def test_string_crs_keeps_a_json_string_crs_decoded():
    """A JSON string CRS is the string it encodes, as upstream reads it."""
    import geoarrow.pyarrow as ga
    from geoarrow.types import crs

    from geoparquet_io.core.geoarrow_compat import install

    install()
    assert crs.StringCrs('"OGC:CRS84"').__geoarrow_crs_json_values__() == {"crs": "OGC:CRS84"}
    typ = ga.wkb().with_crs('"OGC:CRS84"')
    assert json.loads(typ.__arrow_ext_serialize__()) == {"crs": "OGC:CRS84"}


class _FixedCrsModule:
    """A geoarrow-types whose numeric crs round trip already works."""

    class StringCrs:
        def __init__(self, text):
            self._crs = text

        def __geoarrow_crs_json_values__(self):
            return {"crs": self._crs}

    @classmethod
    def create(cls, obj):
        if isinstance(obj, str):
            return cls.StringCrs(obj)
        raise ValueError(obj)


class _BrokenCrsModule(_FixedCrsModule):
    """The geoarrow-types 0.3 to 0.4.1 shape: ``"0"`` decodes to the integer 0."""

    class StringCrs(_FixedCrsModule.StringCrs):
        def __geoarrow_crs_json_values__(self):
            return {"crs": json.loads(self._crs)}


def test_the_probe_tells_a_broken_crs_round_trip_from_a_fixed_one():
    from geoparquet_io.core.geoarrow_compat import crs_round_trip_is_broken

    assert crs_round_trip_is_broken(_BrokenCrsModule) is True
    assert crs_round_trip_is_broken(_FixedCrsModule) is False


def test_the_probe_matches_the_installed_geoarrow_types():
    """Pins the upstream bug (geoarrow/geoarrow-python#73) the shim exists for.

    The child loads the shim's file alone, so geoarrow-types is probed before
    anything has patched it. When an upstream release fixes the round trip,
    this fails and the shim can go.
    """
    from geoparquet_io.core import geoarrow_compat

    result = _run(
        f"""
        import importlib.util
        from geoarrow.types import crs
        spec = importlib.util.spec_from_file_location("compat", {geoarrow_compat.__file__!r})
        compat = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(compat)
        print("broken", compat.crs_round_trip_is_broken(crs), flush=True)
        """
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert "broken True" in result.stdout


def test_a_fixed_geoarrow_types_is_left_unpatched():
    from geoparquet_io.core.geoarrow_compat import _patch_crs

    module = type("M", (_FixedCrsModule,), {})
    original = module.create
    assert _patch_crs(module) is False
    assert module.create == original


@pytest.mark.parametrize(
    ("returned", "drops"),
    [({"crs": "srid:0"}, False), ({"crs": "0"}, True), ({"crs": 0}, True), (ValueError, True)],
)
def test_the_type_spec_probe(returned, drops):
    from geoparquet_io.core.geoarrow_compat import _type_spec_drops_the_srid_prefix

    class _Crs:
        def __geoarrow_crs_json_values__(self):
            if returned is ValueError:
                raise ValueError("bad")
            return returned

    class _Spec:
        crs = _Crs()

    class _TypeSpec:
        @staticmethod
        def from_extension_metadata(_):
            return _Spec()

    module = type("type_spec", (), {"TypeSpec": _TypeSpec})
    assert _type_spec_drops_the_srid_prefix(module) is drops


def test_create_accepts_a_numeric_crs():
    from geoarrow.types import crs

    from geoparquet_io.core.geoarrow_compat import install

    install()
    created = crs.create(5070)
    assert isinstance(created, crs.StringCrs)
    assert created.__geoarrow_crs_json_values__() == {"crs": "5070"}
    assert crs.create("EPSG:4326").__geoarrow_crs_json_values__() == {"crs": "EPSG:4326"}
    with pytest.raises(ValueError):
        crs.create(object())
    with pytest.raises(ValueError):
        crs.create(True)


def test_install_is_idempotent():
    from geoarrow.types import crs

    from geoparquet_io.core.geoarrow_compat import install

    install()
    patched = crs.create
    install()
    assert crs.create is patched
    assert crs.create(0).__geoarrow_crs_json_values__() == {"crs": "0"}


def test_srid_extension_metadata_keeps_the_srid_prefix():
    import geoarrow.types.type_spec  # noqa: F401

    from geoparquet_io.core.geoarrow_compat import install, srid_crs_as_text

    install()
    assert srid_crs_as_text({"crs": "5070", "crs_type": "srid"}) == {"crs": "srid:5070"}
    assert srid_crs_as_text({"crs": "EPSG:4326"}) == {"crs": "EPSG:4326"}
    assert srid_crs_as_text({"crs_type": "srid"}) == {"crs_type": "srid"}

    spec_mod = sys.modules["geoarrow.types.type_spec"]
    spec = spec_mod.TypeSpec.from_extension_metadata('{"crs": "0", "crs_type": "srid"}')
    assert spec.crs.__geoarrow_crs_json_values__() == {"crs": "srid:0"}
    # Empty or non-srid metadata goes to the upstream parser untouched.
    assert spec_mod.TypeSpec.from_extension_metadata("").crs is None
    other = spec_mod.TypeSpec.from_extension_metadata('{"crs": "EPSG:3857"}')
    assert other.crs.__geoarrow_crs_json_values__() == {"crs": "EPSG:3857"}
    with pytest.raises(ValueError):
        spec_mod.TypeSpec.from_extension_metadata("{not json")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("srid:0", None), ("SRID:0", None), ("srid:5070", "srid:5070")],
)
def test_geoarrow_crs_reader_reads_srid_text(raw, expected):
    from geoarrow.types import crs

    from geoparquet_io.core.crs_utils import geoarrow_crs_to_projjson

    assert geoarrow_crs_to_projjson(crs.StringCrs(raw)) == expected
    assert geoarrow_crs_to_projjson(raw) == expected


def _raw_ext_table(ext_metadata: str) -> pa.Table:
    field = pa.field(
        "geometry",
        pa.binary(),
        metadata={
            "ARROW:extension:name": "geoarrow.wkb",
            "ARROW:extension:metadata": ext_metadata,
        },
    )
    return pa.Table.from_arrays([pa.array([b""], pa.binary())], schema=pa.schema([field]))


@pytest.mark.parametrize(
    ("ext_metadata", "expected"),
    [
        ('{"crs": "5070", "crs_type": "srid"}', "srid:5070"),
        ('{"crs": "0", "crs_type": "srid"}', None),
        ('{"crs": "EPSG:3857"}', "EPSG:3857"),
    ],
)
def test_raw_extension_metadata_srid_is_read_as_srid_text(ext_metadata, expected):
    from geoparquet_io.core.crs_utils import _crs_from_geoarrow_field

    assert _crs_from_geoarrow_field(_raw_ext_table(ext_metadata), "geometry") == expected
