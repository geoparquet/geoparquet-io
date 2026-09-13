"""Every write strategy must give a SECONDARY geometry column the right carrier.

Regression tests for #706. A file may declare more than one geometry column
(`geo["columns"]` with a `primary_column` plus secondaries; gpio carries them
through `geometry_info["secondary"]`). Validation applies the same per-version
requirements to *every* column in `geo["columns"]`, but each strategy decided the
physical carrier for the **primary only** and let a secondary keep whatever
DuckDB or the input happened to hand over.

The measured result before this suite existed:

| strategy | secondary at 1.1 | import-state dependent? |
|---|---|---|
| duckdb-kv | native GEOMETRY inside a 1.x file -> invalid | no |
| in-memory | flips plain binary <-> native | **yes** |
| disk-rewrite | plain binary -> valid | no |
| streaming | plain binary -> valid (fixed by #688/#707) | no |

Nothing in the suite covered secondary-column carriers outside the streaming
strategy, which is why the other three went unnoticed.
"""

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
from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory
from geoparquet_io.core.write_strategies.base import resolve_geometry_columns

STRATEGIES = ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"]


def _wkb_point(x: float, y: float) -> bytes:
    return struct.pack("<BI2d", 1, 1, x, y)


def _geo_dict() -> dict:
    return {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Point"]},
            "geom2": {"encoding": "WKB", "geometry_types": ["Point"]},
        },
    }


def _geometry_info() -> dict:
    geo = _geo_dict()
    return {
        "primary": "geometry",
        "secondary": ["geom2"],
        "metadata": {"geom2": geo["columns"]["geom2"]},
    }


@pytest.fixture
def two_geometry_source(tmp_path):
    """A file declaring two WKB geometry columns."""
    path = tmp_path / "two_geom.parquet"
    table = pa.table(
        {
            "id": [1, 2],
            "geometry": [_wkb_point(1.0, 2.0), _wkb_point(3.0, 4.0)],
            "geom2": [_wkb_point(5.0, 6.0), _wkb_point(7.0, 8.0)],
        }
    )
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(_geo_dict()).encode()}), path)
    return str(path)


def _carriers(path: str) -> dict[str, str]:
    """Physical carrier per geometry column: "native" or the Parquet physical type.

    Read without the spatial extension so the answer is the file's own schema,
    not something DuckDB reconstructed.
    """
    con = get_duckdb_connection(load_spatial=False)
    try:
        rows = con.execute(
            f"SELECT name, type, logical_type FROM parquet_schema('{path}')"
        ).fetchall()
    finally:
        con.close()
    return {
        name: "native" if (logical and "Geometry" in str(logical)) else str(typ).lower()
        for name, typ, logical in rows
        if name in ("geometry", "geom2")
    }


def _failed_checks(path: str) -> list[str]:
    """Validator failures, minus the one Windows fails for a platform reason.

    On win32 `native_geo_stats_contains_data_*` reports every geometry as
    outside its own column's geospatial statistics, for any native GEOMETRY
    column written by any code path (#721, #748). The identical write produces a
    clean result on macOS and Linux, and uv.lock pins the same versions
    everywhere, so it is a platform read/write gap rather than anything the
    carrier decision here touches. `tests/test_geoparquet_versions.py` and
    `tests/test_streaming_write_determinism.py` excuse it the same way.

    Excused by name and only on win32: every other check stays enforced
    everywhere, which skipping the affected tests would not have preserved.
    """
    failed = sorted(
        {c.name for c in validate_geoparquet(path).checks if c.status.value == "failed"}
    )
    if sys.platform == "win32":
        failed = [f for f in failed if not f.startswith("native_geo_stats_contains_data")]
    return failed


def _write_via_query(source: str, out: str, version: str, strategy: str) -> None:
    con = get_duckdb_connection(load_spatial=True)
    try:
        metadata, _ = get_parquet_metadata(source)
        write_parquet_with_metadata(
            con,
            f"SELECT * FROM read_parquet('{source}')",
            out,
            original_metadata=metadata,
            geoparquet_version=version,
            write_strategy=strategy,
            input_file=source,
            geometry_info=_geometry_info(),
        )
    finally:
        con.close()


class TestSecondaryCarrierMatchesTheTargetVersion:
    """The carrier decision is per version and applies to every geometry column."""

    @pytest.mark.parametrize("strategy", STRATEGIES)
    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_v1x_writes_plain_wkb_for_both_columns(
        self, strategy, version, two_geometry_source, tmp_path
    ):
        """1.0/1.1 require plain BYTE_ARRAY WKB for every column in geo["columns"]."""
        out = str(tmp_path / f"{strategy}_{version}.parquet")

        _write_via_query(two_geometry_source, out, version, strategy)

        carriers = _carriers(out)
        assert carriers["geometry"] == "byte_array"
        assert carriers["geom2"] == "byte_array", (
            f"{strategy} gave the secondary column a {carriers['geom2']} carrier in a "
            f"{version} file"
        )

    @pytest.mark.parametrize("strategy", STRATEGIES)
    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_v1x_output_is_valid(self, strategy, version, two_geometry_source, tmp_path):
        """The file a native secondary produced failed version_features_match."""
        out = str(tmp_path / f"valid_{strategy}_{version}.parquet")

        _write_via_query(two_geometry_source, out, version, strategy)

        assert _failed_checks(out) == []

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_v20_writes_native_for_both_columns(self, strategy, two_geometry_source, tmp_path):
        """2.0 requires a native Parquet GEOMETRY type for every declared column."""
        out = str(tmp_path / f"{strategy}_20.parquet")

        _write_via_query(two_geometry_source, out, "2.0", strategy)

        carriers = _carriers(out)
        assert carriers["geometry"] == "native"
        assert carriers["geom2"] == "native", (
            f"{strategy} left the secondary column as {carriers['geom2']} in a 2.0 file"
        )
        assert _failed_checks(out) == []

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_rows_survive(self, strategy, two_geometry_source, tmp_path):
        """Converting a second column must not drop or reorder rows."""
        out = str(tmp_path / f"rows_{strategy}.parquet")

        _write_via_query(two_geometry_source, out, "1.1", strategy)

        table = pq.read_table(out)
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 2]


class TestSecondaryCarrierOnTheTableEntryPoint:
    """``write_from_table`` gets no ``geometry_info`` — the names come from the table."""

    @staticmethod
    def _table_with_extension_secondary():
        """A table whose SECONDARY column already carries the geoarrow extension type.

        This is the shape a caller produces in a process that imported
        ``geoarrow.pyarrow``; PyArrow writes such a column as a native Parquet
        GEOMETRY logical type, which is illegal below 2.0.
        """
        import geoarrow.pyarrow as ga

        table = pa.table(
            {
                "id": [1, 2],
                "geometry": pa.array(
                    [_wkb_point(1.0, 2.0), _wkb_point(3.0, 4.0)], type=pa.binary()
                ),
                "geom2": ga.as_wkb(
                    pa.array([_wkb_point(5.0, 6.0), _wkb_point(7.0, 8.0)], type=pa.binary())
                ),
            }
        )
        return table.replace_schema_metadata({b"geo": json.dumps(_geo_dict()).encode()})

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_extension_typed_secondary_is_written_as_plain_wkb_at_1_1(self, strategy, tmp_path):
        out = str(tmp_path / f"table_{strategy}.parquet")
        table = self._table_with_extension_secondary()
        assert table.schema.field("geom2").type.extension_name == "geoarrow.wkb"

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

        assert _carriers(out)["geom2"] == "byte_array", (
            f"{strategy} carried the geoarrow extension type into a 1.1 file"
        )
        assert _failed_checks(out) == []

    @staticmethod
    def _table_with_metadata_only_secondary():
        """The same table, with the extension name carried in FIELD METADATA only.

        This is the shape gpio's own ``add`` operations hand back (``Table.add_kdtree()``
        and friends): plain ``large_binary`` plus ``ARROW:extension:name``, which
        PyArrow leaves unresolved but DuckDB still registers as GEOMETRY. It is the
        same column as the resolved-extension shape above, so it must get the same
        carrier (#727).
        """
        marker = {
            b"ARROW:extension:name": b"geoarrow.wkb",
            b"ARROW:extension:metadata": b"{}",
        }
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("geometry", pa.large_binary(), metadata=marker),
                pa.field("geom2", pa.large_binary(), metadata=marker),
            ],
            metadata={b"geo": json.dumps(_geo_dict()).encode()},
        )
        return pa.table(
            {
                "id": [1, 2],
                "geometry": [_wkb_point(1.0, 2.0), _wkb_point(3.0, 4.0)],
                "geom2": [_wkb_point(5.0, 6.0), _wkb_point(7.0, 8.0)],
            },
            schema=schema,
        )

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_metadata_only_secondary_is_written_as_plain_wkb_at_1_1(self, strategy, tmp_path):
        out = str(tmp_path / f"meta_only_{strategy}.parquet")
        table = self._table_with_metadata_only_secondary()
        assert getattr(table.schema.field("geom2").type, "extension_name", None) is None

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

        assert _carriers(out)["geom2"] == "byte_array", (
            f"{strategy} carried a metadata-declared geoarrow column into a 1.1 file"
        )
        assert _failed_checks(out) == []

    @staticmethod
    def _table_with_plain_binary_secondary():
        """The same table with NO extension marker anywhere on the secondary.

        A declared geometry column that is already the canonical plain ``binary``
        carrier: the writer has nothing to do, and must leave it exactly as it is
        rather than round-tripping it through a cast.
        """
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("geometry", pa.binary()),
                pa.field("geom2", pa.binary()),
            ],
            metadata={b"geo": json.dumps(_geo_dict()).encode()},
        )
        return pa.table(
            {
                "id": [1, 2],
                "geometry": [_wkb_point(1.0, 2.0), _wkb_point(3.0, 4.0)],
                "geom2": [_wkb_point(5.0, 6.0), _wkb_point(7.0, 8.0)],
            },
            schema=schema,
        )

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_unmarked_plain_binary_secondary_is_left_alone_at_1_1(self, strategy, tmp_path):
        out = str(tmp_path / f"unmarked_{strategy}.parquet")
        table = self._table_with_plain_binary_secondary()
        assert table.schema.field("geom2").metadata is None

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

        assert _carriers(out)["geom2"] == "byte_array", (
            f"{strategy} changed the carrier of an already-canonical secondary column"
        )
        assert pq.read_table(out).column("geom2").to_pylist() == [
            _wkb_point(5.0, 6.0),
            _wkb_point(7.0, 8.0),
        ]
        assert _failed_checks(out) == []

    @staticmethod
    def _table_with_native_geoarrow_point_secondary():
        """A secondary declared as native GeoArrow ``point`` over unresolved storage.

        GeoParquet 1.1 allows a geometry column to use the ``point`` encoding: a
        ``struct<x: double, y: double>`` carrying ``ARROW:extension:name =
        geoarrow.point``. When nothing in the process registered
        ``geoarrow.pyarrow``, PyArrow leaves the extension unresolved and the
        column arrives as a plain struct plus that field-metadata marker.

        Those are NOT WKB bytes, so nothing may force-cast them to ``binary`` --
        doing so raises ``ArrowNotImplementedError`` ("Unsupported cast from
        struct<x: double, y: double> to binary") out of the middle of a write.
        """
        geo = _geo_dict()
        geo["columns"]["geom2"] = {"encoding": "point", "geometry_types": ["Point"]}
        point_type = pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.float64())])
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("geometry", pa.binary()),
                pa.field(
                    "geom2",
                    point_type,
                    metadata={
                        b"ARROW:extension:name": b"geoarrow.point",
                        b"ARROW:extension:metadata": b"{}",
                    },
                ),
            ],
            metadata={b"geo": json.dumps(geo).encode()},
        )
        return pa.table(
            {
                "id": [1, 2],
                "geometry": [_wkb_point(1.0, 2.0), _wkb_point(3.0, 4.0)],
                "geom2": pa.array([{"x": 5.0, "y": 6.0}, {"x": 7.0, "y": 8.0}], type=point_type),
            },
            schema=schema,
        )

    def test_native_geoarrow_point_secondary_is_not_cast_to_binary(self, tmp_path):
        """duckdb-kv must not force a non-WKB geoarrow carrier through a binary cast."""
        out = str(tmp_path / "native_point_secondary.parquet")
        table = self._table_with_native_geoarrow_point_secondary()

        WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV).write_from_table(
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

        written = pq.read_table(out)
        assert pa.types.is_struct(written.schema.field("geom2").type)
        assert written.column("geom2").to_pylist() == [
            {"x": 5.0, "y": 6.0},
            {"x": 7.0, "y": 8.0},
        ]


_DETERMINISM_SCRIPT = textwrap.dedent(
    """
    import json, struct, sys
    source, out, do_import = sys.argv[1], sys.argv[2], sys.argv[3] == "import"
    if do_import:
        import geoarrow.pyarrow  # noqa: F401
    from geoparquet_io.core.common import get_parquet_metadata, write_parquet_with_metadata
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=True)
    metadata, _ = get_parquet_metadata(source)
    write_parquet_with_metadata(
        con,
        "SELECT * FROM read_parquet('" + source + "')",
        out,
        original_metadata=metadata,
        geoparquet_version=sys.argv[4],
        write_strategy=sys.argv[5],
        input_file=source,
        geometry_info={
            "primary": "geometry",
            "secondary": ["geom2"],
            "metadata": {"geom2": {"encoding": "WKB", "geometry_types": ["Point"]}},
        },
    )
    con.close()
    """
)


@pytest.mark.parametrize("strategy", STRATEGIES)
@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_secondary_carrier_is_independent_of_geoarrow_import(
    strategy, version, two_geometry_source, tmp_path
):
    """Same input, same bytes out, whatever the process imported.

    ``in-memory`` flipped the secondary column between plain binary and a native
    GEOMETRY type depending on whether anything had imported
    ``geoarrow.pyarrow`` — the #688 nondeterminism, on the secondary column.

    Runs in subprocesses because geoarrow's extension registration is
    process-global and cannot be undone once done.
    """
    outputs = {}
    for label in ("clean", "import"):
        out = tmp_path / f"{strategy}_{version}_{label}.parquet"
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                _DETERMINISM_SCRIPT,
                two_geometry_source,
                str(out),
                label,
                version,
                strategy,
            ],
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=300,
        )
        assert result.returncode == 0, f"subprocess failed:\n{result.stderr}"
        outputs[label] = out

    clean, imported = outputs["clean"], outputs["import"]
    assert (
        hashlib.sha256(clean.read_bytes()).hexdigest()
        == hashlib.sha256(imported.read_bytes()).hexdigest()
    ), (
        f"{strategy} at {version} is import-state dependent: clean wrote "
        f"{_carriers(str(clean))}, after importing geoarrow.pyarrow it wrote "
        f"{_carriers(str(imported))}"
    )
    assert _failed_checks(str(clean)) == []


class TestResolveGeometryColumns:
    """The one place every strategy asks "which columns are geometry?"."""

    def test_primary_only(self):
        assert resolve_geometry_columns("geometry") == {"geometry"}

    def test_includes_secondaries_from_geometry_info(self):
        info = {"primary": "geometry", "secondary": ["geom2", "geom3"], "metadata": {}}
        assert resolve_geometry_columns("geometry", info) == {"geometry", "geom2", "geom3"}

    def test_includes_columns_named_only_in_geo_metadata(self):
        assert resolve_geometry_columns("geometry", None, _geo_dict()) == {"geometry", "geom2"}

    def test_geometry_info_wins_when_geo_metadata_is_absent(self):
        """parquet-geo-only writes no geo metadata, so the names must come from geometry_info."""
        info = {"primary": "geometry", "secondary": ["geom2"], "metadata": {}}
        assert resolve_geometry_columns("geometry", info, None) == {"geometry", "geom2"}

    def test_tolerates_empty_inputs(self):
        assert resolve_geometry_columns("geometry", {}, {}) == {"geometry"}
        assert resolve_geometry_columns("geometry", {"secondary": None}, None) == {"geometry"}


class TestCarrierHelperDefensivePaths:
    """The error and empty-input branches of the helpers this PR adds or fixes."""

    def test_parse_geo_metadata_quietly_handles_every_absent_shape(self):
        from geoparquet_io.core.common import _parse_geo_metadata_quietly

        assert _parse_geo_metadata_quietly(None) == {}
        assert _parse_geo_metadata_quietly({}) == {}
        assert _parse_geo_metadata_quietly({b"other": b"x"}) == {}

    def test_parse_geo_metadata_quietly_accepts_a_str_key(self):
        from geoparquet_io.core.common import _parse_geo_metadata_quietly

        assert _parse_geo_metadata_quietly({"geo": json.dumps(_geo_dict())})["primary_column"] == (
            "geometry"
        )

    @pytest.mark.parametrize(
        "raw",
        [b"{not json", b"\xff\xfe not utf8", b'"a string, not an object"', b"[1, 2]"],
        ids=["bad_json", "bad_utf8", "json_string", "json_list"],
    )
    def test_parse_geo_metadata_quietly_swallows_unreadable_values(self, raw):
        """An unreadable geo key must not break a write; it just names no columns."""
        from geoparquet_io.core.common import _parse_geo_metadata_quietly

        assert _parse_geo_metadata_quietly({b"geo": raw}) == {}

    def test_strip_geoarrow_returns_the_table_when_conversion_fails(self, monkeypatch):
        """A conversion failure must leave the table alone, not raise mid-write."""
        import geoarrow.pyarrow as ga

        from geoparquet_io.core import common

        table = pa.table(
            {"geometry": ga.as_wkb(pa.array([_wkb_point(1.0, 2.0)], type=pa.binary()))}
        )
        monkeypatch.setattr(
            "geoparquet_io.core.write_strategies.arrow_streaming._to_plain_wkb_array",
            lambda array: (_ for _ in ()).throw(ValueError("boom")),
        )

        result = common._strip_geoarrow_to_plain_wkb(table, "geometry", verbose=True)

        assert result is table

    def test_canonicalize_skips_a_column_it_cannot_convert(self, monkeypatch):
        """Same contract for the field-metadata canonicalization pass."""
        from geoparquet_io.core import common

        field = pa.field(
            "geometry", pa.large_binary(), metadata={b"ARROW:extension:name": b"geoarrow.wkb"}
        )
        table = pa.Table.from_arrays(
            [pa.array([_wkb_point(1.0, 2.0)], type=pa.large_binary())],
            schema=pa.schema([field]),
        )
        monkeypatch.setattr(
            "geoparquet_io.core.write_strategies.arrow_streaming._to_plain_wkb_array",
            lambda array: (_ for _ in ()).throw(ValueError("boom")),
        )

        result = common._canonicalize_wkb_columns(table, {"geometry"}, verbose=True)

        assert result.schema.field("geometry").type == pa.large_binary()

    def test_canonicalize_is_a_no_op_when_nothing_needs_changing(self):
        from geoparquet_io.core.common import _canonicalize_wkb_columns

        table = pa.table({"geometry": pa.array([_wkb_point(1.0, 2.0)], type=pa.binary())})

        assert _canonicalize_wkb_columns(table, {"geometry"}) is table

    def test_a_declared_column_missing_from_the_table_is_skipped(self, tmp_path):
        """geo metadata can name a column a projection has already dropped."""
        from geoparquet_io.core.common import _apply_geoparquet_metadata

        table = pa.table(
            {
                "id": [1],
                "geometry": pa.array([_wkb_point(1.0, 2.0)], type=pa.binary()),
            }
        ).replace_schema_metadata({b"geo": json.dumps(_geo_dict()).encode()})

        result = _apply_geoparquet_metadata(
            table,
            geometry_column="geometry",
            geoparquet_version="1.1",
            geometry_info=_geometry_info(),
        )

        assert result.column_names == ["id", "geometry"]


class TestNonGeometryColumnsNamedAsGeometry:
    """A declared geometry column is not always a GEOMETRY column.

    ``geo["columns"]`` and ``geometry_info`` name columns by *declaration*. What
    DuckDB actually reads them as is a separate question, and the 1.x BLOB cast
    has to key on the latter -- ``ST_AsWKB(BLOB)`` does not bind and would abort
    the whole write, while ``ST_AsWKB(VARCHAR)`` binds and silently reinterprets
    a text column as WKT.
    """

    def test_blob_and_text_secondaries_are_left_alone(self):
        from geoparquet_io.core.duckdb_utils import _wrap_query_with_blob_conversion

        con = get_duckdb_connection()
        con.execute(
            "CREATE TABLE t AS SELECT 1 AS id, ST_Point(1, 2) AS geometry, "
            "'x'::BLOB AS already_wkb, 42 AS not_geometry, 'POINT(3 4)' AS text_col"
        )
        query = _wrap_query_with_blob_conversion(
            "SELECT * FROM t",
            "geometry",
            con,
            secondary_columns=["already_wkb", "not_geometry", "text_col"],
        )

        # Only the real GEOMETRY column is rewritten.
        assert 'ST_AsWKB("geometry")' in query
        for untouched in ("already_wkb", "not_geometry", "text_col"):
            assert f'ST_AsWKB("{untouched}")' not in query

        result = con.execute(query).arrow().read_all()
        assert result.column("already_wkb").to_pylist() == [b"x"]
        assert result.column("not_geometry").to_pylist() == [42]
        assert result.column("text_col").to_pylist() == ["POINT(3 4)"]


class TestSchemaCarriedCrsSurvives:
    """A geo block may omit ``crs``; the schema type is still authoritative at 2.0."""

    def test_crs_carried_only_by_the_schema_type_is_not_cleared(self, tmp_path):
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.write_funnels import write_geoparquet_table

        projected = {
            "type": "ProjectedCRS",
            "name": "WGS 84 / Pseudo-Mercator",
            "id": {"authority": "EPSG", "code": 3857},
        }
        base = ga.as_wkb(pa.array([_wkb_point(1.0, 2.0)], type=pa.binary()))
        with_crs = pa.ExtensionArray.from_storage(base.type.with_crs(projected), base.storage)
        assert with_crs.type.crs is not None, "fixture must carry a CRS to be meaningful"

        geo = {
            "version": "2.0.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
        table = pa.table(
            {"id": [1], "geometry": pa.chunked_array([with_crs])}
        ).replace_schema_metadata({b"geo": json.dumps(geo).encode()})

        out = tmp_path / "projected.parquet"
        write_geoparquet_table(table, str(out), geoparquet_version="2.0")

        written = pq.ParquetFile(str(out)).schema_arrow.field("geometry")
        assert "3857" in str(written.type.crs), (
            f"the schema type's CRS was cleared, leaving {written.type.crs!r}"
        )


class TestMalformedCarriedColumns:
    """A carried ``geo`` block is arbitrary JSON from someone else's file."""

    @pytest.mark.parametrize(
        "columns",
        [None, ["geometry"], "geometry", {"geometry": "WKB"}],
        ids=["null", "list", "string", "dict_of_non_dict"],
    )
    def test_a_malformed_columns_value_does_not_reach_the_carrier_logic(self, columns):
        from geoparquet_io.core.common import _parse_geo_metadata_quietly

        raw = {"version": "1.1.0", "primary_column": "geometry", "columns": columns}
        parsed = _parse_geo_metadata_quietly({b"geo": json.dumps(raw).encode()})

        # Either dropped entirely or reduced to the dict-valued entries, so that
        # `columns[name]["crs"]` downstream cannot raise.
        entries = parsed.get("columns", {})
        assert isinstance(entries, dict)
        assert all(isinstance(entry, dict) for entry in entries.values())
        assert resolve_geometry_columns("geometry", None, parsed) == {"geometry"}
