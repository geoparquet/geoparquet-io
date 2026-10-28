"""A native secondary geometry column is described on EVERY write path (#1175).

#1000 was fixed by #1170 for one path only: a **local single-file** rewrite that
passes an ``input_file`` witness. Everything else still left a secondary native
``GEOMETRY``/``GEOGRAPHY`` column out of ``geo.columns`` -- which GeoParquet 2.0
requires for every geometry column in the file, and whose absence makes a 1.1
output's secondary a native type inside a file whose version forbids it.

The five paths, one class each:

* **remote, glob and directory inputs** -- the derivation read the input with
  ``pq.ParquetFile``, so ``https://``, ``s3://``, ``dir/*.parquet`` and hive
  directories derived nothing. It now reads through ``get_schema_info`` /
  ``get_geo_metadata``, the pair ``collect_nonplanar_edges`` already asks the
  same question of, which resolve all four spellings.
* **the 2.0 fast path** -- ``_geo_block_to_carry_on_fast_path`` writes the
  input's own block verbatim, so a 2.0 input whose block omits a native
  secondary kept it undescribed through ``sort``/``extract``.
* **``gpio convert``** -- it passes a ``geometry_info`` built from the ``geo``
  block alone, which for a native-geo-only input names no secondary at all. A
  caller-supplied ``geometry_info`` is now *merged* with the derived one rather
  than suppressing it; the caller's own entries still win.
* **``partition`` staging** -- it passes no ``input_file``. The output query's
  own ``DESCRIBE`` names every ``GEOMETRY`` column it emits and the CRS each one
  carries (``GEOMETRY('EPSG:3857')``), so no witness is needed.
* **the Table API** (``gpio.read(f).write(out)``) -- the table entry points
  derive nothing; the secondaries now come off the table's own schema.

Remote spellings: ``https://`` and ``s3://`` are covered *by construction* --
they go through the same ``get_schema_info``/``get_geo_metadata`` pair as the
glob and directory cases, which these tests do exercise -- not by a test, since
the suite does not reach the network.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.derive_geo_from_file import derive_secondary_geometry_info

EPSG_3857 = {"authority": "EPSG", "code": 3857}
EPSG_5070 = {"authority": "EPSG", "code": 5070}


def _geo(path) -> dict:
    with pq.ParquetFile(str(path)) as reader:
        kv = reader.metadata.metadata or {}
    assert b"geo" in kv, f"{path} has no geo key"
    return json.loads(kv[b"geo"].decode("utf-8"))


def _run_cli(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


def _two_native_geometries(path):
    """``geometry`` EPSG:5070 + ``centroid`` EPSG:3857, native types, no ``geo`` key."""
    import geoarrow.pyarrow as ga

    from tests.native_geo_probes import conus_wkb, projjson, write_native_geo_only

    rows = conus_wkb(
        "ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)",
        "ST_Transform(ST_Centroid(cell), 'EPSG:4326', 'EPSG:3857', always_xy := true)",
    )
    return write_native_geo_only(
        path,
        rows,
        {
            "geometry": (2, ga.wkb().with_crs(projjson(5070))),
            "centroid": (3, ga.wkb().with_crs(projjson(3857))),
        },
    )


@pytest.fixture(scope="module")
def native_pair(tmp_path_factory):
    return _two_native_geometries(tmp_path_factory.mktemp("native_pair") / "pgo.parquet")


@pytest.fixture(scope="module")
def native_pair_dataset(tmp_path_factory, native_pair):
    """A plain directory of two copies, and a hive directory holding one."""
    import shutil

    root = tmp_path_factory.mktemp("native_pair_dataset")
    flat = root / "flat"
    flat.mkdir()
    shutil.copy(native_pair, flat / "a.parquet")
    shutil.copy(native_pair, flat / "b.parquet")
    hive = root / "hive"
    (hive / "grp=g0").mkdir(parents=True)
    shutil.copy(native_pair, hive / "grp=g0" / "part-0.parquet")
    return flat, hive


# ---------------------------------------------------------------------------
# Remote, glob and directory inputs
# ---------------------------------------------------------------------------


class TestMultiFileAndRemoteInputs:
    def test_a_glob_derives_the_secondary_with_its_own_crs(self, native_pair_dataset):
        flat, _ = native_pair_dataset

        info = derive_secondary_geometry_info(f"{flat}/*.parquet", "geometry")

        assert info is not None, "a glob input derived nothing"
        assert info["secondary"] == ["centroid"]
        assert info["metadata"]["centroid"]["crs"]["id"] == EPSG_3857

    def test_a_hive_directory_derives_the_secondary(self, native_pair_dataset):
        _, hive = native_pair_dataset

        info = derive_secondary_geometry_info(str(hive), "geometry")

        assert info is not None, "a hive directory input derived nothing"
        assert info["secondary"] == ["centroid"]
        assert info["metadata"]["centroid"]["crs"]["id"] == EPSG_3857

    def test_extract_from_a_glob_describes_the_secondary(self, native_pair_dataset, tmp_path):
        flat, _ = native_pair_dataset
        output = tmp_path / "out.parquet"

        _run_cli(
            "extract", "geoparquet", f"{flat}/*.parquet", output, "--geoparquet-version", "1.1"
        )

        columns = _geo(output)["columns"]
        assert "centroid" in columns, "the glob's secondary is undescribed"
        assert columns["centroid"]["crs"]["id"] == EPSG_3857
        assert isinstance(columns["centroid"]["geometry_types"], list)

    def test_a_nested_geometry_leaf_does_not_shadow_a_top_level_column(self, tmp_path):
        """``meta.label`` (GEOMETRY) must not lend its logical type to ``label`` (VARCHAR).

        ``get_schema_info``'s pyarrow fast path keyed its logical-type lookup by
        a *leaf* name, so the nested leaf overwrote the top-level column of the
        same name -- which would make the derivation treat a string column as a
        native secondary.
        """
        import geoarrow.pyarrow as ga

        from geoparquet_io.core.duckdb_metadata import get_schema_info
        from geoparquet_io.core.parquet_schema import root_schema_columns
        from tests.native_geo_probes import conus_wkb, projjson

        rows = conus_wkb("cell", "ST_Centroid(cell)")
        nested = pa.ExtensionArray.from_storage(
            ga.wkb().with_crs(projjson(3857)), pa.array([bytes(r[3]) for r in rows], pa.binary())
        )
        table = pa.table(
            {
                "label": pa.array([f"row-{r[0]}" for r in rows]),
                "geometry": pa.ExtensionArray.from_storage(
                    ga.wkb().with_crs(projjson(5070)),
                    pa.array([bytes(r[2]) for r in rows], pa.binary()),
                ),
                "meta": pa.StructArray.from_arrays(
                    [nested, pa.array(["n"] * len(rows))], names=["label", "note"]
                ),
            }
        )
        source = tmp_path / "nested.parquet"
        pq.write_table(table, str(source))

        by_name = {
            column["name"]: column["logical_type"]
            for column in root_schema_columns(get_schema_info(str(source)))
        }
        assert by_name["label"] is None, by_name
        assert (by_name["geometry"] or "").startswith("Geometry"), by_name

        info = derive_secondary_geometry_info(str(source), "geometry")
        assert info is None or "label" not in info["secondary"]


# ---------------------------------------------------------------------------
# `partition` staging: rows from a staging file, witness from the input (#1005)
# ---------------------------------------------------------------------------


class TestPartitionStaging:
    def test_a_1_1_partition_describes_and_converts_the_native_secondary(
        self, native_pair, tmp_path
    ):
        """A 1.1 partition describes the native secondary and converts it to WKB.

        Before: every partition held ``centroid`` as a native Parquet GEOMETRY
        inside a file declaring 1.1 -- which ``check spec`` rejects -- and left it
        out of ``geo.columns`` entirely.
        """
        from tests.native_geo_probes import logical_geo_types

        output = tmp_path / "parts"

        _run_cli(
            "partition",
            "string",
            native_pair,
            output,
            "--column",
            "grp",
            "--geoparquet-version",
            "1.1",
            "--force",
        )

        files = sorted(output.rglob("*.parquet"))
        assert files, "partition wrote nothing"
        for part in files:
            columns = _geo(part)["columns"]
            assert "centroid" in columns, f"{part.name} leaves the secondary undescribed"
            assert isinstance(columns["centroid"]["geometry_types"], list)
            assert columns["centroid"]["crs"]["id"] == EPSG_3857
            assert logical_geo_types(part) == {}, (
                f"{part.name} declares 1.1 but still carries native geo types"
            )

    def test_native_geometry_types_come_off_the_output_query(self, native_pair):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
        from geoparquet_io.core.geometry_detection import native_geometry_types_from_query

        con = get_duckdb_connection(load_spatial=True)
        try:
            types = native_geometry_types_from_query(
                con, f"SELECT * FROM read_parquet({sql_path(str(native_pair))})"
            )
        finally:
            con.close()

        assert sorted(types) == ["centroid", "geometry"]
        assert types["centroid"] == "GeometryType(crs=EPSG:3857)"
        assert types["geometry"] == "GeometryType(crs=EPSG:5070)"


# ---------------------------------------------------------------------------
# `gpio convert`: a caller-supplied geometry_info no longer suppresses the rest
# ---------------------------------------------------------------------------


class TestConvert:
    def test_convert_to_1_1_describes_and_converts_the_native_secondary(
        self, native_pair, tmp_path
    ):
        """``convert`` named its secondaries from the ``geo`` block, which here is absent.

        Before: ``centroid`` stayed a native Parquet GEOMETRY inside a file
        declaring 1.1, undescribed, while ``extract`` of the same input was
        already correct.
        """
        from tests.native_geo_probes import logical_geo_types

        output = tmp_path / "out.parquet"

        _run_cli("convert", "geoparquet", native_pair, output, "--geoparquet-version", "1.1")

        columns = _geo(output)["columns"]
        assert "centroid" in columns, "convert leaves the secondary undescribed"
        assert columns["centroid"]["crs"]["id"] == EPSG_3857
        assert isinstance(columns["centroid"]["geometry_types"], list)
        assert logical_geo_types(output) == {}, (
            "convert declares 1.1 but still carries native geo types"
        )

    def test_the_callers_own_primary_entry_still_wins(self, native_pair, tmp_path):
        """The derivation adds columns; it never overrules what the caller resolved."""
        from geoparquet_io.core.write_funnels import _merge_derived_geometry_info

        caller = {
            "primary": "geometry",
            "secondary": [],
            "metadata": {"geometry": {"encoding": "geoarrow.wkb"}, "centroid": {"crs": None}},
        }
        derived = {
            "primary": "geometry",
            "secondary": ["centroid", "extra"],
            "metadata": {
                "centroid": {"crs": {"id": EPSG_3857}, "geometry_types": []},
                "extra": {"geometry_types": []},
            },
        }

        merged = _merge_derived_geometry_info(caller, derived)

        assert merged["secondary"] == ["centroid", "extra"]
        assert merged["metadata"]["geometry"] == {"encoding": "geoarrow.wkb"}
        # the caller's explicit `crs: null` is not replaced, but the key it left
        # blank is filled in
        assert merged["metadata"]["centroid"] == {"crs": None, "geometry_types": []}
        assert merged["metadata"]["extra"] == {"geometry_types": []}


# ---------------------------------------------------------------------------
# The 2.0 fast path: the carried block is written verbatim
# ---------------------------------------------------------------------------


def _v2_block_omitting_the_secondary(source, path):
    """A 2.0 file whose ``geo`` block describes ``geometry`` only, plus an ``epoch``.

    The ``epoch`` is what makes the block worth carrying at all: without a key
    DuckDB would not generate itself, ``_geo_block_to_carry_on_fast_path`` hands
    the write back to DuckDB's own (complete) block and there is nothing to fix.
    """
    table = pq.read_table(str(source))
    geo = {
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Polygon"],
                "epoch": 2020.5,
            }
        },
    }
    pq.write_table(
        table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")}), str(path)
    )
    return path


class TestTheTwoPointZeroFastPath:
    def test_extract_describes_a_secondary_the_carried_block_omits(self, native_pair, tmp_path):
        source = _v2_block_omitting_the_secondary(native_pair, tmp_path / "carried.parquet")
        output = tmp_path / "out.parquet"

        _run_cli("extract", "geoparquet", source, output, "--geoparquet-version", "2.0")

        geo = _geo(output)
        assert geo["columns"]["geometry"]["epoch"] == 2020.5, (
            "the carried block is no longer carried at all"
        )
        assert "centroid" in geo["columns"], "the fast path leaves the secondary undescribed"
        assert geo["columns"]["centroid"]["crs"]["id"] == EPSG_3857

    def test_a_fast_path_with_no_block_to_carry_derives_nothing(self, native_pair, tmp_path):
        """The derivation can re-read the input's footer -- an HTTP round trip for a
        remote input -- so it is not asked when DuckDB writes its own block, whose
        answer would only be discarded. DuckDB's V2 block describes every geometry
        column in the file by itself.
        """
        from unittest import mock

        from geoparquet_io.core import write_funnels
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

        plain_v2 = tmp_path / "plain_v2.parquet"
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.execute(
                f"""COPY (
                    SELECT i AS id, ST_Point(i, 0) AS geometry, ST_Point(i, 1) AS centroid
                    FROM range(50) t(i)
                ) TO {sql_path(str(plain_v2))} (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')"""
            )
        finally:
            con.close()
        output = tmp_path / "out.parquet"

        with mock.patch.object(
            write_funnels,
            "derive_secondary_geometry_info",
            wraps=write_funnels.derive_secondary_geometry_info,
        ) as derive:
            _run_cli("extract", "geoparquet", plain_v2, output, "--geoparquet-version", "2.0")

        assert derive.call_count == 0
        assert "centroid" in _geo(output)["columns"]


# ---------------------------------------------------------------------------
# The Table API: no query, no input file -- the table's own schema
# ---------------------------------------------------------------------------


class TestTheTableApi:
    @pytest.mark.parametrize("version", [None, "1.1", "2.0"])
    @pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
    def test_read_then_write_describes_the_native_secondary(
        self, native_pair, tmp_path, version, strategy
    ):
        """``gpio.read(f).write(out)`` derived nothing: the table carries no ``geo`` key."""
        import geoparquet_io as gpio
        from tests.native_geo_probes import logical_geo_types

        output = tmp_path / "out.parquet"

        gpio.read(str(native_pair)).write(
            output, geoparquet_version=version, write_strategy=strategy
        )

        columns = _geo(output)["columns"]
        assert "centroid" in columns, "the Table API leaves the secondary undescribed"
        assert columns["centroid"]["crs"]["id"] == EPSG_3857
        assert isinstance(columns["centroid"]["geometry_types"], list)
        if version == "1.1":
            assert "centroid" not in logical_geo_types(output), (
                "a 1.1 file must not carry a native Parquet geo type"
            )

    def test_the_tables_schema_names_its_secondaries(self, native_pair):
        import pyarrow.parquet as pq_

        from geoparquet_io.core.arrow_geo_metadata import table_geometry_info

        info = table_geometry_info(pq_.read_table(str(native_pair)), "geometry")

        assert info is not None
        assert info["secondary"] == ["centroid"]
        assert info["metadata"]["centroid"]["crs"]["id"] == EPSG_3857
        assert info["metadata"]["centroid"]["geometry_types"] == []

    @pytest.mark.parametrize("version", ["1.1", "2.0"])
    @pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
    def test_a_declared_plain_wkb_secondary_survives_a_table_write(
        self, declared_wkb_pair, tmp_path, version, strategy
    ):
        """A secondary the carried ``geo`` key declares is a geometry column too.

        It arrives as plain WKB bytes with no GeoArrow extension name, so a
        schema-only witness skipped it: every strategy dropped it from
        ``geo.columns``, and ``in-memory`` at 2.0 retyped it native while leaving
        it undescribed, which ``check spec`` fails.
        """
        import geoparquet_io as gpio
        from tests.native_geo_probes import logical_geo_types, spec_problems

        output = tmp_path / "out.parquet"

        gpio.read(str(declared_wkb_pair)).write(
            output, geoparquet_version=version, write_strategy=strategy
        )

        columns = _geo(output)["columns"]
        assert "centroid" in columns, "the declared secondary was dropped"
        assert columns["centroid"]["crs"]["id"] == EPSG_3857
        if version == "2.0":
            assert logical_geo_types(output)["centroid"][1] == EPSG_3857
        else:
            assert "centroid" not in logical_geo_types(output)
        assert spec_problems(output) == []


@pytest.fixture(scope="module")
def declared_wkb_pair(tmp_path_factory):
    """A 1.1 file whose ``geo`` key declares a plain-WKB EPSG:3857 secondary."""
    from tests.native_geo_probes import conus_wkb, projjson

    rows = conus_wkb(
        "cell", "ST_Transform(ST_Centroid(cell), 'EPSG:4326', 'EPSG:3857', always_xy := true)"
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Polygon"]},
            "centroid": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": json.loads(projjson(3857)),
            },
        },
    }
    table = pa.table(
        {
            "id": [row[0] for row in rows],
            "geometry": [bytes(row[2]) for row in rows],
            "centroid": [bytes(row[3]) for row in rows],
        }
    ).replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    path = tmp_path_factory.mktemp("declared_wkb_pair") / "declared.parquet"
    pq.write_table(table, str(path))
    return path


@pytest.mark.parametrize("version", ["1.1", "2.0"])
@pytest.mark.parametrize("strategy", ["in-memory", "streaming"])
def test_a_spherical_secondary_keeps_its_edges_through_a_table_write(tmp_path, version, strategy):
    """The table's schema carries a GeoArrow ``edge_type`` as well as a CRS.

    Reading only the CRS turned a spherical secondary into a planar one with no
    warning: ``edges`` was left off the entry, and at 2.0 the native type came out
    a planar GEOMETRY. (duckdb-kv and disk-rewrite refuse non-planar edges outright,
    a separate limitation.)
    """
    import geoarrow.pyarrow as ga

    import geoparquet_io as gpio
    from tests.native_geo_probes import conus_wkb, projjson, write_native_geo_only

    rows = conus_wkb(
        "ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)", "ST_Centroid(cell)"
    )
    source = write_native_geo_only(
        tmp_path / "in.parquet",
        rows,
        {
            "geometry": (2, ga.wkb().with_crs(projjson(5070))),
            "centroid": (3, ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL)),
        },
    )
    output = tmp_path / "out.parquet"

    gpio.read(str(source)).write(output, geoparquet_version=version, write_strategy=strategy)

    assert _geo(output)["columns"]["centroid"].get("edges") == "spherical"
    if version == "2.0":
        with pq.ParquetFile(str(output)) as reader:
            schema = reader.schema
            logical = {
                schema.column(i).name: str(schema.column(i).logical_type)
                for i in range(len(schema))
            }
        assert logical["centroid"].startswith("Geography"), logical["centroid"]


@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_a_fresh_process_table_write_describes_the_native_secondary(tmp_path, version):
    """``gpio.read`` has to see the native secondary without help from the process.

    Plain pyarrow reads a native Parquet GEOMETRY column as bare ``binary`` unless
    ``geoarrow.pyarrow`` has registered its extension types -- a process-global
    side effect this suite's own fixtures trigger, so only a subprocess can see
    the difference. The input is DuckDB-written, so no Arrow schema in the file
    carries the extension name either.
    """
    import subprocess
    import sys

    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    source = tmp_path / "duckdb_native.parquet"
    output = tmp_path / "out.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(
            f"""COPY (
                SELECT i AS id, ST_Point(i, 0) AS geometry,
                       ST_SetCRS(ST_Point(i * 1000, 1), 'EPSG:3857') AS centroid
                FROM range(50) t(i)
            ) TO {sql_path(str(source))} (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')"""
        )
    finally:
        con.close()

    script = (
        "import sys, geoparquet_io as gpio\n"
        "assert 'geoarrow.pyarrow' not in sys.modules\n"
        "gpio.read(sys.argv[1]).write(sys.argv[2], geoparquet_version=sys.argv[3])\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(source), str(output), version],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    columns = _geo(output)["columns"]
    assert "centroid" in columns, "a fresh process left the secondary undescribed"
    assert columns["centroid"]["crs"]["id"] == EPSG_3857


def test_describe_reads_a_crs_whose_text_holds_an_apostrophe():
    """DuckDB renders an embedded ``'`` as ``''`` inside ``GEOMETRY('...')``.

    The pattern stopped at the first quote, so the column was skipped and a
    partition of it was left undescribed -- a native type inside a 1.1 file.
    """
    from pyproj import CRS

    from geoparquet_io.core.duckdb_metadata import parse_geometry_logical_type
    from geoparquet_io.core.duckdb_utils import _escape_sql_string, get_duckdb_connection
    from geoparquet_io.core.geometry_detection import native_geometry_types_from_query

    crs = CRS.from_epsg(3857).to_json_dict()
    crs.pop("id")
    crs["name"] = "O'Brien Mercator"
    con = get_duckdb_connection(load_spatial=True)
    try:
        query = (
            f"SELECT ST_Point(0, 0) AS geometry, "
            f"ST_SetCRS(ST_Point(1, 2), '{_escape_sql_string(json.dumps(crs))}') AS centroid"
        )
        native = native_geometry_types_from_query(con, query)
    finally:
        con.close()

    assert "centroid" in native, "the apostrophe hid the column"
    parsed = parse_geometry_logical_type(native["centroid"])
    assert "O'Brien Mercator" in json.dumps(parsed)


class TestTheEdgesOfTheHelpers:
    def test_the_table_query_converts_only_plain_wkb_secondaries(self):
        from geoparquet_io.core.write_strategies.duckdb_kv import (
            _table_query_with_native_geometry,
        )

        table = pa.table(
            {
                "geometry": pa.array([b"\x01"], type=pa.binary()),
                "centroid": pa.array([b"\x01"], type=pa.large_binary()),
                "label_wkt": pa.array(["POINT (0 0)"]),
            }
        )
        info = {
            "primary": "geometry",
            "secondary": ["centroid", "label_wkt", "dropped"],
            "metadata": {"centroid": {"crs": {"id": {"authority": "OGC", "code": "CRS84"}}}},
        }

        query = _table_query_with_native_geometry(table, "geometry", info, "2.0")

        assert 'ST_GeomFromWKB("centroid") AS "centroid"' in query, "a default CRS is not set"
        assert "label_wkt" not in query, "a WKT string is not ST_GeomFromWKB's to read"
        assert "dropped" not in query
        assert (
            _table_query_with_native_geometry(table, "geometry", info, "1.1").count(
                "ST_GeomFromWKB"
            )
            == 1
        )

    def test_a_describe_that_fails_answers_nothing(self):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.geometry_detection import native_geometry_types_from_query

        con = get_duckdb_connection(load_spatial=True)
        try:
            assert (
                native_geometry_types_from_query(con, "SELECT * FROM nowhere", verbose=True) == {}
            )
        finally:
            con.close()

    def test_the_callers_primary_is_never_added_as_a_secondary(self):
        from geoparquet_io.core.write_funnels import _merge_derived_geometry_info

        caller = {"primary": "geometry", "secondary": [], "metadata": {}}
        derived = {"primary": "other", "secondary": ["geometry", "centroid"], "metadata": {}}

        merged = _merge_derived_geometry_info(caller, derived)

        assert merged["secondary"] == ["centroid"]
        assert caller["secondary"] == [], "the caller's own dict was mutated"
