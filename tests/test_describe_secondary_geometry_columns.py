"""Secondary geometry columns are described and their emptied stats recomputed.

Two halves of the same blind spot (#1000, #952) — the write path only really
knew about the PRIMARY geometry column:

* **#1000**: `add *` (and every caller that passes no ``geometry_info``) never
  told ``build_geo_metadata`` a secondary existed, so a secondary NATIVE
  geometry column was left out of ``geo.columns`` entirely — which GeoParquet
  2.0 requires for every geometry column in the file. The funnel now derives
  ``geometry_info`` from the ``input_file`` witness itself
  (:func:`geoparquet_io.core.derive_geo_from_file.derive_secondary_geometry_info`),
  each secondary with its OWN CRS read from its logical type — never the
  primary's/file-level CRS (pinned by
  ``test_add_attaches_the_witness_crs_to_the_primary_column_only``).

* **#952**: a merge/partition write that could not carry a secondary's stats
  writes the spec's "not known" sentinel ``geometry_types: []`` — and
  ``duckdb_kv._compute_missing_metadata`` gated its recompute on the key being
  absent, so the sentinel was sticky: no file → file command ever replaced it.
  The gate now mirrors ``backfill_derived_stats``: an empty list is a gap,
  computed once, for every declared geometry column; a genuinely empty result
  writes ``[]`` again without a warning and without another pass.
"""

import json
import logging
from collections import Counter
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core import derive_geo_from_file
from geoparquet_io.core.derive_geo_from_file import derive_secondary_geometry_info
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.write_strategies import duckdb_kv


def _geo(path) -> dict:
    with pq.ParquetFile(str(path)) as pf:
        kv = pf.metadata.metadata or {}
    assert b"geo" in kv, f"{path} has no geo key"
    return json.loads(kv[b"geo"].decode("utf-8"))


def _scans(monkeypatch) -> Counter:
    """Count duckdb-kv's stats scans per column."""
    seen: Counter = Counter()
    real = duckdb_kv.compute_geo_stats_via_sql

    def counting(con, query, column, **kwargs):
        seen[column] += 1
        return real(con, query, column, **kwargs)

    monkeypatch.setattr(duckdb_kv, "compute_geo_stats_via_sql", counting)
    return seen


def _duckdb_rows(path) -> int:
    con = get_duckdb_connection(load_spatial=True)
    try:
        return con.execute(f"SELECT count(*) FROM read_parquet({sql_path(str(path))})").fetchone()[
            0
        ]
    finally:
        con.close()


def _run_cli(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


def _write_two_geometry_file(path, rows: bool = True, geometry_types=None) -> None:
    """Two WKB geometry columns; ``geometry_types`` values land verbatim.

    ``geometry_types=None`` writes the real lists; ``[]`` writes the #952
    sentinel on both columns. ``rows=False`` writes a zero-row table, built
    with ``from_pydict`` (never ``from_batches([])``: geoarrow aborts on
    zero-chunk arrays).
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    points, polys = [], []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        points.append(con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y}))").fetchone()[0])
        wkt = f"POLYGON(({x - 0.5} {y - 0.5}, {x + 0.5} {y - 0.5}, {x + 0.5} {y + 0.5}, {x - 0.5} {y + 0.5}, {x - 0.5} {y - 0.5}))"
        polys.append(con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()[0])
    con.close()

    if not rows:
        points, polys = [], []
    table = pa.Table.from_pydict(
        {
            "id": pa.array(range(len(points)), type=pa.int32()),
            "geometry": pa.array(points, type=pa.binary()),
            "boundary": pa.array(polys, type=pa.binary()),
        }
    )

    point_types = ["Point"] if geometry_types is None else geometry_types
    polygon_types = ["Polygon"] if geometry_types is None else geometry_types
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": point_types},
            "boundary": {"encoding": "WKB", "geometry_types": polygon_types},
        },
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(geo_meta).encode("utf-8")})
    pq.write_table(table, str(path))


# ---------------------------------------------------------------------------
# #952: the emptied sentinel is a gap, not a value
# ---------------------------------------------------------------------------


class TestEmptiedGeometryTypesAreRecomputed:
    def test_convert_geoparquet_restores_the_real_list_on_both_columns(self, tmp_path):
        """The #952 repro: before, geometry came back ['Point'] but boundary stayed []."""
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        _write_two_geometry_file(input_file, geometry_types=[])

        _run_cli("convert", "geoparquet", input_file, output_file)

        columns = _geo(output_file)["columns"]
        assert columns["geometry"]["geometry_types"] == ["Point"]
        assert columns["boundary"]["geometry_types"] == ["Polygon"]

    def test_the_recompute_also_fills_the_secondarys_missing_bbox(self, tmp_path):
        """bbox and geometry_types come out of the same single scan."""
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        _write_two_geometry_file(input_file, geometry_types=[])

        _run_cli("convert", "geoparquet", input_file, output_file)

        boundary = _geo(output_file)["columns"]["boundary"]
        assert boundary["bbox"] == [-0.5, -0.5, 2.5, 2.5]

    def test_a_genuinely_empty_file_keeps_the_sentinel_quietly(self, tmp_path, caplog, monkeypatch):
        """[] is also the honest answer for zero rows: one scan per column, no warning."""
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        _write_two_geometry_file(input_file, rows=False, geometry_types=[])
        scans = _scans(monkeypatch)

        with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
            result = CliRunner().invoke(
                cli, ["convert", "geoparquet", str(input_file), str(output_file)]
            )
        assert result.exit_code == 0, result.output

        columns = _geo(output_file)["columns"]
        assert columns["geometry"]["geometry_types"] == []
        assert columns["boundary"]["geometry_types"] == []
        assert scans == {"geometry": 1, "boundary": 1}
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        # The only legitimate one is convert's "nothing to measure" Hilbert note.
        assert all("Hilbert" in message for message in warnings), warnings

    def test_known_values_are_left_alone(self, tmp_path, monkeypatch):
        """This fills gaps, it does not audit: a known list is not rescanned at all.

        Not even for the optional ``bbox`` it lacks: a secondary is scanned only
        when its spec-required ``geometry_types`` is missing or ``[]``.
        """
        input_file = tmp_path / "in.parquet"
        output_file = tmp_path / "out.parquet"
        _write_two_geometry_file(input_file)
        scans = _scans(monkeypatch)

        _run_cli("convert", "geoparquet", input_file, output_file)

        columns = _geo(output_file)["columns"]
        assert columns["boundary"]["geometry_types"] == ["Polygon"]
        assert "boundary" not in scans

    def test_an_emptied_primary_is_recomputed_too(self, tmp_path):
        """The same gate covers the primary: `[]` is a gap on a single-geometry file."""
        source = tmp_path / "in.parquet"
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.execute(
                f"""COPY (SELECT 1 AS id, ST_Point(0, 0) AS geometry)
                    TO {sql_path(str(source))} (FORMAT PARQUET)"""
            )
        finally:
            con.close()
        table = pq.read_table(str(source))
        geo = json.loads(table.schema.metadata[b"geo"])
        geo["columns"]["geometry"]["geometry_types"] = []
        pq.write_table(
            table.replace_schema_metadata(
                {**table.schema.metadata, b"geo": json.dumps(geo).encode("utf-8")}
            ),
            str(source),
        )
        output = tmp_path / "out.parquet"

        _run_cli("add", "bbox", source, output)

        assert _geo(output)["columns"]["geometry"]["geometry_types"] == ["Point"]


# ---------------------------------------------------------------------------
# #1000: geometry_info derived from the input-file witness
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def two_native_columns(tmp_path_factory):
    """``geometry`` EPSG:5070 + ``centroid`` EPSG:3857, native types, no geo key."""
    import geoarrow.pyarrow as ga

    from tests.native_geo_probes import conus_wkb, projjson, write_native_geo_only

    rows = conus_wkb(
        "ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)",
        "ST_Transform(ST_Centroid(cell), 'EPSG:4326', 'EPSG:3857', always_xy := true)",
    )
    return write_native_geo_only(
        tmp_path_factory.mktemp("two_native") / "pgo.parquet",
        rows,
        {
            "geometry": (2, ga.wkb().with_crs(projjson(5070))),
            "centroid": (3, ga.wkb().with_crs(projjson(3857))),
        },
    )


class TestDeriveSecondaryGeometryInfo:
    def test_native_secondary_is_derived_with_its_own_crs(self, two_native_columns):
        info = derive_secondary_geometry_info(str(two_native_columns), "geometry")

        assert info is not None
        assert info["primary"] == "geometry"
        assert info["secondary"] == ["centroid"]
        crs = info["metadata"]["centroid"]["crs"]
        assert crs["id"] == {"authority": "EPSG", "code": 3857}, (
            "the secondary's CRS must come from ITS logical type, never the primary's"
        )

    def test_single_geometry_file_derives_nothing(self, tmp_path):
        import duckdb

        path = tmp_path / "single.parquet"
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"COPY (SELECT 1 AS id, ST_Point(0, 0) AS geometry) TO {sql_path(str(path))}")
        con.close()

        assert derive_secondary_geometry_info(str(path), "geometry") is None

    def test_a_projected_away_secondary_is_not_derived(self, two_native_columns):
        """extract --exclude-cols must not resurrect a column the output drops."""
        info = derive_secondary_geometry_info(
            str(two_native_columns), "geometry", output_columns=["cell", "geometry"]
        )
        assert info is None

    def test_declared_secondaries_are_named_without_stale_stats(self, tmp_path):
        """A geo-block secondary is listed, but its derived metadata carries no
        bbox/geometry_types: those flow through original_metadata, where the
        caller's invalidation (#934) has already had its say."""
        input_file = tmp_path / "declared.parquet"
        _write_two_geometry_file(input_file)

        info = derive_secondary_geometry_info(str(input_file), "geometry")

        assert info is not None
        assert info["secondary"] == ["boundary"]
        assert "geometry_types" not in info["metadata"]["boundary"]
        assert "bbox" not in info["metadata"]["boundary"]

    def test_an_unreadable_input_derives_nothing(self, tmp_path):
        missing = tmp_path / "nope.parquet"
        assert derive_secondary_geometry_info(str(missing), "geometry") is None


# ---------------------------------------------------------------------------
# Review regressions (#1170): every strategy, odd schemas, odd CRSs
# ---------------------------------------------------------------------------


class TestDerivedSecondariesOnEveryStrategy:
    """A derived native secondary must not make any strategy's output unreadable."""

    @pytest.mark.parametrize(
        "extra",
        [
            ["--geoparquet-version", "1.1", "--write-strategy", "in-memory"],
            ["--geoparquet-version", "1.1", "--write-strategy", "streaming"],
            ["--geoparquet-version", "1.1", "--write-strategy", "disk-rewrite"],
            ["--geoparquet-version", "1.1", "--write-strategy", "duckdb-kv"],
            ["--geoparquet-version", "1.1-geoarrow"],
        ],
        ids=["in-memory", "streaming", "disk-rewrite", "duckdb-kv", "1.1-geoarrow"],
    )
    def test_the_output_opens_in_duckdb(self, two_native_columns, tmp_path, extra):
        """A 1.x entry without geometry_types is one DuckDB refuses to read."""
        output = tmp_path / "out.parquet"

        _run_cli("extract", "geoparquet", two_native_columns, output, *extra)

        centroid = _geo(output)["columns"]["centroid"]
        assert isinstance(centroid["geometry_types"], list)
        assert centroid["crs"]["id"] == {"authority": "EPSG", "code": 3857}
        assert _duckdb_rows(output) == 200


def _with_nested_geometry(path):
    """[id, label (string), geometry (native), meta: struct<label: GEOMETRY, note>]."""
    import geoarrow.pyarrow as ga

    from tests.native_geo_probes import conus_wkb, projjson

    rows = conus_wkb("cell", "ST_Centroid(cell)")
    geometry = pa.ExtensionArray.from_storage(
        ga.wkb().with_crs(projjson(4326)), pa.array([bytes(r[2]) for r in rows], pa.binary())
    )
    nested = pa.ExtensionArray.from_storage(
        ga.wkb().with_crs(projjson(3857)), pa.array([bytes(r[3]) for r in rows], pa.binary())
    )
    table = pa.table(
        {
            "id": pa.array([r[0] for r in rows], pa.int64()),
            "label": pa.array([f"row-{r[0]}" for r in rows]),
            "geometry": geometry,
            "meta": pa.StructArray.from_arrays(
                [nested, pa.array(["n"] * len(rows))], names=["label", "note"]
            ),
        }
    )
    pq.write_table(table, str(path))
    return path


class TestOddInputs:
    def test_a_nested_geometry_leaf_is_not_a_top_level_secondary(self, tmp_path):
        """`meta.label` is not the top-level string column `label`."""
        source = _with_nested_geometry(tmp_path / "nested.parquet")
        with pq.ParquetFile(str(source)) as pf:
            schema = pf.metadata.schema
            leaves = {
                schema.column(i).path: str(schema.column(i).logical_type)
                for i in range(len(schema))
            }
        assert leaves["meta.label"].startswith("Geometry"), leaves  # the shape under test

        info = derive_secondary_geometry_info(str(source), "geometry")
        assert info is None or "label" not in info["secondary"]

        output = tmp_path / "out.parquet"
        _run_cli("add", "bbox", source, output)
        assert "label" not in _geo(output)["columns"]

    def test_an_unresolvable_secondary_crs_is_unknown_not_a_failed_write(
        self, two_native_columns, monkeypatch
    ):
        def boom(logical, parquet_file):
            raise AttributeError("'NoneType' object has no attribute 'upper'")

        monkeypatch.setattr(derive_geo_from_file, "_crs_from_geo_logical", boom)

        info = derive_secondary_geometry_info(str(two_native_columns), "geometry")

        assert info["metadata"]["centroid"]["crs"] is None

    def test_a_declared_crs_is_not_resolved_again(self, two_native_columns, tmp_path, monkeypatch):
        """The block's crs wins the merge; re-resolving the type only risked a false warning."""
        table = pq.read_table(str(two_native_columns))
        crs = json.loads(
            pq.read_schema(str(two_native_columns)).field("centroid").type.crs.to_json()
        )
        geo = {
            "version": "2.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {"encoding": "WKB", "geometry_types": ["Polygon"]},
                "centroid": {"encoding": "WKB", "geometry_types": ["Point"], "crs": crs},
            },
        }
        declared = tmp_path / "declared.parquet"
        pq.write_table(
            table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")}),
            str(declared),
        )
        calls = []
        real = derive_geo_from_file._crs_from_geo_logical

        def spy(logical, parquet_file):
            calls.append(logical)
            return real(logical, parquet_file)

        monkeypatch.setattr(derive_geo_from_file, "_crs_from_geo_logical", spy)

        info = derive_secondary_geometry_info(str(declared), "geometry")

        assert info["secondary"] == ["centroid"]
        assert calls == []
        assert "geometry_types" not in info["metadata"]["centroid"]

    def test_a_geo_block_without_a_columns_object_keeps_the_native_half(
        self, two_native_columns, tmp_path
    ):
        table = pq.read_table(str(two_native_columns))
        odd = tmp_path / "odd.parquet"
        pq.write_table(
            table.replace_schema_metadata(
                {b"geo": json.dumps({"version": "1.1.0", "columns": []}).encode("utf-8")}
            ),
            str(odd),
        )

        info = derive_secondary_geometry_info(str(odd), "geometry", verbose=True)

        assert info["secondary"] == ["centroid"]

    def test_a_geo_key_that_does_not_parse_keeps_the_native_half(
        self, two_native_columns, tmp_path
    ):
        table = pq.read_table(str(two_native_columns))
        odd = tmp_path / "broken_geo.parquet"
        pq.write_table(table.replace_schema_metadata({b"geo": b"{not json"}), str(odd))

        info = derive_secondary_geometry_info(str(odd), "geometry")

        assert info["secondary"] == ["centroid"]

    def test_a_plain_binary_secondary_in_the_query_is_left_alone(self, tmp_path):
        """A secondary the query carries as BLOB (a non-gpio Arrow stream) cannot be measured.

        Binding ST_* to it aborted the whole write; it keeps its carried metadata.
        """
        from geoparquet_io.core.write_funnels import write_parquet_with_metadata

        source = tmp_path / "in.parquet"
        _write_two_geometry_file(source, geometry_types=[])
        table = pq.read_table(str(source))
        assert pa.types.is_binary(table.schema.field("boundary").type)
        output = tmp_path / "out.parquet"
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.register("t", table)
            write_parquet_with_metadata(
                con,
                "SELECT id, ST_GeomFromWKB(geometry) AS geometry, boundary FROM t",
                str(output),
                original_metadata=dict(table.schema.metadata),
                geoparquet_version="1.1",
            )
        finally:
            con.close()

        columns = _geo(output)["columns"]
        assert columns["geometry"]["geometry_types"] == ["Point"]
        assert columns["boundary"]["geometry_types"] == []


class TestThePrimaryCrsIsThePrimarysOwn:
    """A block-less input's CRS is read off the primary's type, not the first in schema order."""

    @pytest.mark.parametrize(
        ("columns", "expected"),
        [
            # secondary first in schema order, in another CRS than the primary
            ({"centroid": (3, 3857), "geometry": (2, 5070)}, {"authority": "EPSG", "code": 5070}),
            # a CRS84 primary beside a projected secondary
            ({"geometry": (2, None), "centroid": (3, 3857)}, None),
        ],
        ids=["secondary-first", "crs84-primary"],
    )
    def test_add_bbox_labels_the_primary_with_its_own_crs(self, tmp_path, columns, expected):
        import geoarrow.pyarrow as ga

        from tests.native_geo_probes import (
            NO_CRS_KEY,
            NO_NATIVE_CRS,
            NO_NATIVE_GEO_TYPE,
            conus_wkb,
            geo_block_crs_id,
            logical_crs_id,
            projjson,
        )

        rows = conus_wkb(
            "ST_Transform(cell, 'EPSG:4326', 'EPSG:5070', always_xy := true)",
            "ST_Transform(ST_Centroid(cell), 'EPSG:4326', 'EPSG:3857', always_xy := true)",
        )
        if expected is None:
            rows = conus_wkb(
                "cell",
                "ST_Transform(ST_Centroid(cell), 'EPSG:4326', 'EPSG:3857', always_xy := true)",
            )
        from tests.native_geo_probes import write_native_geo_only

        spec = {
            name: (index, ga.wkb().with_crs(projjson(epsg)) if epsg else ga.wkb())
            for name, (index, epsg) in columns.items()
        }
        source = write_native_geo_only(tmp_path / "in.parquet", rows, spec)
        output = tmp_path / "out.parquet"

        _run_cli("add", "bbox", source, output)

        if expected is None:
            # OGC:CRS84, stated the spec's way: no crs key, no CRS in the type.
            assert geo_block_crs_id(output, "geometry") == NO_CRS_KEY
            assert logical_crs_id(output, "geometry") in (NO_NATIVE_CRS, NO_NATIVE_GEO_TYPE)
        else:
            assert geo_block_crs_id(output, "geometry") == expected
            assert logical_crs_id(output, "geometry") in (expected, NO_NATIVE_GEO_TYPE)
        assert geo_block_crs_id(output, "centroid") == {"authority": "EPSG", "code": 3857}


class TestWhatANativeSecondaryDerives:
    """Each thing a native secondary's own type can (or cannot) say."""

    @staticmethod
    def _two_native(tmp_path, centroid_type):
        import geoarrow.pyarrow as ga

        from tests.native_geo_probes import conus_wkb, projjson, write_native_geo_only

        rows = conus_wkb("cell", "ST_Centroid(cell)")
        return write_native_geo_only(
            tmp_path / "in.parquet",
            rows,
            {"geometry": (2, ga.wkb().with_crs(projjson(4326))), "centroid": (3, centroid_type)},
        )

    def test_a_default_crs_is_stated_by_omission(self, tmp_path):
        import geoarrow.pyarrow as ga

        info = derive_secondary_geometry_info(str(self._two_native(tmp_path, ga.wkb())), "geometry")

        assert info["metadata"] == {"centroid": {"geometry_types": []}}

    def test_a_geography_secondary_keeps_its_edges(self, tmp_path):
        import geoarrow.pyarrow as ga

        spherical = ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL)
        info = derive_secondary_geometry_info(
            str(self._two_native(tmp_path, spherical)), "geometry"
        )

        assert info["metadata"]["centroid"]["edges"] == "spherical"
        assert info["metadata"]["centroid"]["geometry_types"] == []


@pytest.mark.parametrize("verbose", [False, True])
def test_an_unreadable_output_schema_derives_nothing(two_native_columns, tmp_path, caplog, verbose):
    """The derivation needs the output's columns; without them it stays out of the way."""
    from geoparquet_io.core import write_funnels

    def failing_probe(con, query):
        raise duckdb.Error("boom")

    derived = []
    real_derive = write_funnels.derive_secondary_geometry_info

    def spy(*args, **kwargs):
        derived.append(args)
        return real_derive(*args, **kwargs)

    output = tmp_path / "out.parquet"
    con = get_duckdb_connection(load_spatial=True)
    try:
        with (
            patch.object(write_funnels, "_get_query_columns", side_effect=failing_probe),
            patch.object(write_funnels, "derive_secondary_geometry_info", side_effect=spy),
            caplog.at_level(logging.DEBUG, logger="geoparquet_io"),
        ):
            write_funnels.write_parquet_with_metadata(
                con,
                f"SELECT * FROM read_parquet({sql_path(str(two_native_columns))})",
                str(output),
                original_metadata=None,
                input_file=str(two_native_columns),
                geoparquet_version="1.1",
                verbose=verbose,
            )
    finally:
        con.close()

    assert derived == []
    assert _duckdb_rows(output) == 200
    assert ("Could not read output schema" in caplog.text) is verbose
