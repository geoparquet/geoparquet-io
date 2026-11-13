"""A partition write keeps what only the input's Parquet schema says (#1233, #1234).

Every partition file is rewritten from a staging file DuckDB wrote with
``GEOPARQUET_VERSION 'NONE'``. Two facts about a native-geo-only input (a Parquet
``GEOMETRY``/``GEOGRAPHY`` logical type and no ``geo`` key) live nowhere but that
logical type:

* **the CRS.** At 2.0 DuckDB carries it through the native type, but a 1.x
  output has only the ``geo`` block to put it in -- and the write had no witness
  to read it from, so the block came out with no ``crs`` key, which GeoParquet
  reads as ``OGC:CRS84``: an EPSG:5070 dataset labelled lon/lat (#1233).
* **a GEOGRAPHY's spherical edges.** DuckDB reads a ``GEOGRAPHY`` as a planar
  ``GEOMETRY``, so the staging file has already lost them; only the input
  can say so, and nothing asked it (#1234).

The index-adding drivers never showed either: their scratch ``add <index>``
write carries a ``geo`` block with both (#993). ``partition string`` hands the
write the user's own metadata, and for this input class that has no ``geo`` key.
The fix is the witness every other rewrite already has: ``finalize_partition_file``
passes the file whose rows the partition holds as ``input_file`` (#1005).

Refs: https://github.com/geoparquet/geoparquet-io/issues/1005
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.native_geo_probes import (
    EPSG_5070,
    conus_wkb,
    geo_block,
    geo_block_crs_id,
    spec_problems,
    write_native_geo_only,
)


def _partition_string(source, out_dir, *version_args) -> list[Path]:
    result = CliRunner().invoke(
        cli,
        [
            "partition",
            "string",
            str(source),
            str(out_dir),
            "--column",
            "grp",
            "--skip-analysis",
            *version_args,
        ],
    )
    assert result.exit_code == 0, result.output
    parts = sorted(Path(out_dir).rglob("*.parquet"))
    assert len(parts) == 3, "expected one part file per `grp` value"
    return parts


def _edges(path, column: str = "geometry"):
    return ((geo_block(path) or {}).get("columns") or {}).get(column, {}).get("edges")


@pytest.fixture(scope="module")
def spherical_geography(tmp_path_factory) -> Path:
    """A native-geo-only file whose only geometry column is a spherical GEOGRAPHY."""
    import geoarrow.pyarrow as ga

    return write_native_geo_only(
        tmp_path_factory.mktemp("geography") / "pgo.parquet",
        conus_wkb("cell"),
        {"geometry": (2, ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL))},
    )


@pytest.fixture(scope="module")
def spherical_secondary(tmp_path_factory) -> Path:
    """A planar ``geometry`` primary beside a spherical GEOGRAPHY ``route``, both native."""
    import geoarrow.pyarrow as ga

    return write_native_geo_only(
        tmp_path_factory.mktemp("geography_secondary") / "pgo.parquet",
        conus_wkb("cell", "ST_Boundary(cell)"),
        {
            "geometry": (2, ga.wkb()),
            "route": (3, ga.wkb().with_edge_type(ga.EdgeType.SPHERICAL)),
        },
    )


VERSIONS = [
    pytest.param(("--geoparquet-version", "1.0"), id="1.0"),
    pytest.param(("--geoparquet-version", "1.1"), id="1.1"),
    pytest.param(("--geoparquet-version", "2.0"), id="2.0"),
    pytest.param((), id="auto"),
]


@pytest.mark.parametrize("version_args", VERSIONS)
def test_partition_string_keeps_a_native_only_crs(version_args, projected_conus, tmp_path):
    """Every part file names EPSG:5070 in its ``geo`` block, and survives ``check spec``.

    ``check spec`` is the second oracle: an absent ``crs`` reads as CRS84, and
    Albers metres are far outside CRS84's coordinate range.
    """
    parts = _partition_string(projected_conus, tmp_path / "parts", *version_args)

    assert [geo_block_crs_id(part) for part in parts] == [EPSG_5070] * len(parts)
    assert [spec_problems(part) for part in parts] == [[]] * len(parts)


@pytest.mark.parametrize("version_args", VERSIONS)
def test_partition_string_keeps_a_geography_spherical(version_args, spherical_geography, tmp_path):
    """The ``geo`` block still says ``spherical``, at every output version.

    Only the ``geo`` block is asserted: a 2.0 output's Parquet logical type
    comes back planar on every write path, which is #999 and not this.
    """
    parts = _partition_string(spherical_geography, tmp_path / "parts", *version_args)

    assert [_edges(part) for part in parts] == ["spherical"] * len(parts)


def test_partition_string_keeps_a_geography_secondary_spherical(spherical_secondary, tmp_path):
    """A secondary's edges come from the same witness as the primary's.

    The output query's ``DESCRIBE`` -- what partition had in its place -- sees a
    GEOGRAPHY as a planar GEOMETRY, so it named the column and dropped its edges.
    """
    parts = _partition_string(
        spherical_secondary, tmp_path / "parts", "--geoparquet-version", "1.1"
    )

    assert [_edges(part, "route") for part in parts] == ["spherical"] * len(parts)
    assert [_edges(part) for part in parts] == [None] * len(parts)


def test_every_partition_write_options_names_its_witness():
    """Every ``PartitionWriteOptions`` construction passes ``input_file``.

    The version alone was guarded (``test_write_facade_version_owner``), and a
    write with the version but no witness is how a 1.1 partition came to drop a
    CRS the input stated only in its schema. An invariant with an exempt
    construction site is a comment, so this checks every site there is,
    including ones added later (#1005).
    """
    import geoparquet_io

    sites = []
    for source in sorted(Path(geoparquet_io.__file__).parent.rglob("*.py")):
        tree = ast.parse(source.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if getattr(node.func, "id", None) != "PartitionWriteOptions":
                continue
            witness = next((kw.value for kw in node.keywords if kw.arg == "input_file"), None)
            named = witness is not None and not (
                isinstance(witness, ast.Constant) and witness.value is None
            )
            sites.append((f"{source}:{node.lineno}", named))

    import inspect
    import textwrap

    from geoparquet_io.core.partition.staging import PartitionWriteOptions

    kwargs_tree = ast.parse(
        textwrap.dedent(inspect.getsource(PartitionWriteOptions.as_write_kwargs))
    )
    assert not any(isinstance(node, ast.IfExp) for node in ast.walk(kwargs_tree))
    assert len(sites) >= 3, sites
    assert all(named for _, named in sites), [site for site, ok in sites if not ok]


@pytest.mark.parametrize("source_fixture", ["projected_conus", "spherical_secondary"])
def test_remote_witness_is_read_once_per_run(source_fixture, request, tmp_path, monkeypatch):
    import fsspec

    import geoparquet_io.core.partition.staging as staging
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path

    source = request.getfixturevalue(source_fixture)
    from geoparquet_io.core.duckdb_metadata import get_kv_metadata, get_schema_info

    reads = []
    resolutions = []
    remote = "gs://bucket/data.parquet"
    if source_fixture == "spherical_secondary":
        remote = "s3://bucket/parts/*.parquet"
    concrete = remote.replace("*.parquet", "first.parquet")

    def no_fsspec(*args, **kwargs):
        raise ImportError("Please install gcsfs")

    def resolve(url):
        resolutions.append(url)
        return concrete

    def read_kv(url):
        reads.append(("kv", url))
        return get_kv_metadata(str(source))

    def read_schema(url):
        reads.append(("schema", url))
        return get_schema_info(str(source))

    monkeypatch.setattr(fsspec, "open", no_fsspec)
    monkeypatch.setattr(staging, "get_first_parquet_file", resolve)
    monkeypatch.setattr(staging, "get_kv_metadata", read_kv)
    monkeypatch.setattr(staging, "get_schema_info", read_schema)
    options = staging.PartitionWriteOptions(
        input_file=remote,
        geoparquet_version="1.1",
    )
    con = get_duckdb_connection()
    try:
        for i in range(3):
            directory = tmp_path / f"stage{i}"
            directory.mkdir()
            con.execute(
                f"COPY (SELECT * FROM {sql_path(str(source))}) TO "
                f"{sql_path(str(directory / 'part.parquet'))} "
                "(FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')"
            )
            output = tmp_path / f"out{i}.parquet"
            staging.finalize_partition_file(
                con, str(directory), str(output), {}, False, False, options
            )
            if source_fixture == "projected_conus":
                assert geo_block_crs_id(output) == EPSG_5070
            else:
                assert _edges(output, "route") == "spherical"
        assert sorted(reads) == [("kv", concrete), ("schema", concrete)]
        assert resolutions == [remote]
    finally:
        con.close()


def test_a_local_witness_is_forwarded(projected_conus):
    from geoparquet_io.core.partition.staging import PartitionWriteOptions

    options = PartitionWriteOptions(input_file=str(projected_conus))
    assert options.as_write_kwargs()["input_file"] == str(projected_conus)
    assert PartitionWriteOptions().as_write_kwargs()["input_file"] is None
