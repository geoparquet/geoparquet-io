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
