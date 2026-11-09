"""Regressions for the final #1367 correctness review."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyproj import CRS
from shapely import Point, to_wkb

from geoparquet_io.api import ops
from geoparquet_io.api.table import Table
from geoparquet_io.core.crs_utils import crs_equal
from geoparquet_io.core.exceptions import PartitionError
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet
from geoparquet_io.core.write_funnels import write_geoparquet_table


@pytest.mark.parametrize("strategy", ["in-memory", "streaming", "duckdb-kv", "disk-rewrite"])
@pytest.mark.parametrize("version", ["1.1", "2.0"])
@pytest.mark.parametrize("secondary", [None, CRS.from_epsg(3857).to_json_dict()])
@pytest.mark.parametrize("declaration", [{"orientation": "counterclockwise"}, {"epoch": 2020.0}])
def test_table_write_preserves_resolved_column_crs(
    tmp_path, strategy, version, secondary, declaration
):
    """``Table.write`` half of #1400; ``write_geoparquet_table`` is A4's
    ``{orientation,epoch}_{projected,unknown}_secondary`` shapes
    (tests/test_write_contract_two_geometry.py)."""
    columns = {
        "geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": None, **declaration},
        "other": {"encoding": "WKB", "geometry_types": ["Point"], "crs": secondary},
    }
    table = pa.table(
        {"geometry": [to_wkb(Point(1, 2))], "other": [to_wkb(Point(100, 200))]}
    ).replace_schema_metadata(
        {
            b"geo": json.dumps(
                {"version": "1.1.0", "primary_column": "geometry", "columns": columns}
            ).encode()
        }
    )
    out = tmp_path / "out.parquet"
    Table(table).write(out, write_strategy=strategy, geoparquet_version=version)
    result = json.loads(pq.read_metadata(out).metadata[b"geo"])["columns"]
    assert result["geometry"]["crs"] is None
    assert result["other"]["crs"] == secondary


@pytest.mark.parametrize("latitude", [float("nan"), float("inf"), float("-inf")])
def test_quadkey_nonfinite(latitude):
    table = pa.table({"geometry": [to_wkb(Point(0, latitude))]})
    if latitude != latitude:
        assert ops.add_quadkey(table, resolution=3)["quadkey"].to_pylist() == [None]
    else:
        with pytest.raises(Exception, match="non-finite.*infinite"):
            ops.add_quadkey(table, resolution=3)


@pytest.mark.parametrize("latitude", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("path", ["sort", "partition"])
def test_quadkey_nonfinite_sort_partition(tmp_path, latitude, path):
    table = pa.table({"geometry": [to_wkb(Point(0, latitude))]})

    def run():
        if path == "sort":
            return ops.sort_quadkey(table, resolution=3)
        ops.partition_by_quadkey(
            table,
            tmp_path / "partitions",
            resolution=3,
            partition_resolution=3,
            keep_quadkey_column=True,
        )
        files = list((tmp_path / "partitions").rglob("*.parquet"))
        return pq.read_table(files) if files else None

    if latitude != latitude and path == "partition":
        with pytest.raises(PartitionError, match="No non-NULL values"):
            run()
    elif latitude != latitude:
        result = run()
        if result is not None:
            assert result["quadkey"].to_pylist() == [None]
    else:
        with pytest.raises(Exception, match="non-finite.*infinite"):
            run()


def test_quadkey_null_geometry():
    table = pa.table({"geometry": pa.array([None], type=pa.binary())})
    assert ops.add_quadkey(table, resolution=3)["quadkey"].to_pylist() == [None]


def _wkb_field(name: str, crs: object = ...) -> pa.Field:
    """A plain ``binary`` field whose GeoArrow identity lives only in field metadata."""
    metadata = {b"ARROW:extension:name": b"geoarrow.wkb"}
    if crs is not ...:
        metadata[b"ARROW:extension:metadata"] = json.dumps({"crs": crs}).encode()
    return pa.field(name, pa.binary(), metadata=metadata)


EPSG_3857 = CRS.from_epsg(3857).to_json_dict()
#: The output ``crs`` for each spelling a field's metadata can give it. The
#: 3857 and default rows are what main's ``_build_geo_block`` probe wrote in its
#: two cells (2.0 in-memory and the default strategy); every cell now agrees.
#: Deliberately not main: ``srid:3857`` crashed it (pyproj cannot read the
#: text) and ``""`` came out as the default; both are unknown, so null (#1267).
#: An ``init=`` text is never read (main wrote it through verbatim): unknown.
#: ``srid:0`` was null on main too.
_ABSENT = object()
FIELD_CRS_FORMS = {
    "projjson": (EPSG_3857, EPSG_3857),
    "auth_code": ("EPSG:3857", EPSG_3857),
    "projjson_text": (json.dumps(EPSG_3857), EPSG_3857),
    "wkt2": (CRS.from_epsg(3857).to_wkt(), EPSG_3857),
    "ogc_crs84": ("OGC:CRS84", _ABSENT),
    "epsg_4326": ("EPSG:4326", _ABSENT),
    "srid_3857": ("srid:3857", None),
    "empty": ("", None),
    "srid_0": ("srid:0", None),
    "init_file": ("+init={init}:x", None),
}


def _written_secondary_crs(out) -> object:
    other = json.loads(pq.read_schema(out).metadata[b"geo"])["columns"]["other"]
    return other.get("crs", _ABSENT)


@pytest.mark.parametrize("strategy", ["in-memory", "streaming", "duckdb-kv", "disk-rewrite", None])
@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_table_write_keeps_a_field_metadata_only_secondary_crs(tmp_path, version, strategy):
    """A secondary whose CRS is only in raw ``ARROW:extension:metadata``, in every
    spelling that metadata carries. Reading just ``field.type.crs`` wrote no crs
    (OGC:CRS84) on every strategy but #1400's two probe cells, and reading only a
    PROJJSON dict there dropped the string forms. One test per cell, the forms
    looped inside, keeps the fast suite's collected count down."""
    init = tmp_path / "defs"
    init.write_text("<x> +proj=utm +zone=31 +datum=WGS84 +units=m +no_defs <>\n")
    wrong = {}
    for form, (value, want) in FIELD_CRS_FORMS.items():
        if form == "init_file":
            value = value.format(init=init)
        schema = pa.schema([_wkb_field("geometry"), _wkb_field("other", value)])
        table = pa.table(
            # (1, 2) is in range for lon/lat as well as for EPSG:3857.
            {"geometry": [to_wkb(Point(1, 2))], "other": [to_wkb(Point(1, 2))]},
            schema=schema,
        )
        out = tmp_path / f"{form}.parquet"
        try:
            write_geoparquet_table(
                table, str(out), geoparquet_version=version, write_strategy=strategy
            )
        except Exception as exc:  # the crash is the finding, reported with the rest
            wrong[form] = f"raised {type(exc).__name__}: {exc}"[:200]
            continue
        got = _written_secondary_crs(out)
        if isinstance(want, dict):
            ok = isinstance(got, dict) and crs_equal(got, want)
        else:
            ok = got is want
        failed = [
            c.message
            for c in validate_geoparquet(str(out)).checks
            if c.status == CheckStatus.FAILED
        ]
        if not ok or failed:
            wrong[form] = {"crs": "absent" if got is _ABSENT else got, "failed": failed}
    assert not wrong, wrong
