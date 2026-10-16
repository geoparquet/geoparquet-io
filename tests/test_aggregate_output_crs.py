"""New aggregate geometry must not inherit a projected input's CRS (#1257)."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyproj import CRS
from shapely import box, from_wkb, to_wkb, total_bounds

import geoparquet_io as gpio
from geoparquet_io.core.crs_utils import extract_crs_from_parquet
from geoparquet_io.core.geo_metadata import parse_geo_metadata


@pytest.fixture(scope="module")
def projected_pair(tmp_path_factory):
    converted = gpio.convert(Path(__file__).parent / "data" / "buildings_test_6933.gpkg").head(3)
    path = tmp_path_factory.mktemp("aggregate-crs") / "projected.parquet"
    converted.write(str(path), geoparquet_version="1.1")
    return converted, gpio.read(str(path))


def _assert_geographic_output(result, path, version="1.1"):
    assert result.crs is not None
    assert CRS.from_user_input(result.crs).equals(CRS("OGC:CRS84"))
    result.write(str(path), geoparquet_version=version)
    assert extract_crs_from_parquet(str(path)) is None
    geo = parse_geo_metadata(pq.read_schema(path).metadata)
    if version == "parquet-geo-only":
        assert geo is None
    else:
        assert "crs" not in geo["columns"]["geometry"]


@pytest.mark.parametrize(("scheme", "resolution"), [("h3", 7), ("a5", 10)])
@pytest.mark.parametrize("out_geometry", ["polygon", "centroid", "both", "none"])
@pytest.mark.parametrize("version", ["1.0", "1.1", "2.0", "parquet-geo-only"])
def test_grid_aggregate_replaces_the_source_crs(
    projected_pair, tmp_path, scheme, resolution, out_geometry, version
):
    converted, read_back = projected_pair
    source_crs = converted.crs
    result = getattr(converted, f"aggregate_{scheme}")(resolution, out_geometry=out_geometry)
    control = getattr(read_back, f"aggregate_{scheme}")(resolution, out_geometry=out_geometry)

    cell = f"{scheme}_cell"
    assert sorted(result.table.column(cell).to_pylist()) == sorted(
        control.table.column(cell).to_pylist()
    )
    assert converted.crs == source_crs
    if out_geometry == "none":
        assert result.geometry_column is None
        assert result.crs is None
        path = tmp_path / "counts.parquet"
        result.write(str(path), geoparquet_version=version)
        assert b"geo" not in (pq.read_schema(path).metadata or {})
        return

    bounds = total_bounds(from_wkb(result.table.column("geometry").to_pylist()))
    assert 6 < bounds[0] <= bounds[2] < 7
    assert 50 < bounds[1] <= bounds[3] < 51
    _assert_geographic_output(result, tmp_path / "cells.parquet", version)


@pytest.mark.parametrize("out_geometry", ["polygon", "centroid", "both", "none"])
def test_admin_aggregate_uses_the_boundary_crs(projected_pair, tmp_path, monkeypatch, out_geometry):
    """Model the file delegate's CRS84 metadata without downloading boundaries."""
    import geoparquet_io.api.ops as ops

    columns = {"admin_code": ["country"], "admin_name": ["Country"], "count": [3]}
    boundary = box(6.0, 50.0, 6.2, 50.2)
    if out_geometry != "none":
        geometry = boundary.centroid if out_geometry == "centroid" else boundary
        columns["geometry"] = [to_wkb(geometry)]
    if out_geometry == "both":
        columns["centroid"] = [to_wkb(boundary.centroid)]
    arrow = pa.table(columns)
    if out_geometry != "none":
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": [geometry.geom_type]}},
        }
        arrow = arrow.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    monkeypatch.setattr(ops, "aggregate_admin", lambda *args, **kwargs: arrow)

    result = projected_pair[0].aggregate_admin(out_geometry=out_geometry)

    if out_geometry == "none":
        assert result.crs is None
    else:
        _assert_geographic_output(result, tmp_path / "admin.parquet")


@pytest.mark.parametrize(("scheme", "resolution"), [("h3", 7), ("a5", 10)])
def test_overview_replaces_a_projected_aggregate_crs(projected_pair, tmp_path, scheme, resolution):
    """Rollup regenerates geographic parent cells even if child geometry was reprojected."""
    aggregate = getattr(projected_pair[1], f"aggregate_{scheme}")(resolution)
    projected = aggregate.reproject("EPSG:6933")
    assert CRS.from_user_input(projected.crs).to_epsg() == 6933

    result = projected.overview(resolution - 1)

    assert result.table.column("count").to_pylist() == [3]
    _assert_geographic_output(result, tmp_path / "overview.parquet")
