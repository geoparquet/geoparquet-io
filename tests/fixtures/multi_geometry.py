"""Fixtures for testing multi-geometry column support."""

import json

import pyarrow as pa
import pyarrow.parquet as pq


def create_multi_geometry_geoparquet(output_path: str) -> str:
    """Create a GeoParquet file with two geometry columns.

    Creates a file with:
    - id: integer
    - name: string
    - geometry: Point (primary) - location
    - boundary: Polygon (secondary) - bounding area

    Returns path to created file.
    """
    import duckdb

    # Use DuckDB to generate proper WKB bytes
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Generate WKB for points
    point_wkbs = []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        result = con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y}))").fetchone()
        point_wkbs.append(result[0])

    # Generate WKB for polygons (1x1 boxes around each point)
    polygon_wkbs = []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        wkt = f"POLYGON(({x - 0.5} {y - 0.5}, {x + 0.5} {y - 0.5}, {x + 0.5} {y + 0.5}, {x - 0.5} {y + 0.5}, {x - 0.5} {y - 0.5}))"
        result = con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()
        polygon_wkbs.append(result[0])

    con.close()

    # Create Arrow table
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["A", "B", "C"], type=pa.string()),
            "geometry": pa.array(point_wkbs, type=pa.binary()),
            "boundary": pa.array(polygon_wkbs, type=pa.binary()),
        }
    )

    # GeoParquet metadata with two geometry columns
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": {"authority": "EPSG", "code": 4326},
                },
                "bbox": [0.0, 0.0, 2.0, 2.0],
            },
            "boundary": {
                "encoding": "WKB",
                "geometry_types": ["Polygon"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": {"authority": "EPSG", "code": 4326},
                },
                "bbox": [-0.5, -0.5, 2.5, 2.5],
            },
        },
    }

    # Write with metadata
    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b"geo": json.dumps(geo_meta).encode("utf-8")}
    table = table.replace_schema_metadata(new_meta)

    pq.write_table(table, output_path)
    return output_path


def create_multi_geometry_geoparquet_different_crs(output_path: str) -> str:
    """Create a GeoParquet with two geometry columns having different CRS.

    - geometry: Point in EPSG:4326 (WGS84)
    - boundary: Polygon in EPSG:3857 (Web Mercator)

    Returns path to created file.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Points in WGS84
    point_wkbs = []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        result = con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y}))").fetchone()
        point_wkbs.append(result[0])

    # Polygons in Web Mercator coordinates (rough equivalent)
    polygon_wkbs = []
    for x, y in [(0, 0), (111319, 111325), (222638, 222684)]:
        wkt = f"POLYGON(({x - 50000} {y - 50000}, {x + 50000} {y - 50000}, {x + 50000} {y + 50000}, {x - 50000} {y + 50000}, {x - 50000} {y - 50000}))"
        result = con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()
        polygon_wkbs.append(result[0])

    con.close()

    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "geometry": pa.array(point_wkbs, type=pa.binary()),
            "boundary": pa.array(polygon_wkbs, type=pa.binary()),
        }
    )

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": {"authority": "EPSG", "code": 4326},
                },
            },
            "boundary": {
                "encoding": "WKB",
                "geometry_types": ["Polygon"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "ProjectedCRS",
                    "name": "WGS 84 / Pseudo-Mercator",
                    "id": {"authority": "EPSG", "code": 3857},
                },
            },
        },
    }

    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b"geo": json.dumps(geo_meta).encode("utf-8")}
    table = table.replace_schema_metadata(new_meta)

    pq.write_table(table, output_path)
    return output_path


def create_geoparquet_with_custom_column_name(
    output_path: str, primary_column_name: str = "the_geom"
) -> str:
    """Create a GeoParquet file with a non-standard geometry column name.

    This fixture tests that conversion preserves the original column name
    instead of renaming to "geometry" (issue #328).

    Args:
        output_path: Path to write the file
        primary_column_name: Name of the primary geometry column

    Returns:
        Path to created file.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Generate WKB for points
    point_wkbs = []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        result = con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y}))").fetchone()
        point_wkbs.append(result[0])

    con.close()

    # Create Arrow table with custom column name
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["A", "B", "C"], type=pa.string()),
            primary_column_name: pa.array(point_wkbs, type=pa.binary()),
        }
    )

    # GeoParquet metadata with custom primary column name
    geo_meta = {
        "version": "1.1.0",
        "primary_column": primary_column_name,
        "columns": {
            primary_column_name: {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": {"authority": "EPSG", "code": 4326},
                },
                "bbox": [0.0, 0.0, 2.0, 2.0],
            },
        },
    }

    # Write with metadata
    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b"geo": json.dumps(geo_meta).encode("utf-8")}
    table = table.replace_schema_metadata(new_meta)

    pq.write_table(table, output_path)
    return output_path


def create_multi_geometry_with_custom_primary_name(
    output_path: str, primary_column_name: str = "the_geom"
) -> str:
    """Create a GeoParquet with multiple geometry columns and custom primary name.

    Combines non-standard naming with multi-geometry support.

    Args:
        output_path: Path to write the file
        primary_column_name: Name of the primary geometry column

    Returns:
        Path to created file.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Generate WKB for points (primary)
    point_wkbs = []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        result = con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y}))").fetchone()
        point_wkbs.append(result[0])

    # Generate WKB for polygons (secondary)
    polygon_wkbs = []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        wkt = f"POLYGON(({x - 0.5} {y - 0.5}, {x + 0.5} {y - 0.5}, {x + 0.5} {y + 0.5}, {x - 0.5} {y + 0.5}, {x - 0.5} {y - 0.5}))"
        result = con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()
        polygon_wkbs.append(result[0])

    con.close()

    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            primary_column_name: pa.array(point_wkbs, type=pa.binary()),
            "boundary": pa.array(polygon_wkbs, type=pa.binary()),
        }
    )

    geo_meta = {
        "version": "1.1.0",
        "primary_column": primary_column_name,
        "columns": {
            primary_column_name: {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": {"authority": "EPSG", "code": 4326},
                },
            },
            "boundary": {
                "encoding": "WKB",
                "geometry_types": ["Polygon"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": {"authority": "EPSG", "code": 4326},
                },
            },
        },
    }

    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b"geo": json.dumps(geo_meta).encode("utf-8")}
    table = table.replace_schema_metadata(new_meta)

    pq.write_table(table, output_path)
    return output_path


def create_multi_geometry_with_secondary_bbox(
    output_path: str,
    declare_boundary_covering: bool = False,
    bbox_name: str = "boundary_bbox",
    primary_covering_column: str | None = None,
) -> str:
    """Create a multi-geometry GeoParquet whose SECONDARY column has a bbox struct.

    The #953 shape: primary Point ``geometry``, secondary Polygon ``boundary``,
    and a ``boundary_bbox`` struct column holding the *boundary*'s extents. The
    primary declares no covering, so a writer that name-matches any ``*_bbox``
    struct would wrongly declare ``boundary_bbox`` as the primary's covering.

    Args:
        output_path: Path to write the file
        declare_boundary_covering: When True, the input's ``boundary`` entry
            declares ``covering.bbox`` over the bbox struct (provenance on the
            secondary, still none on the primary).
        bbox_name: Name of the boundary's bbox struct column; ``"bbox"`` makes
            the exact conventional name belong to the secondary.
        primary_covering_column: When set, a second struct with the POINTS'
            extents is written under this name and the primary declares its
            covering over it.

    Returns:
        Path to created file.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    point_wkbs = []
    polygon_wkbs = []
    bbox_structs = []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        point_wkbs.append(con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y}))").fetchone()[0])
        wkt = f"POLYGON(({x - 0.5} {y - 0.5}, {x + 0.5} {y - 0.5}, {x + 0.5} {y + 0.5}, {x - 0.5} {y + 0.5}, {x - 0.5} {y - 0.5}))"
        polygon_wkbs.append(con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()[0])
        bbox_structs.append({"xmin": x - 0.5, "ymin": y - 0.5, "xmax": x + 0.5, "ymax": y + 0.5})

    con.close()

    bbox_type = pa.struct(
        [
            ("xmin", pa.float64()),
            ("ymin", pa.float64()),
            ("xmax", pa.float64()),
            ("ymax", pa.float64()),
        ]
    )
    columns = {
        "id": pa.array([1, 2, 3], type=pa.int32()),
        "geometry": pa.array(point_wkbs, type=pa.binary()),
        "boundary": pa.array(polygon_wkbs, type=pa.binary()),
        bbox_name: pa.array(bbox_structs, type=bbox_type),
    }
    geometry_meta = {"encoding": "WKB", "geometry_types": ["Point"]}
    if primary_covering_column:
        point_boxes = [
            {"xmin": float(x), "ymin": float(y), "xmax": float(x), "ymax": float(y)}
            for x, y in [(0, 0), (1, 1), (2, 2)]
        ]
        columns[primary_covering_column] = pa.array(point_boxes, type=bbox_type)
        geometry_meta["covering"] = {
            "bbox": {
                axis: [primary_covering_column, axis] for axis in ("xmin", "ymin", "xmax", "ymax")
            }
        }
    table = pa.table(columns)

    boundary_meta = {"encoding": "WKB", "geometry_types": ["Polygon"]}
    if declare_boundary_covering:
        boundary_meta["covering"] = {
            "bbox": {axis: [bbox_name, axis] for axis in ("xmin", "ymin", "xmax", "ymax")}
        }

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": geometry_meta,
            "boundary": boundary_meta,
        },
    }

    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b"geo": json.dumps(geo_meta).encode("utf-8")}
    table = table.replace_schema_metadata(new_meta)

    pq.write_table(table, output_path)
    return output_path
