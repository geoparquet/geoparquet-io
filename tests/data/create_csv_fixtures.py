#!/usr/bin/env python3
"""
Generate CSV/TSV test fixtures for convert command testing.
"""

import csv

# Test data: points around the world
test_points_wkt = [
    {"id": 1, "name": "New York", "wkt": "POINT(-74.006 40.7128)", "population": 8336817},
    {"id": 2, "name": "London", "wkt": "POINT(-0.1276 51.5074)", "population": 8982000},
    {"id": 3, "name": "Tokyo", "wkt": "POINT(139.6917 35.6895)", "population": 13960000},
    {"id": 4, "name": "Sydney", "wkt": "POINT(151.2093 -33.8688)", "population": 5312000},
    {"id": 5, "name": "São Paulo", "wkt": "POINT(-46.6333 -23.5505)", "population": 12325000},
]

test_points_latlon = [
    {"id": 1, "name": "New York", "lat": 40.7128, "lon": -74.006, "population": 8336817},
    {"id": 2, "name": "London", "lat": 51.5074, "lon": -0.1276, "population": 8982000},
    {"id": 3, "name": "Tokyo", "lat": 35.6895, "lon": 139.6917, "population": 13960000},
    {"id": 4, "name": "Sydney", "lat": -33.8688, "lon": 151.2093, "population": 5312000},
    {"id": 5, "name": "São Paulo", "lat": -23.5505, "lon": -46.6333, "population": 12325000},
]

# CSV with WKT column (standard name)
with open("points_wkt.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "wkt", "population"])
    writer.writeheader()
    writer.writerows(test_points_wkt)

# CSV with WKT column (alternate name: geometry)
with open("points_geometry.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "geometry", "population"])
    writer.writeheader()
    for row in test_points_wkt:
        writer.writerow(
            {
                "id": row["id"],
                "name": row["name"],
                "geometry": row["wkt"],
                "population": row["population"],
            }
        )

# CSV with lat/lon columns (standard names)
with open("points_latlon.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "lat", "lon", "population"])
    writer.writeheader()
    writer.writerows(test_points_latlon)

# CSV with lat/lon columns (alternate names: latitude/longitude)
with open("points_latitude_longitude.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "latitude", "longitude", "population"])
    writer.writeheader()
    for row in test_points_latlon:
        writer.writerow(
            {
                "id": row["id"],
                "name": row["name"],
                "latitude": row["lat"],
                "longitude": row["lon"],
                "population": row["population"],
            }
        )

# TSV with WKT column
with open("points_wkt.tsv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "wkt", "population"], delimiter="\t")
    writer.writeheader()
    writer.writerows(test_points_wkt)

# CSV with semicolon delimiter
with open("points_semicolon.txt", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "wkt", "population"], delimiter=";")
    writer.writeheader()
    writer.writerows(test_points_wkt)

# CSV with invalid WKT (for testing --skip-invalid)
test_points_invalid = [
    {"id": 1, "name": "Valid", "wkt": "POINT(-74.006 40.7128)"},
    {"id": 2, "name": "Invalid", "wkt": "NOT_A_WKT"},
    {"id": 3, "name": "Valid", "wkt": "POINT(139.6917 35.6895)"},
    {"id": 4, "name": "Null", "wkt": ""},
]

with open("points_invalid_wkt.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "wkt"])
    writer.writeheader()
    writer.writerows(test_points_invalid)

# CSV with out-of-range lat/lon (for testing validation)
test_points_invalid_latlon = [
    {"id": 1, "name": "Valid", "lat": 40.7128, "lon": -74.006},
    {"id": 2, "name": "Invalid lat", "lat": 95.0, "lon": -74.006},  # lat > 90
    {"id": 3, "name": "Invalid lon", "lat": 40.7128, "lon": 200.0},  # lon > 180
]

with open("points_invalid_latlon.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "name", "lat", "lon"])
    writer.writeheader()
    writer.writerows(test_points_invalid_latlon)

# CSV with mixed geometry types (POINTs and POLYGONs)
test_mixed_geoms = [
    {"id": 1, "type": "point", "geometry": "POINT(-74.006 40.7128)"},
    {
        "id": 2,
        "type": "polygon",
        "geometry": "POLYGON((-74.0 40.7, -74.0 40.8, -73.9 40.8, -73.9 40.7, -74.0 40.7))",
    },
    {"id": 3, "type": "point", "geometry": "POINT(139.6917 35.6895)"},
]

with open("mixed_geometries.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "type", "geometry"])
    writer.writeheader()
    writer.writerows(test_mixed_geoms)

print("Created CSV/TSV test fixtures:")
print("  - points_wkt.csv (WKT column)")
print("  - points_geometry.csv (geometry column)")
print("  - points_latlon.csv (lat/lon columns)")
print("  - points_latitude_longitude.csv (latitude/longitude columns)")
print("  - points_wkt.tsv (TSV format)")
print("  - points_semicolon.txt (semicolon delimiter)")
print("  - points_invalid_wkt.csv (invalid WKT)")
print("  - points_invalid_latlon.csv (invalid lat/lon)")
print("  - mixed_geometries.csv (mixed geometry types)")
