#!/usr/bin/env python3
"""
Generate additional test fixtures for comprehensive conversion testing.

Creates:
- buildings_test_32632.gpkg - EPSG:32632 (UTM Zone 32N) test data
- fields_v1_1_5070.parquet - GeoParquet 1.1 with EPSG:5070
- fields_v2_5070.parquet - GeoParquet 2.0 with EPSG:5070 in both locations
- unsorted.parquet - spatially unsorted places, in enough row groups to prove it
- curved_geometry_test.gdb - the curved GeoPackage as a FileGDB, which has no pre-scan
"""

import sys
from pathlib import Path

# Add parent directory to path to import geoparquet_io
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from geoparquet_io.core.convert import convert_to_geoparquet

# Test data directory
TEST_DATA_DIR = Path(__file__).parent

#: Rows per row group in unsorted.parquet.
#:
#: A row group is the unit the spatial-order check compares, so the fixture's
#: 1,445 rows have to be spread over several of them: written as one group, the
#: check has no pairs to compare and short-circuits to "well ordered" whatever
#: the rows contain (#940). 100 rows gives 15 groups -- comfortably above the
#: 3 the locality metrics need, at ~37 KB of extra footer and page overhead.
UNSORTED_ROW_GROUP_ROWS = 100


def generate_buildings_32632():
    """
    Generate buildings_test_32632.gpkg with EPSG:32632 CRS.

    Uses existing buildings_test.gpkg and reprojects to UTM Zone 32N.
    """
    print("Generating buildings_test_32632.gpkg...")

    input_file = TEST_DATA_DIR / "buildings_test.gpkg"
    output_file = TEST_DATA_DIR / "buildings_test_32632.gpkg"

    if not input_file.exists():
        print(f"  ERROR: {input_file} not found")
        return False

    try:
        import duckdb

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        # Read, reproject, and write with new CRS
        con.execute(f"""
            COPY (
                SELECT
                    *,
                    ST_Transform(geometry, 'EPSG:4326', 'EPSG:32632') as geometry_32632
                FROM ST_Read('{input_file}')
            ) TO '{output_file}'
            WITH (FORMAT GDAL, DRIVER 'GPKG', SRS 'EPSG:32632')
        """)

        # Fix the geometry column name
        con.execute(f"""
            COPY (
                SELECT * EXCLUDE geometry_32632, geometry_32632 as geometry
                FROM ST_Read('{output_file}')
            ) TO '{output_file}'
            WITH (FORMAT GDAL, DRIVER 'GPKG', SRS 'EPSG:32632')
        """)

        con.close()
        print(f"  ✓ Created {output_file}")
        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def generate_curved_gdb():
    """
    Generate curved_geometry_test.gdb from curved_geometry_test.gpkg.

    A FileGDB has no header pre-scan for curved types, so it is the input
    shape that reaches the write before anything parses its geometry (#985).
    Needs GDAL's ogr2ogr with the OpenFileGDB driver (GDAL >= 3.6). The
    optional ``timestamps`` file is removed: it is one line of text without a
    trailing newline, which the end-of-file-fixer hook would rewrite, and the
    driver reads the dataset without it. A rerun gives the same file set; two
    system tables (a00000004, a00000006) differ in bytes because they record
    the creation time, so do not expect a clean ``git status`` afterwards.
    """
    print("Generating curved_geometry_test.gdb...")

    import shutil
    import subprocess

    input_file = TEST_DATA_DIR / "curved_geometry_test.gpkg"
    output_dir = TEST_DATA_DIR / "curved_geometry_test.gdb"

    try:
        if output_dir.exists():
            shutil.rmtree(output_dir)
        subprocess.run(
            ["ogr2ogr", "-f", "OpenFileGDB", str(output_dir), str(input_file)],
            check=True,
            capture_output=True,
            text=True,
        )
        (output_dir / "timestamps").unlink(missing_ok=True)
        print(f"  ✓ Created {output_dir.name} ({len(list(output_dir.iterdir()))} files)")
        return True
    except (OSError, subprocess.CalledProcessError) as e:
        detail = getattr(e, "stderr", None) or str(e)
        print(f"  ✗ Error creating {output_dir.name}: {detail}")
        return False


def generate_fields_v1_1_5070():
    """
    Generate fields_v1_1_5070.parquet - GeoParquet 1.1 with EPSG:5070.

    Converts from existing fields_geom_type_only_5070.parquet to v1.1.
    """
    print("Generating fields_v1_1_5070.parquet...")

    input_file = TEST_DATA_DIR / "fields_geom_type_only_5070.parquet"
    output_file = TEST_DATA_DIR / "fields_v1_1_5070.parquet"

    if not input_file.exists():
        print(f"  ERROR: {input_file} not found")
        return False

    try:
        convert_to_geoparquet(
            str(input_file),
            str(output_file),
            skip_hilbert=True,
            geoparquet_version="1.1",
            verbose=False,
        )

        print(f"  ✓ Created {output_file}")
        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def generate_fields_v2_5070():
    """
    Generate fields_v2_5070.parquet - GeoParquet 2.0 with EPSG:5070.

    Converts from existing fields_geom_type_only_5070.parquet to v2.0.
    This will trigger the dual-write path (CRS in both schema and metadata).
    """
    print("Generating fields_v2_5070.parquet...")

    input_file = TEST_DATA_DIR / "fields_geom_type_only_5070.parquet"
    output_file = TEST_DATA_DIR / "fields_v2_5070.parquet"

    if not input_file.exists():
        print(f"  ERROR: {input_file} not found")
        return False

    try:
        convert_to_geoparquet(
            str(input_file),
            str(output_file),
            skip_hilbert=True,
            geoparquet_version="2.0",
            verbose=False,
        )

        print(f"  ✓ Created {output_file}")
        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def generate_unsorted():
    """
    Generate unsorted.parquet - 1,445 spatially unsorted points, 15 row groups.

    The rows are the Overture places of country_partition/El_Salvador.parquet,
    projected to six columns and ordered by ``id``. The ids are UUIDs, so id
    order is spatially random: this is the fixture for "somebody wrote a file
    without sorting it".

    It is written in ``UNSORTED_ROW_GROUP_ROWS``-row groups because the check it
    exists to drive reads row-group bounding boxes. Until #940 the file was a
    single row group, which made ``check spatial`` report ratio 0.0 -- perfectly
    ordered -- for data that is as unordered as a shuffle.

    Rows, row order, schema and ``geo`` metadata all match what the file had
    before; only the row-group layout changed.
    """
    print("Generating unsorted.parquet...")

    import json

    import pyarrow.parquet as pq

    input_file = TEST_DATA_DIR / "country_partition" / "El_Salvador.parquet"
    output_file = TEST_DATA_DIR / "unsorted.parquet"

    if not input_file.exists():
        print(f"  ERROR: {input_file} not found")
        return False

    try:
        table = pq.read_table(input_file)

        # The source is GeoParquet 1.1 with a declared covering; this fixture is
        # the 1.0 file it has always been, so no covering is advertised.
        geo_meta = json.loads(table.schema.metadata[b"geo"])
        geo_meta["version"] = "1.0.0"
        geo_meta["columns"]["geometry"].pop("covering", None)

        table = table.select(
            ["id", "geometry", "bbox", "version", "operating_status", "theme"]
        ).sort_by([("id", "ascending")])
        table = table.replace_schema_metadata(
            {b"geo": json.dumps(geo_meta, separators=(",", ":")).encode("utf-8")}
        )

        pq.write_table(
            table,
            output_file,
            row_group_size=UNSORTED_ROW_GROUP_ROWS,
            compression="snappy",
        )

        print(f"  ✓ Created {output_file}")
        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def verify_generated_files():
    """Verify that generated files have expected properties."""
    import json

    import pyarrow.parquet as pq

    print("\nVerifying generated files...")

    # Verify buildings_test_32632.gpkg
    gpkg_32632 = TEST_DATA_DIR / "buildings_test_32632.gpkg"
    if gpkg_32632.exists():
        try:
            import duckdb

            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")

            # Check CRS
            result = con.execute(f"""
                SELECT ST_SRID(geometry) as srid
                FROM ST_Read('{gpkg_32632}')
                LIMIT 1
            """).fetchone()

            if result and result[0] == 32632:
                print(f"  ✓ {gpkg_32632.name} has correct CRS (EPSG:32632)")
            else:
                print(f"  ✗ {gpkg_32632.name} has incorrect CRS: {result}")

            con.close()

        except Exception as e:
            print(f"  ✗ Error verifying {gpkg_32632.name}: {e}")

    # Verify fields_v1_1_5070.parquet
    v1_1_file = TEST_DATA_DIR / "fields_v1_1_5070.parquet"
    if v1_1_file.exists():
        try:
            pf = pq.ParquetFile(v1_1_file)
            metadata = pf.schema_arrow.metadata

            if metadata and b"geo" in metadata:
                geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
                version = geo_meta.get("version")
                crs = geo_meta.get("columns", {}).get("geometry", {}).get("crs")

                if version == "1.1.0":
                    print(f"  ✓ {v1_1_file.name} has correct version (1.1.0)")
                else:
                    print(f"  ✗ {v1_1_file.name} has incorrect version: {version}")

                if crs and crs.get("id", {}).get("code") == 5070:
                    print(f"  ✓ {v1_1_file.name} has CRS in metadata (EPSG:5070)")
                else:
                    print(f"  ✗ {v1_1_file.name} missing or incorrect CRS")

        except Exception as e:
            print(f"  ✗ Error verifying {v1_1_file.name}: {e}")

    # Verify fields_v2_5070.parquet
    v2_file = TEST_DATA_DIR / "fields_v2_5070.parquet"
    if v2_file.exists():
        try:
            from tests.geo_schema_helpers import parse_geometry_type_from_schema

            pf = pq.ParquetFile(v2_file)
            metadata = pf.schema_arrow.metadata
            schema_str = str(pf.metadata.schema)

            # Check version
            if metadata and b"geo" in metadata:
                geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
                version = geo_meta.get("version")
                metadata_crs = geo_meta.get("columns", {}).get("geometry", {}).get("crs")

                if version == "2.0.0":
                    print(f"  ✓ {v2_file.name} has correct version (2.0.0)")
                else:
                    print(f"  ✗ {v2_file.name} has incorrect version: {version}")

                if metadata_crs and metadata_crs.get("id", {}).get("code") == 5070:
                    print(f"  ✓ {v2_file.name} has CRS in metadata (EPSG:5070)")
                else:
                    print(f"  ✗ {v2_file.name} missing CRS in metadata")

            # Check schema CRS
            geom_details = parse_geometry_type_from_schema("geometry", schema_str)
            if geom_details and "crs" in geom_details:
                schema_crs = geom_details["crs"]
                if schema_crs.get("id", {}).get("code") == 5070:
                    print(f"  ✓ {v2_file.name} has CRS in Parquet schema (EPSG:5070)")
                else:
                    print(f"  ✗ {v2_file.name} has incorrect schema CRS")
            else:
                print(f"  ✗ {v2_file.name} missing CRS in Parquet schema")

        except Exception as e:
            print(f"  ✗ Error verifying {v2_file.name}: {e}")

    # Verify unsorted.parquet: it has to be readable as several row groups, and
    # it has to actually fail the check it exists to drive (#940).
    unsorted_file = TEST_DATA_DIR / "unsorted.parquet"
    if unsorted_file.exists():
        try:
            from geoparquet_io.core.check_spatial_order import check_spatial_order

            pf = pq.ParquetFile(unsorted_file)
            groups = pf.metadata.num_row_groups
            if pf.metadata.num_rows == 1445:
                print(f"  ✓ {unsorted_file.name} has 1,445 rows")
            else:
                print(f"  ✗ {unsorted_file.name} has {pf.metadata.num_rows} rows, expected 1,445")

            if groups >= 3:
                print(f"  ✓ {unsorted_file.name} has {groups} row groups")
            else:
                print(f"  ✗ {unsorted_file.name} has {groups} row group(s), need at least 3")

            result = check_spatial_order(
                str(unsorted_file),
                random_sample_size=50,
                limit_rows=500,
                verbose=False,
                return_results=True,
                quiet=True,
            )
            if result["passed"]:
                print(
                    f"  ✗ {unsorted_file.name} reads as spatially ordered (ratio {result['ratio']})"
                )
            else:
                print(f"  ✓ {unsorted_file.name} reads as unsorted (ratio {result['ratio']})")

        except Exception as e:
            print(f"  ✗ Error verifying {unsorted_file.name}: {e}")


def main():
    """Generate all test fixtures."""
    print("=" * 60)
    print("Generating Test Fixtures for Comprehensive Coverage")
    print("=" * 60)
    print()

    results = {}

    # Generate each fixture
    results["buildings_32632"] = generate_buildings_32632()
    results["fields_v1_1_5070"] = generate_fields_v1_1_5070()
    results["fields_v2_5070"] = generate_fields_v2_5070()
    results["curved_gdb"] = generate_curved_gdb()
    results["unsorted"] = generate_unsorted()

    # Verify generated files
    verify_generated_files()

    # Summary
    print()
    print("=" * 60)
    print("Summary:")
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    print(f"  Successfully generated: {success_count}/{total_count} files")

    if success_count == total_count:
        print("  ✓ All test fixtures generated successfully!")
        return 0
    else:
        print("  ✗ Some fixtures failed to generate")
        return 1


if __name__ == "__main__":
    sys.exit(main())
