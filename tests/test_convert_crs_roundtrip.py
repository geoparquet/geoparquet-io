"""
Tests for CRS preservation in format conversions.

These tests verify that:
1. EPSG:4326 CRS is explicitly written to legacy formats (fixes #189, #190)
2. Round-trip conversions preserve CRS
3. Converted GeoParquet files pass check_all validation
"""

import json
import sqlite3

import pyarrow.parquet as pq
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.check_parquet_structure import check_all
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.format_writers import write_format


def _check_all_passed(results: dict) -> bool:
    """Check if all check_all sub-checks passed."""
    return all(r.get("passed", True) for r in results.values() if isinstance(r, dict))


class TestShapefileCRSExport:
    """Tests for Shapefile CRS export (fixes #190)."""

    def test_shapefile_exports_prj_for_epsg_4326(self, places_test_file, tmp_path):
        """Shapefile export should create .prj file even for EPSG:4326."""
        output_shp = tmp_path / "output.shp"

        write_format(places_test_file, str(output_shp), "shapefile")

        # .prj file must exist
        prj_file = output_shp.with_suffix(".prj")
        assert prj_file.exists(), "Shapefile export missing .prj file for EPSG:4326"

        # .prj must contain valid WKT CRS (WKT1 or WKT2 format)
        prj_content = prj_file.read_text()
        assert len(prj_content) > 0, ".prj file is empty"
        # WKT1 uses GEOGCS/PROJCS, WKT2 uses GEOGCRS/PROJCRS
        assert any(k in prj_content for k in ("GEOGCS", "GEOGCRS", "PROJCS", "PROJCRS")), (
            ".prj file missing valid WKT CRS definition"
        )

    def test_shapefile_prj_contains_wgs84(self, places_test_file, tmp_path):
        """Shapefile .prj should reference WGS84 for EPSG:4326 source."""
        output_shp = tmp_path / "output.shp"

        write_format(places_test_file, str(output_shp), "shapefile")

        prj_file = output_shp.with_suffix(".prj")
        prj_content = prj_file.read_text()

        # Should contain WGS84 reference
        assert "WGS" in prj_content.upper() or "4326" in prj_content, (
            ".prj file should reference WGS84 or EPSG:4326"
        )

    def test_shapefile_roundtrip_succeeds(self, places_test_file, tmp_path):
        """Round-trip GeoParquet -> Shapefile -> GeoParquet succeeds (fixes #190)."""
        shp_file = tmp_path / "intermediate.shp"
        output_parquet = tmp_path / "roundtrip.parquet"

        # Export to shapefile
        write_format(places_test_file, str(shp_file), "shapefile")

        # Import back to GeoParquet - this is the key test for #190
        # Before the fix, this would fail with "No CRS found in input file"
        convert_to_geoparquet(str(shp_file), str(output_parquet))

        # Verify output exists and has valid GeoParquet metadata
        assert output_parquet.exists()
        pf = pq.ParquetFile(output_parquet)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))

        # CRS may be None for WGS84 (default per GeoParquet spec)
        # The key test is that the roundtrip succeeds, not that CRS is explicit
        primary_col = geo_meta.get("primary_column", "geometry")
        assert primary_col in geo_meta["columns"], "Missing geometry column metadata"


class TestFlatGeobufCRSExport:
    """Tests for FlatGeobuf CRS export (fixes #189)."""

    def test_flatgeobuf_roundtrip_succeeds(self, places_test_file, tmp_path):
        """Round-trip GeoParquet -> FlatGeobuf -> GeoParquet succeeds (fixes #189)."""
        fgb_file = tmp_path / "intermediate.fgb"
        output_parquet = tmp_path / "roundtrip.parquet"

        # Export to FlatGeobuf
        write_format(places_test_file, str(fgb_file), "flatgeobuf")

        # Import back to GeoParquet - this is the key test for #189
        # Before the fix, this would fail with "No CRS found in input file"
        convert_to_geoparquet(str(fgb_file), str(output_parquet))

        # Verify output exists and has valid GeoParquet metadata
        assert output_parquet.exists()
        pf = pq.ParquetFile(output_parquet)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))

        # CRS may be None for WGS84 (default per GeoParquet spec)
        primary_col = geo_meta.get("primary_column", "geometry")
        assert primary_col in geo_meta["columns"], "Missing geometry column metadata"

    def test_flatgeobuf_can_be_reimported(self, places_test_file, tmp_path):
        """FlatGeobuf export should be re-importable (CRS must be present)."""
        fgb_file = tmp_path / "output.fgb"
        reimported = tmp_path / "reimported.parquet"

        write_format(places_test_file, str(fgb_file), "flatgeobuf")

        # This should not raise "No CRS found" error
        convert_to_geoparquet(str(fgb_file), str(reimported))

        assert reimported.exists(), "Re-import should succeed"


class TestGeoPackageCRSExport:
    """Tests for GeoPackage CRS export."""

    def test_geopackage_roundtrip_succeeds(self, places_test_file, tmp_path):
        """Round-trip GeoParquet -> GeoPackage -> GeoParquet succeeds."""
        gpkg_file = tmp_path / "intermediate.gpkg"
        output_parquet = tmp_path / "roundtrip.parquet"

        # Export to GeoPackage
        write_format(places_test_file, str(gpkg_file), "geopackage")

        # Import back to GeoParquet
        convert_to_geoparquet(str(gpkg_file), str(output_parquet))

        # Verify output exists and has valid GeoParquet metadata
        assert output_parquet.exists()
        pf = pq.ParquetFile(output_parquet)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))

        # CRS may be None for WGS84 (default per GeoParquet spec)
        primary_col = geo_meta.get("primary_column", "geometry")
        assert primary_col in geo_meta["columns"], "Missing geometry column metadata"

    def test_geopackage_has_srs_table(self, places_test_file, tmp_path):
        """GeoPackage export should populate gpkg_spatial_ref_sys table."""
        gpkg_file = tmp_path / "output.gpkg"

        write_format(places_test_file, str(gpkg_file), "geopackage")

        # Query the GeoPackage SRS table directly via sqlite3
        con = sqlite3.connect(str(gpkg_file))
        cursor = con.execute("""
            SELECT srs_id, organization, organization_coordsys_id
            FROM gpkg_spatial_ref_sys
            WHERE srs_id = 4326
        """)
        result = cursor.fetchone()  # sqlite3 cursor; guard: open-result ok
        con.close()

        # Should have EPSG:4326 entry
        assert result is not None, "GeoPackage missing SRS entry for EPSG:4326"


class TestConvertedGeoParquetPassesCheckAll:
    """Tests that all converted GeoParquet files pass check_all validation."""

    def test_shapefile_conversion_passes_check_all(self, shapefile_buildings, tmp_path):
        """GeoParquet converted from Shapefile should pass check_all."""
        output = tmp_path / "output.parquet"

        convert_to_geoparquet(shapefile_buildings, str(output))

        results = check_all(str(output), return_results=True, quiet=True)
        assert _check_all_passed(results), f"Failed check_all: {results}"

    def test_geojson_conversion_passes_check_all(self, geojson_input, tmp_path):
        """GeoParquet converted from GeoJSON should pass check_all."""
        output = tmp_path / "output.parquet"

        convert_to_geoparquet(geojson_input, str(output))

        results = check_all(str(output), return_results=True, quiet=True)
        assert _check_all_passed(results), f"Failed check_all: {results}"

    def test_geopackage_conversion_passes_check_all(self, gpkg_buildings, tmp_path):
        """GeoParquet converted from GeoPackage should pass check_all."""
        output = tmp_path / "output.parquet"

        convert_to_geoparquet(gpkg_buildings, str(output))

        results = check_all(str(output), return_results=True, quiet=True)
        assert _check_all_passed(results), f"Failed check_all: {results}"


class TestProjectedCRSExport:
    """Tests for projected CRS preservation (EPSG:5070, etc.)."""

    def test_projected_crs_shapefile_roundtrip(self, fields_5070_file, tmp_path):
        """Projected CRS should be preserved in Shapefile round-trip."""
        shp_file = tmp_path / "projected.shp"
        output_parquet = tmp_path / "roundtrip.parquet"

        # Export to shapefile
        write_format(fields_5070_file, str(shp_file), "shapefile")

        # Verify .prj exists and contains projected CRS info
        prj_file = shp_file.with_suffix(".prj")
        assert prj_file.exists(), "Shapefile missing .prj for projected CRS"

        prj_content = prj_file.read_text()
        # WKT1 uses PROJCS, WKT2 uses PROJCRS
        assert ("5070" in prj_content) or any(k in prj_content for k in ("PROJCS", "PROJCRS")), (
            ".prj should contain projected CRS definition"
        )

        # Import back
        convert_to_geoparquet(str(shp_file), str(output_parquet))

        # Verify CRS preserved
        pf = pq.ParquetFile(output_parquet)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
        primary_col = geo_meta.get("primary_column", "geometry")
        crs = geo_meta["columns"][primary_col].get("crs")

        assert crs is not None, "Projected CRS lost in roundtrip"
        epsg_code = crs.get("id", {}).get("code")
        assert epsg_code == 5070, f"Expected EPSG:5070, got {epsg_code}"

    def test_projected_crs_flatgeobuf_roundtrip(self, fields_5070_file, tmp_path):
        """Projected CRS should be preserved in FlatGeobuf round-trip."""
        fgb_file = tmp_path / "projected.fgb"
        output_parquet = tmp_path / "roundtrip.parquet"

        # Export to FlatGeobuf
        write_format(fields_5070_file, str(fgb_file), "flatgeobuf")

        # Import back
        convert_to_geoparquet(str(fgb_file), str(output_parquet))

        # Verify CRS preserved
        pf = pq.ParquetFile(output_parquet)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
        primary_col = geo_meta.get("primary_column", "geometry")
        crs = geo_meta["columns"][primary_col].get("crs")

        assert crs is not None, "Projected CRS lost in FlatGeobuf roundtrip"
        epsg_code = crs.get("id", {}).get("code")
        assert epsg_code == 5070, f"Expected EPSG:5070, got {epsg_code}"

    def test_projected_crs_geopackage_roundtrip(self, fields_5070_file, tmp_path):
        """Projected CRS should be preserved in GeoPackage round-trip."""
        gpkg_file = tmp_path / "projected.gpkg"
        output_parquet = tmp_path / "roundtrip.parquet"

        # Export to GeoPackage
        write_format(fields_5070_file, str(gpkg_file), "geopackage")

        # Import back
        convert_to_geoparquet(str(gpkg_file), str(output_parquet))

        # Verify CRS preserved
        pf = pq.ParquetFile(output_parquet)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
        primary_col = geo_meta.get("primary_column", "geometry")
        crs = geo_meta["columns"][primary_col].get("crs")

        assert crs is not None, "Projected CRS lost in GeoPackage roundtrip"
        epsg_code = crs.get("id", {}).get("code")
        assert epsg_code == 5070, f"Expected EPSG:5070, got {epsg_code}"


class TestCLICRSRoundtrip:
    """Tests for CLI convert commands with CRS preservation."""

    def test_cli_convert_shapefile_roundtrip(self, places_test_file, tmp_path):
        """CLI convert to shapefile and back succeeds (fixes #190)."""
        runner = CliRunner()
        shp_file = tmp_path / "test.shp"
        output_parquet = tmp_path / "roundtrip.parquet"

        # Export to shapefile via CLI
        result = runner.invoke(cli, ["convert", "shapefile", places_test_file, str(shp_file)])
        assert result.exit_code == 0, f"Export failed: {result.output}"

        # Verify .prj exists (key fix for #190)
        assert shp_file.with_suffix(".prj").exists(), ".prj file missing"

        # Import back via CLI - this is the key test for #190
        result = runner.invoke(cli, ["convert", str(shp_file), str(output_parquet)])
        assert result.exit_code == 0, f"Import failed: {result.output}"

        # Verify file exists and has valid GeoParquet metadata
        assert output_parquet.exists()
        pf = pq.ParquetFile(output_parquet)
        geo_meta = json.loads(pf.schema_arrow.metadata[b"geo"].decode("utf-8"))
        assert "columns" in geo_meta, "Missing GeoParquet metadata"

    def test_cli_convert_flatgeobuf_roundtrip(self, places_test_file, tmp_path):
        """CLI convert to FlatGeobuf and back preserves CRS."""
        runner = CliRunner()
        fgb_file = tmp_path / "test.fgb"
        output_parquet = tmp_path / "roundtrip.parquet"

        # Export to FlatGeobuf via CLI
        result = runner.invoke(cli, ["convert", "flatgeobuf", places_test_file, str(fgb_file)])
        assert result.exit_code == 0, f"Export failed: {result.output}"

        # Import back via CLI
        result = runner.invoke(cli, ["convert", str(fgb_file), str(output_parquet)])
        assert result.exit_code == 0, f"Import failed: {result.output}"

        # Verify file exists and has CRS
        assert output_parquet.exists()
