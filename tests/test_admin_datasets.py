"""
Tests for admin dataset abstraction layer.
"""

import click
import pytest

from geoparquet_io.core.admin_datasets import (
    AdminDatasetFactory,
    CurrentAdminDataset,
    GAULAdminDataset,
    OvertureAdminDataset,
)


class TestCurrentAdminDataset:
    """Test CurrentAdminDataset implementation."""

    def test_get_dataset_name(self):
        dataset = CurrentAdminDataset()
        assert dataset.get_dataset_name() == "Current (source.coop countries)"

    def test_get_default_source(self):
        dataset = CurrentAdminDataset()
        source = dataset.get_default_source()
        assert source.startswith("https://data.source.coop/")
        assert "countries.parquet" in source

    def test_get_available_levels(self):
        dataset = CurrentAdminDataset()
        levels = dataset.get_available_levels()
        assert levels == ["country"]

    def test_get_level_column_mapping(self):
        dataset = CurrentAdminDataset()
        mapping = dataset.get_level_column_mapping()
        assert mapping == {"country": "country"}

    def test_get_geometry_column(self):
        dataset = CurrentAdminDataset()
        assert dataset.get_geometry_column() == "geometry"

    def test_get_bbox_column(self):
        dataset = CurrentAdminDataset()
        assert dataset.get_bbox_column() == "bbox"

    def test_is_remote_with_default(self):
        dataset = CurrentAdminDataset()
        assert dataset.is_remote() is True

    def test_is_remote_with_local_path(self):
        dataset = CurrentAdminDataset(source_path="/local/path/data.parquet")
        assert dataset.is_remote() is False

    def test_get_partition_columns(self):
        dataset = CurrentAdminDataset()
        columns = dataset.get_partition_columns(["country"])
        assert columns == ["country"]

    def test_validate_levels_valid(self):
        dataset = CurrentAdminDataset()
        # Should not raise
        dataset.validate_levels(["country"])

    def test_validate_levels_invalid(self):
        dataset = CurrentAdminDataset()
        with pytest.raises(click.UsageError) as exc_info:
            dataset.validate_levels(["continent"])
        assert "Invalid levels" in str(exc_info.value)
        assert "continent" in str(exc_info.value)


class TestGAULAdminDataset:
    """Test GAULAdminDataset implementation."""

    def test_get_dataset_name(self):
        dataset = GAULAdminDataset()
        assert dataset.get_dataset_name() == "GAUL L2 Admin Boundaries"

    def test_get_default_source(self):
        dataset = GAULAdminDataset()
        source = dataset.get_default_source()
        assert "gaul-l2-admin" in source.lower()
        assert source.endswith("*.parquet")

    def test_get_available_levels(self):
        dataset = GAULAdminDataset()
        levels = dataset.get_available_levels()
        assert levels == ["continent", "country", "department"]

    def test_get_level_column_mapping(self):
        dataset = GAULAdminDataset()
        mapping = dataset.get_level_column_mapping()
        assert mapping["continent"] == "continent"
        assert mapping["country"] == "gaul0_name"
        assert mapping["department"] == "gaul2_name"

    def test_get_geometry_column(self):
        dataset = GAULAdminDataset()
        assert dataset.get_geometry_column() == "geometry"

    def test_get_bbox_column(self):
        dataset = GAULAdminDataset()
        assert dataset.get_bbox_column() == "geometry_bbox"

    def test_get_partition_columns_single_level(self):
        dataset = GAULAdminDataset()
        columns = dataset.get_partition_columns(["continent"])
        assert columns == ["continent"]

    def test_get_partition_columns_multi_level(self):
        dataset = GAULAdminDataset()
        columns = dataset.get_partition_columns(["continent", "country", "department"])
        assert columns == ["continent", "gaul0_name", "gaul2_name"]

    def test_validate_levels_all_valid(self):
        dataset = GAULAdminDataset()
        # Should not raise
        dataset.validate_levels(["continent", "country", "department"])

    def test_validate_levels_partial_valid(self):
        dataset = GAULAdminDataset()
        # Should not raise
        dataset.validate_levels(["continent", "country"])

    def test_validate_levels_invalid(self):
        dataset = GAULAdminDataset()
        with pytest.raises(click.UsageError) as exc_info:
            dataset.validate_levels(["region"])
        assert "Invalid levels" in str(exc_info.value)


class TestOvertureAdminDataset:
    """Test OvertureAdminDataset implementation."""

    def test_get_dataset_name(self):
        dataset = OvertureAdminDataset()
        assert dataset.get_dataset_name() == "Overture Maps Divisions"

    def test_get_default_source(self):
        dataset = OvertureAdminDataset()
        source = dataset.get_default_source()
        assert source.startswith("s3://")
        assert "overturemaps" in source.lower()
        assert "divisions" in source.lower()

    def test_get_available_levels(self):
        dataset = OvertureAdminDataset()
        levels = dataset.get_available_levels()
        # At minimum should have some levels defined
        assert len(levels) > 0
        assert isinstance(levels, list)

    def test_get_geometry_column(self):
        dataset = OvertureAdminDataset()
        assert dataset.get_geometry_column() == "geometry"

    def test_get_bbox_column(self):
        dataset = OvertureAdminDataset()
        assert dataset.get_bbox_column() == "bbox"

    def test_get_read_parquet_options(self):
        dataset = OvertureAdminDataset()
        options = dataset.get_read_parquet_options()
        assert "hive_partitioning" in options
        assert options["hive_partitioning"] == 1

    def test_get_subtype_filter(self):
        dataset = OvertureAdminDataset()
        # Test with country level
        filter_country = dataset.get_subtype_filter(["country"])
        assert filter_country == "subtype IN ('country')"

        # Test with both levels
        filter_both = dataset.get_subtype_filter(["country", "region"])
        assert "country" in filter_both
        assert "region" in filter_both
        assert "subtype IN" in filter_both

    def test_get_column_transform_region(self):
        """Test that region level returns SQL transformation for Vecorel compliance."""
        dataset = OvertureAdminDataset()
        transform = dataset.get_column_transform("region")

        # Should return transformation SQL to strip country prefix
        assert transform is not None
        assert "CASE WHEN region LIKE '%-%'" in transform
        assert "split_part(region, '-', 2)" in transform
        assert "ELSE region END" in transform

    def test_get_column_transform_country(self):
        """Test that country level returns None (no transformation needed)."""
        dataset = OvertureAdminDataset()
        transform = dataset.get_column_transform("country")

        # Country doesn't need transformation
        assert transform is None

    def test_get_column_transform_unknown_level(self):
        """Test that unknown levels return None."""
        dataset = OvertureAdminDataset()
        transform = dataset.get_column_transform("unknown_level")

        assert transform is None

    def test_get_output_column_name_country(self):
        """Test Vecorel-compliant country column name."""
        dataset = OvertureAdminDataset()
        col_name = dataset.get_output_column_name("country")

        # Should map to Vecorel column name
        assert col_name == "admin:country_code"

    def test_get_output_column_name_region(self):
        """Test Vecorel-compliant subdivision column name."""
        dataset = OvertureAdminDataset()
        col_name = dataset.get_output_column_name("region")

        # Should map to Vecorel column name
        assert col_name == "admin:subdivision_code"

    def test_get_output_column_name_fallback(self):
        """Test fallback to default pattern for unknown levels."""
        dataset = OvertureAdminDataset()
        col_name = dataset.get_output_column_name("unknown_level")

        # Should fall back to default pattern
        assert col_name == "admin:unknown_level"


class TestBaseAdminDatasetDefaults:
    """Test base AdminDataset default implementations for backwards compatibility."""

    def test_get_column_transform_default(self):
        """Test that base class returns None by default (no transformation)."""
        # Use CurrentAdminDataset as a concrete implementation
        dataset = CurrentAdminDataset()
        transform = dataset.get_column_transform("country")

        # Default implementation should return None
        assert transform is None

    def test_get_output_column_name_default(self):
        """Test that base class uses default 'admin:{level}' pattern."""
        # Use CurrentAdminDataset as a concrete implementation
        dataset = CurrentAdminDataset()
        col_name = dataset.get_output_column_name("country")

        # Default implementation should use pattern
        assert col_name == "admin:country"

    def test_gaul_uses_default_column_names(self):
        """Test that GAUL dataset uses default column naming (not Vecorel)."""
        dataset = GAULAdminDataset()

        # GAUL should use default naming pattern
        assert dataset.get_output_column_name("continent") == "admin:continent"
        assert dataset.get_output_column_name("country") == "admin:country"
        assert dataset.get_output_column_name("department") == "admin:department"

    def test_gaul_no_column_transforms(self):
        """Test that GAUL dataset has no column transformations."""
        dataset = GAULAdminDataset()

        # GAUL should not transform columns
        assert dataset.get_column_transform("continent") is None
        assert dataset.get_column_transform("country") is None
        assert dataset.get_column_transform("department") is None


class TestAdminDatasetFactory:
    """Test AdminDatasetFactory."""

    def test_get_available_datasets(self):
        datasets = AdminDatasetFactory.get_available_datasets()
        assert "current" in datasets
        assert "gaul" in datasets
        assert "overture" in datasets

    def test_create_current_dataset(self):
        dataset = AdminDatasetFactory.create("current")
        assert isinstance(dataset, CurrentAdminDataset)

    def test_create_gaul_dataset(self):
        dataset = AdminDatasetFactory.create("gaul")
        assert isinstance(dataset, GAULAdminDataset)

    def test_create_overture_dataset(self):
        dataset = AdminDatasetFactory.create("overture")
        assert isinstance(dataset, OvertureAdminDataset)

    def test_create_with_custom_source(self):
        dataset = AdminDatasetFactory.create("current", source_path="/custom/path.parquet")
        assert dataset.source_path == "/custom/path.parquet"
        assert dataset.get_source() == "/custom/path.parquet"

    def test_create_with_verbose(self):
        dataset = AdminDatasetFactory.create("current", verbose=True)
        assert dataset.verbose is True

    def test_create_invalid_dataset(self):
        with pytest.raises(click.UsageError) as exc_info:
            AdminDatasetFactory.create("invalid_dataset")
        assert "Unknown admin dataset" in str(exc_info.value)
        assert "invalid_dataset" in str(exc_info.value)


class TestAdminDatasetIntegration:
    """Integration tests for admin datasets."""

    def test_current_dataset_full_workflow(self):
        """Test typical workflow with current dataset."""
        dataset = AdminDatasetFactory.create("current")

        # Validate levels
        dataset.validate_levels(["country"])

        # Get partition columns
        columns = dataset.get_partition_columns(["country"])
        assert columns == ["country"]

        # Check remote status
        assert dataset.is_remote() is True

    def test_gaul_dataset_hierarchical_workflow(self):
        """Test hierarchical workflow with GAUL dataset."""
        dataset = AdminDatasetFactory.create("gaul")

        # Test single level
        dataset.validate_levels(["continent"])
        columns = dataset.get_partition_columns(["continent"])
        assert columns == ["continent"]

        # Test two levels
        dataset.validate_levels(["continent", "country"])
        columns = dataset.get_partition_columns(["continent", "country"])
        assert columns == ["continent", "gaul0_name"]

        # Test all three levels
        dataset.validate_levels(["continent", "country", "department"])
        columns = dataset.get_partition_columns(["continent", "country", "department"])
        assert columns == ["continent", "gaul0_name", "gaul2_name"]

    def test_custom_source_override(self):
        """Test using custom source instead of default."""
        custom_path = "/my/custom/gaul.parquet"
        dataset = AdminDatasetFactory.create("gaul", source_path=custom_path)

        assert dataset.get_source() == custom_path
        assert dataset.is_remote() is False

    def test_remote_url_detection(self):
        """Test detection of remote vs local sources."""
        # HTTP URL
        dataset = AdminDatasetFactory.create("gaul", source_path="http://example.com/data.parquet")
        assert dataset.is_remote() is True

        # HTTPS URL
        dataset = AdminDatasetFactory.create("gaul", source_path="https://example.com/data.parquet")
        assert dataset.is_remote() is True

        # S3 URL
        dataset = AdminDatasetFactory.create("gaul", source_path="s3://bucket/data.parquet")
        assert dataset.is_remote() is True

        # Local path
        dataset = AdminDatasetFactory.create("gaul", source_path="/local/path/data.parquet")
        assert dataset.is_remote() is False
