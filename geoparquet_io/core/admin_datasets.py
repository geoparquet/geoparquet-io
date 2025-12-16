#!/usr/bin/env python3

"""
Admin partition dataset abstraction layer.

This module provides a plugin-like architecture for different administrative
boundary datasets with hierarchical level support. Datasets can be local files
or remote URLs, with automatic caching and error handling.
"""

import os
from abc import ABC, abstractmethod

import click
import duckdb

from geoparquet_io.core.logging_config import debug


class AdminDataset(ABC):
    """
    Base class for administrative partition datasets.

    Provides a common interface for different admin boundary datasets with
    hierarchical level support (e.g., continent → country → subdivisions).
    """

    def __init__(self, source_path: str | None = None, verbose: bool = False):
        """
        Initialize the admin dataset.

        Args:
            source_path: Path or URL to the dataset. If None, uses dataset's default.
            verbose: Enable verbose logging
        """
        self.source_path = source_path
        self.verbose = verbose

    @abstractmethod
    def get_dataset_name(self) -> str:
        """
        Get the human-readable name of this dataset.

        Returns:
            Dataset name (e.g., "GAUL L2 Admin Boundaries")
        """
        pass

    @abstractmethod
    def get_default_source(self) -> str:
        """
        Get the default source URL/path for this dataset.

        Returns:
            Default URL or file path
        """
        pass

    @abstractmethod
    def get_available_levels(self) -> list[str]:
        """
        Get list of available hierarchical levels for this dataset.

        Returns:
            List of level names (e.g., ["continent", "country", "department"])
        """
        pass

    @abstractmethod
    def get_level_column_mapping(self) -> dict[str, str]:
        """
        Get mapping from level names to dataset column names.

        Returns:
            Dictionary mapping level names to column names
            (e.g., {"continent": "continent", "country": "gaul0_name"})
        """
        pass

    @abstractmethod
    def get_geometry_column(self) -> str:
        """
        Get the name of the geometry column in this dataset.

        Returns:
            Geometry column name
        """
        pass

    @abstractmethod
    def get_bbox_column(self) -> str | None:
        """
        Get the name of the bbox column in this dataset, if available.

        Returns:
            Bbox column name or None if not available
        """
        pass

    def get_source(self) -> str:
        """
        Get the data source path (either custom or default).

        Returns:
            Path or URL to the dataset
        """
        return self.source_path if self.source_path else self.get_default_source()

    def is_remote(self) -> bool:
        """
        Check if the data source is remote (HTTP/HTTPS/S3).

        Returns:
            True if remote, False if local file
        """
        source = self.get_source()
        return source.startswith(("http://", "https://", "s3://"))

    def validate_levels(self, levels: list[str]) -> None:
        """
        Validate that requested levels are available in this dataset.

        Args:
            levels: List of level names to validate

        Raises:
            click.UsageError: If any level is not available
        """
        available = self.get_available_levels()
        invalid = [level for level in levels if level not in available]
        if invalid:
            raise click.UsageError(
                f"Invalid levels for {self.get_dataset_name()}: {', '.join(invalid)}. "
                f"Available levels: {', '.join(available)}"
            )

    def get_partition_columns(self, levels: list[str]) -> list[str]:
        """
        Get the actual column names for the requested hierarchical levels.

        Args:
            levels: List of level names (e.g., ["continent", "country"])

        Returns:
            List of column names in the dataset

        Raises:
            click.UsageError: If any level is invalid
        """
        self.validate_levels(levels)
        mapping = self.get_level_column_mapping()
        return [mapping[level] for level in levels]

    def get_read_parquet_options(self) -> dict:
        """
        Get additional options to pass to read_parquet() for this dataset.

        Returns:
            Dictionary of option names to values
        """
        return {}

    def get_subtype_filter(self, levels: list[str]) -> str | None:
        """
        Get SQL WHERE clause to filter by subtype (for datasets that use subtype).

        Args:
            levels: List of level names to include

        Returns:
            SQL WHERE clause string or None if not applicable
        """
        return None

    def get_column_transform(self, level_name: str) -> str | None:
        """
        Get SQL expression to transform a column value for Vecorel compliance.

        This method allows datasets to specify transformations needed to make
        their native column values conform to the Vecorel administrative division
        extension specification.

        Args:
            level_name: The level name (e.g., "country", "region")

        Returns:
            SQL transformation expression or None if no transform needed
        """
        return None

    def get_output_column_name(self, level_name: str) -> str:
        """
        Get the output column name for a given administrative level.

        This allows datasets to specify custom output column names that differ
        from the simple "admin:{level}" pattern, enabling compliance with
        specifications like Vecorel that use specific column names.

        Args:
            level_name: The level name (e.g., "country", "region")

        Returns:
            Output column name (e.g., "admin:country_code", "admin:subdivision_code")
        """
        # Default behavior: use "admin:{level}" pattern
        return f"admin:{level_name}"

    @abstractmethod
    def configure_s3(self, con: duckdb.DuckDBPyConnection) -> None:
        """
        Configure S3 settings for this dataset.

        Default implementation does nothing (uses standard AWS S3).
        Subclasses can override this method if they require custom S3 configuration
        (e.g., custom endpoints like source.coop).

        Args:
            con: DuckDB connection to configure
        """
        pass  # Default: no custom S3 configuration needed (standard AWS S3)

    def prepare_data_source(self, con: duckdb.DuckDBPyConnection) -> str:
        """
        Prepare the data source for querying.

        For remote sources, uses direct remote access with spatial extent filtering.
        For local sources, verifies the file exists and returns the path.

        Args:
            con: DuckDB connection to use for queries

        Returns:
            SQL table reference or file path to use in queries
        """
        source = self.get_source()
        if self.is_remote():
            # For remote sources, use direct remote access
            if self.verbose:
                debug(f"Using remote dataset: {source}")
            return f"'{source}'"
        else:
            # For local sources, verify the file exists
            if not os.path.exists(source):
                raise click.ClickException(f"Data source file not found: {source}")
            if self.verbose:
                debug(f"Using local data source: {source}")
            return f"'{source}'"


class CurrentAdminDataset(AdminDataset):
    """
    Current built-in admin dataset (countries from source.coop).

    This is a wrapper around the existing country-level partition functionality.
    """

    def get_dataset_name(self) -> str:
        return "Current (source.coop countries)"

    def get_default_source(self) -> str:
        return "https://data.source.coop/cholmes/admin-boundaries/countries.parquet"

    def get_available_levels(self) -> list[str]:
        return ["country"]

    def get_level_column_mapping(self) -> dict[str, str]:
        return {"country": "country"}

    def get_geometry_column(self) -> str:
        return "geometry"

    def get_bbox_column(self) -> str | None:
        return "bbox"

    def configure_s3(self, con: duckdb.DuckDBPyConnection) -> None:
        """Configure S3 for source.coop endpoint."""
        con.execute("SET s3_endpoint='data.source.coop';")
        con.execute("SET s3_url_style='path';")
        con.execute("SET s3_use_ssl=true;")


class GAULAdminDataset(AdminDataset):
    """
    GAUL L2 Admin Boundaries dataset.

    Provides hierarchical administrative boundaries at three levels:
    - continent: Continental grouping
    - country: Country level (GAUL0)
    - department: Second-level admin units (GAUL2)
    """

    def get_dataset_name(self) -> str:
        return "GAUL L2 Admin Boundaries"

    def get_default_source(self) -> str:
        # Using S3 URL with wildcard pattern for by_country partitioning
        # DuckDB configured with source.coop endpoint in calling code
        return "s3://nlebovits/gaul-l2-admin/by_country/*.parquet"

    def get_available_levels(self) -> list[str]:
        return ["continent", "country", "department"]

    def get_level_column_mapping(self) -> dict[str, str]:
        return {
            "continent": "continent",
            "country": "gaul0_name",
            "department": "gaul2_name",
        }

    def get_geometry_column(self) -> str:
        return "geometry"

    def get_bbox_column(self) -> str | None:
        return "geometry_bbox"

    def configure_s3(self, con: duckdb.DuckDBPyConnection) -> None:
        """Configure S3 for source.coop endpoint."""
        con.execute("SET s3_endpoint='data.source.coop';")
        con.execute("SET s3_url_style='path';")
        con.execute("SET s3_use_ssl=true;")


class OvertureAdminDataset(AdminDataset):
    """
    Overture Maps Divisions dataset (release 2025-10-22.0).

    Provides hierarchical administrative boundaries at two levels, compliant with
    the Vecorel administrative division extension specification:
    - country: Country level (219 unique countries) → admin:country_code
    - region: First-level subdivisions (3,544 unique regions) → admin:subdivision_code

    Vecorel Compliance:
    - Outputs ISO 3166-1 alpha-2 country codes (e.g., "US", "AR", "DE")
    - Outputs ISO 3166-2 subdivision codes WITHOUT country prefix (e.g., "CA" not "US-CA")
    - Automatically transforms Overture's region column to strip country prefix

    Schema includes:
    - country: ISO 3166-1 alpha-2 code (maps to admin:country_code)
    - region: ISO 3166-2 code with country prefix (e.g., "US-CA", transformed to "CA")
    - subtype: Category (country, region, locality, etc.)
    - names.primary: Primary name for the division
    - geometry: Polygon geometry (GEOMETRY type)
    - bbox: Bounding box struct (xmin, xmax, ymin, ymax)

    See: https://docs.overturemaps.org/guides/divisions/
    See: https://vecorel.org/administrative-division-extension/v0.1.0/schema.yaml
    """

    def get_dataset_name(self) -> str:
        return "Overture Maps Divisions"

    def get_default_source(self) -> str:
        # Latest release with divisions theme and division_area type (polygons)
        return (
            "s3://overturemaps-us-west-2/release/2025-10-22.0/theme=divisions/type=division_area/*"
        )

    def get_available_levels(self) -> list[str]:
        return ["country", "region"]

    def get_level_column_mapping(self) -> dict[str, str]:
        return {
            "country": "country",  # Maps to admin:country_code
            "region": "region",  # Maps to admin:subdivision_code (needs transform)
        }

    def get_geometry_column(self) -> str:
        return "geometry"

    def get_bbox_column(self) -> str | None:
        return "bbox"

    def get_read_parquet_options(self) -> dict:
        """Overture uses Hive partitioning."""
        return {"hive_partitioning": 1}

    def get_subtype_filter(self, levels: list[str]) -> str | None:
        """Filter by subtype to only load relevant admin levels."""
        # Map level names to Overture subtype values
        level_to_subtype = {
            "country": "country",
            "region": "region",
        }
        subtypes = [level_to_subtype[level] for level in levels if level in level_to_subtype]
        if subtypes:
            subtype_list = ", ".join([f"'{s}'" for s in subtypes])
            return f"subtype IN ({subtype_list})"
        return None

    def get_column_transform(self, level_name: str) -> str | None:
        """
        Get SQL expression to transform a column value for Vecorel compliance.

        For region codes, strips the country prefix from ISO 3166-2 codes.
        Example: 'US-CA' becomes 'CA', 'AR-U' becomes 'U'

        Args:
            level_name: The level name (e.g., "country", "region")

        Returns:
            SQL transformation expression or None if no transform needed
        """
        if level_name == "region":
            # Strip country prefix for Vecorel compliance
            return "CASE WHEN region LIKE '%-%' THEN split_part(region, '-', 2) ELSE region END"
        return None

    def get_output_column_name(self, level_name: str) -> str:
        """
        Get the output column name for Vecorel compliance.

        Maps level names to Vecorel-compliant column names.

        Args:
            level_name: The level name (e.g., "country", "region")

        Returns:
            Vecorel-compliant column name
        """
        # Map to Vecorel column names
        vecorel_mapping = {
            "country": "admin:country_code",
            "region": "admin:subdivision_code",
        }
        return vecorel_mapping.get(level_name, f"admin:{level_name}")

    def configure_s3(self, con: duckdb.DuckDBPyConnection) -> None:
        """Configure S3 for AWS us-west-2 region where Overture data is stored."""
        con.execute("SET s3_region='us-west-2';")


class AdminDatasetFactory:
    """
    Factory for creating admin dataset instances.

    Provides a centralized way to instantiate the correct dataset class
    based on user selection.
    """

    _datasets = {
        "current": CurrentAdminDataset,
        "gaul": GAULAdminDataset,
        "overture": OvertureAdminDataset,
    }

    @classmethod
    def get_available_datasets(cls) -> list[str]:
        """
        Get list of available dataset names.

        Returns:
            List of dataset identifiers
        """
        return list(cls._datasets.keys())

    @classmethod
    def create(
        cls, dataset_name: str, source_path: str | None = None, verbose: bool = False
    ) -> AdminDataset:
        """
        Create an admin dataset instance.

        Args:
            dataset_name: Name of the dataset ("current", "gaul", "overture")
            source_path: Optional custom path/URL to dataset
            verbose: Enable verbose logging

        Returns:
            AdminDataset instance

        Raises:
            click.UsageError: If dataset_name is invalid
        """
        if dataset_name not in cls._datasets:
            raise click.UsageError(
                f"Unknown admin dataset: {dataset_name}. "
                f"Available: {', '.join(cls.get_available_datasets())}"
            )

        dataset_class = cls._datasets[dataset_name]
        return dataset_class(source_path=source_path, verbose=verbose)
