#!/usr/bin/env python3

"""
Admin partition dataset abstraction layer.

This module provides a plugin-like architecture for different administrative
boundary datasets with hierarchical level support. Datasets can be local files
or remote URLs, with automatic caching and error handling.
"""

import os
from abc import ABC, abstractmethod
from typing import Optional

import click
import duckdb


class AdminDataset(ABC):
    """
    Base class for administrative partition datasets.

    Provides a common interface for different admin boundary datasets with
    hierarchical level support (e.g., continent → country → subdivisions).
    """

    def __init__(self, source_path: Optional[str] = None, verbose: bool = False):
        """
        Initialize the admin dataset.

        Args:
            source_path: Path or URL to the dataset. If None, uses dataset's default.
            verbose: Enable verbose logging
        """
        self.source_path = source_path
        self.verbose = verbose
        self._cached_path: Optional[str] = None

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
    def get_bbox_column(self) -> Optional[str]:
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

    def prepare_data_source(self, con: duckdb.DuckDBPyConnection) -> str:
        """
        Prepare the data source for querying.

        For remote sources, this downloads and caches the dataset locally.
        For local sources, this just returns the path.

        Args:
            con: DuckDB connection to use for queries

        Returns:
            SQL table reference or file path to use in queries
        """
        source = self.get_source()
        if self.is_remote():
            # For remote sources, use caching
            from geoparquet_io.core.admin_cache import cache_dataset, get_cached_file

            # Check if already cached
            cached_path = get_cached_file(source, self.get_dataset_name())
            if cached_path:
                if self.verbose:
                    click.echo(f"Using cached dataset: {cached_path}")
                return f"'{cached_path}'"

            # Download and cache
            if self.verbose:
                click.echo(f"Downloading and caching {self.get_dataset_name()}...")

            try:
                cached_path = cache_dataset(source, self.get_dataset_name(), self.verbose)
                return f"'{cached_path}'"
            except Exception as e:
                # Fall back to direct remote access if caching fails
                if self.verbose:
                    click.echo(
                        click.style(
                            f"Warning: Caching failed, using direct remote access: {e}",
                            fg="yellow",
                        )
                    )
                return f"'{source}'"
        else:
            # For local sources, verify the file exists
            if not os.path.exists(source):
                raise click.ClickException(f"Data source file not found: {source}")
            if self.verbose:
                click.echo(f"Using local data source: {source}")
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

    def get_bbox_column(self) -> Optional[str]:
        return "bbox"


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

    def get_bbox_column(self) -> Optional[str]:
        return "geometry_bbox"


class OvertureAdminDataset(AdminDataset):
    """
    Overture Maps Divisions dataset.

    Provides hierarchical administrative boundaries. The exact levels will be
    determined by inspecting the data structure.

    Schema includes:
    - country: ISO 3166-1 alpha-2 code
    - subtype: Category (country, region, locality, etc.)
    - names: Primary name and translations
    - region: ISO 3166-2 principal subdivision code
    - parent_division_id: Parent division ID for hierarchy
    - hierarchies: Hierarchical relationships
    """

    def get_dataset_name(self) -> str:
        return "Overture Maps Divisions"

    def get_default_source(self) -> str:
        return "s3://overturemaps-us-west-2/release/2025-10-22.0/theme=divisions/type=division/*"

    def get_available_levels(self) -> list[str]:
        # TODO: These levels will be refined after inspecting actual data structure
        # For now, using placeholder levels based on schema
        return ["country", "region", "locality"]

    def get_level_column_mapping(self) -> dict[str, str]:
        # TODO: This mapping will be refined after data inspection
        # For now, using schema fields that seem most relevant
        return {
            "country": "country",
            "region": "region",
            "locality": "names.primary",  # May need special handling for STRUCT
        }

    def get_geometry_column(self) -> str:
        return "geometry"

    def get_bbox_column(self) -> Optional[str]:
        return "bbox"


class CustomAdminDataset(AdminDataset):
    """
    Custom user-provided admin dataset.

    Allows users to provide their own boundaries dataset with custom partition columns.
    This provides maximum flexibility for any admin boundary dataset.
    """

    def __init__(
        self,
        source_path: str,
        partition_columns: list[str],
        geometry_column: str = "geometry",
        bbox_column: Optional[str] = "bbox",
        verbose: bool = False,
    ):
        """
        Initialize custom dataset.

        Args:
            source_path: Path or URL to the boundaries dataset
            partition_columns: List of column names to partition by
            geometry_column: Name of geometry column (default: "geometry")
            bbox_column: Name of bbox column (default: "bbox", None if not available)
            verbose: Enable verbose logging
        """
        super().__init__(source_path=source_path, verbose=verbose)
        self._partition_columns = partition_columns
        self._geometry_column = geometry_column
        self._bbox_column = bbox_column

    def get_dataset_name(self) -> str:
        return "Custom boundaries dataset"

    def get_default_source(self) -> str:
        # Custom datasets always require source_path
        raise NotImplementedError("Custom datasets require source_path to be provided")

    def get_available_levels(self) -> list[str]:
        # For custom datasets, "levels" are just the partition column names
        return self._partition_columns

    def get_level_column_mapping(self) -> dict[str, str]:
        # For custom datasets, level names ARE the column names (identity mapping)
        return {col: col for col in self._partition_columns}

    def get_geometry_column(self) -> str:
        return self._geometry_column

    def get_bbox_column(self) -> Optional[str]:
        return self._bbox_column


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
        cls, dataset_name: str, source_path: Optional[str] = None, verbose: bool = False
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

    @classmethod
    def create_custom(
        cls,
        source_path: str,
        partition_columns: list[str],
        geometry_column: str = "geometry",
        bbox_column: Optional[str] = "bbox",
        verbose: bool = False,
    ) -> CustomAdminDataset:
        """
        Create a custom admin dataset instance.

        This allows users to provide any boundaries dataset with custom partition columns.

        Args:
            source_path: Path or URL to the boundaries dataset
            partition_columns: List of column names to partition by
            geometry_column: Name of geometry column (default: "geometry")
            bbox_column: Name of bbox column (default: "bbox", None if not available)
            verbose: Enable verbose logging

        Returns:
            CustomAdminDataset instance
        """
        return CustomAdminDataset(
            source_path=source_path,
            partition_columns=partition_columns,
            geometry_column=geometry_column,
            bbox_column=bbox_column,
            verbose=verbose,
        )

    @classmethod
    def register_dataset(cls, name: str, dataset_class: type):
        """
        Register a new admin dataset class.

        This allows for extensibility - external packages can register
        their own admin datasets.

        Args:
            name: Dataset identifier
            dataset_class: Class that implements AdminDataset
        """
        if not issubclass(dataset_class, AdminDataset):
            raise TypeError(f"{dataset_class} must be a subclass of AdminDataset")
        cls._datasets[name] = dataset_class
