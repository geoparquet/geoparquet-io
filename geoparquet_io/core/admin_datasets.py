#!/usr/bin/env python3

"""
Admin partition dataset abstraction layer.

This module provides a plugin-like architecture for different administrative
boundary datasets with hierarchical level support. Datasets can be local files
or remote URLs, with automatic caching and error handling.
"""

import os
import time
from abc import ABC, abstractmethod
from pathlib import Path

import duckdb

from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    get_duckdb_connection,
    spill_directory,
    sql_path,
)
from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.logging_config import debug, info, warn
from geoparquet_io.core.overture import OVERTURE_FALLBACK_RELEASE

# =============================================================================
# Cache Configuration
# =============================================================================

# Cache age threshold in seconds (6 months)
CACHE_AGE_THRESHOLD_SECONDS = 6 * 30 * 24 * 60 * 60  # ~180 days

# Geometry simplification tolerance (in degrees) applied when building the
# per-level Overture admin caches. 0.0001° is ~11 m near the equator (~7 m at
# 50°N). This trades near-border attribution accuracy for much smaller caches
# and faster ST_Intersects joins. Country and region layers are simplified
# independently, so shared borders are not perfectly coincident (see todo 016).
_OVERTURE_SIMPLIFY_TOLERANCE_DEG = 0.0001

# Land-only filter shared by every Overture level. ``IS NOT FALSE`` (not
# ``= true``) drops only explicitly-maritime polygons and keeps land polygons
# whose flag is NULL, so genuine territory with an unset flag is not silently
# lost to SQL three-valued logic. See _build_level_cache_query for why maritime
# (EEZ) polygons have to go.
_OVERTURE_LAND_FILTER = "is_land IS NOT FALSE"

# Overture files three ISO-coded islands under placeholder X* codes: Saba (XS)
# and Sint Eustatius (XE) are both part of Bonaire, Sint Eustatius and Saba
# (ISO 3166-1 BQ), and Jan Mayen (XJ) is part of Svalbard and Jan Mayen (SJ).
# They are not disputed, so the X* filter below would drop real territory
# (NULL -> 'ZZ' under --vecorel). Every other X* code (XK for Kosovo, ...) is a
# genuine placeholder for a territory with no ISO code and stays excluded —
# that policy predates #819.
_OVERTURE_PLACEHOLDER_CODE_REMAP = {"XE": "BQ", "XS": "BQ", "XJ": "SJ"}


def _overture_country_code_sql(col: str = "country") -> str:
    """The country code for ``col``, with Overture's placeholder codes remapped.

    Used both by the cache producer (so the remapped rows survive the X*
    filter, which is applied to this expression rather than to the raw column)
    and by the join-time column transform (so a ``--no-cache`` run, which reads
    the raw release, emits the same codes the cache stores). Re-applying it to
    an already-remapped value is a no-op, so the cached path is unaffected.
    """
    whens = " ".join(
        f"WHEN {col} = '{_escape_sql_string(raw)}' THEN '{_escape_sql_string(iso)}'"
        for raw, iso in _OVERTURE_PLACEHOLDER_CODE_REMAP.items()
    )
    return f"CASE {whens} ELSE {col} END"


# Per-level cache schema for Overture. Declares, in one place, the Overture
# subtypes each level draws from, the columns its cache projects, and any
# level-specific row filters, so the cache producer (_build_level_cache_query)
# and the no-cache subtype filter share a single source of truth and an unknown
# level fails loudly instead of being silently treated as a region.
_OVERTURE_LEVEL_CACHE_CONFIG: dict[str, dict[str, list[str]]] = {
    "country": {
        # Overture splits its two country-shaped classes: "country" (219 land
        # rows — sovereign states plus a few that are not, such as MF, SX, TW
        # and XK) from dependent territories ("dependency", 53: French Guiana,
        # Puerto Rico, Reunion, Guadeloupe, Mayotte, New Caledonia, Greenland,
        # Hong Kong, Macao, Guam, the Channel Islands, ...) — 197 and 53
        # respectively once the X*/AQ filters below apply, the three
        # placeholder-coded dependencies being remapped rather than dropped.
        # Both classes carry an ISO 3166-1 alpha-2 `country` code, so the
        # country level takes both; filtering to 'country' alone left every
        # dependency unattributed (NULL -> 'ZZ' under --vecorel). See #819.
        #
        # This keeps the level effectively non-overlapping, which the
        # memory-safe plain LEFT JOIN relies on: a dependency is not contained
        # in its sovereign's polygon (Overture's FR is metropolitan France
        # only). Measured on the simplified geometry this cache actually stores
        # (release 2026-07-22.0), the only country/dependency intersections are
        # six shared borders — SR/GF, HK/CN, BR/GF, MO/CN, ES/GI, CA/GL — and
        # they are sub-hectare slivers (3.6e-07 deg² at worst, ~0 for CA/GL)
        # left by simplifying each polygon independently. That is six orders of
        # magnitude below the country-country overlaps the join already lives
        # with (CL-AR alone is 0.192 deg²), so the invariant is approximate, not
        # exact: a feature sitting inside such a sliver is emitted twice.
        "subtypes": ["country", "dependency"],
        "cols": ["bbox", "country", "subtype"],
        # Exclude Antarctica and the disputed/uncoded X* territories — applied
        # to the remapped code so BQ/SJ survive.
        "extra_filters": [f"{_overture_country_code_sql()} NOT LIKE 'X%'", "country != 'AQ'"],
    },
    "region": {
        # Only the country level widens: a dependency polygon is country-shaped,
        # so admitting it here would shadow the regions nested inside it.
        "subtypes": ["region"],
        "cols": ["bbox", "country", "region", "subtype"],
        "extra_filters": [],
    },
}


def _overture_subtype_sql(subtypes: list[str]) -> str:
    """Render an ``IN`` predicate over the given subtypes."""
    return "subtype IN ({})".format(", ".join(f"'{_escape_sql_string(s)}'" for s in subtypes))


def _overture_subtypes_for_levels(levels: list[str]) -> list[str]:
    """Overture subtypes backing the given levels, in order, without repeats."""
    subtypes: list[str] = []
    for level in levels:
        for subtype in _OVERTURE_LEVEL_CACHE_CONFIG.get(level, {}).get("subtypes", []):
            if subtype not in subtypes:
                subtypes.append(subtype)
    return subtypes


def _level_cache_config(level: str) -> dict[str, list[str]]:
    """The cache config for ``level``, or raise for an unconfigured level."""
    try:
        return _OVERTURE_LEVEL_CACHE_CONFIG[level]
    except KeyError:
        raise ValueError(f"No per-level cache config for admin level: {level!r}") from None


def _remove_stale_level_caches(cache_dir: Path, version: str, level: str, cache_path: Path) -> None:
    """Delete this level's superseded cache files for ``version``.

    The cache key encodes the filters baked into the file, so it changes
    whenever those filters change: the "-land" rename (todo 031) orphaned the
    unsuffixed file, and the "-dependency" segment (#819) orphaned the ~40MB
    ``-country-land.parquet`` one. Anything matching this level and version
    that is not the file we are about to use is dead weight.
    """
    for stale in cache_dir.glob(f"overture-{version}-{level}*.parquet"):
        if stale == cache_path:
            continue
        try:
            stale.unlink()
        except OSError:
            pass  # A cache we cannot delete is not worth failing the run over.


def _overture_level_predicate(level: str) -> str | None:
    """The full row predicate for one Overture admin level.

    Subtypes, the land-only filter and the level's own extra filters, ANDed —
    exactly the ``WHERE`` the level's cache is built with. ``None`` for a level
    with no cache config.
    """
    config = _OVERTURE_LEVEL_CACHE_CONFIG.get(level)
    if not config:
        return None
    return " AND ".join(
        [
            _overture_subtype_sql(config["subtypes"]),
            _OVERTURE_LAND_FILTER,
            *config["extra_filters"],
        ]
    )


def _overture_levels_predicate(levels: list[str]) -> str | None:
    """The row predicate admitting every configured level in ``levels``.

    Levels are OR'd, not AND'd, and each keeps its own filters: the country
    level's placeholder/Antarctica filters must not leak onto the region level.
    A repeated level contributes once. The result is parenthesised when it
    contains an ``OR``, because callers AND it with an extent filter.
    """
    seen: list[str] = []
    for level in levels:
        if level not in seen:
            seen.append(level)
    predicates = [p for p in (_overture_level_predicate(level) for level in seen) if p]
    if not predicates:
        return None
    if len(predicates) == 1:
        return predicates[0]
    return "({})".format(" OR ".join(f"({p})" for p in predicates))


def get_cache_dir() -> Path:
    """
    Get the cache directory for admin datasets.

    Returns:
        Path to cache directory: ~/.geoparquet-io/cache/admin/
    """
    return Path.home() / ".geoparquet-io" / "cache" / "admin"


def get_cached_path(dataset: "AdminDataset") -> Path:
    """
    Get the expected cache file path for a dataset.

    Args:
        dataset: AdminDataset instance

    Returns:
        Path where the cached file should be stored
    """
    cache_dir = get_cache_dir()
    dataset_name = dataset.get_default_prefix()  # "gaul", "overture", "current"
    version = dataset.get_version()
    filename = f"{dataset_name}-{version}.parquet"
    return cache_dir / filename


def check_cache_age(cache_file: Path) -> str | None:
    """
    Check if a cache file is older than the threshold (6 months).

    Args:
        cache_file: Path to the cache file

    Returns:
        Warning message if cache is old, None otherwise
    """
    if not cache_file.exists():
        return None

    file_mtime = cache_file.stat().st_mtime
    age_seconds = time.time() - file_mtime

    if age_seconds >= CACHE_AGE_THRESHOLD_SECONDS:
        age_days = int(age_seconds / (24 * 60 * 60))
        age_months = age_days // 30
        return (
            f"Cached admin dataset is {age_months} months old. "
            f"Consider clearing cache with --clear-cache to get updated data."
        )

    return None


def clear_cache(confirm: bool = False) -> dict | None:
    """
    Clear all cached admin datasets.

    Args:
        confirm: If True, actually delete files. If False, return without action.

    Returns:
        Dictionary with deletion stats: {"files_deleted": int, "bytes_freed": int}
        Returns None or {"cancelled": True} if confirm is False.
    """
    if not confirm:
        return {"cancelled": True}

    cache_dir = get_cache_dir()

    if not cache_dir.exists():
        return {"files_deleted": 0, "bytes_freed": 0}

    files_deleted = 0
    bytes_freed = 0

    # Only delete .parquet files
    for cache_file in cache_dir.glob("*.parquet"):
        try:
            bytes_freed += cache_file.stat().st_size
            cache_file.unlink()
            files_deleted += 1
        except OSError:
            pass  # Ignore deletion errors

    return {"files_deleted": files_deleted, "bytes_freed": bytes_freed}


def get_or_cache_dataset(
    dataset: "AdminDataset",
    no_cache: bool = False,
    verbose: bool = False,
) -> str:
    """
    Get the data source for a dataset, using cache if available.

    For remote datasets:
    1. If no_cache=True, return remote URL directly
    2. If cached file exists and is valid, return cached path
    3. Otherwise, download and cache the dataset, return cached path

    For local/custom datasets:
    - Return the path as-is (no caching)

    Args:
        dataset: AdminDataset instance
        no_cache: If True, skip cache and use remote directly
        verbose: Enable verbose logging

    Returns:
        Path or URL to use for the dataset
    """
    # Custom/local sources are not cached
    if dataset.source_path is not None:
        if verbose:
            debug(f"Using custom source (not cached): {dataset.source_path}")
        return dataset.source_path

    # Local files are not cached
    if not dataset.is_remote():
        return dataset.get_source()

    # If no_cache is requested, return remote URL directly
    if no_cache:
        if verbose:
            debug("Cache disabled, using remote source directly")
        return dataset.get_default_source()

    # Check for cached version
    cached_path = get_cached_path(dataset)

    # Check if cache exists and is valid (non-empty)
    if cached_path.exists() and cached_path.stat().st_size > 0:
        # Check and warn about old cache
        age_warning = check_cache_age(cached_path)
        if age_warning:
            warn(age_warning)

        if verbose:
            debug(f"Using cached dataset: {cached_path}")

        return str(cached_path)

    # Cache miss - need to download
    try:
        # Ensure cache directory exists
        cache_dir = get_cache_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)

        info(f"Downloading {dataset.get_dataset_name()} to local cache...")
        info("This is a one-time download. Future runs will use the cached version.")

        # Download to cache
        result_path = dataset._download_to_cache(cached_path)

        info(f"Cached dataset at: {result_path}")
        return str(result_path)

    except PermissionError:
        warn("Cannot create cache directory. Using remote source directly.")
        return dataset.get_default_source()
    except Exception as e:
        warn(f"Failed to cache dataset: {e}. Using remote source directly.")
        return dataset.get_default_source()


class AdminDataset(ABC):
    """
    Base class for administrative partition datasets.

    Provides a common interface for different admin boundary datasets with
    hierarchical level support (e.g., continent → country → subdivisions).
    """

    # Version identifier for this dataset release.
    # Subclasses MUST define this attribute with their release version.
    VERSION: str = "unknown"

    def __init__(self, source_path: str | None = None, verbose: bool = False):
        """
        Initialize the admin dataset.

        Args:
            source_path: Path or URL to the dataset. If None, uses dataset's default.
            verbose: Enable verbose logging
        """
        self.source_path = source_path
        self.verbose = verbose

    def get_version(self) -> str:
        """
        Get the version identifier for this dataset.

        Returns:
            Version string (e.g., "2024-12-19" or "2026-05-20.0")
        """
        return self.VERSION

    def _download_to_cache(self, cache_path: Path) -> Path:
        """
        Download the dataset to the cache location.

        This method downloads the full remote dataset and stores it locally.
        The default implementation uses DuckDB to read and write the parquet file.

        Args:
            cache_path: Path where the cached file should be written

        Returns:
            Path to the cached file

        Raises:
            Exception: If download fails
        """
        from geoparquet_io.core.duckdb_utils import s3_config_scope

        source = self.get_default_source()
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        with s3_config_scope(self.get_s3_config()):
            # Spill onto the cache volume to bound memory on the remote scan
            # (todo 013). A private leaf under it, never the cache dir itself:
            # DuckDB's spill filenames carry no connection identity, so two runs
            # sharing this well-known directory overwrite each other's blocks.
            con = get_duckdb_connection(
                load_spatial=True,
                load_httpfs=True,
                temp_directory=spill_directory(cache_path.parent),
            )
            try:
                # Get read options
                read_options = self.get_read_parquet_options()
                if read_options:
                    options_str = ", ".join([f"{k}={v}" for k, v in read_options.items()])
                    query = f"SELECT * FROM read_parquet({sql_path(source)}, {options_str})"
                else:
                    query = f"SELECT * FROM read_parquet({sql_path(source)})"

                # Write to cache
                con.execute(f"COPY ({query}) TO {sql_path(cache_path)} (FORMAT PARQUET)")
            finally:
                con.close()

        return cache_path

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
            InvalidParameterError: If any level is not available
        """
        available = self.get_available_levels()
        invalid = [level for level in levels if level not in available]
        if invalid:
            raise InvalidParameterError(
                "levels",
                f"Invalid levels for {self.get_dataset_name()}: {', '.join(invalid)}. "
                f"Available levels: {', '.join(available)}",
            )

    def get_partition_columns(self, levels: list[str]) -> list[str]:
        """
        Get the actual column names for the requested hierarchical levels.

        Args:
            levels: List of level names (e.g., ["continent", "country"])

        Returns:
            List of column names in the dataset

        Raises:
            InvalidParameterError: If any level is invalid
        """
        self.validate_levels(levels)
        mapping = self.get_level_column_mapping()
        return [mapping[level] for level in levels]

    def supports_per_level_sources(self) -> bool:
        """Whether this dataset provides separate source files per admin level."""
        return False

    def get_read_parquet_options(self) -> dict:
        """
        Get additional options to pass to read_parquet() for this dataset.

        Returns:
            Dictionary of option names to values
        """
        return {}

    def get_subtype_filter(self, levels: list[str], source: str | None = None) -> str | None:
        """
        Get SQL WHERE clause to filter by subtype (for datasets that use subtype).

        Args:
            levels: List of level names to include
            source: The resolved source the clause will run against. Datasets
                whose filters depend on which source is being read (a raw
                release vs. a pre-filtered cache) use it; others ignore it.

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

    def get_default_prefix(self) -> str:
        """
        Get the default prefix for this dataset's output columns.

        Default implementation extracts the first word from the dataset name
        and lowercases it. Subclasses can override for custom behavior.

        Returns:
            Default prefix string (e.g., "gaul", "overture", "current")

        Examples:
            "GAUL L2 Admin Boundaries" -> "gaul"
            "Overture Maps Divisions" -> "overture"
            "Current (source.coop countries)" -> "current"
        """
        # Extract first word from dataset name and lowercase
        dataset_name = self.get_dataset_name()
        first_word = dataset_name.split()[0]
        return first_word.lower()

    def get_output_column_name(self, level_name: str, prefix: str | None = None) -> str:
        """
        Get the output column name for a given administrative level.

        This allows datasets to specify custom output column names with
        configurable prefixes to support multi-dataset workflows.

        Args:
            level_name: The level name (e.g., "country", "region")
            prefix: Optional prefix for column names. If None, uses get_default_prefix().
                   If "admin", uses colon format (admin:level).
                   If "vecorel", uses Vecorel-compliant column names.
                   Otherwise uses underscore format (prefix_level).

        Returns:
            Output column name (e.g., "gaul_country", "admin:country", "custom_country")

        Examples:
            get_output_column_name("country", prefix=None) -> "gaul_country" (for GAUL)
            get_output_column_name("country", prefix="admin") -> "admin:country"
            get_output_column_name("country", prefix="vecorel") -> "admin:country_code"
            get_output_column_name("country", prefix="mycustom") -> "mycustom_country"
        """
        if prefix is None:
            # Use dataset's default prefix with underscore format
            prefix = self.get_default_prefix()
            return f"{prefix}_{level_name}"
        elif prefix == "admin":
            # Special case: use colon format for "admin" prefix
            return f"admin:{level_name}"
        elif prefix == "vecorel":
            return self.get_vecorel_column_name(level_name)
        else:
            # Custom prefix with underscore format
            return f"{prefix}_{level_name}"

    def get_vecorel_column_name(self, level_name: str) -> str:
        """Get Vecorel-compliant output column name for an admin level.

        Subclasses can override for dataset-specific mappings.
        Default maps to admin:{level_name}.
        """
        return f"admin:{level_name}"

    def get_s3_config(self) -> dict:
        """Return S3 configuration for this dataset. Override in subclasses."""
        return {}

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
            A SQL table reference: the source path as a quoted, escaped literal
            (:func:`sql_path`), ready to interpolate (#802).
        """
        source = self.get_source()
        if self.is_remote():
            # For remote sources, use direct remote access
            if self.verbose:
                debug(f"Using remote dataset: {source}")
            return sql_path(source)
        else:
            # For local sources, verify the file exists
            if not os.path.exists(source):
                raise FileNotFoundGeoParquetError(source, "admin dataset")
            if self.verbose:
                debug(f"Using local data source: {source}")
            return sql_path(source)


class CurrentAdminDataset(AdminDataset):
    """
    Current built-in admin dataset (countries from source.coop).

    This is a wrapper around the existing country-level partition functionality.
    """

    # Version from source.coop countries dataset
    VERSION = "2024-01-01"

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

    def get_s3_config(self) -> dict:
        return {"s3_endpoint": "data.source.coop", "s3_use_ssl": True}

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

    Version corresponds to the data release date from source.coop.
    """

    # GAUL dataset version (from source.coop release)
    VERSION = "2024-12-19"

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

    def get_s3_config(self) -> dict:
        return {"s3_endpoint": "data.source.coop", "s3_use_ssl": True}

    def configure_s3(self, con: duckdb.DuckDBPyConnection) -> None:
        """Configure S3 for source.coop endpoint."""
        con.execute("SET s3_endpoint='data.source.coop';")
        con.execute("SET s3_url_style='path';")
        con.execute("SET s3_use_ssl=true;")


class OvertureAdminDataset(AdminDataset):
    """
    Overture Maps Divisions dataset (release 2026-05-20.0).

    Provides hierarchical administrative boundaries at two levels, compliant with
    the Vecorel administrative division extension specification:
    - country: Country level → admin:country_code. Covers both of Overture's
      two country-shaped classes, ``subtype`` ``country`` (219 rows: sovereign
      states plus a few that are not, such as MF, SX, TW and XK) and
      ``dependency`` (53 dependent territories such as French Guiana, Puerto
      Rico and Greenland), since both carry ISO 3166-1 alpha-2 codes.
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

    VERSION = OVERTURE_FALLBACK_RELEASE

    def get_dataset_name(self) -> str:
        return "Overture Maps Divisions"

    def get_version(self) -> str:
        import re

        from geoparquet_io.core.overture import get_latest_overture_release

        release = get_latest_overture_release(verbose=self.verbose)
        if not re.match(r"^\d{4}-\d{2}-\d{2}\.\d+$", release):
            from geoparquet_io.core.logging_config import warn

            warn(f"Unexpected Overture release format: {release!r}, using fallback")
            return self.VERSION
        return release

    def get_default_source(self) -> str:
        from geoparquet_io.core.overture import get_overture_divisions_url

        return get_overture_divisions_url(verbose=self.verbose)

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

    def _is_prefiltered_source(self, source: str | None) -> bool:
        """Whether ``source`` already holds exactly the rows a level selects.

        True for the per-level caches this class produces —
        :meth:`_build_level_cache_query` bakes every filter in, and the cache
        projects only ``bbox``/``country``/``subtype`` (no ``is_land``), so the
        full predicate could not even bind against it — and for a user-supplied
        ``--admin-source``, whose schema gpio does not control.
        """
        if self.source_path is not None:
            return True
        if source is None:
            return False
        return Path(source).parent == get_cache_dir()

    def get_subtype_filter(self, levels: list[str], source: str | None = None) -> str | None:
        """The row filter that selects the requested admin levels.

        Against Overture's raw release — the ``--no-cache`` path — this is the
        whole per-level predicate: subtypes, the land-only filter and the
        level's own extra filters, exactly the ``WHERE`` its cache is built
        with, so both paths admit the same rows (#819). Emitting the subtype
        clause alone let a feature match both a territory's land polygon and
        its maritime (EEZ) polygon, and the plain LEFT JOIN then emitted it
        twice — for every sovereign, and for the 53 dependencies this level now
        attributes.

        Against a pre-filtered source (see :meth:`_is_prefiltered_source`) only
        the subtype clause is emitted: the rest is already baked in, and
        ``is_land`` is not there to filter on.
        """
        if self._is_prefiltered_source(source):
            subtypes = _overture_subtypes_for_levels(levels)
            return _overture_subtype_sql(subtypes) if subtypes else None
        return _overture_levels_predicate(levels)

    def get_column_transform(self, level_name: str) -> str | None:
        """
        Get SQL expression to transform a column value for Vecorel compliance.

        For country codes, remaps Overture's placeholder X* codes for the three
        ISO-coded islands that carry them (XS/XE -> BQ, XJ -> SJ). The cache
        already stores the remapped code, so this only bites on the
        ``--no-cache`` path, where it reads the raw release; re-applying it to a
        cached BQ/SJ is a no-op.

        For region codes, strips the country prefix from ISO 3166-2 codes.
        Example: 'US-CA' becomes 'CA', 'AR-U' becomes 'U'

        Args:
            level_name: The level name (e.g., "country", "region")

        Returns:
            SQL transformation expression or None if no transform needed
        """
        if level_name == "country":
            # Qualified with the admin alias `b.` (and quoted) for the same
            # reason as the region transform below: the input may carry its own
            # `country` column, which `a.*` forwards through the join.
            return _overture_country_code_sql('b."country"')
        if level_name == "region":
            # Strip country prefix for Vecorel compliance. Qualify with the admin
            # alias `b.` (and quote) so the ref is unambiguous when the input
            # already carries a `region` column (a.* forwards it via the join).
            return (
                "CASE WHEN b.\"region\" LIKE '%-%' "
                'THEN split_part(b."region", \'-\', 2) ELSE b."region" END'
            )
        return None

    def get_vecorel_column_name(self, level_name: str) -> str:
        """Map Overture levels to Vecorel admin division column names."""
        mapping = {
            "country": "admin:country_code",
            "region": "admin:subdivision_code",
        }
        return mapping.get(level_name, f"admin:{level_name}")

    def supports_per_level_sources(self) -> bool:
        return True

    def get_cached_path_for_level(self, level: str) -> Path:
        cache_dir = get_cache_dir()
        version = self.get_version()
        # The filename encodes the filters baked into the file so a stale cache
        # is never silently reused. "-land" invalidates pre-fix caches that
        # mixed in maritime (EEZ) polygons (see _build_level_cache_query); the
        # subtypes a level draws on beyond its own name are appended, so the
        # dependency-aware country cache (#819) gets a fresh key while the
        # unchanged region cache keeps its own and is not re-downloaded.
        # An unknown level raises here rather than naming a file that
        # _build_level_cache_query would then refuse to fill.
        config = _level_cache_config(level)
        extra = [s for s in config["subtypes"] if s != level]
        suffix = f"-{'-'.join(extra)}" if extra else ""
        return cache_dir / f"overture-{version}-{level}{suffix}-land.parquet"

    def _build_level_cache_query(self, level: str, source: str) -> str:
        """Build the SELECT that produces a non-overlapping per-level cache.

        Only land polygons are kept. Overture stores a separate maritime (EEZ)
        polygon per division whose geometry spans the whole territory —
        including the entire landmass — so keeping both classes makes every
        land feature match two polygons per level, inflating spatial-join
        output ~2x per level. Land-only keeps each level's polygons
        non-overlapping, which is what the plain (memory-safe) LEFT JOIN relies
        on instead of an OOM-prone dedup window.

        The filter is ``is_land IS NOT FALSE`` (not ``= true``): it drops only
        explicitly-maritime polygons and keeps land polygons whose flag is
        NULL, so genuine territory with an unset flag is not silently lost to
        SQL three-valued logic. Columns and level-specific filters come from
        :data:`_OVERTURE_LEVEL_CACHE_CONFIG` so an unknown level raises rather
        than being mis-projected, and the ``WHERE`` is the same predicate
        :meth:`get_subtype_filter` hands the ``--no-cache`` path.

        The projected ``country`` column carries the placeholder-code remap
        (see :func:`_overture_country_code_sql`), so the cache stores BQ/SJ
        rather than Overture's XS/XE/XJ.
        """
        config = _level_cache_config(level)
        tol = _OVERTURE_SIMPLIFY_TOLERANCE_DEG
        cols = ", ".join(
            _overture_country_code_sql() + " AS country" if col == "country" else col
            for col in config["cols"]
        )
        where = _overture_level_predicate(level)
        return (
            f"SELECT ST_SimplifyPreserveTopology(geometry, {tol}) as geometry, "
            f"{cols} "
            f"FROM read_parquet({sql_path(source)}, hive_partitioning=1) "
            f"WHERE {where}"
        )

    def get_source_for_level(self, level: str, no_cache: bool = False) -> str:
        """Get the cached source path for a specific admin level."""
        if self.source_path is not None:
            return self.source_path
        if no_cache:
            return self.get_default_source()

        cache_path = self.get_cached_path_for_level(level)
        if cache_path.exists() and cache_path.stat().st_size > 0:
            age_warning = check_cache_age(cache_path)
            if age_warning:
                from geoparquet_io.core.logging_config import warn

                warn(age_warning)
            return str(cache_path)

        self._download_per_level_caches()
        return str(cache_path)

    def _download_per_level_caches(self) -> None:
        """Download separate country and region cache files.

        The full Overture divisions dataset is ~4.5GB with 1M+ rows. We split
        into per-level, land-only files (see :meth:`_build_level_cache_query`)
        so spatial joins run against non-overlapping polygons, preventing row
        multiplication. Geometries are simplified to ~11m tolerance to keep
        files small.
        """
        from geoparquet_io.core.duckdb_utils import s3_config_scope
        from geoparquet_io.core.logging_config import info

        cache_dir = get_cache_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)

        source = self.get_default_source()
        info(f"Downloading {self.get_dataset_name()} to local cache...")
        info("This is a one-time download. Future runs will use the cached version.")

        with s3_config_scope(self.get_s3_config()):
            # Spill onto the cache volume so the remote scan + simplification of
            # the ~4.5GB dataset bounds peak memory rather than OOM-ing (todo
            # 013), into a leaf of its own so concurrent runs cannot collide.
            con = get_duckdb_connection(
                load_spatial=True, load_httpfs=True, temp_directory=spill_directory(cache_dir)
            )
            version = self.get_version()
            try:
                for level in self.get_available_levels():
                    cache_path = self.get_cached_path_for_level(level)
                    # Drop every superseded cache for this level/version so
                    # renamed keys don't strand ~40MB files in the cache dir.
                    _remove_stale_level_caches(cache_dir, version, level, cache_path)

                    if cache_path.exists() and cache_path.stat().st_size > 0:
                        continue

                    query = self._build_level_cache_query(level, source)
                    con.execute(
                        f"COPY ({query}) TO {sql_path(cache_path)} "
                        "(FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                    info(f"Cached {level} dataset at: {cache_path}")
            finally:
                con.close()

    def _download_to_cache(self, cache_path: Path) -> Path:
        """Download per-level caches (called by base class fallback)."""
        self._download_per_level_caches()
        return cache_path

    def get_s3_config(self) -> dict:
        return {"s3_region": "us-west-2"}

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
            InvalidParameterError: If dataset_name is invalid
        """
        if dataset_name not in cls._datasets:
            raise InvalidParameterError(
                "dataset_name",
                f"Unknown admin dataset: {dataset_name}. "
                f"Available: {', '.join(cls.get_available_datasets())}",
            )

        dataset_class = cls._datasets[dataset_name]
        return dataset_class(source_path=source_path, verbose=verbose)


def default_admin_levels(dataset_name: str, source_path: str | None = None) -> list[str]:
    """Return the levels to add when the caller did not name any.

    Single source of truth for "no levels specified" across both front doors:
    ``gpio add admin-divisions`` with no ``--levels`` and
    ``ops.add_admin_divisions`` / ``Table.add_admin_divisions`` with
    ``levels=None`` both add every level the dataset provides, so the two
    produce the same output schema.

    Args:
        dataset_name: Name of the dataset ("current", "gaul", "overture")
        source_path: Optional custom path/URL to the dataset

    Returns:
        Every level the dataset exposes, e.g. ``["continent", "country",
        "department"]`` for GAUL.

    Raises:
        InvalidParameterError: If dataset_name is invalid
    """
    dataset = AdminDatasetFactory.create(dataset_name, source_path, verbose=False)
    return dataset.get_available_levels()
