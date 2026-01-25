"""
Fluent Table API for GeoParquet transformations.

Provides a chainable API for common GeoParquet operations:

    gpio.read('input.parquet') \\
        .add_bbox() \\
        .add_quadkey(resolution=12) \\
        .sort_hilbert() \\
        .write('output.parquet')
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.common import write_geoparquet_table

if TYPE_CHECKING:
    from pathlib import Path

    from geoparquet_io.api.check import CheckResult


def _safe_unlink(path: Path, attempts: int = 3) -> None:
    """
    Safely unlink a file with retry for Windows file handle release.

    Args:
        path: Path to the file to delete
        attempts: Number of retry attempts (default: 3)
    """
    import time

    for attempt in range(attempts):
        try:
            path.unlink(missing_ok=True)
            break
        except OSError:
            time.sleep(0.1 * (attempt + 1))


def _run_partition_with_temp_file(
    table: pa.Table,
    geometry_column: str | None,
    core_fn,
    output_dir: str | Path,
    *,
    temp_prefix: str = "gpio_partition",
    core_kwargs: dict,
    compression: str = "ZSTD",
    compression_level: int = 15,
    collect_stats: bool = False,
) -> dict:
    """
    Run a partition operation using a temporary file.

    Handles temp file creation, writing, partition function call, and cleanup.

    Args:
        table: The PyArrow table to partition
        geometry_column: Name of geometry column
        core_fn: The partition core function to call
        output_dir: Output directory path
        temp_prefix: Prefix for the temp file name
        core_kwargs: Keyword arguments for the core function
        compression: Compression codec
        compression_level: Compression level
        collect_stats: If True, return file count stats instead of core_fn result

    Returns:
        dict with partition results or file stats if collect_stats=True
    """
    import tempfile
    import uuid
    from pathlib import Path as PathLib

    temp_input = PathLib(tempfile.gettempdir()) / f"{temp_prefix}_{uuid.uuid4()}.parquet"

    try:
        write_geoparquet_table(
            table,
            str(temp_input),
            geometry_column=geometry_column,
            compression=compression,
            compression_level=compression_level,
        )

        result = core_fn(
            input_parquet=str(temp_input),
            output_folder=str(output_dir),
            **core_kwargs,
            verbose=False,
        )

        if collect_stats:
            output_path = PathLib(output_dir)
            parquet_files = list(output_path.rglob("*.parquet"))
            return {
                "output_dir": str(output_path),
                "file_count": len(parquet_files),
                "hive": core_kwargs.get("hive", True),
            }

        return result if result else {"status": "completed"}
    finally:
        _safe_unlink(temp_input)


def _calculate_bounds_from_table(
    table: pa.Table,
    geometry_column: str | None,
) -> tuple[float, float, float, float] | None:
    """
    Calculate bounding box from an in-memory PyArrow Table.

    Uses DuckDB to compute the bbox from geometry column.

    Args:
        table: PyArrow Table
        geometry_column: Name of geometry column

    Returns:
        Tuple of (xmin, ymin, xmax, ymax) or None if empty/error
    """
    if geometry_column is None or geometry_column not in table.column_names:
        return None

    if table.num_rows == 0:
        return None

    import duckdb

    con = None
    try:
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.register("input_table", table)

        # Use ST_Extent to get the bounding box of all geometries
        query = f"""
            SELECT
                ST_XMin(ST_Extent_Agg(ST_GeomFromWKB("{geometry_column}"))),
                ST_YMin(ST_Extent_Agg(ST_GeomFromWKB("{geometry_column}"))),
                ST_XMax(ST_Extent_Agg(ST_GeomFromWKB("{geometry_column}"))),
                ST_YMax(ST_Extent_Agg(ST_GeomFromWKB("{geometry_column}")))
            FROM input_table
            WHERE "{geometry_column}" IS NOT NULL
        """
        result = con.execute(query).fetchone()

        if result and all(v is not None for v in result):
            return (result[0], result[1], result[2], result[3])
        return None

    except Exception:
        return None
    finally:
        if con is not None:
            con.close()


def read(path: str | Path, **kwargs) -> Table:
    """
    Read a GeoParquet file into a Table.

    This is the main entry point for the fluent API.

    Args:
        path: Path to GeoParquet file
        **kwargs: Additional arguments passed to pyarrow.parquet.read_table

    Returns:
        Table: Fluent Table wrapper for chaining operations

    Example:
        >>> import geoparquet_io as gpio
        >>> table = gpio.read('data.parquet')
        >>> table.add_bbox().write('output.parquet')
    """
    arrow_table = pq.read_table(str(path), **kwargs)
    return Table(arrow_table)


def read_partition(
    path: str | Path,
    *,
    hive_input: bool | None = None,
    allow_schema_diff: bool = False,
) -> Table:
    """
    Read a Hive-partitioned GeoParquet dataset.

    Supports reading from:
    - Hive-partitioned directories (e.g., `output/quadkey=0123/data.parquet`)
    - Glob patterns (e.g., `data/quadkey=*/*.parquet`)
    - Flat directories containing multiple parquet files

    Args:
        path: Path to partition root directory or glob pattern
        hive_input: Explicitly enable/disable hive partitioning. None = auto-detect.
        allow_schema_diff: If True, allow merging schemas across files with
                           different columns (uses DuckDB union_by_name)

    Returns:
        Table containing all partition data combined

    Example:
        >>> import geoparquet_io as gpio
        >>> table = gpio.read_partition('partitioned_output/')
        >>> table = gpio.read_partition('data/quadkey=*/*.parquet')
    """
    from geoparquet_io.core.common import get_duckdb_connection, needs_httpfs
    from geoparquet_io.core.partition_reader import build_read_parquet_expr
    from geoparquet_io.core.streaming import find_geometry_column_from_table

    path_str = str(path)
    expr = build_read_parquet_expr(
        path_str,
        allow_schema_diff=allow_schema_diff,
        hive_input=hive_input,
    )

    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(path_str))
    try:
        arrow_table = con.execute(f"SELECT * FROM {expr}").fetch_arrow_table()
    finally:
        con.close()

    # Detect geometry column from the combined table
    geometry_column = find_geometry_column_from_table(arrow_table)

    return Table(arrow_table, geometry_column=geometry_column)


def convert(
    path: str | Path,
    *,
    geometry_column: str = "geometry",
    wkt_column: str | None = None,
    lat_column: str | None = None,
    lon_column: str | None = None,
    delimiter: str | None = None,
    skip_invalid: bool = False,
    profile: str | None = None,
) -> Table:
    """
    Convert a geospatial file to a Table.

    Supports: GeoPackage, GeoJSON, Shapefile, FlatGeobuf, CSV/TSV (with WKT or lat/lon).
    Unlike the CLI convert command, this does NOT apply Hilbert sorting by default.
    Chain .sort_hilbert() explicitly if you want spatial ordering.

    Args:
        path: Path to input file (local or S3 URL)
        geometry_column: Name for geometry column in output (default: 'geometry')
        wkt_column: For CSV: column containing WKT geometry
        lat_column: For CSV: latitude column
        lon_column: For CSV: longitude column
        delimiter: For CSV: field delimiter (auto-detected if not specified)
        skip_invalid: Skip invalid geometries instead of erroring
        profile: AWS profile name for S3 authentication (default: None)

    Returns:
        Table for chaining operations

    Example:
        >>> import geoparquet_io as gpio
        >>> gpio.convert('data.gpkg').sort_hilbert().write('out.parquet')
        >>> gpio.convert('data.csv', lat_column='lat', lon_column='lon').write('out.parquet')
        >>> gpio.convert('s3://bucket/data.gpkg', profile='my-aws').write('out.parquet')
    """
    from geoparquet_io.core.convert import read_spatial_to_arrow

    arrow_table, detected_crs, geom_col = read_spatial_to_arrow(
        str(path),
        verbose=False,
        wkt_column=wkt_column,
        lat_column=lat_column,
        lon_column=lon_column,
        delimiter=delimiter,
        skip_invalid=skip_invalid,
        profile=profile,
        geometry_column=geometry_column,
    )

    return Table(arrow_table, geometry_column=geom_col)


def extract_arcgis(
    service_url: str,
    *,
    token: str | None = None,
    token_file: str | None = None,
    username: str | None = None,
    password: str | None = None,
    portal_url: str | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    limit: int | None = None,
) -> Table:
    """
    Extract features from an ArcGIS Feature Service to a Table.

    Downloads features from an ArcGIS REST Feature Service URL
    and creates a Table for further processing.

    Server-side filtering is applied for efficiency:
    - where: SQL WHERE clause pushed to server
    - bbox: Spatial filter pushed to server
    - include_cols: Field selection pushed to server
    - limit: Row limit applied during pagination

    Unlike the CLI extract command, this does NOT apply Hilbert sorting by default.
    Chain .sort_hilbert() explicitly if you want spatial ordering.

    Args:
        service_url: ArcGIS Feature Service URL with layer ID
            (e.g., https://services.arcgis.com/.../FeatureServer/0)
        token: Pre-generated authentication token
        token_file: Path to file containing token
        username: ArcGIS Online/Enterprise username
        password: ArcGIS Online/Enterprise password
        portal_url: Enterprise portal URL for token generation
        where: SQL WHERE clause to filter features (default: "1=1" = all)
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        include_cols: Comma-separated column names to include (server-side)
        exclude_cols: Comma-separated column names to exclude (client-side)
        limit: Maximum number of features to return

    Returns:
        Table for chaining operations

    Example:
        >>> import geoparquet_io as gpio
        >>> # Extract all features
        >>> gpio.extract_arcgis('https://services.arcgis.com/.../FeatureServer/0') \\
        ...     .sort_hilbert() \\
        ...     .write('output.parquet')
        >>>
        >>> # Extract with server-side filtering
        >>> gpio.extract_arcgis(url, bbox=(-122.5, 37.5, -122.0, 38.0), limit=1000) \\
        ...     .add_bbox() \\
        ...     .write('output.parquet')
    """
    from geoparquet_io.core.arcgis import ArcGISAuth, arcgis_to_table

    auth = None
    if any([token, token_file, username, password]):
        auth = ArcGISAuth(
            token=token,
            token_file=token_file,
            username=username,
            password=password,
            portal_url=portal_url,
        )

    arrow_table = arcgis_to_table(
        service_url=service_url,
        auth=auth,
        where=where,
        bbox=bbox,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        limit=limit,
        verbose=False,
    )

    return Table(arrow_table, geometry_column="geometry")


class Table:
    """
    Fluent wrapper around PyArrow Table for GeoParquet operations.

    Provides chainable methods for common transformations:
    - add_bbox(): Add bounding box column
    - add_quadkey(): Add quadkey column
    - sort_hilbert(): Reorder by Hilbert curve
    - extract(): Filter columns and rows

    All methods return a new Table, preserving immutability.

    Example:
        >>> table = gpio.read('input.parquet')
        >>> result = table.add_bbox().sort_hilbert()
        >>> result.write('output.parquet')
    """

    def __init__(self, table: pa.Table, geometry_column: str | None = None):
        """
        Create a Table wrapper.

        Args:
            table: PyArrow Table containing GeoParquet data
            geometry_column: Name of geometry column (auto-detected if None)
        """
        self._table = table
        self._geometry_column = geometry_column or self._detect_geometry_column()

    def _detect_geometry_column(self) -> str | None:
        """Detect geometry column from metadata or common names."""
        from geoparquet_io.core.streaming import find_geometry_column_from_table

        return find_geometry_column_from_table(self._table)

    @classmethod
    def from_bigquery(
        cls,
        table_id: str,
        *,
        project: str | None = None,
        credentials_file: str | None = None,
        where: str | None = None,
        bbox: str | None = None,
        bbox_mode: str = "auto",
        bbox_threshold: int = 500000,
        limit: int | None = None,
        columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
    ) -> Table:
        """
        Read data from a BigQuery table.

        Uses DuckDB's BigQuery extension with the Storage Read API for
        efficient Arrow-based scanning with filter pushdown.

        BigQuery GEOGRAPHY columns are automatically converted to GeoParquet
        geometry with spherical edges (edges: "spherical" in metadata).

        Args:
            table_id: Fully qualified BigQuery table ID (project.dataset.table)
            project: GCP project ID (overrides project in table_id if set)
            credentials_file: Path to service account JSON file
            where: SQL WHERE clause for filtering (BigQuery SQL syntax)
            bbox: Bounding box for spatial filter as "minx,miny,maxx,maxy"
            bbox_mode: Filtering mode - "auto" (default), "server", or "local"
            bbox_threshold: Row count threshold for auto mode (default: 500000)
            limit: Maximum rows to extract
            columns: Columns to include (None = all)
            exclude_columns: Columns to exclude

        Returns:
            Table for chaining operations

        Raises:
            FileNotFoundError: If credentials_file doesn't exist
            RuntimeError: If BigQuery query fails

        Note:
            **Cannot read BigQuery views or external tables** - this is a
            limitation of the BigQuery Storage Read API.

        Example:
            >>> import geoparquet_io as gpio
            >>> table = gpio.Table.from_bigquery('myproject.geodata.buildings')
            >>> table.write('output.parquet')

            >>> # With filtering
            >>> table = gpio.Table.from_bigquery(
            ...     'myproject.geodata.buildings',
            ...     where="area_sqm > 1000",
            ...     columns=['id', 'name', 'geography'],
            ...     limit=10000
            ... )
        """
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        # Convert columns list to comma-separated string for the core function
        include_cols = ",".join(columns) if columns else None
        exclude_cols = ",".join(exclude_columns) if exclude_columns else None

        # Get PyArrow table (don't write to file)
        arrow_table = extract_bigquery(
            table_id=table_id,
            output_parquet=None,  # Return table instead of writing
            project=project,
            credentials_file=credentials_file,
            where=where,
            bbox=bbox,
            bbox_mode=bbox_mode,
            bbox_threshold=bbox_threshold,
            limit=limit,
            include_cols=include_cols,
            exclude_cols=exclude_cols,
            verbose=False,
        )

        if arrow_table is None:
            raise RuntimeError(
                f"Failed to read from BigQuery table: {table_id}. "
                "Possible causes: (1) Authentication/credentials issue - verify your service "
                "account has BigQuery Data Viewer role or check gcloud auth; "
                "(2) Table not found - verify the table_id format (project.dataset.table); "
                "(3) Views or external tables are not supported by the BigQuery Storage Read API."
            )

        return cls(arrow_table)

    def _format_crs_display(self, crs: dict | str | None) -> str:
        """Format CRS for human-readable display."""
        if crs is None:
            return "OGC:CRS84 (default)"
        if isinstance(crs, dict) and "id" in crs:
            crs_id = crs["id"]
            if isinstance(crs_id, dict):
                return f"{crs_id.get('authority', 'EPSG')}:{crs_id.get('code', '?')}"
            return str(crs_id)
        return str(crs)

    @property
    def table(self) -> pa.Table:
        """Get the underlying PyArrow Table."""
        return self._table

    @property
    def geometry_column(self) -> str | None:
        """Get the geometry column name."""
        return self._geometry_column

    @property
    def num_rows(self) -> int:
        """Get number of rows in the table."""
        return self._table.num_rows

    @property
    def column_names(self) -> list[str]:
        """Get list of column names."""
        return self._table.column_names

    @property
    def crs(self) -> dict | str | None:
        """
        Get the Coordinate Reference System (CRS) of the geometry column.

        Returns CRS as a PROJJSON dict (full definition) or string identifier.
        Returns None if no CRS is specified, which means OGC:CRS84 by default
        per the GeoParquet specification.

        Returns:
            PROJJSON dict, string identifier, or None (OGC:CRS84 default)

        Example:
            >>> table = gpio.read('data.parquet')
            >>> print(table.crs)  # e.g., {'id': {'authority': 'EPSG', 'code': 4326}, ...}
        """
        from geoparquet_io.core.streaming import extract_crs_from_table

        return extract_crs_from_table(self._table, self._geometry_column)

    @property
    def bounds(self) -> tuple[float, float, float, float] | None:
        """
        Get the bounding box of all geometries in the table.

        Returns a tuple of (xmin, ymin, xmax, ymax) representing the
        total extent of all geometries.

        Returns:
            Tuple of (xmin, ymin, xmax, ymax) or None if empty/error

        Example:
            >>> table = gpio.read('data.parquet')
            >>> print(table.bounds)  # e.g., (-122.5, 37.5, -122.0, 38.0)
        """
        return _calculate_bounds_from_table(self._table, self._geometry_column)

    @property
    def schema(self) -> pa.Schema:
        """
        Get the PyArrow schema of the table.

        Returns:
            PyArrow Schema object

        Example:
            >>> table = gpio.read('data.parquet')
            >>> for field in table.schema:
            ...     print(f"{field.name}: {field.type}")
        """
        return self._table.schema

    @property
    def geoparquet_version(self) -> str | None:
        """
        Get the GeoParquet version from metadata.

        Returns the version string (e.g., '1.1.0', '2.0.0') or None
        if no GeoParquet metadata is present.

        Returns:
            Version string or None

        Example:
            >>> table = gpio.read('data.parquet')
            >>> print(table.geoparquet_version)  # e.g., '1.1.0'
        """
        from geoparquet_io.core.streaming import extract_version_from_metadata

        return extract_version_from_metadata(self._table.schema.metadata)

    def info(self, verbose: bool = True) -> dict | None:
        """
        Print or return summary information about the Table.

        When verbose=True, prints a formatted summary to stdout.
        When verbose=False, returns a dictionary with all metadata.

        Args:
            verbose: If True, print to stdout and return None.
                     If False, return dict with metadata.

        Returns:
            dict with metadata if verbose=False, else None

        Example:
            >>> table = gpio.read('data.parquet')
            >>> table.info()
            Table: 766 rows, 6 columns
            Geometry: geometry
            CRS: OGC:CRS84 (default)
            Bounds: [-122.500000, 37.500000, -122.000000, 38.000000]
            GeoParquet: 1.1

            >>> info_dict = table.info(verbose=False)
            >>> print(info_dict['rows'])
            766
        """
        info_dict = {
            "rows": self.num_rows,
            "columns": len(self.column_names),
            "column_names": list(self.column_names),
            "geometry_column": self._geometry_column,
            "crs": self.crs,
            "bounds": self.bounds,
            "geoparquet_version": self.geoparquet_version,
        }

        if not verbose:
            return info_dict

        # Print formatted summary
        print(f"Table: {self.num_rows:,} rows, {len(self.column_names)} columns")
        print(f"Geometry: {self._geometry_column}")
        print(f"CRS: {self._format_crs_display(self.crs)}")

        # Format bounds
        bounds = self.bounds
        if bounds:
            print(f"Bounds: [{bounds[0]:.6f}, {bounds[1]:.6f}, {bounds[2]:.6f}, {bounds[3]:.6f}]")

        # GeoParquet version
        version = self.geoparquet_version
        if version:
            print(f"GeoParquet: {version}")

        return None

    def to_arrow(self) -> pa.Table:
        """
        Convert to PyArrow Table.

        Returns:
            The underlying PyArrow Table
        """
        return self._table

    def write(
        self,
        path: str | Path,
        format: str | None = None,
        compression: str = "ZSTD",
        compression_level: int | None = None,
        row_group_size_mb: float | None = None,
        row_group_rows: int | None = None,
        geoparquet_version: str | None = None,
        write_strategy: str = "auto",
        profile: str | None = None,
        # Format-specific options
        overwrite: bool = False,
        layer_name: str = "features",
        include_wkt: bool = True,
        include_bbox: bool = True,
        encoding: str = "UTF-8",
        precision: int = 7,
        write_bbox: bool = False,
        id_field: str | None = None,
        pretty: bool = False,
        keep_crs: bool = False,
    ) -> Path:
        """
        Write the table to any format (GeoParquet, GeoPackage, FlatGeobuf, CSV, Shapefile, GeoJSON).

        Supports both local paths and cloud URLs (s3://, gs://, etc.).
        Format is auto-detected from file extension unless explicitly specified.

        Args:
            path: Output file path (local or cloud URL)
            format: Override format detection ('parquet', 'geopackage', 'flatgeobuf',
                    'csv', 'shapefile', 'geojson'). Default: auto-detect from extension
            compression: Compression type for GeoParquet (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
            compression_level: Compression level for GeoParquet
            row_group_size_mb: Target row group size in MB for GeoParquet
            row_group_rows: Exact rows per row group for GeoParquet
            geoparquet_version: GeoParquet version (1.0, 1.1, 2.0, or None to preserve)
            write_strategy: Write strategy for GeoParquet ('auto', 'in-memory', 'streaming',
                           'duckdb-kv', 'disk-rewrite'). Default: 'auto'
            profile: AWS profile for S3 operations
            overwrite: Overwrite existing file (GeoPackage, Shapefile)
            layer_name: Layer name for GeoPackage (default: 'features')
            include_wkt: Include WKT geometry column for CSV (default: True)
            include_bbox: Include bbox column for CSV (default: True)
            encoding: Character encoding for Shapefile (default: 'UTF-8')
            precision: Coordinate precision for GeoJSON (default: 7)
            write_bbox: Include bbox property for GeoJSON features (default: False)
            id_field: Field to use as feature 'id' for GeoJSON
            pretty: Pretty-print GeoJSON output (default: False)
            keep_crs: Keep original CRS for GeoJSON instead of WGS84 (default: False)

        Returns:
            Path to written file (local temp path if uploaded to cloud)

        Examples:
            >>> table.write('output.parquet')              # GeoParquet (auto-detect)
            >>> table.write('output.gpkg')                 # GeoPackage (auto-detect)
            >>> table.write('output.geojson')              # GeoJSON (auto-detect)
            >>> table.write('s3://bucket/output.fgb')      # FlatGeobuf to S3
            >>> table.write('output.dat', format='csv')    # Explicit format
        """

        # Detect format from extension if not explicitly provided
        # Normalize to lowercase for case-insensitive comparison
        detected_format = format.lower() if format else self._detect_format(path)

        # Handle GeoParquet format
        if detected_format == "parquet":
            return self._write_geoparquet(
                path,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                geoparquet_version=geoparquet_version,
                write_strategy=write_strategy,
                profile=profile,
            )

        # Handle other formats
        return self._write_format(
            path,
            detected_format,
            profile=profile,
            overwrite=overwrite,
            layer_name=layer_name,
            include_wkt=include_wkt,
            include_bbox=include_bbox,
            encoding=encoding,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
            pretty=pretty,
            keep_crs=keep_crs,
        )

    @staticmethod
    def _detect_format(path: str | Path) -> str:
        """Detect output format from file extension."""
        from pathlib import Path as PathLib

        ext = PathLib(path).suffix.lower()
        EXTENSION_MAP = {
            ".parquet": "parquet",
            ".gpkg": "geopackage",
            ".fgb": "flatgeobuf",
            ".csv": "csv",
            ".shp": "shapefile",
            ".geojson": "geojson",
            ".json": "geojson",
        }
        return EXTENSION_MAP.get(ext, "parquet")  # Default to parquet

    def _write_geoparquet(
        self,
        path: str | Path,
        compression: str,
        compression_level: int | None,
        row_group_size_mb: float | None,
        row_group_rows: int | None,
        geoparquet_version: str | None,
        write_strategy: str,
        profile: str | None,
    ) -> Path:
        """Write table to GeoParquet format (supports local and cloud)."""
        from pathlib import Path as PathLib

        from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory

        output_path = PathLib(path)

        # Get the appropriate write strategy
        strategy_enum = WriteStrategy(write_strategy)
        strategy = WriteStrategyFactory.get_strategy(strategy_enum)

        strategy.write_from_table(
            table=self._table,
            output_path=str(output_path),
            geometry_column=self._geometry_column,
            geoparquet_version=geoparquet_version or "1.1",
            compression=compression,
            compression_level=compression_level or 15,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            verbose=False,
        )

        return output_path

    def _write_format(
        self,
        path: str | Path,
        format: str,
        profile: str | None,
        **format_options,
    ) -> Path:
        """Write table to non-parquet format (handles local and cloud)."""
        import tempfile
        import uuid
        from pathlib import Path as PathLib

        from geoparquet_io.core.common import is_remote_url, setup_aws_profile_if_needed
        from geoparquet_io.core.upload import upload

        # Check if destination is remote
        path_str = str(path)
        is_remote = is_remote_url(path_str)

        # Determine output path (temp file for remote, direct for local)
        if is_remote:
            temp_dir = PathLib(tempfile.gettempdir())
            output_path = temp_dir / f"gpio_write_{uuid.uuid4()}{PathLib(path).suffix}"
        else:
            output_path = PathLib(path)

        # Initialize temp files before try block for cleanup in finally
        temp_parquet = None
        zip_path = None

        try:
            # Write table to temp parquet first
            temp_parquet = self._table_to_temp_parquet()

            # Convert to target format
            if format == "geopackage":
                from geoparquet_io.core.format_writers import write_geopackage

                write_geopackage(
                    str(temp_parquet),
                    str(output_path),
                    overwrite=format_options.get("overwrite", False),
                    layer_name=format_options.get("layer_name", "features"),
                    verbose=False,
                    profile=profile,
                )
            elif format == "flatgeobuf":
                from geoparquet_io.core.format_writers import write_flatgeobuf

                write_flatgeobuf(
                    str(temp_parquet),
                    str(output_path),
                    verbose=False,
                    profile=profile,
                )
            elif format == "csv":
                from geoparquet_io.core.format_writers import write_csv

                write_csv(
                    str(temp_parquet),
                    str(output_path),
                    include_wkt=format_options.get("include_wkt", True),
                    include_bbox=format_options.get("include_bbox", True),
                    verbose=False,
                    profile=profile,
                )
            elif format == "shapefile":
                from geoparquet_io.core.format_writers import write_shapefile

                write_shapefile(
                    str(temp_parquet),
                    str(output_path),
                    overwrite=format_options.get("overwrite", False),
                    encoding=format_options.get("encoding", "UTF-8"),
                    verbose=False,
                    profile=profile,
                )
            elif format == "geojson":
                from geoparquet_io.core.format_writers import write_geojson

                write_geojson(
                    str(temp_parquet),
                    str(output_path),
                    precision=format_options.get("precision", 7),
                    write_bbox=format_options.get("write_bbox", False),
                    id_field=format_options.get("id_field"),
                    pretty=format_options.get("pretty", False),
                    keep_crs=format_options.get("keep_crs", False),
                    verbose=False,
                    profile=profile,
                )
            else:
                raise ValueError(f"Unsupported format: {format}")

            # Upload to remote if needed
            if is_remote:
                setup_aws_profile_if_needed(profile, path_str)

                # Special handling for shapefiles: zip all sidecars into .shp.zip
                if format == "shapefile":
                    from geoparquet_io.core.common import create_shapefile_zip

                    # Create zip archive with all sidecar files
                    zip_path = create_shapefile_zip(output_path, verbose=False)

                    # Upload the zip file with .shp.zip extension
                    remote_zip_path = path_str.replace(".shp", ".shp.zip")
                    upload(
                        source=zip_path,
                        destination=remote_zip_path,
                        profile=profile,
                    )

                    # Return remote zip path (cleanup happens in finally)
                    return PathLib(remote_zip_path)
                else:
                    # Normal single-file upload
                    upload(
                        source=output_path,
                        destination=path_str,
                        profile=profile,
                    )
                    # Return remote path, not local temp path
                    return PathLib(path_str)

            return output_path

        finally:
            # Clean up temp parquet
            if temp_parquet:
                temp_parquet.unlink(missing_ok=True)
            # Clean up zip file if created
            if zip_path:
                zip_path.unlink(missing_ok=True)
            # Clean up temp output if remote
            if is_remote and output_path.exists():
                output_path.unlink(missing_ok=True)
            # Clean up shapefile sidecars if remote
            if is_remote and format == "shapefile":
                # Remove all sidecar files (.shx, .dbf, .prj, etc.)
                stem = output_path.stem
                parent = output_path.parent
                for sidecar in parent.glob(f"{stem}.*"):
                    sidecar.unlink(missing_ok=True)

    def _table_to_temp_parquet(self) -> Path:
        """Write table to temporary parquet file for format conversion.

        Uses write_geoparquet_table to preserve GeoParquet metadata (CRS, geometry column).
        This is critical for format conversions that need metadata (e.g., GeoJSON reprojection).
        """
        import tempfile
        import uuid
        from pathlib import Path as PathLib

        from geoparquet_io.core.common import write_geoparquet_table

        temp_dir = PathLib(tempfile.gettempdir())
        temp_path = temp_dir / f"gpio_table_{uuid.uuid4()}.parquet"

        # Use write_geoparquet_table to preserve metadata
        write_geoparquet_table(
            self._table,
            output_file=str(temp_path),
            geometry_column=self._geometry_column,
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            geoparquet_version=None,  # Use existing version from table
            verbose=False,
            profile=None,
        )

        return temp_path

    def add_bbox(self, column_name: str = "bbox") -> Table:
        """
        Add a bounding box struct column.

        Args:
            column_name: Name for the bbox column (default: 'bbox')

        Returns:
            New Table with bbox column added
        """
        from geoparquet_io.core.add_bbox_column import add_bbox_table

        result = add_bbox_table(
            self._table,
            bbox_column_name=column_name,
            geometry_column=self._geometry_column,
        )
        return Table(result, self._geometry_column)

    def add_quadkey(
        self,
        column_name: str = "quadkey",
        resolution: int = 13,
        use_centroid: bool = False,
    ) -> Table:
        """
        Add a quadkey column based on geometry location.

        Args:
            column_name: Name for the quadkey column (default: 'quadkey')
            resolution: Quadkey zoom level 0-23 (default: 13)
            use_centroid: Force centroid even if bbox exists

        Returns:
            New Table with quadkey column added
        """
        from geoparquet_io.core.add_quadkey_column import add_quadkey_table

        result = add_quadkey_table(
            self._table,
            quadkey_column_name=column_name,
            resolution=resolution,
            use_centroid=use_centroid,
            geometry_column=self._geometry_column,
        )
        return Table(result, self._geometry_column)

    def sort_hilbert(self) -> Table:
        """
        Reorder rows using Hilbert curve ordering.

        Returns:
            New Table with rows reordered by Hilbert curve
        """
        from geoparquet_io.core.hilbert_order import hilbert_order_table

        result = hilbert_order_table(
            self._table,
            geometry_column=self._geometry_column,
        )
        return Table(result, self._geometry_column)

    def extract(
        self,
        columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        where: str | None = None,
        limit: int | None = None,
    ) -> Table:
        """
        Extract columns and rows with optional filtering.

        Args:
            columns: Columns to include (None = all)
            exclude_columns: Columns to exclude
            bbox: Bounding box filter (xmin, ymin, xmax, ymax)
            where: SQL WHERE clause
            limit: Maximum rows to return

        Returns:
            New filtered Table
        """
        from geoparquet_io.core.extract import extract_table

        result = extract_table(
            self._table,
            columns=columns,
            exclude_columns=exclude_columns,
            bbox=bbox,
            where=where,
            limit=limit,
            geometry_column=self._geometry_column,
        )
        return Table(result, self._geometry_column)

    def add_h3(
        self,
        column_name: str = "h3_cell",
        resolution: int = 9,
    ) -> Table:
        """
        Add an H3 cell column based on geometry location.

        Args:
            column_name: Name for the H3 column (default: 'h3_cell')
            resolution: H3 resolution level 0-15 (default: 9)

        Returns:
            New Table with H3 column added
        """
        from geoparquet_io.core.add_h3_column import add_h3_table

        result = add_h3_table(
            self._table,
            h3_column_name=column_name,
            resolution=resolution,
        )
        return Table(result, self._geometry_column)

    def add_kdtree(
        self,
        column_name: str = "kdtree_cell",
        iterations: int = 9,
        sample_size: int = 100000,
    ) -> Table:
        """
        Add a KD-tree cell column based on geometry location.

        Args:
            column_name: Name for the KD-tree column (default: 'kdtree_cell')
            iterations: Number of recursive splits 1-20 (default: 9)
            sample_size: Number of points to sample for boundaries (default: 100000)

        Returns:
            New Table with KD-tree column added
        """
        from geoparquet_io.core.add_kdtree_column import add_kdtree_table

        result = add_kdtree_table(
            self._table,
            kdtree_column_name=column_name,
            iterations=iterations,
            sample_size=sample_size,
        )
        return Table(result, self._geometry_column)

    def sort_column(
        self,
        column_name: str,
        descending: bool = False,
    ) -> Table:
        """
        Sort rows by the specified column.

        Args:
            column_name: Column name to sort by
            descending: Sort in descending order (default: False)

        Returns:
            New Table with rows sorted by the column
        """
        from geoparquet_io.core.sort_by_column import sort_by_column_table

        result = sort_by_column_table(
            self._table,
            columns=column_name,
            descending=descending,
        )
        return Table(result, self._geometry_column)

    def sort_quadkey(
        self,
        column_name: str = "quadkey",
        resolution: int = 13,
        use_centroid: bool = False,
        remove_column: bool = False,
    ) -> Table:
        """
        Sort rows by quadkey column.

        If the quadkey column doesn't exist, it will be auto-added.

        Args:
            column_name: Name of the quadkey column (default: 'quadkey')
            resolution: Quadkey resolution for auto-adding (0-23, default: 13)
            use_centroid: Use geometry centroid when auto-adding
            remove_column: Remove the quadkey column after sorting

        Returns:
            New Table with rows sorted by quadkey
        """
        from geoparquet_io.core.sort_quadkey import sort_by_quadkey_table

        result = sort_by_quadkey_table(
            self._table,
            quadkey_column_name=column_name,
            resolution=resolution,
            use_centroid=use_centroid,
            remove_quadkey_column=remove_column,
        )
        return Table(result, self._geometry_column)

    def reproject(
        self,
        target_crs: str = "EPSG:4326",
        source_crs: str | None = None,
    ) -> Table:
        """
        Reproject geometry to a different coordinate reference system.

        Args:
            target_crs: Target CRS (default: EPSG:4326)
            source_crs: Source CRS. If None, detected from metadata.

        Returns:
            New Table with reprojected geometry
        """
        from geoparquet_io.core.reproject import reproject_table

        result = reproject_table(
            self._table,
            target_crs=target_crs,
            source_crs=source_crs,
            geometry_column=self._geometry_column,
        )
        return Table(result, self._geometry_column)

    def partition_by_quadkey(
        self,
        output_dir: str | Path,
        *,
        resolution: int = 13,
        partition_resolution: int = 6,
        compression: str = "ZSTD",
        hive: bool = True,
        overwrite: bool = False,
    ) -> dict:
        """
        Partition the table into Hive-partitioned directory by quadkey.

        Args:
            output_dir: Output directory path
            resolution: Quadkey resolution for sorting (0-23, default: 13)
            partition_resolution: Resolution for partition boundaries (default: 6)
            compression: Compression codec (default: ZSTD)
            hive: Use Hive-style partitioning (default: True)
            overwrite: Overwrite existing output directory

        Returns:
            dict with partition statistics (file_count, etc.)

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_quadkey('output/', resolution=12)
            >>> print(f"Created {stats['file_count']} files")
        """
        from geoparquet_io.core.partition_by_quadkey import partition_by_quadkey

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_quadkey,
            output_dir,
            temp_prefix="gpio_part_qk",
            core_kwargs={
                "resolution": resolution,
                "partition_resolution": partition_resolution,
                "hive": hive,
                "overwrite": overwrite,
            },
            compression=compression,
            collect_stats=True,
        )

    def partition_by_h3(
        self,
        output_dir: str | Path,
        *,
        resolution: int = 9,
        compression: str = "ZSTD",
        hive: bool = True,
        overwrite: bool = False,
    ) -> dict:
        """
        Partition the table into Hive-partitioned directory by H3 cell.

        Args:
            output_dir: Output directory path
            resolution: H3 resolution level 0-15 (default: 9)
            compression: Compression codec (default: ZSTD)
            hive: Use Hive-style partitioning (default: True)
            overwrite: Overwrite existing output directory

        Returns:
            dict with partition statistics (file_count, etc.)

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_h3('output/', resolution=6)
            >>> print(f"Created {stats['file_count']} files")
        """
        from geoparquet_io.core.partition_by_h3 import partition_by_h3

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_h3,
            output_dir,
            temp_prefix="gpio_part_h3",
            core_kwargs={
                "resolution": resolution,
                "hive": hive,
                "overwrite": overwrite,
            },
            compression=compression,
            collect_stats=True,
        )

    def upload(
        self,
        destination: str,
        *,
        compression: str = "ZSTD",
        compression_level: int | None = None,
        row_group_size_mb: float | None = None,
        row_group_rows: int | None = None,
        geoparquet_version: str | None = None,
        profile: str | None = None,
        s3_endpoint: str | None = None,
        s3_region: str | None = None,
        s3_use_ssl: bool = True,
        chunk_concurrency: int = 12,
    ) -> None:
        """
        Write and upload the table to cloud object storage.

        Supports S3, S3-compatible (MinIO, Rook/Ceph, source.coop), GCS, and Azure.
        Writes the table to a temporary local file, then uploads it to the destination.

        Args:
            destination: Object store URL (e.g., s3://bucket/path/data.parquet)
            compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
            compression_level: Compression level
            row_group_size_mb: Target row group size in MB
            row_group_rows: Exact rows per row group
            geoparquet_version: GeoParquet version (1.0, 1.1, 2.0, or None to preserve)
            profile: AWS profile name for S3
            s3_endpoint: Custom S3-compatible endpoint (e.g., "minio.example.com:9000")
            s3_region: S3 region (default: us-east-1 when using custom endpoint)
            s3_use_ssl: Whether to use HTTPS for S3 endpoint (default: True)
            chunk_concurrency: Max concurrent chunks per file upload (default: 12)

        Example:
            >>> gpio.read('data.parquet').sort_hilbert().upload(
            ...     's3://bucket/data.parquet',
            ...     s3_endpoint='minio.example.com:9000',
            ...     s3_use_ssl=False,
            ... )
        """
        import tempfile
        import time
        import uuid
        from pathlib import Path

        from geoparquet_io.core.common import setup_aws_profile_if_needed
        from geoparquet_io.core.upload import upload as do_upload

        setup_aws_profile_if_needed(profile, destination)

        # Write to temp file with uuid to avoid Windows file locking issues
        temp_path = Path(tempfile.gettempdir()) / f"gpio_upload_{uuid.uuid4()}.parquet"

        try:
            self.write(
                temp_path,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                geoparquet_version=geoparquet_version,
            )

            do_upload(
                source=temp_path,
                destination=destination,
                profile=profile,
                s3_endpoint=s3_endpoint,
                s3_region=s3_region,
                s3_use_ssl=s3_use_ssl,
                chunk_concurrency=chunk_concurrency,
            )
        finally:
            # Retry cleanup with incremental backoff for Windows file handle release
            for attempt in range(3):
                try:
                    temp_path.unlink(missing_ok=True)
                    break
                except OSError:
                    time.sleep(0.1 * (attempt + 1))

    def head(self, n: int = 10) -> Table:
        """
        Return the first n rows as a new Table.

        Args:
            n: Number of rows to return (default: 10). Must be non-negative.

        Returns:
            New Table with the first n rows

        Raises:
            ValueError: If n is negative

        Example:
            >>> table = gpio.read('data.parquet')
            >>> first_10 = table.head()
            >>> first_100 = table.head(100)
        """
        if n < 0:
            raise ValueError(f"n must be non-negative, got {n}")
        n = min(n, self.num_rows)
        return Table(self._table.slice(0, n), self._geometry_column)

    def tail(self, n: int = 10) -> Table:
        """
        Return the last n rows as a new Table.

        Args:
            n: Number of rows to return (default: 10). Must be non-negative.

        Returns:
            New Table with the last n rows

        Raises:
            ValueError: If n is negative

        Example:
            >>> table = gpio.read('data.parquet')
            >>> last_10 = table.tail()
            >>> last_100 = table.tail(100)
        """
        if n < 0:
            raise ValueError(f"n must be non-negative, got {n}")
        n = min(n, self.num_rows)
        offset = max(0, self.num_rows - n)
        return Table(self._table.slice(offset, n), self._geometry_column)

    def stats(self) -> dict:
        """
        Calculate column statistics.

        Computes statistics for each column including:
        - nulls: Number of null values
        - min: Minimum value (non-geometry columns only)
        - max: Maximum value (non-geometry columns only)
        - unique: Approximate unique count (non-geometry columns only)

        Returns:
            dict: Statistics per column name

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.stats()
            >>> print(stats['population']['min'])
            1000
            >>> print(stats['population']['max'])
            10000000
        """
        import duckdb

        con = None
        try:
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            con.register("input_table", self._table)

            stats = {}

            # Separate geometry and non-geometry columns
            geometry_cols = []
            regular_cols = []
            for field in self._table.schema:
                col_name = field.name
                if col_name == self._geometry_column:
                    geometry_cols.append(col_name)
                else:
                    regular_cols.append(col_name)

            # Build a single batched query for all non-geometry columns
            if regular_cols:
                select_parts = []
                for col_name in regular_cols:
                    escaped_col = col_name.replace('"', '""')
                    select_parts.extend(
                        [
                            f'COUNT(*) FILTER (WHERE "{escaped_col}" IS NULL)',
                            f'MIN("{escaped_col}")',
                            f'MAX("{escaped_col}")',
                            f'APPROX_COUNT_DISTINCT("{escaped_col}")',
                        ]
                    )

                query = f"SELECT {', '.join(select_parts)} FROM input_table"
                try:
                    result = con.execute(query).fetchone()
                    if result:
                        for i, col_name in enumerate(regular_cols):
                            base_idx = i * 4
                            stats[col_name] = {
                                "nulls": result[base_idx],
                                "min": result[base_idx + 1],
                                "max": result[base_idx + 2],
                                "unique": result[base_idx + 3],
                            }
                    else:
                        for col_name in regular_cols:
                            stats[col_name] = {
                                "nulls": 0,
                                "min": None,
                                "max": None,
                                "unique": None,
                            }
                except Exception:
                    # If batched query fails, fall back to per-column queries
                    import logging

                    logger = logging.getLogger(__name__)
                    logger.debug(
                        "Batched stats query failed, falling back to per-column queries",
                        exc_info=True,
                    )
                    for col_name in regular_cols:
                        escaped_col = col_name.replace('"', '""')
                        try:
                            query = f"""
                                SELECT
                                    COUNT(*) FILTER (WHERE "{escaped_col}" IS NULL),
                                    MIN("{escaped_col}"),
                                    MAX("{escaped_col}"),
                                    APPROX_COUNT_DISTINCT("{escaped_col}")
                                FROM input_table
                            """
                            result = con.execute(query).fetchone()
                            if result:
                                stats[col_name] = {
                                    "nulls": result[0],
                                    "min": result[1],
                                    "max": result[2],
                                    "unique": result[3],
                                }
                            else:
                                stats[col_name] = {
                                    "nulls": 0,
                                    "min": None,
                                    "max": None,
                                    "unique": None,
                                }
                        except Exception:
                            # If stats fail for this column, provide basic info
                            logger.debug(
                                "Stats query failed for column '%s'",
                                col_name,
                                exc_info=True,
                            )
                            stats[col_name] = {
                                "nulls": 0,
                                "min": None,
                                "max": None,
                                "unique": None,
                            }

            # Handle geometry columns separately (only null count)
            for col_name in geometry_cols:
                escaped_col = col_name.replace('"', '""')
                query = f"""
                    SELECT COUNT(*) FILTER (WHERE "{escaped_col}" IS NULL)
                    FROM input_table
                """
                result = con.execute(query).fetchone()
                stats[col_name] = {
                    "nulls": result[0] if result else 0,
                    "min": None,
                    "max": None,
                    "unique": None,
                }

            return stats

        finally:
            if con is not None:
                con.close()

    def metadata(self, include_parquet_metadata: bool = False) -> dict:
        """
        Get GeoParquet and schema metadata from the table.

        Returns metadata including:
        - geoparquet_version: GeoParquet version string
        - primary_column: Primary geometry column name
        - crs: Coordinate Reference System (PROJJSON dict or string)
        - geometry_types: List of geometry types
        - bounds: Bounding box (xmin, ymin, xmax, ymax)
        - columns: List of column info dicts
        - geo_metadata: Full 'geo' metadata dict (if present)

        Args:
            include_parquet_metadata: If True, include raw Parquet schema metadata

        Returns:
            dict: Metadata dictionary

        Example:
            >>> table = gpio.read('data.parquet')
            >>> meta = table.metadata()
            >>> print(meta['geoparquet_version'])
            1.1.0
            >>> print(meta['crs'])
            {'id': {'authority': 'EPSG', 'code': 4326}}
        """
        import json

        result = {
            "rows": self.num_rows,
            "columns_count": len(self.column_names),
            "geometry_column": self._geometry_column,
            "geoparquet_version": self.geoparquet_version,
            "crs": self.crs,
            "bounds": self.bounds,
            "columns": [
                {
                    "name": field.name,
                    "type": str(field.type),
                    "is_geometry": field.name == self._geometry_column,
                }
                for field in self._table.schema
            ],
        }

        # Extract full geo metadata if available
        schema_metadata = self._table.schema.metadata
        if schema_metadata and b"geo" in schema_metadata:
            try:
                geo_meta = json.loads(schema_metadata[b"geo"].decode("utf-8"))
                result["geo_metadata"] = geo_meta

                # Extract geometry_types from geo metadata
                columns_meta = geo_meta.get("columns", {})
                if self._geometry_column and self._geometry_column in columns_meta:
                    col_meta = columns_meta[self._geometry_column]
                    result["geometry_types"] = col_meta.get("geometry_types")
                    result["edges"] = col_meta.get("edges")
                    result["orientation"] = col_meta.get("orientation")

                    # Check for covering/bbox info
                    covering = col_meta.get("covering")
                    if covering:
                        result["covering"] = covering
            except (json.JSONDecodeError, UnicodeDecodeError):
                result["geo_metadata"] = None

        # Optionally include raw Parquet schema metadata
        if include_parquet_metadata and schema_metadata:
            result["parquet_metadata"] = {
                k.decode("utf-8") if isinstance(k, bytes) else k: (
                    v.decode("utf-8") if isinstance(v, bytes) else v
                )
                for k, v in schema_metadata.items()
                if k != b"geo"  # Already included above
            }

        return result

    def to_geojson(
        self,
        output_path: str | None = None,
        *,
        precision: int = 7,
        write_bbox: bool = False,
        id_field: str | None = None,
    ) -> str | None:
        """
        Convert the table to GeoJSON.

        If output_path is provided, writes a GeoJSON FeatureCollection file.
        If output_path is None, writes GeoJSON to stdout and returns None.

        This method delegates to convert_to_geojson from the ops module.

        Args:
            output_path: Output file path, or None to write to stdout
            precision: Coordinate decimal precision (default 7 per RFC 7946)
            write_bbox: Include bbox property for each feature
            id_field: Column to use as feature 'id' member

        Returns:
            Output path if writing to file, None if writing to stdout

        Example:
            >>> table = gpio.read('data.parquet')
            >>> table.to_geojson('output.geojson')  # Writes to file, returns path
            >>> table.to_geojson()  # Writes to stdout, returns None
        """
        from geoparquet_io.api.ops import convert_to_geojson

        return convert_to_geojson(
            self._table,
            output_path=output_path,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
        )

    def _with_temp_file(self, func, *args, **kwargs):
        """
        Execute a file-based function with a temporary file containing this table.

        Writes the table to a temp file, runs the function, and cleans up.

        Args:
            func: Function to call with temp file path as first argument
            *args: Additional positional arguments for func
            **kwargs: Additional keyword arguments for func

        Returns:
            Result from func
        """
        import tempfile
        import uuid
        from pathlib import Path

        temp_path = Path(tempfile.gettempdir()) / f"gpio_check_{uuid.uuid4()}.parquet"
        try:
            # Write table to temp file
            write_geoparquet_table(
                self._table,
                str(temp_path),
                geometry_column=self._geometry_column,
            )
            # Call the function with temp file
            return func(str(temp_path), *args, **kwargs)
        finally:
            _safe_unlink(temp_path)

    def _with_temp_io_files(self, func, **kwargs) -> pa.Table:
        """
        Execute an input->output file transformation using temp files.

        Writes the table to a temp input file, calls func which writes
        to a temp output file, reads the output, and cleans up both.

        Args:
            func: Function to call with input_parquet and output_parquet kwargs
            **kwargs: Additional keyword arguments for func

        Returns:
            PyArrow Table from the output file
        """
        import tempfile
        import uuid
        from pathlib import Path

        temp_input = Path(tempfile.gettempdir()) / f"gpio_in_{uuid.uuid4()}.parquet"
        temp_output = Path(tempfile.gettempdir()) / f"gpio_out_{uuid.uuid4()}.parquet"

        try:
            # Write table to temp input file
            write_geoparquet_table(
                self._table,
                str(temp_input),
                geometry_column=self._geometry_column,
            )

            # Call the function with input and output paths
            func(
                input_parquet=str(temp_input),
                output_parquet=str(temp_output),
                **kwargs,
            )

            # Read the output file
            return pq.read_table(str(temp_output))
        finally:
            _safe_unlink(temp_input)
            _safe_unlink(temp_output)

    def check(self) -> CheckResult:
        """
        Run all best-practice checks on the table.

        Checks include:
        - Row group optimization
        - Bbox structure and metadata
        - Compression settings

        Returns:
            CheckResult with pass/fail status and details

        Example:
            >>> table = gpio.read('data.parquet')
            >>> result = table.check()
            >>> if result.passed():
            ...     print("All checks passed!")
            >>> else:
            ...     for failure in result.failures():
            ...         print(failure)
        """
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.check_parquet_structure import check_all

        results = self._with_temp_file(check_all, verbose=False, return_results=True, quiet=True)
        return CheckResult(results, check_type="all")

    def check_spatial(self, sample_size: int = 100, limit_rows: int = 100000) -> CheckResult:
        """
        Check if data is spatially ordered.

        Compares distance between consecutive features vs random pairs.
        A ratio < 0.5 indicates good spatial clustering.

        Args:
            sample_size: Number of random pairs to sample
            limit_rows: Maximum rows to analyze

        Returns:
            CheckResult with spatial ordering analysis

        Example:
            >>> table = gpio.read('data.parquet')
            >>> result = table.check_spatial()
            >>> if result.passed():
            ...     print("Data is spatially ordered")
            >>> else:
            ...     print("Consider using sort_hilbert()")
        """
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.check_spatial_order import check_spatial_order

        results = self._with_temp_file(
            check_spatial_order,
            random_sample_size=sample_size,
            limit_rows=limit_rows,
            verbose=False,
            return_results=True,
            quiet=True,
        )
        return CheckResult(results, check_type="spatial")

    def check_compression(self) -> CheckResult:
        """
        Check compression settings on geometry column.

        Recommends ZSTD compression for best performance.

        Returns:
            CheckResult with compression analysis

        Example:
            >>> table = gpio.read('data.parquet')
            >>> result = table.check_compression()
            >>> print(result.to_dict())
        """
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.check_parquet_structure import check_compression

        results = self._with_temp_file(
            check_compression, verbose=False, return_results=True, quiet=True
        )
        return CheckResult(results, check_type="compression")

    def check_bbox(self) -> CheckResult:
        """
        Check bbox structure and metadata.

        Verifies:
        - Bbox column exists and has correct structure
        - GeoParquet covering metadata is present

        Returns:
            CheckResult with bbox analysis

        Example:
            >>> table = gpio.read('data.parquet')
            >>> result = table.check_bbox()
            >>> if not result.passed():
            ...     table = table.add_bbox()
        """
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

        results = self._with_temp_file(
            check_metadata_and_bbox, verbose=False, return_results=True, quiet=True
        )
        return CheckResult(results, check_type="bbox")

    def check_row_groups(self) -> CheckResult:
        """
        Check row group optimization.

        Checks if row group sizes are optimal for cloud-native access
        (recommended: 64-256 MB per group, 10k-200k rows per group).

        Returns:
            CheckResult with row group analysis

        Example:
            >>> table = gpio.read('data.parquet')
            >>> result = table.check_row_groups()
            >>> print(result.recommendations())
        """
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.check_parquet_structure import check_row_groups

        results = self._with_temp_file(
            check_row_groups, verbose=False, return_results=True, quiet=True
        )
        return CheckResult(results, check_type="row_groups")

    def validate(self, version: str | None = None) -> CheckResult:
        """
        Validate against GeoParquet specification.

        Checks compliance with GeoParquet 1.0, 1.1, 2.0, or auto-detects version.

        Args:
            version: Target GeoParquet version (None for auto-detect)

        Returns:
            CheckResult with validation results

        Example:
            >>> table = gpio.read('data.parquet')
            >>> result = table.validate()
            >>> if result.passed():
            ...     print(f"Valid GeoParquet {table.geoparquet_version}")
        """
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.validate import validate_geoparquet

        validation_result = self._with_temp_file(
            validate_geoparquet,
            target_version=version,
            validate_data=True,
            sample_size=1000,
            verbose=False,
        )

        # Convert ValidationResult to dict for CheckResult
        results = {
            "passed": validation_result.is_valid,
            "file_path": validation_result.file_path,
            "detected_version": validation_result.detected_version,
            "target_version": validation_result.target_version,
            "passed_count": validation_result.passed_count,
            "failed_count": validation_result.failed_count,
            "warning_count": validation_result.warning_count,
            "issues": [
                f"{c.name}: {c.message}"
                for c in validation_result.checks
                if c.status.value == "failed"
            ],
        }
        return CheckResult(results, check_type="validate")

    def add_admin_divisions(
        self,
        *,
        dataset: str = "overture",
        levels: list[str] | None = None,
    ) -> Table:
        """
        Add administrative division columns via spatial join.

        Enriches each row with country codes and/or admin subdivision codes
        based on spatial intersection with an administrative boundaries dataset.

        Args:
            dataset: Boundaries dataset ("overture", "gaul", or custom URL)
            levels: Admin levels to add (e.g., ["country", "admin1"])

        Returns:
            Table with admin division columns added

        Example:
            >>> table = gpio.read('data.parquet')
            >>> enriched = table.add_admin_divisions(levels=["country", "admin1"])
        """
        from geoparquet_io.core.add_admin_divisions_multi import add_admin_divisions_multi

        result_table = self._with_temp_io_files(
            add_admin_divisions_multi,
            dataset_name=dataset,
            levels=levels or ["country"],
            verbose=False,
        )
        return Table(result_table, self._geometry_column)

    def add_bbox_metadata(self, bbox_column: str = "bbox") -> Table:
        """
        Add bbox covering metadata to the table schema.

        Updates the GeoParquet metadata to indicate that the bbox column
        provides per-feature bounding boxes for the geometry column.
        This enables query engines to use bbox for efficient filtering.

        Note: This requires the bbox column to already exist. Use add_bbox()
        first if the table doesn't have a bbox column.

        Args:
            bbox_column: Name of the bbox column (default "bbox")

        Returns:
            Table with updated metadata

        Example:
            >>> table = gpio.read('data.parquet')
            >>> table = table.add_bbox().add_bbox_metadata()
        """
        import json

        # Guard against None geometry column
        if self._geometry_column is None:
            raise ValueError(
                "Cannot add bbox metadata: no geometry column detected. "
                "Ensure the table has a valid geometry column."
            )

        if bbox_column not in self.column_names:
            raise ValueError(f"Bbox column '{bbox_column}' not found. Use add_bbox() first.")

        geom_col = str(self._geometry_column)

        # Get existing metadata
        schema = self._table.schema
        schema_metadata = dict(schema.metadata) if schema.metadata else {}

        # Parse existing geo metadata or create new
        if b"geo" in schema_metadata:
            try:
                geo_meta = json.loads(schema_metadata[b"geo"].decode("utf-8"))
                # Ensure geo_meta is a dict and has "columns" key
                if not isinstance(geo_meta, dict):
                    geo_meta = {}
                if "columns" not in geo_meta or not isinstance(geo_meta.get("columns"), dict):
                    geo_meta["columns"] = {}
            except (json.JSONDecodeError, UnicodeDecodeError):
                geo_meta = {"columns": {}}
        else:
            geo_meta = {
                "version": "1.1.0",
                "primary_column": geom_col,
                "columns": {},
            }

        # Add covering metadata for the geometry column
        if geom_col not in geo_meta["columns"]:
            geo_meta["columns"][geom_col] = {}

        geo_meta["columns"][geom_col]["covering"] = {
            "bbox": {
                "xmin": [bbox_column, "xmin"],
                "ymin": [bbox_column, "ymin"],
                "xmax": [bbox_column, "xmax"],
                "ymax": [bbox_column, "ymax"],
            }
        }

        # Update schema with new metadata (metadata-only change, not a cast)
        schema_metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
        new_table = self._table.replace_schema_metadata(schema_metadata)

        return Table(new_table, self._geometry_column)

    def partition_by_string(
        self,
        output_dir: str | Path,
        column: str,
        *,
        chars: int | None = None,
        hive: bool = True,
        overwrite: bool = False,
        compression: str = "ZSTD",
        compression_level: int = 15,
    ) -> dict:
        """
        Partition by string column values.

        Creates partitioned output files based on unique values (or prefixes)
        of a string column.

        Args:
            output_dir: Output directory for partition files
            column: Column name to partition by
            chars: Use first N characters as prefix (None for full value)
            hive: Use Hive-style partitioning (column=value/)
            overwrite: Overwrite existing files
            compression: Compression codec
            compression_level: Compression level

        Returns:
            dict with partition statistics

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_string(
            ...     'output/',
            ...     column='country_code',
            ...     hive=True
            ... )
        """
        from geoparquet_io.core.partition_by_string import partition_by_string

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_string,
            output_dir,
            temp_prefix="gpio_part_str",
            core_kwargs={
                "column": column,
                "chars": chars,
                "hive": hive,
                "overwrite": overwrite,
            },
            compression=compression,
            compression_level=compression_level,
        )

    def partition_by_kdtree(
        self,
        output_dir: str | Path,
        *,
        iterations: int = 9,
        hive: bool = True,
        overwrite: bool = False,
        compression: str = "ZSTD",
        compression_level: int = 15,
    ) -> dict:
        """
        Partition by KD-tree spatial cells.

        Recursively splits the data spatially using KD-tree algorithm,
        creating balanced partitions based on geometry distribution.

        Args:
            output_dir: Output directory for partition files
            iterations: Number of KD-tree splits (creates 2^iterations partitions)
            hive: Use Hive-style partitioning
            overwrite: Overwrite existing files
            compression: Compression codec
            compression_level: Compression level

        Returns:
            dict with partition statistics

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_kdtree('output/', iterations=6)
        """
        from geoparquet_io.core.partition_by_kdtree import partition_by_kdtree

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_kdtree,
            output_dir,
            temp_prefix="gpio_part_kd",
            core_kwargs={
                "iterations": iterations,
                "hive": hive,
                "overwrite": overwrite,
            },
            compression=compression,
            compression_level=compression_level,
        )

    def partition_by_admin(
        self,
        output_dir: str | Path,
        *,
        dataset: str = "gaul",
        levels: list[str] | None = None,
        hive: bool = True,
        overwrite: bool = False,
        compression: str = "ZSTD",
        compression_level: int = 15,
    ) -> dict:
        """
        Partition by administrative boundaries.

        Partitions data based on country codes and/or admin subdivisions
        using a spatial join with an administrative boundaries dataset.

        Args:
            output_dir: Output directory for partition files
            dataset: Boundaries dataset ("gaul", "overture", or custom URL)
            levels: Admin levels to partition by (e.g., ["country", "admin1"])
            hive: Use Hive-style partitioning
            overwrite: Overwrite existing files
            compression: Compression codec
            compression_level: Compression level

        Returns:
            dict with partition statistics

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_admin(
            ...     'output/',
            ...     dataset='gaul',
            ...     levels=['country', 'admin1']
            ... )
        """
        from geoparquet_io.core.partition_admin_hierarchical import (
            partition_by_admin_hierarchical,
        )

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_admin_hierarchical,
            output_dir,
            temp_prefix="gpio_part_adm",
            core_kwargs={
                "dataset": dataset,
                "levels": levels or ["country"],
                "hive": hive,
                "overwrite": overwrite,
            },
            compression=compression,
            compression_level=compression_level,
        )

    def __repr__(self) -> str:
        """String representation of the Table."""
        geom_str = f", geometry='{self._geometry_column}'" if self._geometry_column else ""
        return f"Table(rows={self.num_rows}, columns={len(self.column_names)}{geom_str})"
