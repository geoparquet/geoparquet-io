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
        compression: str = "ZSTD",
        compression_level: int | None = None,
        row_group_size_mb: float | None = None,
        row_group_rows: int | None = None,
        geoparquet_version: str | None = None,
    ) -> Path:
        """
        Write the table to a GeoParquet file.

        Args:
            path: Output file path
            compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
            compression_level: Compression level
            row_group_size_mb: Target row group size in MB
            row_group_rows: Exact rows per row group
            geoparquet_version: GeoParquet version (1.0, 1.1, 2.0, or None to preserve)

        Returns:
            Path: The output file path

        Example:
            >>> path = table.write('output.parquet')
            >>> print(f"Wrote to {path}")
        """
        from pathlib import Path as PathLib

        output_path = PathLib(path)

        # Use write_geoparquet_table for proper metadata preservation
        # It handles compression normalization, row group size estimation,
        # and GeoParquet metadata (bbox, version, geo metadata) correctly
        write_geoparquet_table(
            self._table,
            output_file=str(output_path),
            geometry_column=self._geometry_column,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            geoparquet_version=geoparquet_version,
            verbose=False,
        )

        return output_path

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

    def _partition_with_temp_file(
        self,
        partition_func,
        output_dir: str | Path,
        partition_kwargs: dict,
        compression: str,
    ) -> dict:
        """
        Common helper for partition operations using a temp file.

        Handles temp file creation, writing, partition function call,
        stats collection, and cleanup with retry.

        Args:
            partition_func: The partition function to call
            output_dir: Output directory path
            partition_kwargs: Keyword arguments for the partition function
            compression: Compression codec for temp file

        Returns:
            dict with partition statistics (output_dir, file_count, hive)
        """
        import tempfile
        import time
        import uuid
        from pathlib import Path as PathLib

        temp_path = PathLib(tempfile.gettempdir()) / f"gpio_partition_{uuid.uuid4()}.parquet"

        try:
            self.write(temp_path, compression=compression)

            partition_func(
                input_parquet=str(temp_path),
                output_folder=str(output_dir),
                **partition_kwargs,
            )

            # Return basic stats
            output_path = PathLib(output_dir)
            parquet_files = list(output_path.rglob("*.parquet"))
            return {
                "output_dir": str(output_path),
                "file_count": len(parquet_files),
                "hive": partition_kwargs.get("hive", True),
            }
        finally:
            for attempt in range(3):
                try:
                    temp_path.unlink(missing_ok=True)
                    break
                except OSError:
                    time.sleep(0.1 * (attempt + 1))

    def partition_by_quadkey(
        self,
        output_dir: str | Path,
        *,
        resolution: int = 13,
        partition_resolution: int = 6,
        compression: str = "ZSTD",
        hive: bool = True,
        overwrite: bool = False,
        verbose: bool = False,
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
            verbose: Print progress information

        Returns:
            dict with partition statistics (file_count, etc.)

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_quadkey('output/', resolution=12)
            >>> print(f"Created {stats['file_count']} files")
        """
        from geoparquet_io.core.partition_by_quadkey import partition_by_quadkey

        return self._partition_with_temp_file(
            partition_func=partition_by_quadkey,
            output_dir=output_dir,
            partition_kwargs={
                "resolution": resolution,
                "partition_resolution": partition_resolution,
                "hive": hive,
                "overwrite": overwrite,
                "verbose": verbose,
            },
            compression=compression,
        )

    def partition_by_h3(
        self,
        output_dir: str | Path,
        *,
        resolution: int = 9,
        compression: str = "ZSTD",
        hive: bool = True,
        overwrite: bool = False,
        verbose: bool = False,
    ) -> dict:
        """
        Partition the table into Hive-partitioned directory by H3 cell.

        Args:
            output_dir: Output directory path
            resolution: H3 resolution level 0-15 (default: 9)
            compression: Compression codec (default: ZSTD)
            hive: Use Hive-style partitioning (default: True)
            overwrite: Overwrite existing output directory
            verbose: Print progress information

        Returns:
            dict with partition statistics (file_count, etc.)

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_h3('output/', resolution=6)
            >>> print(f"Created {stats['file_count']} files")
        """
        from geoparquet_io.core.partition_by_h3 import partition_by_h3

        return self._partition_with_temp_file(
            partition_func=partition_by_h3,
            output_dir=output_dir,
            partition_kwargs={
                "resolution": resolution,
                "hive": hive,
                "overwrite": overwrite,
                "verbose": verbose,
            },
            compression=compression,
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

    def __repr__(self) -> str:
        """String representation of the Table."""
        geom_str = f", geometry='{self._geometry_column}'" if self._geometry_column else ""
        return f"Table(rows={self.num_rows}, columns={len(self.column_names)}{geom_str})"
