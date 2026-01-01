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


def convert(
    path: str | Path,
    *,
    geometry_column: str = "geometry",
    wkt_column: str | None = None,
    lat_column: str | None = None,
    lon_column: str | None = None,
    delimiter: str | None = None,
    crs: str = "EPSG:4326",
    skip_invalid: bool = False,
) -> Table:
    """
    Convert a geospatial file to a Table.

    Supports: GeoPackage, GeoJSON, Shapefile, FlatGeobuf, CSV/TSV (with WKT or lat/lon).
    Unlike the CLI convert command, this does NOT apply Hilbert sorting by default.
    Chain .sort_hilbert() explicitly if you want spatial ordering.

    Args:
        path: Path to input file
        geometry_column: Name for geometry column in output (default: 'geometry')
        wkt_column: For CSV: column containing WKT geometry
        lat_column: For CSV: latitude column
        lon_column: For CSV: longitude column
        delimiter: For CSV: field delimiter (auto-detected if not specified)
        crs: Coordinate reference system (default: EPSG:4326)
        skip_invalid: Skip invalid geometries instead of erroring

    Returns:
        Table for chaining operations

    Example:
        >>> import geoparquet_io as gpio
        >>> gpio.convert('data.gpkg').sort_hilbert().write('out.parquet')
        >>> gpio.convert('data.csv', lat_column='lat', lon_column='lon').write('out.parquet')
    """
    from geoparquet_io.core.common import get_duckdb_connection, safe_file_url

    con = get_duckdb_connection(load_spatial=True)
    safe_url = safe_file_url(str(path))
    is_csv = _is_csv_file(str(path))

    try:
        if is_csv:
            query = _build_csv_query(
                con, safe_url, delimiter, wkt_column, lat_column, lon_column, skip_invalid
            )
        else:
            query = _build_spatial_query(con, safe_url, geometry_column)

        result = con.execute(query)
        arrow_table = result.fetch_arrow_table()
    finally:
        con.close()

    return Table(arrow_table, geometry_column=geometry_column)


def _is_csv_file(path: str) -> bool:
    """Check if file is CSV/TSV format."""
    import os

    ext = os.path.splitext(path)[1].lower()
    return ext in [".csv", ".tsv", ".txt"]


def _build_csv_read_expr(path: str, delimiter: str | None) -> str:
    """Build DuckDB CSV read expression."""
    if delimiter:
        return f"read_csv('{path}', delim='{delimiter}', header=true, AUTO_DETECT=TRUE)"
    return f"read_csv_auto('{path}')"


def _find_wkt_column(col_names_lower: dict) -> str | None:
    """Find WKT column by common names."""
    for candidate in ["wkt", "geometry", "geom", "the_geom", "shape"]:
        if candidate in col_names_lower:
            return col_names_lower[candidate]
    return None


def _find_latlon_columns(col_names_lower: dict) -> tuple[str | None, str | None]:
    """Find lat/lon columns by common names."""
    lat_candidates = ["lat", "latitude", "y"]
    lon_candidates = ["lon", "lng", "long", "longitude", "x"]
    found_lat = next((col_names_lower[n] for n in lat_candidates if n in col_names_lower), None)
    found_lon = next((col_names_lower[n] for n in lon_candidates if n in col_names_lower), None)
    return found_lat, found_lon


def _detect_geometry_columns(con, csv_read: str, wkt_column, lat_column, lon_column):
    """Detect geometry columns in CSV/TSV."""
    # Handle explicit columns
    if wkt_column:
        return {"type": "wkt", "wkt_column": wkt_column}
    if lat_column and lon_column:
        return {"type": "latlon", "lat_column": lat_column, "lon_column": lon_column}

    # Auto-detect from column names
    columns = con.execute(f"SELECT * FROM {csv_read} LIMIT 0").description
    col_names_lower = {col[0].lower(): col[0] for col in columns}

    wkt_col = _find_wkt_column(col_names_lower)
    if wkt_col:
        return {"type": "wkt", "wkt_column": wkt_col}

    found_lat, found_lon = _find_latlon_columns(col_names_lower)
    if found_lat and found_lon:
        return {"type": "latlon", "lat_column": found_lat, "lon_column": found_lon}

    raise ValueError(
        "Could not detect geometry columns in CSV/TSV file. "
        "Use wkt_column or lat_column/lon_column parameters."
    )


def _build_csv_query(
    con, path: str, delimiter, wkt_column, lat_column, lon_column, skip_invalid
) -> str:
    """Build SQL query for CSV/TSV conversion."""
    csv_read = _build_csv_read_expr(path, delimiter)
    geom_info = _detect_geometry_columns(con, csv_read, wkt_column, lat_column, lon_column)

    # Convert to WKB for compatibility with geoarrow/pyarrow
    if geom_info["type"] == "wkt":
        wkt_col = geom_info["wkt_column"]
        if skip_invalid:
            return f"""
                SELECT * EXCLUDE ({wkt_col}),
                       ST_AsWKB(TRY_CAST({wkt_col} AS GEOMETRY)) AS geometry
                FROM {csv_read}
                WHERE TRY_CAST({wkt_col} AS GEOMETRY) IS NOT NULL
            """
        return f"""
            SELECT * EXCLUDE ({wkt_col}), ST_AsWKB(ST_GeomFromText({wkt_col})) AS geometry
            FROM {csv_read}
            WHERE {wkt_col} IS NOT NULL
        """
    else:  # latlon
        lat_col = geom_info["lat_column"]
        lon_col = geom_info["lon_column"]
        return f"""
            SELECT * EXCLUDE ({lat_col}, {lon_col}),
                   ST_AsWKB(ST_Point(CAST({lon_col} AS DOUBLE), CAST({lat_col} AS DOUBLE))) AS geometry
            FROM {csv_read}
            WHERE {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL
        """


def _build_spatial_query(con, path: str, geometry_column: str) -> str:
    """Build SQL query for spatial file conversion."""
    # Detect actual geometry column name from ST_Read
    sample_query = f"SELECT * FROM ST_Read('{path}') LIMIT 0"
    columns = con.execute(sample_query).description
    col_names = [col[0] for col in columns]

    # Find geometry column (common names from DuckDB spatial)
    geom_col = None
    for name in ["geom", "geometry", "wkb_geometry", "shape"]:
        if name in col_names:
            geom_col = name
            break

    if geom_col is None:
        # No known geometry column name, just return as-is
        return f"SELECT * FROM ST_Read('{path}')"

    # Convert geometry to WKB for compatibility with geoarrow/pyarrow
    return f"""
        SELECT * EXCLUDE ({geom_col}), ST_AsWKB({geom_col}) AS {geometry_column}
        FROM ST_Read('{path}')
    """


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
    ) -> None:
        """
        Write the table to a GeoParquet file.

        Args:
            path: Output file path
            compression: Compression type (ZSTD, GZIP, BROTLI, LZ4, SNAPPY, UNCOMPRESSED)
            compression_level: Compression level
            row_group_size_mb: Target row group size in MB
            row_group_rows: Exact rows per row group
            geoparquet_version: GeoParquet version (1.0, 1.1, 2.0, or None to preserve)
        """
        # Use write_geoparquet_table for proper metadata preservation
        # It handles compression normalization, row group size estimation,
        # and GeoParquet metadata (bbox, version, geo metadata) correctly
        write_geoparquet_table(
            self._table,
            output_file=str(path),
            geometry_column=self._geometry_column,
            compression=compression,
            compression_level=compression_level,
            row_group_size_mb=row_group_size_mb,
            row_group_rows=row_group_rows,
            geoparquet_version=geoparquet_version,
            verbose=False,
        )

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
        from pathlib import Path

        from geoparquet_io.core.common import setup_aws_profile_if_needed
        from geoparquet_io.core.upload import upload as do_upload

        setup_aws_profile_if_needed(profile, destination)

        # Write to temp file
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            temp_path = Path(tmp.name)

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
            temp_path.unlink(missing_ok=True)

    def __repr__(self) -> str:
        """String representation of the Table."""
        geom_str = f", geometry='{self._geometry_column}'" if self._geometry_column else ""
        return f"Table(rows={self.num_rows}, columns={len(self.column_names)}{geom_str})"
