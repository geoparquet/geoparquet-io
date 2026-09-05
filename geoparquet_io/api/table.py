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

from geoparquet_io.core.add.kdtree import (
    DEFAULT_KDTREE_TARGET_ROWS,
    ITERATIONS_CONFLICT,
    ITERATIONS_MISSING,
)
from geoparquet_io.core.check_parquet_structure import CheckProfile
from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.duckdb_utils import quote_identifier
from geoparquet_io.core.str_order import DEFAULT_STR_TILE_SIZE
from geoparquet_io.core.wfs import DEFAULT_WFS_PAGE_SIZE

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


def _require_auto_or_resolution(
    auto: bool,
    values: dict[str, int | None],
    *,
    conflict: str,
    missing: str,
) -> None:
    """
    Mirror the core partition gate before any I/O is spent on the call.

    The auto-capable partition methods hand an in-memory table to core through a
    temporary parquet file. Core raises for a call that names neither a
    resolution nor ``auto`` -- but only after the whole table has been
    serialized. Checking here makes an invalid call cost nothing; core keeps its
    own gate as the backstop for every other caller.

    Args:
        auto: Whether the caller asked for auto-sizing
        values: The resolution-ish parameters by their public name, in the order
            core reports them
        conflict: Reason for the error when ``auto`` is combined with an
            explicit value (worded as core words it)
        missing: Reason for the error when neither was given

    Raises:
        InvalidParameterError: If ``auto`` conflicts with an explicit value, or
            neither was given
    """
    from geoparquet_io.core.exceptions import InvalidParameterError

    given = [name for name, value in values.items() if value is not None]

    if auto:
        if given:
            raise InvalidParameterError("auto", conflict)
        return

    if len(given) != len(values):
        raise InvalidParameterError(next(iter(values)), missing)


def _run_partition_with_temp_file(
    table: pa.Table,
    geometry_column: str | None,
    core_fn,
    output_dir: str | Path,
    *,
    temp_prefix: str = "gpio_partition",
    core_kwargs: dict,
    compression: str = "ZSTD",
    compression_level: int | None = None,
) -> dict:
    """
    Run a partition operation using a temporary file.

    Handles temp file creation, writing, partition function call, and cleanup.

    The core partition functions disagree on what they return -- an ``int``
    partition count (admin), ``None`` (kdtree, string), nothing the cell-index
    ones promise -- so the core result is not passed through. Every
    ``Table.partition_by_*`` method gets the same dict from here, counted off
    the output directory (#822).

    Args:
        table: The PyArrow table to partition
        geometry_column: Name of geometry column
        core_fn: The partition core function to call
        output_dir: Output directory path
        temp_prefix: Prefix for the temp file name
        core_kwargs: Keyword arguments for the core function
        compression: Compression codec
        compression_level: Compression level (None lets the codec pick its own)

    Returns:
        ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the output
        directory, the number of ``.parquet`` files written under it, and
        whether they are laid out Hive-style.
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

        core_fn(
            input_parquet=str(temp_input),
            output_folder=str(output_dir),
            **core_kwargs,
            verbose=False,
        )

        output_path = PathLib(output_dir)
        parquet_files = list(output_path.rglob("*.parquet"))
        return {
            "output_dir": str(output_path),
            "file_count": len(parquet_files),
            "hive": core_kwargs.get("hive", False),
        }
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

    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = None
    try:
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        con.register("input_table", table)

        # Use ST_Extent to get the bounding box of all geometries
        query = f"""
            SELECT
                ST_XMin(ST_Extent_Agg(ST_GeomFromWKB({quote_identifier(geometry_column)}))),
                ST_YMin(ST_Extent_Agg(ST_GeomFromWKB({quote_identifier(geometry_column)}))),
                ST_XMax(ST_Extent_Agg(ST_GeomFromWKB({quote_identifier(geometry_column)}))),
                ST_YMax(ST_Extent_Agg(ST_GeomFromWKB({quote_identifier(geometry_column)})))
            FROM input_table
            WHERE {quote_identifier(geometry_column)} IS NOT NULL
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


def _surviving_geometry_column(current: str | None, result: pa.Table) -> str | None:
    """Which geometry column a projected table should report, if any.

    A column projection can drop the geometry column the Table was pointing at.
    Keeping the old name made ``write()`` ask the writer for a column that is no
    longer there (#731), but collapsing straight to ``None`` was equally wrong
    when the input declared more than one geometry column: the survivor's geo
    metadata was then silently thrown away. So prefer the carried
    ``primary_column`` (``extract_table`` has already re-pointed it at a
    survivor), then any other declared geometry column, and only then ``None``
    — the same state ``read()`` reports for a plain Parquet file.
    """
    if current is not None and current in result.column_names:
        return current

    from geoparquet_io.core.geo_metadata import parse_geo_metadata

    geo = parse_geo_metadata(result.schema.metadata)
    if not geo:
        return None

    declared = geo.get("columns")
    candidates = [geo.get("primary_column")] + list(declared if isinstance(declared, dict) else [])
    for name in candidates:
        if isinstance(name, str) and name in result.column_names:
            return name
    return None


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
    from geoparquet_io.core.common import resolve_geoparquet_version_from_file

    arrow_table = pq.read_table(str(path), **kwargs)
    table = Table(arrow_table)
    # CLI-parity auto-version hint (todo 043): remember the file-based decision
    # so write() picks the same version the CLI would for this input, even when
    # native geometry columns were demoted to plain binary by pq.read_table.
    table._auto_version_hint = resolve_geoparquet_version_from_file(str(path))
    return table


def read_partition(
    path: str | Path,
    *,
    hive_input: bool | None = None,
    allow_schema_diff: bool = False,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool = True,
    aws_profile: str | None = None,
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
        s3_endpoint: Custom S3 endpoint (e.g., 'data.source.coop')
        s3_region: S3 region for custom endpoints
        s3_use_ssl: Whether to use SSL (default: True)
        aws_profile: AWS profile name for S3 operations

    Returns:
        Table containing all partition data combined

    Example:
        >>> import geoparquet_io as gpio
        >>> table = gpio.read_partition('partitioned_output/')
        >>> table = gpio.read_partition('data/quadkey=*/*.parquet')
    """
    from geoparquet_io.core.duckdb_utils import (
        _wrap_query_with_wkb_conversion,
        get_duckdb_connection,
        s3_config_scope,
    )
    from geoparquet_io.core.partition.reader import build_read_parquet_expr
    from geoparquet_io.core.remote import (
        needs_httpfs,
        resolve_s3_config,
        setup_aws_profile_if_needed,
    )
    from geoparquet_io.core.streaming import find_geometry_column_from_table

    s3_config = resolve_s3_config(
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
        s3_no_ssl=not s3_use_ssl,
        aws_profile=aws_profile,
    )
    if s3_config["profile"]:
        setup_aws_profile_if_needed(s3_config["profile"], str(path))

    path_str = str(path)
    expr = build_read_parquet_expr(
        path_str,
        allow_schema_diff=allow_schema_diff,
        hive_input=hive_input,
    )

    with s3_config_scope(s3_config):
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(path_str))
        try:
            query = f"SELECT * FROM {expr}"
            for row in con.execute(f"DESCRIBE ({query})").fetchall():
                if row[1] and row[1].upper() == "GEOMETRY":
                    query = _wrap_query_with_wkb_conversion(query, row[0], con=con)

            arrow_table = con.execute(query).arrow().read_all()
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
    layer: str | None = None,
    repair_geometry: bool = True,
    linearize_curves: bool = True,
    max_angle_deg: float | None = None,
) -> Table:
    """
    Convert a geospatial file to a Table.

    Supports: GeoPackage, GeoJSON, Shapefile, FlatGeobuf, FileGDB, CSV/TSV (with WKT or lat/lon).
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
        layer: Layer name for multi-layer formats (GeoPackage, FileGDB). If not specified,
               reads the first/default layer.
        linearize_curves: Stroke curved geometries (CircularString..MultiSurface) into
               their linear equivalents when DuckDB cannot parse them (default: True,
               mirroring repair_geometry). False raises an actionable error instead.
        max_angle_deg: Maximum angular step per stroked arc segment in degrees
               (default: 4.0, GDAL's OGR_ARC_STEPSIZE default).

    Returns:
        Table for chaining operations

    Example:
        >>> import geoparquet_io as gpio
        >>> gpio.convert('data.gpkg').sort_hilbert().write('out.parquet')
        >>> gpio.convert('data.csv', lat_column='lat', lon_column='lon').write('out.parquet')
        >>> gpio.convert('s3://bucket/data.gpkg', profile='my-aws').write('out.parquet')
        >>> gpio.convert('multilayer.gpkg', layer='buildings').write('buildings.parquet')
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
        layer=layer,
        repair_geometry=repair_geometry,
        linearize_curves=linearize_curves,
        max_angle_deg=max_angle_deg,
    )

    return Table(arrow_table, geometry_column=geom_col, crs=detected_crs)


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
    max_workers: int = 1,
    output_crs: str | None = None,
    max_allowable_offset: float | None = None,
    repair_geometry: bool = True,
    timeout: float = 60.0,
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
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)
        output_crs: Preserve native CRS. 'native' uses the layer's advertised SR,
            or pass an EPSG code (e.g. EPSG:25830). Default None reprojects to WGS84.
        max_allowable_offset: Server-side geometry generalization tolerance, in
            output-CRS units (degrees on the default WGS84 path).
        timeout: Per-request HTTP timeout in seconds (default 60). Increase for
            layers with very large/complex geometries that are slow to serialize.

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
        >>>
        >>> # Extract large dataset with parallel fetching
        >>> gpio.extract_arcgis(url, limit=100000, max_workers=3) \\
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
        max_workers=max_workers,
        output_crs=output_crs,
        max_allowable_offset=max_allowable_offset,
        verbose=False,
        repair_geometry=repair_geometry,
        timeout=timeout,
    )

    return Table(arrow_table, geometry_column="geometry")


class Table:
    """
    Fluent wrapper around PyArrow Table for GeoParquet operations.

    Provides chainable methods for common transformations:
    - add_bbox(): Add bounding box column
    - add_quadkey(): Add quadkey column
    - sort_hilbert(): Reorder by Hilbert curve
    - sort_str(): Reorder with Sort-Tile-Recursive packing
    - extract(): Filter columns and rows

    All methods return a new Table, preserving immutability.

    Example:
        >>> table = gpio.read('input.parquet')
        >>> result = table.add_bbox().sort_hilbert()
        >>> result.write('output.parquet')
    """

    def __init__(
        self,
        table: pa.Table,
        geometry_column: str | None = None,
        crs: dict | str | None = None,
    ):
        """
        Create a Table wrapper.

        Args:
            table: PyArrow Table containing GeoParquet data
            geometry_column: Name of geometry column (auto-detected if None)
            crs: CRS detected at read time (PROJJSON dict or string), used as a
                fallback when the Arrow table itself carries no CRS. Spatial
                sources read through DuckDB arrive as bare WKB with no embedded
                CRS, so without this hint the written GeoParquet silently
                claimed OGC:CRS84 for projected data (issue #625).
        """
        self._table = table
        self._geometry_column = geometry_column or self._detect_geometry_column()
        # Auto-version hint captured at read time (see read()): pq.read_table
        # demotes native geometry columns to plain binary when geoarrow isn't
        # registered, so the table alone can't always tell a native-geo-only
        # source from generic WKB. None when the table wasn't read from a file.
        self._auto_version_hint: str | None = None
        self._crs_hint: dict | str | None = crs

    _KEEP_CRS_HINT = object()

    def _wrap(
        self,
        table: pa.Table,
        geometry_column: str | None,
        crs: dict | str | None | object = _KEEP_CRS_HINT,
    ) -> Table:
        """Build a derived Table, carrying the read-time hints forward.

        Chained operations (add_bbox, sort_hilbert, ...) produce new Arrow
        tables that still contain no embedded CRS, so the CRS hint (and the
        auto-version hint) must survive the chain to reach write(). An
        operation that changes the CRS (reproject) passes ``crs=`` explicitly
        so the hint never mislabels transformed coordinates.
        """
        hint = self._crs_hint if crs is Table._KEEP_CRS_HINT else crs
        derived = Table(table, geometry_column=geometry_column, crs=hint)
        derived._auto_version_hint = self._auto_version_hint
        return derived

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
        geography_column: str | None = None,
        geometry_format: str = "wkt",
        edges: str | None = None,
        repair_geometry: bool = True,
    ) -> Table:
        """
        Read data from a BigQuery table.

        Uses DuckDB's BigQuery extension with the Storage Read API for
        efficient Arrow-based scanning with filter pushdown.

        Native GEOGRAPHY columns are automatically detected and converted
        with spherical edges. VARCHAR columns containing WKT or GeoJSON
        can be parsed by specifying geography_column and geometry_format.

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
            geography_column: Column containing geometry data. Auto-detected
                for native GEOGRAPHY columns. Specify to parse VARCHAR columns.
            geometry_format: Format of VARCHAR geometry ("wkt" or "geojson")
            edges: Edge interpretation ("spherical" or "planar"). Native
                GEOGRAPHY columns default to "spherical", VARCHAR to "planar".

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

            >>> # Parse VARCHAR column as WKT geometry
            >>> table = gpio.Table.from_bigquery(
            ...     'myproject.dataset.table',
            ...     geography_column='geometry',
            ...     geometry_format='wkt'
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
            geography_column=geography_column,
            geometry_format=geometry_format,
            edges=edges,
            verbose=False,
            repair_geometry=repair_geometry,
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

    @classmethod
    def from_wfs(
        cls,
        service_url: str,
        typename: str,
        version: str = "auto",
        bbox: tuple[float, float, float, float] | None = None,
        limit: int | None = None,
        max_workers: int = 1,
        page_size: int = DEFAULT_WFS_PAGE_SIZE,
        axis_order: str = "auto",
        strict_crs: bool = False,
        auto_tile: bool = True,
        repair_geometry: bool = True,
    ) -> Table:
        """
        Create Table from WFS layer.

        Uses DuckDB's native HTTP streaming for fast extraction. For very large
        datasets (1M+ features), use max_workers > 1 to enable parallel pagination.

        Args:
            service_url: WFS service URL
            typename: Feature type name (e.g., 'cities' or 'ns:cities')
            version: WFS version ('auto', '2.0.0', '1.1.0', '1.0.0'). Default 'auto'
                tries 2.0.0, then 1.1.0, then 1.0.0.
            bbox: Optional bounding box filter (xmin, ymin, xmax, ymax)
            limit: Maximum features to fetch
            max_workers: Parallel requests for large datasets (default: 1)
            page_size: Features per page when using parallel mode (default: 100000)
            axis_order: Bbox axis order ('auto', 'xy', 'latlon'). 'auto' detects from
                CRS format - URN CRS with WFS 1.1.0+ uses lat,lon per OGC spec.
            strict_crs: If True, fail when the server returns a different CRS than
                requested. If False (default), warn and use the server's actual
                CRS. The CRS the server declares in its GeoJSON response is
                authoritative; gpio never guesses from coordinates when the
                server states it (#499).
            auto_tile: Automatically subdivide into spatial tiles when the server
                caps responses (maxFeatures or startIndex limits). Matches the CLI
                default; setting it False accepts silently truncated results
                (default: True)

        Returns:
            Table for chaining operations

        Example:
            >>> import geoparquet_io as gpio
            >>> gpio.Table.from_wfs('https://geo.example.com/wfs', 'cities').add_bbox().write('cities.parquet')
            >>> # Accept whatever a capped server returns, without tiling:
            >>> gpio.Table.from_wfs('https://geo.example.com/wfs', 'parcels', auto_tile=False)
        """
        from geoparquet_io.core.wfs import wfs_to_table

        table = wfs_to_table(
            service_url,
            typename,
            version=version,
            bbox=bbox,
            limit=limit,
            max_workers=max_workers,
            page_size=page_size,
            axis_order=axis_order,
            strict_crs=strict_crs,
            auto_tile=auto_tile,
            repair_geometry=repair_geometry,
        )
        return cls(table)

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

        embedded = extract_crs_from_table(self._table, self._geometry_column)
        if embedded is not None:
            return embedded
        return self._crs_hint

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
        write_strategy: str = "duckdb-kv",
        profile: str | None = None,
        verbose: bool = False,
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
            geoparquet_version: GeoParquet version (1.0, 1.1, 1.1-geoarrow, 2.0, or None to preserve).
                1.1-geoarrow converts geometry from any input to native GeoArrow nested-coordinate
                encoding and omits the bbox column; columns with incompatible mixed types fall back to WKB.
            write_strategy: Write strategy for GeoParquet ('in-memory', 'streaming',
                           'duckdb-kv', 'disk-rewrite'). Default: 'duckdb-kv'
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
                verbose=verbose,
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
        verbose: bool = False,
    ) -> Path:
        """Write table to GeoParquet format (supports local and cloud)."""
        import tempfile
        import uuid
        from pathlib import Path as PathLib

        import pyarrow as pa

        from geoparquet_io.core.remote import is_remote_url
        from geoparquet_io.core.upload import upload
        from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory

        # Auto mode: resolve to a concrete version with the same decision the
        # CLI uses (native-geo-only → 2.0), falling back to the hint captured
        # at read() time, then the default. Passing a concrete version keeps
        # output self-consistent (a None version made duckdb-kv keep the native
        # GEOMETRY logical type while stamping 1.1.0/WKB geo metadata).
        if geoparquet_version is None:
            from geoparquet_io.core.common import resolve_geoparquet_version_from_table
            from geoparquet_io.core.geo_metadata import DEFAULT_GEOPARQUET_VERSION

            geoparquet_version = (
                resolve_geoparquet_version_from_table(self._table, verbose)
                or self._auto_version_hint
                or DEFAULT_GEOPARQUET_VERSION
            )

        path_str = str(path)
        is_remote = is_remote_url(path_str)

        # For remote destinations, write to temp file first
        if is_remote:
            temp_dir = PathLib(tempfile.gettempdir())
            local_path = temp_dir / f"gpio_write_{uuid.uuid4()}.parquet"
        else:
            local_path = PathLib(path)

        # No AWS_PROFILE env mutation: profile= is handed straight to upload(),
        # which resolves the credentials for it explicitly, so a library call
        # never rewrites the host process environment.
        try:
            # 1.1-geoarrow produces native GeoArrow encoding from WKB geometry, which
            # requires the streaming strategy (duckdb-kv COPY TO can only emit WKB).
            # Already-native nested geometry is left on the default strategy.
            if geoparquet_version == "1.1-geoarrow" and self._geometry_column:
                geom_field = self._table.schema.field(self._geometry_column)
                geom_type = geom_field.type
                is_wkb = (
                    pa.types.is_binary(geom_type)
                    or pa.types.is_large_binary(geom_type)
                    or getattr(geom_type, "extension_name", "") == "geoarrow.wkb"
                )
                if is_wkb and write_strategy != "streaming":
                    if verbose:
                        from geoparquet_io.core.logging_config import debug

                        debug("Routing 1.1-geoarrow WKB input through streaming strategy")
                    write_strategy = "streaming"

            # Get the appropriate write strategy
            strategy_enum = WriteStrategy(write_strategy)
            strategy = WriteStrategyFactory.get_strategy(strategy_enum)

            # Forward the input CRS so a non-default CRS survives the write (otherwise
            # the geometry silently defaults to OGC:CRS84). write_from_table expects a
            # PROJJSON dict, so normalise a string identifier (e.g. "EPSG:3857").
            input_crs = self.crs
            if isinstance(input_crs, str):
                from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson

                input_crs = parse_crs_string_to_projjson(input_crs)

            # Carry the table's non-geo file-level KV metadata (fiboa, vecorel,
            # STAC fragments) into the write. The Arrow strategies copied it
            # incidentally off the schema while the DuckDB COPY strategies —
            # including the default — rebuilt the KV block from scratch and
            # dropped it, so preservation depended on the strategy (#690).
            # 'geo' is deliberately excluded: it is regenerated from the table.
            from geoparquet_io.core.common import extract_preserved_kv_metadata

            preserved_kv = extract_preserved_kv_metadata(self._table.schema.metadata)

            strategy.write_from_table(
                table=self._table,
                output_path=str(local_path),
                geometry_column=self._geometry_column,
                geoparquet_version=geoparquet_version,
                compression=compression,
                compression_level=compression_level,
                row_group_size_mb=row_group_size_mb,
                row_group_rows=row_group_rows,
                verbose=verbose,
                input_crs=input_crs,
                extra_kv_metadata=preserved_kv or None,
            )

            # Upload to remote if needed
            if is_remote:
                upload(local_path, path_str, profile=profile)
                return PathLib(path)

            return local_path
        finally:
            # Clean up temp file for remote writes
            if is_remote and local_path.exists():
                local_path.unlink()

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

        from geoparquet_io.core.remote import is_remote_url
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
                # profile= is passed explicitly to upload() below, so there is no
                # AWS_PROFILE env mutation to leak into the host process.
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
        from geoparquet_io.core.add.bbox import add_bbox_table

        result = add_bbox_table(
            self._table,
            bbox_column_name=column_name,
            geometry_column=self._geometry_column,
        )
        return self._wrap(result, self._geometry_column)

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
        from geoparquet_io.core.add.quadkey import add_quadkey_table

        result = add_quadkey_table(
            self._table,
            quadkey_column_name=column_name,
            resolution=resolution,
            use_centroid=use_centroid,
            geometry_column=self._geometry_column,
        )
        return self._wrap(result, self._geometry_column)

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
        return self._wrap(result, self._geometry_column)

    def sort_str(self, tile_size: int = DEFAULT_STR_TILE_SIZE) -> Table:
        """Reorder rows using Sort-Tile-Recursive packing.

        Args:
            tile_size: Roughly the number of rows you intend to put in a Parquet
                row group. STR uses it only to pick the number of X strips
                (default: 50,000).

        Returns:
            New Table with rows packed into spatially compact tiles

        Note:
            The default is imported from ``core.str_order`` rather than spelled
            out, because the CLI has no ``tile_size`` option: the CLI<->API
            default-parity harness has nothing to compare this against and so
            cannot catch it drifting from ``ops.sort_str``.
        """
        from geoparquet_io.core.str_order import str_order_table

        result = str_order_table(
            self._table,
            geometry_column=self._geometry_column,
            tile_size=tile_size,
        )
        return self._wrap(result, self._geometry_column)

    def extract(
        self,
        columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        where: str | None = None,
        limit: int | None = None,
        repair_geometry: bool = True,
    ) -> Table:
        """
        Extract columns and rows with optional filtering.

        Column names are validated against the schema: an unknown name raises
        rather than being silently ignored, and a name may not appear in both
        ``columns`` and ``exclude_columns`` (geometry and bbox excepted).

        Args:
            columns: Columns to include (None = all)
            exclude_columns: Columns to exclude
            bbox: Bounding box filter (xmin, ymin, xmax, ymax)
            where: SQL WHERE clause
            limit: Maximum rows to return
            repair_geometry: Repair invalid geometry with ST_MakeValid (default: True)

        Returns:
            New filtered Table. Excluding every geometry column yields an
            attribute table, which ``write()`` emits as plain Parquet.

        Raises:
            InvalidParameterError: If a requested column does not exist, if a
                column is in both lists, or if ``bbox`` is used on a table with
                no geometry column.
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
            repair_geometry=repair_geometry,
        )
        # Excluding the geometry column is a documented way to build an
        # attribute table, so the derived Table must re-resolve which geometry
        # column (if any) it now points at (#731).
        return self._wrap(result, _surviving_geometry_column(self._geometry_column, result))

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
        from geoparquet_io.core.add.h3 import add_h3_table

        result = add_h3_table(
            self._table,
            h3_column_name=column_name,
            resolution=resolution,
        )
        return self._wrap(result, self._geometry_column)

    def add_a5(
        self,
        column_name: str = "a5_cell",
        resolution: int = 15,
    ) -> Table:
        """
        Add an A5 cell column based on geometry location.

        Args:
            column_name: Name for the A5 column (default: 'a5_cell')
            resolution: A5 resolution level 0-30 (default: 15)

        Returns:
            New Table with A5 column added
        """
        from geoparquet_io.core.add.a5 import add_a5_table

        result = add_a5_table(
            self._table,
            a5_column_name=column_name,
            resolution=resolution,
        )
        return self._wrap(result, self._geometry_column)

    def aggregate_a5(
        self,
        resolution: int,
        metric: str | None = None,
        breakdown: str | None = None,
        breakdown_limit: int = 20,
        out_geometry: str = "polygon",
        where: str | None = None,
        metric_nodata: str | None = None,
        bucket_point: str = "geometry",
        bbox_column: str | None = None,
    ) -> Table:
        """
        Aggregate features into A5 grid cells with per-cell statistics.

        Args:
            resolution: A5 resolution level 0-30
            metric: Aggregation metric, e.g. "sum:area" or "mean:value"
            breakdown: Column name to pivot into per-category count columns
            breakdown_limit: Max number of breakdown categories (default: 20)
            out_geometry: Output geometry type: "polygon", "centroid", "both", or "none"
            where: DuckDB WHERE clause filtering input rows before aggregation
            metric_nodata: NoData sentinel value(s) mapped to NULL in metric columns
            bucket_point: Keying point source: "geometry" (centroid, default),
                "bbox" (center of a bbox covering column), or a point column name
            bbox_column: Bbox covering column for bucket_point="bbox"

        Returns:
            New Table with one row per A5 cell
        """
        from geoparquet_io.core.process.aggregate.by_a5 import aggregate_a5_table

        result = aggregate_a5_table(
            self._table,
            resolution=resolution,
            metric=metric,
            breakdown=breakdown,
            breakdown_limit=breakdown_limit,
            out_geometry=out_geometry,
            geometry_column=self._geometry_column,
            where=where,
            metric_nodata=metric_nodata,
            bucket_point=bucket_point,
            bbox_column=bbox_column,
        )
        return self._wrap(result, "geometry" if out_geometry != "none" else None)

    def aggregate_h3(
        self,
        resolution: int,
        metric: str | None = None,
        breakdown: str | None = None,
        breakdown_limit: int = 20,
        out_geometry: str = "polygon",
        where: str | None = None,
        metric_nodata: str | None = None,
        bucket_point: str = "geometry",
        bbox_column: str | None = None,
    ) -> Table:
        """
        Aggregate features into H3 grid cells with per-cell statistics.

        Args:
            resolution: H3 resolution level 0-15
            metric: Aggregation metric, e.g. "sum:area" or "mean:value"
            breakdown: Column name to pivot into per-category count columns
            breakdown_limit: Max number of breakdown categories (default: 20)
            out_geometry: Output geometry type: "polygon", "centroid", "both", or "none"
            where: DuckDB WHERE clause filtering input rows before aggregation
            metric_nodata: NoData sentinel value(s) mapped to NULL in metric columns
            bucket_point: Keying point source: "geometry" (centroid, default),
                "bbox" (center of a bbox covering column), or a point column name
            bbox_column: Bbox covering column for bucket_point="bbox"

        Returns:
            New Table with one row per H3 cell
        """
        from geoparquet_io.core.process.aggregate.by_h3 import aggregate_h3_table

        result = aggregate_h3_table(
            self._table,
            resolution=resolution,
            metric=metric,
            breakdown=breakdown,
            breakdown_limit=breakdown_limit,
            out_geometry=out_geometry,
            geometry_column=self._geometry_column,
            where=where,
            metric_nodata=metric_nodata,
            bucket_point=bucket_point,
            bbox_column=bbox_column,
        )
        return self._wrap(result, "geometry" if out_geometry != "none" else None)

    def aggregate_admin(
        self,
        level: str = "country",
        metric: str | None = None,
        breakdown: str | None = None,
        breakdown_limit: int = 20,
        out_geometry: str = "polygon",
        where: str | None = None,
        metric_nodata: str | None = None,
        bucket_point: str = "geometry",
        bbox_column: str | None = None,
    ) -> Table:
        """
        Aggregate features into administrative regions with per-region statistics.

        Args:
            level: Admin level to aggregate by ("country", "region", "subregion")
            metric: Aggregation metric, e.g. "sum:area" or "mean:value"
            breakdown: Column name to pivot into per-category count columns
            breakdown_limit: Max number of breakdown categories (default: 20)
            out_geometry: Output geometry type: "polygon", "centroid", "both", or "none"
            where: DuckDB WHERE clause filtering input rows before aggregation
            metric_nodata: NoData sentinel value(s) mapped to NULL in metric columns
            bucket_point: Join-point source: "geometry" (centroid, default),
                "bbox" (center of a bbox covering column), or a point column name
            bbox_column: Bbox covering column for bucket_point="bbox"

        Returns:
            New Table with one row per admin region
        """
        from geoparquet_io.api.ops import aggregate_admin

        result = aggregate_admin(
            self._table,
            level=level,
            metric=metric,
            breakdown=breakdown,
            breakdown_limit=breakdown_limit,
            out_geometry=out_geometry,
            where=where,
            metric_nodata=metric_nodata,
            bucket_point=bucket_point,
            bbox_column=bbox_column,
        )
        return self._wrap(result, "geometry" if out_geometry != "none" else None)

    def overview(
        self,
        level: int | str,
        cell_column: str | None = None,
        scheme: str | None = None,
    ) -> Table:
        """
        Roll an aggregate table up to a coarser overview level.

        The table must be a `process aggregate` output (carrying an
        ``a5_cell``/``h3_cell``/``admin_code`` column plus ``count``). Counts,
        sums, mins, maxes, and breakdown counts roll up exactly; averages are
        count-weighted (exact when the metric had no NULLs).

        Args:
            level: Target level -- a coarser grid resolution, or ``"country"``
                for a region-level admin aggregate
            cell_column: Cell id column when auto-detection fails
            scheme: Bucketing scheme (``a5``/``h3``/``admin``) when inference
                is ambiguous, e.g. H3 ids stored as integers

        Returns:
            New Table with one row per parent cell
        """
        from geoparquet_io.core.process.overview import rollup_table

        result = rollup_table(self._table, level, cell_column=cell_column, scheme=scheme)
        has_geometry = "geometry" in result.column_names
        return self._wrap(result, "geometry" if has_geometry else None)

    def add_s2(
        self,
        column_name: str = "s2_cell",
        level: int = 13,
    ) -> Table:
        """
        Add an S2 cell column based on geometry location.

        Uses Google's S2 spherical geometry library to compute cell IDs
        from geometry centroids. Cell IDs are stored as hex tokens for portability.

        Args:
            column_name: Name for the S2 column (default: 's2_cell')
            level: S2 level 0-30 (default: 13, ~1.2 km² cells)

        Returns:
            New Table with S2 column added

        Example:
            >>> table = gpio.read('data.parquet')
            >>> table.add_s2(level=13).write('output.parquet')
        """
        from geoparquet_io.core.add.s2 import add_s2_table

        result = add_s2_table(
            self._table,
            s2_column_name=column_name,
            level=level,
        )
        return self._wrap(result, self._geometry_column)

    def add_geometry_metrics(
        self,
        vecorel: bool = True,
    ) -> Table:
        """
        Add geodesic area (m²) and perimeter (m) columns.

        Uses WGS84 spheroid-based calculations. Adds metrics:area
        and metrics:perimeter columns.

        Args:
            vecorel: Add Vecorel schema metadata (default: True)

        Returns:
            New Table with geometry metrics added

        Example:
            >>> table = gpio.read('data.parquet')
            >>> table.add_geometry_metrics().write('output.parquet')
        """
        from geoparquet_io.core.add.geometry_metrics import add_geometry_metrics

        result_table = self._with_temp_io_files(
            add_geometry_metrics,
            vecorel=vecorel,
        )
        return self._wrap(result_table, self._geometry_column)

    def add_kdtree(
        self,
        column_name: str = "kdtree_cell",
        iterations: int | None = None,
        sample_size: int = 100000,
        *,
        auto: bool = False,
        target_rows: int = DEFAULT_KDTREE_TARGET_ROWS,
    ) -> Table:
        """
        Add a KD-tree cell column based on geometry location.

        Like ``gpio add kdtree``, this refuses to guess: pass ``iterations``, or
        pass ``auto=True`` to size the tree from the row count.

        Args:
            column_name: Name for the KD-tree column (default: 'kdtree_cell')
            iterations: Number of recursive splits 1-20, giving ``2 ** iterations``
                cells. Required unless ``auto=True``. Mirrors ``--partitions``,
                which the CLI spells as the partition count itself.
            sample_size: Number of points to sample for boundaries (default: 100000)
            auto: Size the tree from the row count (default: False). Mirrors the
                CLI's default auto mode; mutually exclusive with ``iterations``.
            target_rows: Target rows per cell when ``auto=True`` (default: 120000).
                Mirrors ``--auto N``.

        Returns:
            New Table with KD-tree column added

        Raises:
            InvalidParameterError: If both ``iterations`` and ``auto`` were given,
                or neither was

        Example:
            >>> table = gpio.read('data.parquet')
            >>> sized = table.add_kdtree(iterations=6)  # 64 cells
            >>> auto = table.add_kdtree(auto=True)  # sized from the row count
        """
        from geoparquet_io.core.add.kdtree import add_kdtree_table

        result = add_kdtree_table(
            self._table,
            kdtree_column_name=column_name,
            iterations=iterations,
            sample_size=sample_size,
            auto_target_rows=("rows", target_rows) if auto else None,
        )
        return self._wrap(result, self._geometry_column)

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
        return self._wrap(result, self._geometry_column)

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
        return self._wrap(result, self._geometry_column)

    def reproject(
        self,
        target_crs: str = "EPSG:4326",
        source_crs: str | None = None,
        assume_crs84: bool = False,
    ) -> Table:
        """
        Reproject geometry to a different coordinate reference system.

        Args:
            target_crs: Target CRS (default: EPSG:4326)
            source_crs: Source CRS. If None, detected from metadata.
            assume_crs84: Treat an unknown/null input CRS as OGC:CRS84 instead of
                whatever detection finds (no coordinate change).

        Returns:
            New Table with reprojected geometry
        """
        from geoparquet_io.core.reproject import reproject_table

        result = reproject_table(
            self._table,
            target_crs=target_crs,
            source_crs=source_crs,
            geometry_column=self._geometry_column,
            assume_crs84=assume_crs84,
        )
        # The source-CRS hint would mislabel transformed coordinates; the
        # reprojected table's CRS is target_crs by construction.
        return self._wrap(result, self._geometry_column, crs=target_crs)

    def partition_by_quadkey(
        self,
        output_dir: str | Path,
        *,
        resolution: int | None = None,
        partition_resolution: int | None = None,
        auto: bool = False,
        target_rows: int = 100000,
        max_partitions: int = 10000,
        compression: str = "ZSTD",
        hive: bool = False,
        keep_quadkey_column: bool | None = None,
        overwrite: bool = False,
    ) -> dict:
        """
        Partition the table into a directory of files split by quadkey.

        Like ``gpio partition quadkey``, this refuses to guess: pass both
        ``resolution`` and ``partition_resolution``, or pass ``auto=True`` to
        size them from the data.

        Args:
            output_dir: Output directory path
            resolution: Quadkey resolution for sorting (0-23). Required unless
                ``auto=True``. Mirrors ``--resolution``.
            partition_resolution: Resolution for partition boundaries (0-23).
                Required unless ``auto=True``. Mirrors ``--partition-resolution``.
            auto: Calculate both resolutions from the data (default: False).
                Mirrors ``--auto``; mutually exclusive with the two above.
            target_rows: Target rows per partition when ``auto=True``
                (default: 100000). Mirrors ``--target-rows``.
            max_partitions: Maximum partitions when ``auto=True``
                (default: 10000). Mirrors ``--max-partitions``.
            compression: Compression codec (default: ZSTD)
            hive: Use Hive-style partitioning (default: False, matches CLI --hive)
            keep_quadkey_column: Keep the generated ``quadkey`` column in the output
                files. None (default) follows ``hive``: kept for Hive-style
                output, dropped for flat output where the value is already in
                the file name. Mirrors ``--keep-quadkey-column``.
            overwrite: Overwrite existing output directory

        Returns:
            ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the
            same dict every ``partition_by_*`` method returns, where
            ``file_count`` counts the ``.parquet`` files under ``output_dir``.

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_quadkey(
            ...     'output/', resolution=12, partition_resolution=6
            ... )
            >>> print(f"Created {stats['file_count']} files")
            >>> auto = table.partition_by_quadkey('output/', auto=True)
        """
        from geoparquet_io.core.partition.by_quadkey import partition_by_quadkey

        _require_auto_or_resolution(
            auto,
            {"resolution": resolution, "partition_resolution": partition_resolution},
            conflict="cannot specify --resolution or --partition-resolution with --auto",
            missing="must specify either --auto or both --resolution and --partition-resolution",
        )

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_quadkey,
            output_dir,
            temp_prefix="gpio_part_qk",
            core_kwargs={
                "resolution": resolution,
                "partition_resolution": partition_resolution,
                "auto": auto,
                "target_rows": target_rows,
                "max_partitions": max_partitions,
                "hive": hive,
                "keep_quadkey_column": keep_quadkey_column,
                "overwrite": overwrite,
            },
            compression=compression,
        )

    def partition_by_h3(
        self,
        output_dir: str | Path,
        *,
        resolution: int | None = None,
        auto: bool = False,
        target_rows: int = 100000,
        max_partitions: int = 10000,
        compression: str = "ZSTD",
        hive: bool = False,
        keep_h3_column: bool | None = None,
        overwrite: bool = False,
    ) -> dict:
        """
        Partition the table into a directory of files split by H3 cell.

        Like ``gpio partition h3``, this refuses to guess: pass a
        ``resolution``, or pass ``auto=True`` to size one from the data.

        Args:
            output_dir: Output directory path
            resolution: H3 resolution level 0-15. Required unless ``auto=True``.
                Mirrors ``--resolution``.
            auto: Calculate the resolution from the data (default: False).
                Mirrors ``--auto``; mutually exclusive with ``resolution``.
            target_rows: Target rows per partition when ``auto=True``
                (default: 100000). Mirrors ``--target-rows``.
            max_partitions: Maximum partitions when ``auto=True``
                (default: 10000). Mirrors ``--max-partitions``.
            compression: Compression codec (default: ZSTD)
            hive: Use Hive-style partitioning (default: False, matches CLI --hive)
            keep_h3_column: Keep the generated ``h3_cell`` column in the output
                files. None (default) follows ``hive``: kept for Hive-style
                output, dropped for flat output where the value is already in
                the file name. Mirrors ``--keep-h3-column``.
            overwrite: Overwrite existing output directory

        Returns:
            ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the
            same dict every ``partition_by_*`` method returns, where
            ``file_count`` counts the ``.parquet`` files under ``output_dir``.

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_h3('output/', resolution=6)
            >>> print(f"Created {stats['file_count']} files")
            >>> auto = table.partition_by_h3('output/', auto=True)
        """
        from geoparquet_io.core.partition.by_h3 import partition_by_h3

        _require_auto_or_resolution(
            auto,
            {"resolution": resolution},
            conflict="cannot specify both --auto and --resolution",
            missing="must specify either --resolution or --auto",
        )

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_h3,
            output_dir,
            temp_prefix="gpio_part_h3",
            core_kwargs={
                "resolution": resolution,
                "auto": auto,
                "target_rows": target_rows,
                "max_partitions": max_partitions,
                "hive": hive,
                "keep_h3_column": keep_h3_column,
                "overwrite": overwrite,
            },
            compression=compression,
        )

    def partition_by_s2(
        self,
        output_dir: str | Path,
        *,
        level: int | None = None,
        auto: bool = False,
        target_rows: int = 100000,
        max_partitions: int = 10000,
        compression: str = "ZSTD",
        hive: bool = False,
        keep_s2_column: bool | None = None,
        overwrite: bool = False,
    ) -> dict:
        """
        Partition the table into a directory of files split by S2 cell.

        Uses Google's S2 spherical geometry library to partition data
        by cell boundaries at the specified level.

        Like ``gpio partition s2``, this refuses to guess: pass a ``level``,
        or pass ``auto=True`` to size one from the data.

        Args:
            output_dir: Output directory path
            level: S2 level 0-30 (13 is ~1.2 km² cells). Required unless
                ``auto=True``. Mirrors ``--level``.
            auto: Calculate the level from the data (default: False).
                Mirrors ``--auto``; mutually exclusive with ``level``.
            target_rows: Target rows per partition when ``auto=True``
                (default: 100000). Mirrors ``--target-rows``.
            max_partitions: Maximum partitions when ``auto=True``
                (default: 10000). Mirrors ``--max-partitions``.
            compression: Compression codec (default: ZSTD)
            hive: Use Hive-style partitioning (default: False, matches CLI --hive)
            keep_s2_column: Keep the generated ``s2_cell`` column in the output
                files. None (default) follows ``hive``: kept for Hive-style
                output, dropped for flat output where the value is already in
                the file name. Mirrors ``--keep-s2-column``.
            overwrite: Overwrite existing output directory

        Returns:
            ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the
            same dict every ``partition_by_*`` method returns, where
            ``file_count`` counts the ``.parquet`` files under ``output_dir``.

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_s2('output/', level=10)
            >>> print(f"Created {stats['file_count']} files")
            >>> auto = table.partition_by_s2('output/', auto=True)
        """
        from geoparquet_io.core.partition.by_s2 import partition_by_s2

        _require_auto_or_resolution(
            auto,
            {"level": level},
            conflict="cannot specify both --auto and --level",
            missing="must specify either --level or --auto",
        )

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_s2,
            output_dir,
            temp_prefix="gpio_part_s2",
            core_kwargs={
                "level": level,
                "auto": auto,
                "target_rows": target_rows,
                "max_partitions": max_partitions,
                "hive": hive,
                "keep_s2_column": keep_s2_column,
                "overwrite": overwrite,
            },
            compression=compression,
        )

    def partition_by_a5(
        self,
        output_dir: str | Path,
        *,
        resolution: int | None = None,
        auto: bool = False,
        target_rows: int = 100000,
        max_partitions: int = 10000,
        compression: str = "ZSTD",
        hive: bool = False,
        keep_a5_column: bool | None = None,
        overwrite: bool = False,
    ) -> dict:
        """
        Partition the table into a directory of files split by A5 cell.

        A5 is a hierarchical grid system similar to H3 but with different
        properties. Uses fixed precision levels from 0-30.

        Like ``gpio partition a5``, this refuses to guess: pass a
        ``resolution``, or pass ``auto=True`` to size one from the data.

        Args:
            output_dir: Output directory path
            resolution: A5 resolution level 0-30. Required unless ``auto=True``.
                Mirrors ``--resolution``.
            auto: Calculate the resolution from the data (default: False).
                Mirrors ``--auto``; mutually exclusive with ``resolution``.
            target_rows: Target rows per partition when ``auto=True``
                (default: 100000). Mirrors ``--target-rows``.
            max_partitions: Maximum partitions when ``auto=True``
                (default: 10000). Mirrors ``--max-partitions``.
            compression: Compression codec (default: ZSTD)
            hive: Use Hive-style partitioning (default: False, matches CLI --hive)
            keep_a5_column: Keep the generated ``a5_cell`` column in the output
                files. None (default) follows ``hive``: kept for Hive-style
                output, dropped for flat output where the value is already in
                the file name. Mirrors ``--keep-a5-column``.
            overwrite: Overwrite existing output directory

        Returns:
            ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the
            same dict every ``partition_by_*`` method returns, where
            ``file_count`` counts the ``.parquet`` files under ``output_dir``.

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_a5('output/', resolution=12)
            >>> print(f"Created {stats['file_count']} files")
            >>> auto = table.partition_by_a5('output/', auto=True)
        """
        from geoparquet_io.core.partition.by_a5 import partition_by_a5

        _require_auto_or_resolution(
            auto,
            {"resolution": resolution},
            conflict="cannot specify both --auto and --resolution",
            missing="must specify either --resolution or --auto",
        )

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_a5,
            output_dir,
            temp_prefix="gpio_part_a5",
            core_kwargs={
                "resolution": resolution,
                "auto": auto,
                "target_rows": target_rows,
                "max_partitions": max_partitions,
                "hive": hive,
                "keep_a5_column": keep_a5_column,
                "overwrite": overwrite,
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
            compression_level: Compression level. None lets the codec pick its
                own default, matching the CLI -- a fixed value is rejected by
                codecs whose valid range excludes it (GZIP accepts 1-9).
            row_group_size_mb: Target row group size in MB
            row_group_rows: Exact rows per row group
            geoparquet_version: GeoParquet version (1.0, 1.1, 1.1-geoarrow, 2.0, or None to preserve).
                1.1-geoarrow converts geometry from any input to native GeoArrow nested-coordinate
                encoding and omits the bbox column; columns with incompatible mixed types fall back to WKB.
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

        from geoparquet_io.core.upload import upload as do_upload

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
        return self._wrap(self._table.slice(0, n), self._geometry_column)

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
        return self._wrap(self._table.slice(offset, n), self._geometry_column)

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
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        con = None
        try:
            con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
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
                    quoted_col = quote_identifier(col_name)
                    select_parts.extend(
                        [
                            f"COUNT(*) FILTER (WHERE {quoted_col} IS NULL)",
                            f"MIN({quoted_col})",
                            f"MAX({quoted_col})",
                            f"APPROX_COUNT_DISTINCT({quoted_col})",
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
                        quoted_col = quote_identifier(col_name)
                        try:
                            query = f"""
                                SELECT
                                    COUNT(*) FILTER (WHERE {quoted_col} IS NULL),
                                    MIN({quoted_col}),
                                    MAX({quoted_col}),
                                    APPROX_COUNT_DISTINCT({quoted_col})
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
                quoted_col = quote_identifier(col_name)
                query = f"""
                    SELECT COUNT(*) FILTER (WHERE {quoted_col} IS NULL)
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
        repair_geometry: bool = True,
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
            repair_geometry=repair_geometry,
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

    def check_spatial(
        self,
        sample_size: int = 100,
        limit_rows: int = 500000,
        query_fraction: float = 0.1,
        num_samples: int = 20,
        seed: int = 42,
        min_efficiency: float = 0.7,
    ) -> CheckResult:
        """
        Check if data is spatially ordered.

        For a file with per-row-group bbox statistics, the verdict is how well
        those row groups let a reader skip work, as a fraction of what the file's
        row-group count allows. Files without such statistics fall back to
        comparing the distance between consecutive features against random pairs.

        Args:
            sample_size: Number of random pairs to sample (default: 100).
                Sampling fallback only.
            limit_rows: Maximum rows to analyze (default: 500000, matching
                `gpio check spatial --limit-rows`). Sampling fallback only.
            query_fraction: Fraction of each dimension a sample query window
                spans (default: 0.1).
            num_samples: Number of sample query windows (default: 20).
            seed: Random seed for the sample windows, for reproducibility.
            min_efficiency: Minimum skip rate as a fraction of what this
                row-group count allows (default: 0.7). 1.0 demands an ideal grid
                tiling.

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
            query_fraction=query_fraction,
            num_samples=num_samples,
            seed=seed,
            efficiency_threshold=min_efficiency,
        )
        return CheckResult(results, check_type="spatial")

    def check_spatial_pushdown(self) -> CheckResult:
        """
        Check spatial filter pushdown readiness (prospective).

        Evaluates whether the table would support efficient spatial filter
        pushdown by writing to a temp file and analyzing per-row-group bbox
        statistics. Returns an estimated skip rate representing how many row
        groups a typical regional query can skip.

        Note:
            This method writes the table to a temporary file with default
            settings to compute metrics. For metrics on an existing file's
            actual row group structure, use the CLI command
            ``gpio check spatial --file`` or the standalone function
            ``geoparquet_io.check_spatial_pushdown(file_path)``.

        Returns:
            CheckResult with pushdown readiness metrics

        Example:
            >>> table = gpio.read('data.parquet')
            >>> result = table.check_spatial_pushdown()
            >>> if result.passed():
            ...     print("Good pushdown readiness")
            >>> print(result.details())
        """
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.check_spatial_order import check_spatial_pushdown_readiness

        results = self._with_temp_file(
            check_spatial_pushdown_readiness,
            verbose=False,
        )
        return CheckResult(results, check_type="spatial_pushdown")

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

    def check_row_groups(self, profile: CheckProfile | None = None) -> CheckResult:
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
            check_row_groups, verbose=False, return_results=True, quiet=True, profile=profile
        )
        return CheckResult(results, check_type="row_groups")

    def check_bloom_filters(self) -> CheckResult:
        """
        Check bloom filter presence on columns.

        Bloom filters enable efficient point lookups on low-cardinality columns
        (city names, land use types, integer ranges).

        Returns:
            CheckResult with bloom filter analysis

        Example:
            >>> table = gpio.read('data.parquet')
            >>> result = table.check_bloom_filters()
            >>> print(result.to_dict())
        """
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.check_parquet_structure import check_bloom_filters

        results = self._with_temp_file(
            check_bloom_filters, verbose=False, return_results=True, quiet=True
        )
        return CheckResult(results, check_type="bloom_filters")

    def check_optimization(self) -> CheckResult:
        """
        Check combined spatial query optimization.

        Evaluates five factors that affect spatial query performance:
        native geo types, geo bbox stats, spatial sorting, row group size,
        and compression.

        Returns:
            CheckResult with optimization analysis including score and level

        Example:
            >>> table = gpio.read('data.parquet')
            >>> result = table.check_optimization()
            >>> print(result.to_dict()['score'], '/', result.to_dict()['total_checks'])
        """
        from geoparquet_io.api.check import CheckResult
        from geoparquet_io.core.check_optimization import check_optimization

        results = self._with_temp_file(
            check_optimization, verbose=False, return_results=True, quiet=True
        )
        return CheckResult(results, check_type="optimization")

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
        dataset: str = "gaul",
        levels: list[str] | None = None,
        vecorel: bool = False,
        prefix: str | None = None,
    ) -> Table:
        """
        Add administrative division columns via spatial join.

        Enriches each row with country codes and/or admin subdivision codes
        based on spatial intersection with an administrative boundaries dataset.

        Args:
            dataset: Boundaries dataset ("gaul", "overture", or custom URL).
                Default matches the CLI
                (`gpio add admin-divisions --dataset`).
            levels: Admin levels to add (e.g., ["country", "department"]). None
                adds every level the dataset provides, matching the CLI with no
                ``--levels``: ``["continent", "country", "department"]`` for
                GAUL, ``["country", "region"]`` for Overture.
            vecorel: Output Vecorel-compliant columns. Forces Overture dataset
                with country,region levels. (default: False)
            prefix: Column name prefix, as with the CLI's ``--prefix``. None
                uses the dataset's own name (``gaul_country``,
                ``overture_country``); "admin" produces ``admin:country``.

        Returns:
            Table with admin division columns added

        Example:
            >>> table = gpio.read('data.parquet')
            >>> enriched = table.add_admin_divisions(levels=["country"])
            >>> # Keep the pre-1.4 column names when switching datasets
            >>> enriched = table.add_admin_divisions(
            ...     dataset="overture", prefix="overture"
            ... )
            >>> # Vecorel-compliant output
            >>> enriched = table.add_admin_divisions(vecorel=True)
        """
        from geoparquet_io.core.add.admin_divisions import add_admin_divisions_multi
        from geoparquet_io.core.admin_datasets import default_admin_levels

        if vecorel:
            dataset = "overture"
            levels = ["country", "region"]

        result_table = self._with_temp_io_files(
            add_admin_divisions_multi,
            dataset_name=dataset,
            levels=levels or default_admin_levels(dataset),
            vecorel=vecorel,
            prefix=prefix,
            verbose=False,
        )
        return self._wrap(result_table, self._geometry_column)

    def add_bbox_metadata(self, bbox_column: str = "bbox") -> Table:
        """
        Add bbox covering metadata to the table schema.

        Updates the GeoParquet metadata to indicate that the bbox column
        provides per-feature bounding boxes for the geometry column.
        This enables query engines to use bbox for efficient filtering.

        Note: This requires the bbox column to already exist. Use add_bbox()
        first if the table doesn't have a bbox column.

        The table must already carry GeoParquet `geo` metadata: `covering`
        describes a geometry column that `encoding` and `geometry_types` define,
        and inventing that block produced metadata failing its own spec checks
        (#713). A table read from plain Parquet is refused here exactly as
        `gpio add bbox-metadata` refuses the file.

        Args:
            bbox_column: Name of the bbox column (default "bbox")

        Returns:
            Table with updated metadata

        Raises:
            GeoParquetError: If the table carries no GeoParquet metadata
            ValueError: If the geometry or bbox column is missing, or the
                declared version predates GeoParquet 1.1

        Example:
            >>> table = gpio.read('data.parquet')
            >>> table = table.add_bbox().add_bbox_metadata()
        """
        from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata_table

        geom_col = str(self._geometry_column) if self._geometry_column is not None else None
        new_table = add_bbox_metadata_table(
            self._table,
            bbox_column=bbox_column,
            geometry_column=geom_col,
        )

        return self._wrap(new_table, self._geometry_column)

    def partition_by_string(
        self,
        output_dir: str | Path,
        column: str,
        *,
        chars: int | None = None,
        hive: bool = False,
        overwrite: bool = False,
        compression: str = "ZSTD",
        compression_level: int | None = None,
    ) -> dict:
        """
        Partition by string column values.

        Creates partitioned output files based on unique values (or prefixes)
        of a string column.

        Args:
            output_dir: Output directory for partition files
            column: Column name to partition by
            chars: Use first N characters as prefix (None for full value)
            hive: Use Hive-style partitioning (column=value/) (default: False, matches CLI --hive)
            overwrite: Overwrite existing files
            compression: Compression codec
            compression_level: Compression level. None lets the codec pick its
                own default, matching the CLI -- a fixed value is rejected by
                codecs whose valid range excludes it (GZIP accepts 1-9).

        Returns:
            ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the
            same dict every ``partition_by_*`` method returns, where
            ``file_count`` counts the ``.parquet`` files under ``output_dir``.

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_string(
            ...     'output/',
            ...     column='country_code',
            ...     hive=True
            ... )
        """
        from geoparquet_io.core.partition.by_string import partition_by_string

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
        iterations: int | None = None,
        auto: bool = False,
        target_rows: int = DEFAULT_KDTREE_TARGET_ROWS,
        hive: bool = False,
        keep_kdtree_column: bool | None = None,
        overwrite: bool = False,
        compression: str = "ZSTD",
        compression_level: int | None = None,
    ) -> dict:
        """
        Partition by KD-tree spatial cells.

        Recursively splits the data spatially using KD-tree algorithm,
        creating balanced partitions based on geometry distribution.

        Like ``gpio partition kdtree``, this refuses to guess: pass
        ``iterations``, or pass ``auto=True`` to size the tree from the row count.
        A table already carrying a ``kdtree_cell`` column needs neither -- the
        existing cells drive the partition and no tree is built.

        Args:
            output_dir: Output directory for partition files
            iterations: Number of KD-tree splits (creates 2^iterations partitions).
                Required unless ``auto=True``. Mirrors ``--partitions``, which the
                CLI spells as the partition count itself.
            auto: Size the tree from the row count (default: False). Mirrors the
                CLI's default auto mode; mutually exclusive with ``iterations``.
            target_rows: Target rows per partition when ``auto=True``
                (default: 120000). Mirrors ``--auto N``.
            hive: Use Hive-style partitioning (default: False, matches CLI --hive)
            keep_kdtree_column: Keep the generated ``kdtree_cell`` column in the output
                files. None (default) follows ``hive``: kept for Hive-style
                output, dropped for flat output where the value is already in
                the file name. Mirrors ``--keep-kdtree-column``.
            overwrite: Overwrite existing files
            compression: Compression codec
            compression_level: Compression level. None lets the codec pick its
                own default, matching the CLI -- a fixed value is rejected by
                codecs whose valid range excludes it (GZIP accepts 1-9).

        Returns:
            ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the
            same dict every ``partition_by_*`` method returns, where
            ``file_count`` counts the ``.parquet`` files under ``output_dir``.

        Raises:
            InvalidParameterError: If both ``iterations`` and ``auto`` were given,
                or neither was and the table does not already carry the column

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_kdtree('output/', iterations=6)
            >>> auto = table.partition_by_kdtree('output/', auto=True)
        """
        from geoparquet_io.core.partition.by_kdtree import partition_by_kdtree

        if "kdtree_cell" in self._table.column_names:
            # The table already carries the kdtree column: the add step is
            # skipped and the existing cells drive the partition, so there is
            # no tree to size (`add_kdtree(...).partition_by_kdtree(dir)` needs
            # no sizing parameter). Only the contradiction of naming both is
            # still refused, before any I/O is spent on it.
            if auto and iterations is not None:
                from geoparquet_io.core.exceptions import InvalidParameterError

                raise InvalidParameterError("auto", ITERATIONS_CONFLICT)
        else:
            _require_auto_or_resolution(
                auto,
                {"iterations": iterations},
                conflict=ITERATIONS_CONFLICT,
                missing=ITERATIONS_MISSING,
            )

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_kdtree,
            output_dir,
            temp_prefix="gpio_part_kd",
            core_kwargs={
                "iterations": iterations,
                "auto_target_rows": ("rows", target_rows) if auto else None,
                "hive": hive,
                "keep_kdtree_column": keep_kdtree_column,
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
        hive: bool = False,
        overwrite: bool = False,
        vecorel: bool = False,
        compression: str = "ZSTD",
        compression_level: int | None = None,
    ) -> dict:
        """
        Partition by administrative boundaries.

        Partitions data based on country codes and/or admin subdivisions
        using a spatial join with an administrative boundaries dataset.

        Args:
            output_dir: Output directory for partition files
            dataset: Boundaries dataset ("gaul", "overture", or custom URL)
            levels: Admin levels to partition by (e.g., ["country",
                "department"] for GAUL, ["country", "region"] for Overture)
            hive: Use Hive-style partitioning (default: False, matches CLI --hive)
            overwrite: Overwrite existing files
            vecorel: Output Vecorel-compliant admin columns
                (admin:country_code, admin:subdivision_code) in each partition
                with schema metadata. Forces the Overture dataset with
                country,region levels. (default: False)
            compression: Compression codec
            compression_level: Compression level. None lets the codec pick its
                own default, matching the CLI -- a fixed value is rejected by
                codecs whose valid range excludes it (GZIP accepts 1-9).

        Returns:
            ``{'output_dir': str, 'file_count': int, 'hive': bool}`` -- the
            same dict every ``partition_by_*`` method returns, where
            ``file_count`` counts the ``.parquet`` files under ``output_dir``.

        Example:
            >>> table = gpio.read('data.parquet')
            >>> stats = table.partition_by_admin(
            ...     'output/',
            ...     dataset='gaul',
            ...     levels=['country', 'department']
            ... )
        """
        from geoparquet_io.core.partition.admin_hierarchical import (
            partition_by_admin_hierarchical,
        )

        if vecorel:
            dataset = "overture"
            levels = ["country", "region"]

        return _run_partition_with_temp_file(
            self._table,
            self._geometry_column,
            partition_by_admin_hierarchical,
            output_dir,
            temp_prefix="gpio_part_adm",
            core_kwargs={
                "dataset_name": dataset,
                "levels": levels or ["country"],
                "hive": hive,
                "overwrite": overwrite,
                "vecorel": vecorel,
            },
            compression=compression,
            compression_level=compression_level,
        )

    @classmethod
    def explain_analyze(
        cls,
        file_path: str,
        query: str | None = None,
    ) -> dict:
        """
        Run EXPLAIN ANALYZE on a DuckDB query against a Parquet file.

        Shows per-operator timing, cardinality, filter pushdown detection,
        and row group pruning analysis.

        Args:
            file_path: Path to the input Parquet file.
            query: Optional SQL query. Use {file} as placeholder for the file path.
                   Defaults to SELECT * FROM read_parquet('{file}').

        Returns:
            Dictionary with operators, timing, and analysis results.

        Example:
            >>> result = Table.explain_analyze('input.parquet')
            >>> for op in result['operators']:
            ...     print(f"{op['name']}: {op['timing']:.6f}s")
        """
        from geoparquet_io.core.benchmark import explain_analyze as _explain_analyze

        return _explain_analyze(
            file_path=file_path,
            query=query,
        )

    def __repr__(self) -> str:
        """String representation of the Table."""
        geom_str = f", geometry='{self._geometry_column}'" if self._geometry_column else ""
        return f"Table(rows={self.num_rows}, columns={len(self.column_names)}{geom_str})"
