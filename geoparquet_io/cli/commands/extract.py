"""``gpio extract`` - pull data out of files and services into GeoParquet.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(extract)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

import click

from geoparquet_io.cli._shared import _activate_s3, create_default_group
from geoparquet_io.cli.decorators import (
    GlobAwareCommand,
    SingleFileCommand,
    any_extension_option,
    aws_profile_option,
    compression_options,
    dry_run_option,
    geoparquet_version_option,
    handle_geoparquet_errors,
    output_format_options,
    overwrite_option,
    parse_row_group_options,
    partition_input_options,
    repair_geometry_option,
    row_group_options,
    show_sql_option,
    verbose_option,
    write_strategy_option,
)
from geoparquet_io.core.extract import extract as extract_impl
from geoparquet_io.core.file_utils import validate_parquet_extension
from geoparquet_io.core.logging_config import configure_verbose, setup_cli_logging
from geoparquet_io.core.wfs import DEFAULT_WFS_PAGE_SIZE

ExtractDefaultGroup = create_default_group(
    "geoparquet",
    """Custom Group that invokes 'geoparquet' when no subcommand is provided.

This allows backwards compatibility:
- gpio extract input.parquet output.parquet  -> invokes geoparquet
- gpio extract geoparquet input.parquet output.parquet -> explicit
- gpio extract bigquery project.dataset.table output.parquet -> subcommand""",
)


# Extract commands group
@click.group(cls=ExtractDefaultGroup)
@click.pass_context
def extract(ctx):
    """Extract data from files and services to GeoParquet.

    By default, extracts from GeoParquet files. Use subcommands for other sources.

    \b
    Examples:
        gpio extract data.parquet output.parquet --bbox -122,37,-121,38
        gpio extract geoparquet data.parquet output.parquet  # Explicit
        gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 out.parquet
        gpio extract bigquery project.dataset.table output.parquet
    """
    # Ensure logging is set up (in case this group is invoked directly in tests)
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@extract.command(name="geoparquet", cls=GlobAwareCommand)
@click.argument("input_file")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@click.option(
    "--include-cols",
    help="Comma-separated columns to include (geometry and bbox auto-added unless in --exclude-cols)",
)
@click.option(
    "--exclude-cols",
    help="Comma-separated columns to exclude (can be used with --include-cols to exclude geometry/bbox)",
)
@click.option(
    "--bbox",
    help="Bounding box filter: xmin,ymin,xmax,ymax",
)
@click.option(
    "--geometry",
    help="Geometry filter: GeoJSON, WKT, @filepath, or - for stdin",
)
@click.option(
    "--use-first-geometry",
    is_flag=True,
    help="Use first geometry if FeatureCollection contains multiple",
)
@click.option(
    "--where",
    help="DuckDB WHERE clause for filtering rows. Column names with special "
    'characters need double quotes in SQL (e.g., "crop:name"). Shell escaping varies.',
)
@click.option(
    "--limit",
    type=int,
    help="Maximum number of rows to extract.",
)
@click.option(
    "--skip-count",
    is_flag=True,
    help="Skip counting total matching rows before extraction (faster for large datasets).",
)
@output_format_options
@geoparquet_version_option
@overwrite_option
@write_strategy_option
@partition_input_options
@repair_geometry_option
@dry_run_option
@show_sql_option
@verbose_option
@aws_profile_option
@any_extension_option
@click.pass_context
def extract_geoparquet(
    ctx,
    input_file,
    output_file,
    include_cols,
    exclude_cols,
    bbox,
    geometry,
    use_first_geometry,
    where,
    limit,
    skip_count,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    overwrite,
    write_strategy,
    write_memory,
    allow_schema_diff,
    hive_input,
    repair_geometry,
    dry_run,
    show_sql,
    verbose,
    aws_profile,
    any_extension,
):
    """
    Extract columns and rows from GeoParquet files.

    Supports column selection, spatial filtering, SQL filtering, and
    multiple input files via glob patterns (merged into single output).

    Column Selection:

      --include-cols: Select only specified columns (geometry and bbox
      columns are always included unless in --exclude-cols)

      --exclude-cols: Select all columns except those specified. Can be
      combined with --include-cols to exclude geometry/bbox columns only.

    Spatial Filtering:

      --bbox: Filter by bounding box. Uses bbox column for fast filtering
      when available, otherwise calculates from geometry.

      --geometry: Filter by intersection with a geometry. Accepts:
        - Inline GeoJSON or WKT
        - @filepath to read from file
        - "-" to read from stdin

    SQL Filtering:

      --where: Apply arbitrary DuckDB WHERE clause

    Examples:

        \b
        # Extract specific columns
        gpio extract data.parquet output.parquet --include-cols id,name,area

        \b
        # Exclude columns
        gpio extract data.parquet output.parquet --exclude-cols internal_id,temp

        \b
        # Filter by bounding box
        gpio extract data.parquet output.parquet --bbox -122.5,37.5,-122.0,38.0

        \b
        # Filter by geometry from file
        gpio extract data.parquet output.parquet --geometry @boundary.geojson

        \b
        # Filter by geometry from stdin
        cat boundary.geojson | gpio extract data.parquet output.parquet --geometry -

        \b
        # SQL WHERE filter
        gpio extract data.parquet output.parquet --where "population > 10000"

        \b
        # WHERE with special column names (double quotes in SQL)
        # Note: macOS may show harmless plist warnings with complex escaping
        gpio extract data.parquet output.parquet --where '"crop:name" = '\''wheat'\'''

        \b
        # Combined filters with glob pattern
        gpio extract "data/*.parquet" output.parquet \\
            --include-cols id,name \\
            --bbox -122.5,37.5,-122.0,38.0 \\
            --where "status = 'active'"

        \b
        # Remote file with spatial filter
        gpio extract s3://bucket/data.parquet output.parquet \\
            --aws_profile my-aws \\
            --bbox -122.5,37.5,-122.0,38.0

        \b
        # Extract first 1000 rows
        gpio extract data.parquet output.parquet --limit 1000
    """
    # Validate output early - provides helpful error if no output and not piping
    from geoparquet_io.core.streaming import StreamingError, validate_output

    try:
        validate_output(output_file)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # Validate .parquet extension
    validate_parquet_extension(output_file, any_extension)

    # Parse row group options
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    with _activate_s3(ctx, aws_profile=aws_profile):
        extract_impl(
            input_parquet=input_file,
            output_parquet=output_file,
            include_cols=include_cols,
            exclude_cols=exclude_cols,
            bbox=bbox,
            geometry=geometry,
            where=where,
            limit=limit,
            skip_count=skip_count,
            use_first_geometry=use_first_geometry,
            dry_run=dry_run,
            show_sql=show_sql,
            verbose=verbose,
            compression=compression.upper(),
            compression_level=compression_level,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            geoparquet_version=geoparquet_version,
            allow_schema_diff=allow_schema_diff,
            hive_input=hive_input,
            write_strategy=write_strategy,
            memory_limit=write_memory,
            overwrite=overwrite,
            repair_geometry=repair_geometry,
        )


@extract.command(name="arcgis", cls=SingleFileCommand)
@click.argument("service_url")
@click.argument("output_file", type=click.Path())
@click.option(
    "--token",
    help="ArcGIS authentication token",
)
@click.option(
    "--token-file",
    type=click.Path(exists=True),
    help="Path to file containing authentication token",
)
@click.option(
    "--username",
    help="ArcGIS Online/Enterprise username (requires --password)",
)
@click.option(
    "--password",
    help="ArcGIS Online/Enterprise password (requires --username)",
)
@click.option(
    "--portal-url",
    help="Enterprise portal URL for token generation (default: ArcGIS Online)",
)
@click.option(
    "--where",
    default="1=1",
    help="SQL WHERE clause to filter features (pushed to server, default: '1=1' = all)",
)
@click.option(
    "--bbox",
    help="Bounding box filter: xmin,ymin,xmax,ymax in WGS84 (pushed to server)",
)
@click.option(
    "--include-cols",
    help="Comma-separated columns to include (pushed to server for efficiency)",
)
@click.option(
    "--exclude-cols",
    help="Comma-separated columns to exclude (applied after download)",
)
@click.option(
    "--limit",
    type=int,
    help="Maximum number of features to extract",
)
@click.option(
    "--output-crs",
    help="Output CRS such as EPSG:25830, or 'native' for the layer's "
    "advertised SR. Default reprojects to WGS84.",
)
@click.option(
    "--max-allowable-offset",
    type=float,
    default=None,
    help="Server-side geometry generalization tolerance in output CRS units "
    "(ArcGIS maxAllowableOffset). Reduces vertices per feature, useful for very "
    "large or dense polygons.",
)
@click.option(
    "--skip-hilbert",
    is_flag=True,
    help="Skip Hilbert spatial ordering (faster but less optimal for spatial queries)",
)
@click.option(
    "--skip-bbox",
    is_flag=True,
    help="Skip adding bbox column (bbox enables faster spatial filtering on remote files)",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1, max=10),
    default=1,
    help="Number of concurrent requests (1-10). Default: 1 (sequential). Values 2-3 recommended for speedup. Higher values may trigger rate limits.",
)
@click.option(
    "--batch-size",
    type=click.IntRange(min=1, max=5000),
    default=None,
    help="Features per request. Default: server's maxRecordCount. Auto-reduces on server errors. Use smaller values for layers with complex geometries.",
)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0, min_open=True),
    default=60.0,
    show_default=True,
    help="Per-request HTTP timeout in seconds. Increase for layers with very large or complex geometries that the server is slow to serialize.",
)
@geoparquet_version_option
@overwrite_option
@repair_geometry_option
@verbose_option
@compression_options
@row_group_options
@any_extension_option
@aws_profile_option
@show_sql_option
@click.pass_context
def extract_arcgis(
    ctx,
    service_url,
    output_file,
    token,
    token_file,
    username,
    password,
    portal_url,
    where,
    bbox,
    include_cols,
    exclude_cols,
    limit,
    output_crs,
    max_allowable_offset,
    skip_hilbert,
    skip_bbox,
    workers,
    batch_size,
    timeout,
    geoparquet_version,
    overwrite,
    repair_geometry,
    verbose,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    any_extension,
    aws_profile,
    show_sql,
):
    """
    Extract features from ArcGIS Feature Service to GeoParquet.

    Downloads features from an ArcGIS REST Feature Service and converts
    them to an optimized GeoParquet file with ZSTD compression, bbox metadata,
    and Hilbert spatial ordering.

    SERVICE_URL must be a full ArcGIS Feature Service layer URL including the
    layer ID (e.g., .../FeatureServer/0).

    \b
    Filtering options (pushed to server for efficiency):
      --where          SQL WHERE clause for attribute filtering
      --bbox           Spatial bounding box filter (xmin,ymin,xmax,ymax)
      --include-cols   Select specific columns to download
      --limit          Maximum number of features to return

    \b
    Authentication options (in priority order):
      --token          Direct token string
      --token-file     Path to file containing token
      --username/password  Generate token via ArcGIS REST API

    \b
    Examples:
      # Public service (no auth)
      gpio extract arcgis https://services.arcgis.com/.../FeatureServer/0 out.parquet

      \b
      # Filter by bounding box (server-side)
      gpio extract arcgis https://... out.parquet --bbox -122.5,37.5,-122.0,38.0

      \b
      # Filter by SQL WHERE clause (server-side)
      gpio extract arcgis https://... out.parquet --where "state='CA'"

      \b
      # Extract only specific columns (server-side)
      gpio extract arcgis https://... out.parquet --include-cols name,population

      \b
      # Limit number of features
      gpio extract arcgis https://... out.parquet --limit 1000

      \b
      # Combined filters
      gpio extract arcgis https://... out.parquet \\
          --bbox -122.5,37.5,-122.0,38.0 \\
          --where "population > 10000" \\
          --limit 500
    """
    from geoparquet_io.core.arcgis import convert_arcgis_to_geoparquet

    configure_verbose(verbose)

    # Validate auth options
    if (username and not password) or (password and not username):
        raise click.BadParameter("Both --username and --password are required together")

    # Validate output extension
    if not any_extension:
        validate_parquet_extension(output_file)

    # Validate mutual exclusivity of row group options and get MB value
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    # Parse bbox string if provided
    bbox_tuple = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("bbox must have exactly 4 values")
            bbox_tuple = tuple(parts)
        except ValueError as e:
            raise click.BadParameter(f"Invalid bbox format: {e}. Use xmin,ymin,xmax,ymax") from e

    with _activate_s3(ctx, aws_profile=aws_profile):
        convert_arcgis_to_geoparquet(
            service_url=service_url,
            output_file=output_file,
            token=token,
            token_file=token_file,
            username=username,
            password=password,
            portal_url=portal_url,
            where=where,
            bbox=bbox_tuple,
            include_cols=include_cols,
            exclude_cols=exclude_cols,
            limit=limit,
            output_crs=output_crs,
            max_allowable_offset=max_allowable_offset,
            skip_hilbert=skip_hilbert,
            skip_bbox=skip_bbox,
            max_workers=workers,
            batch_size=batch_size,
            timeout=timeout,
            compression=compression.upper(),
            compression_level=compression_level,
            verbose=verbose,
            geoparquet_version=geoparquet_version,
            profile=aws_profile,
            row_group_size_mb=row_group_mb,
            row_group_rows=row_group_size,
            overwrite=overwrite,
            repair_geometry=repair_geometry,
        )


@extract.command(name="bigquery")
@handle_geoparquet_errors
@click.argument("table_id", metavar="TABLE_ID")
@click.argument("output_file", type=click.Path(), required=False, default=None)
@click.option(
    "--project",
    help="GCP project ID (overrides project in TABLE_ID if specified)",
)
@click.option(
    "--credentials-file",
    type=click.Path(exists=True),
    help="Path to GCP service account JSON file (otherwise uses gcloud auth or "
    "GOOGLE_APPLICATION_CREDENTIALS)",
)
@click.option(
    "--include-cols",
    help="Comma-separated columns to include",
)
@click.option(
    "--exclude-cols",
    help="Comma-separated columns to exclude",
)
@click.option(
    "--where",
    help="SQL WHERE clause for filtering (BigQuery SQL syntax)",
)
@click.option(
    "--bbox",
    help="Bounding box for spatial filter as minx,miny,maxx,maxy",
    type=str,
)
@click.option(
    "--bbox-mode",
    type=click.Choice(["auto", "server", "local"]),
    default="auto",
    help="Bbox filter mode: 'auto' (default) chooses based on table size, "
    "'server' forces BigQuery-side filtering, 'local' forces DuckDB-side filtering",
)
@click.option(
    "--bbox-threshold",
    type=click.IntRange(0, None),
    default=500000,
    help="Row count threshold for auto bbox mode. Tables with more rows use "
    "server-side filtering. Must be non-negative. Default: 500000",
)
@click.option(
    "--limit",
    type=click.IntRange(0, None),
    help="Maximum number of rows to extract. Must be non-negative.",
)
@click.option(
    "--geography-column",
    help="Column containing geometry data. Auto-detected for native GEOGRAPHY columns. "
    "Specify to parse a VARCHAR column as WKT or GeoJSON geometry.",
)
@click.option(
    "--geometry-format",
    type=click.Choice(["wkt", "geojson"], case_sensitive=False),
    default="wkt",
    help="Format of geometry data in VARCHAR columns (default: wkt). "
    "Only used when --geography-column points to a non-GEOGRAPHY column.",
)
@click.option(
    "--edges",
    type=click.Choice(["spherical", "planar"], case_sensitive=False),
    default=None,
    help="Edge interpretation for GeoParquet metadata. "
    "Native GEOGRAPHY columns default to 'spherical' (BigQuery uses S2). "
    "VARCHAR columns default to 'planar'. Use this to override.",
)
@output_format_options(
    write_memory_help=(
        "Memory limit for the DuckDB scan of the BigQuery result (e.g., '512MB', '4GB'). "
        "This command writes through PyArrow, so the limit bounds the read, not the write."
    )
)
@geoparquet_version_option
@overwrite_option
@repair_geometry_option
@dry_run_option
@show_sql_option
@verbose_option
@any_extension_option
def extract_bigquery_cmd(
    table_id,
    output_file,
    project,
    credentials_file,
    include_cols,
    exclude_cols,
    where,
    bbox,
    bbox_mode,
    bbox_threshold,
    limit,
    geography_column,
    geometry_format,
    edges,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    write_memory,
    geoparquet_version,
    overwrite,
    repair_geometry,
    dry_run,
    show_sql,
    verbose,
    any_extension,
):
    """
    Extract data from a BigQuery table to GeoParquet.

    TABLE_ID is the fully qualified BigQuery table identifier:
    PROJECT.DATASET.TABLE or DATASET.TABLE (if --project is set).

    Authentication (in order of precedence):

    \b
    1. --credentials-file: Path to service account JSON
    2. GOOGLE_APPLICATION_CREDENTIALS environment variable
    3. gcloud auth application-default credentials

    Native GEOGRAPHY columns are automatically converted to GeoParquet geometry
    with spherical edges. If your geometry is stored as a VARCHAR column
    (WKT or GeoJSON), use --geography-column and --geometry-format to parse it.
    If no geometry column is found, the output is plain Parquet.

    \b
    Limitations:
    - Cannot read BigQuery views or external tables (Storage Read API limitation)
    - BIGNUMERIC columns are not supported

    Examples:

        \b
        # Extract entire table
        gpio extract bigquery myproject.geodata.buildings output.parquet

        \b
        # Extract with filtering
        gpio extract bigquery myproject.geodata.buildings output.parquet \\
            --where "area > 1000" --limit 10000

        \b
        # Use service account credentials
        gpio extract bigquery myproject.geodata.buildings output.parquet \\
            --credentials-file /path/to/service-account.json

        \b
        # Select specific columns
        gpio extract bigquery myproject.geodata.buildings output.parquet \\
            --include-cols "id,name,geography"

        \b
        # Parse a VARCHAR column as WKT geometry
        gpio extract bigquery myproject.dataset.table output.parquet \\
            --geography-column geometry --geometry-format wkt

        \b
        # Parse a GeoJSON geometry column
        gpio extract bigquery myproject.dataset.table output.parquet \\
            --geography-column geojson_col --geometry-format geojson

        \b
        # Extract without geometry (plain Parquet)
        gpio extract bigquery myproject.dataset.table output.parquet
    """
    from geoparquet_io.core.extract_bigquery import extract_bigquery

    # Validate output early
    from geoparquet_io.core.streaming import StreamingError, validate_output

    try:
        validate_output(output_file)
    except StreamingError as e:
        raise click.ClickException(str(e)) from None

    # Validate .parquet extension
    validate_parquet_extension(output_file, any_extension)

    # Parse row group options
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    extract_bigquery(
        table_id=table_id,
        output_parquet=output_file,
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
        dry_run=dry_run,
        show_sql=show_sql,
        verbose=verbose,
        compression=compression.upper(),
        compression_level=compression_level,
        row_group_size_mb=row_group_mb,
        row_group_rows=row_group_size,
        geoparquet_version=geoparquet_version,
        overwrite=overwrite,
        repair_geometry=repair_geometry,
        memory_limit=write_memory,
    )


def _deprecated_version_callback(ctx, param, value):
    """Callback to warn about deprecated --version flag."""
    if value is not None:
        click.echo(
            "Warning: --version is deprecated, use --wfs-version instead",
            err=True,
        )
    return value


@extract.command(name="wfs")
@handle_geoparquet_errors
@click.argument("service_url")
@click.argument("typename", required=False)
@click.argument("output_file", required=False, type=click.Path())
@click.option(
    "--wfs-version",
    "wfs_version",
    default="1.1.0",
    type=click.Choice(["auto", "2.0.0", "1.1.0", "1.0.0"]),
    help="WFS protocol version. 'auto' tries 2.0.0, then 1.1.0, then 1.0.0. Default: 1.1.0",
)
@click.option(
    "--version",
    "deprecated_version",
    type=click.Choice(["auto", "2.0.0", "1.1.0", "1.0.0"]),
    hidden=True,
    callback=_deprecated_version_callback,
    expose_value=True,
    is_eager=True,
    help="Deprecated: use --wfs-version instead",
)
@click.option(
    "--axis-order",
    type=click.Choice(["auto", "xy", "latlon"]),
    default="auto",
    help="Bbox axis order. 'auto' (default) detects from CRS format. "
    "'xy' forces lon,lat order. 'latlon' forces lat,lon order.",
)
@click.option(
    "--strict-crs",
    is_flag=True,
    help="Fail if server returns coordinates that don't match requested CRS. "
    "Without this flag, a warning is shown and detected CRS is used.",
)
@click.option(
    "--bbox",
    help="Bounding box: xmin,ymin,xmax,ymax in WGS84",
)
@click.option(
    "--bbox-mode",
    type=click.Choice(["auto", "server", "local"]),
    default="auto",
    help="Bbox filter mode: 'auto' (default) chooses based on server capabilities, "
    "'server' forces server-side filtering, 'local' forces client-side filtering",
)
@click.option(
    "--limit",
    type=click.IntRange(0, None),
    help="Maximum number of features to extract. Must be non-negative.",
)
@click.option(
    "--output-crs",
    help="Request specific CRS from server (e.g., EPSG:4326, urn:ogc:def:crs:EPSG::4326)",
)
@click.option(
    "--workers",
    type=click.IntRange(min=1, max=10),
    default=1,
    help="Parallel requests for large datasets. Default: 1 (single streaming request). "
    "Use 2-4 for datasets with 1M+ features to avoid timeouts.",
)
@click.option(
    "--page-size",
    type=click.IntRange(1000, 500000),
    default=DEFAULT_WFS_PAGE_SIZE,
    show_default=True,
    help="Features per page when using --workers > 1.",
)
@click.option(
    "--parallel-layers",
    type=click.IntRange(min=1, max=10),
    default=1,
    help="Number of layers to extract concurrently when extracting multiple layers. "
    "Default: 1 (sequential). Use with comma-separated typename for parallel extraction.",
)
@click.option(
    "--auto-tile/--no-auto-tile",
    default=True,
    help="Automatically subdivide into spatial tiles when server caps responses "
    "(e.g., maxFeatures or startIndex limits). Enabled by default. "
    "Use --no-auto-tile to disable and accept partial data.",
)
@click.option(
    "--sort-by",
    help="Attribute to sort by for stable pagination. Required for layers without a "
    "primary key on GeoServer. If not specified, auto-detected from DescribeFeatureType.",
)
@click.option(
    "--skip-hilbert",
    is_flag=True,
    help="Skip Hilbert curve sorting (faster, but no spatial clustering)",
)
@click.option(
    "--skip-bbox",
    is_flag=True,
    help="Skip adding bbox column (faster, but no per-geometry bbox)",
)
@repair_geometry_option
@compression_options
@row_group_options
@geoparquet_version_option
@overwrite_option
@verbose_option
@any_extension_option
def extract_wfs_cmd(
    service_url,
    typename,
    output_file,
    wfs_version,
    deprecated_version,
    axis_order,
    strict_crs,
    bbox,
    bbox_mode,
    limit,
    output_crs,
    workers,
    page_size,
    parallel_layers,
    auto_tile,
    sort_by,
    skip_hilbert,
    skip_bbox,
    repair_geometry,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    overwrite,
    verbose,
    any_extension,
):
    """
    Extract WFS (Web Feature Service) to GeoParquet.

    SERVICE_URL is the WFS service endpoint URL.

    TYPENAME is the layer to extract (e.g., 'roads', 'buildings').
    Use comma-separated names for multiple layers (e.g., 'roads,buildings,parcels').
    If omitted, lists available layers.

    OUTPUT_FILE is the output path. For single layer, this is the output file.
    For multiple layers, this is the output directory (each layer saved as typename.parquet).

    \b
    Examples:

        \b
        # List available layers
        gpio extract wfs https://geo.example.com/wfs

        \b
        # Extract single layer
        gpio extract wfs https://geo.example.com/wfs cities output.parquet

        \b
        # Extract multiple layers in parallel to a directory
        gpio extract wfs https://geo.example.com/wfs roads,buildings,parcels ./output/ \\
            --workers 2 --parallel-layers 3

        \b
        # With bbox filter (server-side when supported)
        gpio extract wfs https://geo.example.com/wfs roads output.parquet \\
            --bbox -122.5,37.5,-122.0,38.0

        \b
        # Force specific CRS and use parallel extraction
        gpio extract wfs https://geo.example.com/wfs buildings output.parquet \\
            --output-crs EPSG:4326 --workers 3

        \b
        # Limit features and skip optimizations for faster extraction
        gpio extract wfs https://geo.example.com/wfs parcels output.parquet \\
            --limit 10000 --skip-hilbert --skip-bbox
    """
    from geoparquet_io.core.wfs import (
        WFSError,
        convert_wfs_layers_to_directory,
        convert_wfs_to_geoparquet,
        list_available_layers,
        negotiate_wfs_version,
    )

    # Handle deprecated --version flag
    if deprecated_version is not None:
        wfs_version = deprecated_version

    # If no typename, list available layers
    if typename is None:
        try:
            # Handle auto version negotiation for listing
            if wfs_version == "auto":
                negotiated_version, _ = negotiate_wfs_version(service_url)
                layers = list_available_layers(service_url, version=negotiated_version)
            else:
                layers = list_available_layers(service_url, version=wfs_version)
        except WFSError as e:
            raise click.ClickException(str(e)) from None

        if not layers:
            click.echo("No layers found in WFS service.")
            return

        click.echo(f"Available layers in WFS service ({len(layers)} found):\n")
        for layer in layers:
            name = layer.get("typename", "unknown")
            title = layer.get("title", "")
            abstract = layer.get("abstract", "")

            click.echo(f"  {name}")
            if title and title != name:
                click.echo(f"    Title: {title}")
            if abstract:
                # Truncate long abstracts
                if len(abstract) > 100:
                    abstract = abstract[:97] + "..."
                click.echo(f"    Description: {abstract}")
            click.echo()
        return

    # Typename provided but no output file
    if output_file is None:
        raise click.ClickException(
            "OUTPUT_FILE is required when TYPENAME is specified.\n"
            f"Usage: gpio extract wfs {service_url} {typename} OUTPUT_FILE"
        )

    # Parse comma-separated typenames
    typenames = [t.strip() for t in typename.split(",") if t.strip()]
    if not typenames:
        raise click.ClickException(
            "No valid typename(s) provided. Specify one or more comma-separated layer names."
        )
    is_multi_layer = len(typenames) > 1

    # Validate output path based on single/multi layer mode
    if is_multi_layer:
        # Multi-layer mode: output_file is a directory
        if output_file.endswith(".parquet"):
            raise click.ClickException(
                f"Multiple layers specified ({len(typenames)}), but output looks like a file: {output_file}\n"
                "For multi-layer extraction, provide a directory path (e.g., ./output/)."
            )
    else:
        # Single layer mode: validate .parquet extension
        validate_parquet_extension(output_file, any_extension)

    # Parse row group options
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    # Parse bbox if provided
    bbox_tuple = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("Expected 4 values")
            bbox_tuple = tuple(parts)
        except ValueError as e:
            raise click.ClickException(
                f"Invalid bbox format: {bbox}\n"
                "Expected: xmin,ymin,xmax,ymax (e.g., -122.5,37.5,-122.0,38.0)"
            ) from e

    try:
        if is_multi_layer:
            # Multi-layer parallel extraction
            convert_wfs_layers_to_directory(
                service_url=service_url,
                typenames=typenames,
                output_dir=output_file,
                parallel_layers=parallel_layers,
                max_workers=workers,
                page_size=page_size,
                version=wfs_version,
                bbox=bbox_tuple,
                bbox_mode=bbox_mode,
                output_crs=output_crs,
                limit=limit,
                axis_order=axis_order,
                strict_crs=strict_crs,
                skip_hilbert=skip_hilbert,
                skip_bbox=skip_bbox,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                verbose=verbose,
                auto_tile=auto_tile,
                sort_by=sort_by,
                repair_geometry=repair_geometry,
            )
        else:
            # Single layer extraction
            convert_wfs_to_geoparquet(
                service_url=service_url,
                typename=typenames[0],
                output_file=output_file,
                version=wfs_version,
                bbox=bbox_tuple,
                bbox_mode=bbox_mode,
                output_crs=output_crs,
                limit=limit,
                max_workers=workers,
                page_size=page_size,
                axis_order=axis_order,
                strict_crs=strict_crs,
                skip_hilbert=skip_hilbert,
                skip_bbox=skip_bbox,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                verbose=verbose,
                auto_tile=auto_tile,
                sort_by=sort_by,
                repair_geometry=repair_geometry,
            )
    except WFSError as e:
        raise click.ClickException(str(e)) from None


@extract.command(name="carto")
@handle_geoparquet_errors
@click.argument("url")
@click.argument("table_name")
@click.argument("output_file", type=click.Path())
@click.option(
    "--where",
    help="SQL WHERE clause for filtering (e.g., \"status = 'active'\")",
)
@click.option(
    "--bbox",
    help="Bounding box filter: xmin,ymin,xmax,ymax in WGS84",
)
@click.option(
    "--limit",
    type=click.IntRange(0, None),
    help="Maximum number of rows to extract",
)
@click.option(
    "--include-cols",
    help="Comma-separated columns to include (default: all)",
)
@click.option(
    "--exclude-cols",
    help="Comma-separated columns to exclude",
)
@click.option(
    "--timeout",
    type=click.IntRange(1, 3600),
    default=120,
    help="Request timeout in seconds (default: 120)",
)
@click.option(
    "--skip-hilbert",
    is_flag=True,
    help="Skip Hilbert curve sorting (faster, but no spatial clustering)",
)
@click.option(
    "--skip-bbox",
    is_flag=True,
    help="Skip adding bbox column (faster, but no per-geometry bbox)",
)
@click.option(
    "--geometry/--no-geometry",
    "geometry",
    default=None,
    help="Force geometry (GeoParquet) or plain tabular (plain Parquet) "
    "extraction. Default: auto-detect from the table schema.",
)
@compression_options
@row_group_options
@geoparquet_version_option
@overwrite_option
@repair_geometry_option
@verbose_option
@any_extension_option
@aws_profile_option
@click.pass_context
def extract_carto_cmd(
    ctx,
    url,
    table_name,
    output_file,
    where,
    bbox,
    limit,
    include_cols,
    exclude_cols,
    timeout,
    skip_hilbert,
    skip_bbox,
    geometry,
    compression,
    compression_level,
    row_group_size,
    row_group_size_mb,
    geoparquet_version,
    overwrite,
    repair_geometry,
    verbose,
    any_extension,
    aws_profile,
):
    """
    Extract Carto SQL API table to GeoParquet.

    URL is the Carto SQL API endpoint (e.g., https://phl.carto.com/api/v2/sql).
    You can also provide just the base domain (e.g., https://phl.carto.com).

    TABLE_NAME is the table to extract (e.g., 'opa_properties_public').

    OUTPUT_FILE is the output GeoParquet file path.

    \b
    Notes:
        - Geometry column 'the_geom' is renamed to 'geometry' for consistency
        - Tables with no geometry are written as plain Parquet (no geo metadata);
          use --no-geometry to force tabular extraction or --geometry to force
          GeoParquet (default: auto-detect)
        - Filters (--where, --bbox) are pushed to the server for efficiency
          (--bbox applies only to geometry tables)
        - For large tables, use --limit or --where to avoid timeouts
        - Set CARTO_API_KEY env var for authenticated endpoints

    \b
    Examples:

        \b
        # Extract entire table
        gpio extract carto https://phl.carto.com/api/v2/sql \\
            opa_properties_public output.parquet

        \b
        # With WHERE filter
        gpio extract carto https://phl.carto.com/api/v2/sql \\
            opa_properties_public output.parquet \\
            --where "category_code_description LIKE 'LAND%'"

        \b
        # With bbox filter
        gpio extract carto https://phl.carto.com/api/v2/sql \\
            opa_properties_public output.parquet \\
            --bbox "-75.2,39.9,-75.1,40.0"

        \b
        # Select specific columns and limit rows
        gpio extract carto https://phl.carto.com/api/v2/sql \\
            opa_properties_public output.parquet \\
            --include-cols "parcel_number,market_value,the_geom" \\
            --limit 10000
    """
    from geoparquet_io.core.carto import CartoError, convert_carto_to_geoparquet

    # Validate .parquet extension
    validate_parquet_extension(output_file, any_extension)

    # Parse row group options
    row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    # Parse bbox if provided
    bbox_tuple = None
    if bbox:
        try:
            parts = [float(x.strip()) for x in bbox.split(",")]
            if len(parts) != 4:
                raise ValueError("Expected 4 values")
            bbox_tuple = tuple(parts)
        except ValueError as e:
            raise click.ClickException(
                f"Invalid bbox format: {bbox}\n"
                "Expected: xmin,ymin,xmax,ymax (e.g., -75.2,39.9,-75.1,40.0)"
            ) from e

    with _activate_s3(ctx, aws_profile=aws_profile):
        try:
            convert_carto_to_geoparquet(
                url=url,
                table_name=table_name,
                output_file=output_file,
                where=where,
                bbox=bbox_tuple,
                limit=limit,
                include_cols=include_cols,
                exclude_cols=exclude_cols,
                timeout=float(timeout),
                skip_hilbert=skip_hilbert,
                skip_bbox=skip_bbox,
                compression=compression.upper(),
                compression_level=compression_level,
                row_group_size_mb=row_group_mb,
                row_group_rows=row_group_size,
                geoparquet_version=geoparquet_version,
                overwrite=overwrite,
                verbose=verbose,
                repair_geometry=repair_geometry,
                geometry=geometry,
            )
        except CartoError as e:
            raise click.ClickException(str(e)) from None
