"""``gpio publish`` - commands for publishing GeoParquet data.

Registered on the root ``gpio`` group by ``cli/main.py`` via
``cli.add_command(publish)``. This module must not import ``cli.main``: the
dependency runs one way, ``main`` -> ``commands`` -> ``_shared``/``decorators``.
"""

from pathlib import Path

import click

from geoparquet_io.cli._shared import _activate_s3
from geoparquet_io.cli.decorators import (
    aws_profile_option,
    dry_run_option,
    handle_geoparquet_errors,
    verbose_option,
)
from geoparquet_io.core.logging_config import setup_cli_logging
from geoparquet_io.core.upload import check_credentials
from geoparquet_io.core.upload import upload as upload_impl


# STAC commands
def _check_output_stac_item(output_path, output: str, overwrite: bool) -> None:
    """Check if output already exists and is a STAC Item, handle overwrite."""

    from geoparquet_io.core.stac import detect_stac

    if not output_path.exists():
        return

    existing_stac_type = detect_stac(str(output_path))
    if existing_stac_type == "Item":
        if not overwrite:
            raise click.ClickException(
                f"Output file already exists and is a STAC Item: {output}\n"
                "Use --overwrite to overwrite the existing file."
            )
        click.echo(
            click.style(
                f"⚠️  Overwriting existing STAC Item: {output}",
                fg="yellow",
            )
        )


def _check_output_stac_collection(output_path, collection_file, overwrite: bool) -> None:
    """Check if output directory already contains a STAC Collection, handle overwrite."""

    from geoparquet_io.core.stac import detect_stac

    if not collection_file.exists():
        return

    existing_stac_type = detect_stac(str(collection_file))
    if existing_stac_type == "Collection":
        if not overwrite:
            raise click.ClickException(
                f"Output directory already contains a STAC Collection: {collection_file}\n"
                "Use --overwrite to overwrite the existing collection and items."
            )
        click.echo(
            click.style(
                f"⚠️  Overwriting existing STAC Collection: {collection_file}",
                fg="yellow",
            )
        )


def _handle_stac_item(
    input_path,
    output: str,
    bucket: str,
    public_url: str,
    item_id: str,
    overwrite: bool,
    verbose: bool,
) -> None:
    """Handle STAC Item generation for single file."""
    from pathlib import Path

    from geoparquet_io.core.stac import generate_stac_item, write_stac_json

    if verbose:
        click.echo(f"Generating STAC Item for {input_path}")

    output_path = Path(output)
    _check_output_stac_item(output_path, output, overwrite)

    item_dict = generate_stac_item(str(input_path), bucket, public_url, item_id, verbose)
    write_stac_json(item_dict, output, verbose)
    click.echo(f"✓ Created STAC Item: {output}")


def _handle_stac_collection(
    input_path,
    output: str,
    bucket: str,
    public_url: str,
    collection_id: str,
    overwrite: bool,
    verbose: bool,
) -> None:
    """Handle STAC Collection generation for partitioned directory."""
    from pathlib import Path

    from geoparquet_io.core.stac import generate_stac_collection, write_stac_json

    if verbose:
        click.echo(f"Generating STAC Collection for {input_path}")

    # For collections, output can be:
    # 1. A directory path (write collection.json there, items alongside parquet files)
    # 2. None/same as input (write in-place alongside data)
    input_path_obj = Path(input_path)

    # Determine where to write collection.json
    if output:
        output_path = Path(output)
        collection_file = output_path / "collection.json"
    else:
        # Write in-place
        output_path = input_path_obj
        collection_file = output_path / "collection.json"

    _check_output_stac_collection(output_path, collection_file, overwrite)

    collection_dict, item_dicts = generate_stac_collection(
        str(input_path), bucket, public_url, collection_id, verbose
    )

    # Create output directory if needed
    output_path.mkdir(parents=True, exist_ok=True)

    # Write collection
    write_stac_json(collection_dict, str(collection_file), verbose)

    # Write items alongside their parquet files in the input directory
    # This follows STAC best practice of co-locating metadata with data
    for item_dict in item_dicts:
        item_id = item_dict["id"]
        # Find the parquet file in input directory
        parquet_file = input_path_obj / f"{item_id}.parquet"
        if not parquet_file.exists():
            # Check for hive-style partitions
            hive_partitions = list(input_path_obj.glob(f"*/{item_id}.parquet"))
            if hive_partitions:
                parquet_file = hive_partitions[0]

        # Write item JSON next to parquet file
        item_file = parquet_file.parent / f"{item_id}.json"

        # Check if we need to overwrite
        if item_file.exists() and not overwrite:
            from geoparquet_io.core.stac import detect_stac

            if detect_stac(str(item_file)):
                raise click.ClickException(
                    f"STAC Item already exists: {item_file}\nUse --overwrite to replace it."
                )

        write_stac_json(item_dict, str(item_file), verbose)

    click.echo(f"✓ Created STAC Collection: {collection_file}")
    click.echo(f"✓ Created {len(item_dicts)} STAC Items alongside data files in {input_path}")


def _stac_impl(input, output, bucket, public_url, collection_id, item_id, overwrite, verbose):
    """Shared STAC generation implementation for both command paths."""
    from pathlib import Path

    from geoparquet_io.core.stac import detect_stac

    input_path = Path(input)

    # Check if input is already a STAC file/collection
    stac_type = detect_stac(str(input_path))
    if stac_type:
        raise click.ClickException(
            f"Input is already a STAC {stac_type}: {input}\n"
            f"Use 'gpio check stac {input}' to validate it, or provide a GeoParquet file/directory."
        )

    if input_path.is_file():
        _handle_stac_item(input_path, output, bucket, public_url, item_id, overwrite, verbose)
    elif input_path.is_dir():
        _handle_stac_collection(
            input_path, output, bucket, public_url, collection_id, overwrite, verbose
        )
    else:
        raise click.BadParameter(f"Input must be file or directory: {input}")


# Publish commands group
@click.group()
@click.pass_context
def publish(ctx):
    """Commands for publishing GeoParquet data (STAC metadata, cloud uploads)."""
    ctx.ensure_object(dict)
    timestamps = ctx.obj.get("timestamps", False)
    setup_cli_logging(verbose=False, show_timestamps=timestamps)


@publish.command(name="stac")
@handle_geoparquet_errors
@click.argument("input")
@click.argument("output", type=click.Path())
@click.option(
    "--bucket",
    required=True,
    help="S3 bucket prefix for asset hrefs (e.g., s3://source.coop/org/dataset/)",
)
@click.option(
    "--public-url",
    help="Optional public HTTPS URL for assets (e.g., https://data.source.coop/org/dataset/)",
)
@click.option("--collection-id", help="Custom collection ID (for partitioned datasets)")
@click.option("--item-id", help="Custom item ID (for single files)")
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite existing STAC files in output location",
)
@verbose_option
def publish_stac(input, output, bucket, public_url, collection_id, item_id, overwrite, verbose):
    """
    Generate STAC Item or Collection from GeoParquet file(s).

    Single file -> STAC Item JSON

    Partitioned directory -> STAC Collection + Items (co-located with data)

    For partitioned datasets, Items are written alongside their parquet files
    following STAC best practices. collection.json is written to OUTPUT.

    Automatically detects PMTiles overview files and includes them as assets.

    Examples:

      \b
      # Single file
      gpio publish stac input.parquet output.json --bucket s3://my-bucket/roads/

      \b
      # Partitioned dataset - Items written next to parquet files
      gpio publish stac partitions/ . --bucket s3://my-bucket/roads/

      \b
      # With public URL mapping
      gpio publish stac data.parquet output.json \\
        --bucket s3://my-bucket/roads/ \\
        --public-url https://data.example.com/roads/
    """
    _stac_impl(input, output, bucket, public_url, collection_id, item_id, overwrite, verbose)


@publish.command(name="upload")
@handle_geoparquet_errors
@click.argument("source", type=click.Path(exists=True, path_type=Path))
@click.argument("destination", type=str)
@aws_profile_option
@click.option("--pattern", help="Glob pattern for filtering files (e.g., '*.parquet', '**/*.json')")
@click.option(
    "--max-files", default=4, show_default=True, help="Max parallel file uploads for directories"
)
@click.option(
    "--chunk-concurrency",
    default=12,
    show_default=True,
    help="Max concurrent chunks per file",
)
@click.option("--chunk-size", type=int, help="Chunk size in bytes for multipart uploads")
@click.option("--fail-fast", is_flag=True, help="Stop immediately on first error")
@click.option(
    "--s3-endpoint",
    help="Custom S3-compatible endpoint (e.g., 'minio.example.com:9000')",
    hidden=True,
)
@click.option(
    "--s3-region",
    help="S3 region (default: us-east-1 when using custom endpoint)",
    hidden=True,
)
@click.option(
    "--s3-no-ssl",
    is_flag=True,
    help="Disable SSL for S3 endpoint (use HTTP instead of HTTPS)",
    hidden=True,
)
@verbose_option
@dry_run_option
@click.pass_context
def publish_upload(
    ctx,
    source,
    destination,
    aws_profile,
    pattern,
    max_files,
    chunk_concurrency,
    chunk_size,
    fail_fast,
    s3_endpoint,
    s3_region,
    s3_no_ssl,
    dry_run,
    verbose,
):
    """Upload file or directory to object storage.

    Supports S3, GCS, Azure, and HTTP destinations. Automatically handles
    multipart uploads and preserves directory structure.

    Azure destinations name the storage account first:
    az://<account>/<container>/<path>. The account comes from the URL; the
    credential comes from the environment (AZURE_STORAGE_ACCOUNT_KEY or its
    aliases, AZURE_STORAGE_SAS_TOKEN/AZURE_STORAGE_SAS_KEY, the
    AZURE_STORAGE_CLIENT_* client-secret vars, or AZURE_USE_AZURE_CLI=true
    to use an az login session -- az login alone is not enough).

    \b
    Examples:
      # Single file to S3
      gpio publish upload data.parquet s3://bucket/path/data.parquet --aws-profile source-coop

      \b
      # Single file to Azure Blob Storage (account first, then container)
      gpio publish upload data.parquet az://myaccount/mycontainer/data.parquet

      \b
      # Directory to GCS (preserves structure, uploads files in parallel)
      gpio publish upload output/ gs://bucket/dataset/

      \b
      # Only parquet files with increased parallelism
      gpio publish upload output/ s3://bucket/dataset/ --pattern "*.parquet" --max-files 8

      \b
      # Stop on first error instead of continuing
      gpio publish upload output/ s3://bucket/dataset/ --fail-fast
    """
    with _activate_s3(
        ctx,
        aws_profile=aws_profile,
        s3_endpoint=s3_endpoint,
        s3_region=s3_region,
        s3_no_ssl=s3_no_ssl,
    ) as s3_config:
        # Check credentials before attempting upload
        creds_ok, hint = check_credentials(destination, s3_config.get("profile"))
        if not creds_ok:
            raise click.ClickException(f"Authentication failed:\n\n{hint}")

        upload_impl(
            source=source,
            destination=destination,
            profile=s3_config["profile"],
            pattern=pattern,
            max_files=max_files,
            chunk_concurrency=chunk_concurrency,
            chunk_size=chunk_size,
            fail_fast=fail_fast,
            dry_run=dry_run,
            s3_endpoint=s3_config["s3_endpoint"],
            s3_region=s3_config["s3_region"],
            s3_use_ssl=s3_config["s3_use_ssl"],
        )
