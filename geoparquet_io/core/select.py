"""
Core functionality for selecting/excluding fields from GeoParquet files.
"""

import click
import fsspec
import pyarrow.parquet as pq

from geoparquet_io.core.common import (
    find_primary_geometry_column,
    get_duckdb_connection,
    get_parquet_metadata,
    needs_httpfs,
    safe_file_url,
    write_parquet_with_metadata,
)


def parse_fields(fields_str: str) -> list[str]:
    """
    Parse a comma-separated list of field names, handling quoted fields.

    Field names with spaces, commas, or double-quotes should be surrounded with
    starting and ending double-quote characters. Double-quote characters in a
    field name should be escaped with backslash.

    Examples:
        "field1,field2" -> ["field1", "field2"]
        '"field with space",field2' -> ["field with space", "field2"]
        '"field, with comma",field2' -> ["field, with comma", "field2"]
        '"field with \\" quote",field2' -> ['field with " quote', "field2"]

    Args:
        fields_str: Comma-separated list of field names

    Returns:
        list[str]: List of parsed field names (deduplicated, order preserved)
    """
    if not fields_str:
        return []

    fields = []
    seen = set()
    current = ""
    in_quotes = False
    i = 0

    while i < len(fields_str):
        char = fields_str[i]

        if char == "\\" and i + 1 < len(fields_str):
            # Handle escape sequences
            next_char = fields_str[i + 1]
            if next_char == '"':
                current += '"'
                i += 2
                continue
            elif next_char == "\\":
                current += "\\"
                i += 2
                continue

        if char == '"':
            if not in_quotes:
                # Starting a quoted section
                in_quotes = True
            else:
                # Ending a quoted section
                in_quotes = False
            i += 1
            continue

        if char == "," and not in_quotes:
            # End of field
            field = current.strip()
            if field and field not in seen:
                fields.append(field)
                seen.add(field)
            current = ""
            i += 1
            continue

        current += char
        i += 1

    # Don't forget the last field
    field = current.strip()
    if field and field not in seen:
        fields.append(field)
        seen.add(field)

    return fields


def get_schema_field_names(parquet_file: str) -> list[str]:
    """
    Get all field names from a parquet file's schema.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        list[str]: List of field names in schema order
    """
    safe_url = safe_file_url(parquet_file, verbose=False)
    with fsspec.open(safe_url, "rb") as f:
        pf = pq.ParquetFile(f)
        schema = pf.schema_arrow

    return [field.name for field in schema]


def validate_fields(
    requested_fields: list[str],
    available_fields: list[str],
    ignore_missing: bool = False,
) -> list[str]:
    """
    Validate that requested fields exist in the available fields.

    Args:
        requested_fields: Fields requested by user
        available_fields: Fields available in the schema
        ignore_missing: If True, warn instead of error for missing fields

    Returns:
        list[str]: Validated list of fields that exist

    Raises:
        click.ClickException: If a requested field doesn't exist and ignore_missing is False
    """
    valid_fields = []
    available_set = set(available_fields)

    for field in requested_fields:
        if field in available_set:
            valid_fields.append(field)
        else:
            if ignore_missing:
                click.echo(
                    click.style(
                        f"Warning: Field '{field}' not found in input, skipping", fg="yellow"
                    )
                )
            else:
                raise click.ClickException(
                    f"Field '{field}' not found in input file.\n"
                    f"Available fields: {', '.join(available_fields)}\n"
                    f"Use --ignore-missing-fields to skip missing fields with a warning."
                )

    return valid_fields


def select_fields(
    input_parquet: str,
    output_parquet: str,
    fields: list[str],
    exclude: bool = False,
    ignore_missing: bool = False,
    verbose: bool = False,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    row_group_size_mb: float | None = None,
    row_group_rows: int | None = None,
    profile: str | None = None,
) -> None:
    """
    Select or exclude fields from a GeoParquet file.

    Args:
        input_parquet: Path to input file (local or remote URL)
        output_parquet: Path to output file (local or remote URL)
        fields: List of field names to select (or exclude if exclude=True)
        exclude: If True, select all fields EXCEPT those in fields list
        ignore_missing: If True, warn instead of error for missing fields
        verbose: Whether to print verbose output
        compression: Compression type
        compression_level: Compression level
        row_group_size_mb: Target row group size in MB
        row_group_rows: Exact number of rows per row group
        profile: AWS profile name (S3 only)
    """
    # Get input URL
    input_url = safe_file_url(input_parquet, verbose)

    # Get all available fields
    available_fields = get_schema_field_names(input_parquet)

    if verbose:
        click.echo(f"Available fields: {', '.join(available_fields)}")

    # Validate requested fields
    valid_fields = validate_fields(fields, available_fields, ignore_missing)

    if not valid_fields:
        raise click.ClickException(
            "No valid fields specified. At least one field must be selected."
        )

    # Determine final field list
    if exclude:
        # Select all fields except the specified ones
        exclude_set = set(valid_fields)
        final_fields = [f for f in available_fields if f not in exclude_set]
        if verbose:
            click.echo(f"Excluding fields: {', '.join(valid_fields)}")
    else:
        # Use the specified fields in the order given
        final_fields = valid_fields
        if verbose:
            click.echo(f"Selecting fields: {', '.join(valid_fields)}")

    if not final_fields:
        raise click.ClickException(
            "No fields remaining after exclusion. Cannot create an empty output."
        )

    # Get geometry column
    geom_col = find_primary_geometry_column(input_parquet, verbose)

    # Ensure geometry column is included unless explicitly excluded
    if geom_col not in final_fields and geom_col in available_fields:
        if exclude and geom_col in valid_fields:
            # User explicitly excluded the geometry column
            click.echo(
                click.style(
                    f"Warning: Geometry column '{geom_col}' was excluded. "
                    "Output will not be a valid GeoParquet file.",
                    fg="yellow",
                )
            )
        elif not exclude:
            # User didn't include geometry column - add it automatically
            click.echo(
                click.style(
                    f"Note: Adding geometry column '{geom_col}' to preserve GeoParquet format.",
                    fg="cyan",
                )
            )
            final_fields.append(geom_col)

    if verbose:
        click.echo(f"Final output fields: {', '.join(final_fields)}")

    # Get metadata before processing
    metadata, _ = get_parquet_metadata(input_parquet, verbose)

    # Create DuckDB connection
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_parquet))

    # Get total count
    total_count = con.execute(f"SELECT COUNT(*) FROM '{input_url}'").fetchone()[0]
    click.echo(f"Processing {total_count:,} features with {len(final_fields)} fields...")

    # Build the query with properly quoted field names
    quoted_fields = [f'"{f}"' for f in final_fields]
    select_clause = ", ".join(quoted_fields)

    query = f"""
        SELECT {select_clause}
        FROM '{input_url}'
    """

    # Write output
    write_parquet_with_metadata(
        con,
        query,
        output_parquet,
        original_metadata=metadata,
        compression=compression,
        compression_level=compression_level,
        row_group_size_mb=row_group_size_mb,
        row_group_rows=row_group_rows,
        custom_metadata=None,
        verbose=verbose,
        profile=profile,
    )

    click.echo(click.style(f"✓ Created output with {len(final_fields)} fields", fg="green"))
