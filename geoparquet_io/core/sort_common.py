"""Shared dispatch, staging and writing for all four sorts."""

from contextlib import ExitStack, contextmanager

import duckdb

from geoparquet_io.core.add.bbox import prepare_streaming_bbox
from geoparquet_io.core.bbox_structure import get_bbox_advice
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.duckdb_utils import (
    describe_query,
    get_duckdb_connection,
    sql_path,
    uses_aws_profile,
)
from geoparquet_io.core.exceptions import RemoteAccessError
from geoparquet_io.core.file_utils import (
    handle_output_overwrite,
    is_partition_path,
    resolve_file_url,
    unlink_released_file,
)
from geoparquet_io.core.geo_metadata import sanitized_carried_geo
from geoparquet_io.core.geometry_detection import (
    primary_geometry_column,
    resolve_geometry_column,
)
from geoparquet_io.core.logging_config import debug, info, success, warn
from geoparquet_io.core.parquet_writer import resolve_output_geoparquet_version
from geoparquet_io.core.partition.reader import (
    build_read_parquet_expr,
    raise_for_schema_mismatch,
    require_single_file,
)
from geoparquet_io.core.remote import (
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    show_remote_read_message,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import is_stdin, read_stdin_to_temp_file, should_stream_output
from geoparquet_io.core.write_funnels import write_parquet_with_metadata


@uses_aws_profile
def advised_output_version(
    input_parquet: str,
    geoparquet_version: str | None,
    verbose: bool,
    profile: str | None = None,
) -> str | None:
    """The GeoParquet version this run will actually write, or None if unknowable.

    Whether ``gpio sort hilbert`` and ``gpio sort str`` advise
    ``--geoparquet-version 2.0``, and whether they offer their row-group
    guidance at all, both hang off this answer -- so it has to be the *write's*
    answer. It asks the write facade the same question the write asks it, with
    the same two witnesses (the input file, then its carried metadata), and the
    facade's answer includes the default the write would fall back to.

    It used to re-implement that decision instead, reading only the carried
    ``geo`` key. Once the facade started consulting the file, the two answers
    diverged on exactly the input class the facade exists to fix: for a
    native-geo-only input this said "1.1" and advised switching to 2.0 over a
    file it was writing as 2.0 -- #738's bug arriving from the other side --
    while the row-group guidance, gated on 2.0, could never fire for it.

    Returns None rather than a default when the version cannot be established:
    a stdin stream (whose version the writer resolves from the stream's own
    metadata, not from anything visible here) or an input that will not open.
    Callers use None to stay silent. Defaulting instead is what produced the
    original wrong warning -- a failed read is not evidence of a 1.1 input.
    The facade cannot make that distinction on its own: it swallows a failed
    read and answers with the default, which is the guess. So the read stays
    here, as the gate on saying anything at all.
    """
    if geoparquet_version:
        return geoparquet_version
    if is_stdin(input_parquet):
        return None

    try:
        metadata, _ = get_parquet_metadata(input_parquet, verbose)
    except Exception as e:  # noqa: BLE001 - any read failure means "stay quiet"
        debug(f"Could not read {input_parquet} to resolve output version: {e}")
        return None
    return resolve_output_geoparquet_version(
        None,
        input_file=input_parquet,
        original_metadata=metadata,
        verbose=verbose,
    )


@contextmanager
def _file_source(input_path, verbose, allow_schema_diff):
    if allow_schema_diff is None:
        advice = get_bbox_advice(input_path, "bounds_calculation", verbose)
        if advice["needs_warning"]:
            warn(f"\nWarning: {advice['message']}")
            for suggestion in advice["suggestions"]:
                info(f"Tip: {suggestion}")
    show_remote_read_message(input_path, verbose)
    source = (
        sql_path(resolve_file_url(input_path, verbose))
        if allow_schema_diff is None
        else build_read_parquet_expr(
            input_path, allow_schema_diff=allow_schema_diff, verbose=verbose
        )
    )
    metadata, _ = get_parquet_metadata(input_path, verbose)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(input_path))
    try:
        yield source, metadata, con, input_path
    finally:
        con.close()


@uses_aws_profile
def run_sort(
    input_path,
    output_path,
    *,
    command,
    label,
    effective_version,
    build_query,
    geometry_column,
    add_bbox_flag,
    verbose,
    write,
    overwrite,
    build_key=None,
    prepare_input=None,
    allow_schema_diff=None,
    success_message=None,
):
    """Validate before dispatch, then build and write one ordering query.

    ``build_key(con, source, metadata, working)`` resolves the ordering key;
    the default resolves a geometry column. ``build_query`` receives that key
    and returns (query, reordered). ``prepare_input(path, verbose)`` supplies
    any required file-backed columns and returns (working_path, temporary_path).
    The runner stages stdin for that contract and owns both temporary files.
    ``allow_schema_diff=None`` requires a single file; a boolean accepts datasets.
    File queries retain their file witness for bounds and metadata policy.
    """
    if effective_version == "1.1":
        warn(
            f"{label.split()[0]} sorting to GeoParquet v1.1 provides no spatial filter pushdown benefit. "
            "Consider using --geoparquet-version 2.0 to enable native geo_bbox row group statistics."
        )
    stream_output = should_stream_output(output_path)
    streaming = is_stdin(input_path) or stream_output
    if not stream_output:
        handle_output_overwrite(output_path, overwrite, input_path, remove=False)
    validate_profile_for_urls(write.remote_access().profile, input_path, output_path)
    if not is_stdin(input_path) and allow_schema_diff is None:
        require_single_file(input_path, command)
    if stream_output:
        verbose = False
    try:
        with ExitStack() as stack:
            working_input = input_path
            if prepare_input is not None:
                if is_stdin(working_input):
                    working_input = read_stdin_to_temp_file(verbose)
                    stack.callback(unlink_released_file, working_input)
                working_input, temp_file = prepare_input(working_input, verbose)
                stack.callback(unlink_released_file, temp_file)
            context = (
                open_input(
                    working_input, verbose=verbose, stream_output=stream_output, preserve_wkb=True
                )
                if streaming
                else _file_source(
                    working_input,
                    verbose,
                    allow_schema_diff,
                )
            )
            opened = stack.enter_context(context)
            if streaming:
                source, metadata, _, con = opened
                working = None
            else:
                source, metadata, con, working = opened
            described = describe_query(con, f"SELECT * FROM {source}")
            primary = (
                primary_geometry_column(described, geo=sanitized_carried_geo(metadata))
                or geometry_column
            )
            custom_metadata = None
            if add_bbox_flag:
                bbox_query, _, _, custom_metadata = prepare_streaming_bbox(
                    source,
                    con,
                    "bbox",
                    force=False,
                    reuse_existing=True,
                    geoparquet_version=write.geoparquet_version,
                    metadata=metadata,
                    # Added bbox follows the primary, independently of the sort key.
                    geometry_column=primary,
                )
                source = f"({bbox_query})"
            key = (
                build_key(con, source, metadata, working)
                if build_key is not None
                else resolve_geometry_column(
                    working or described,
                    geometry_column,
                    geo=sanitized_carried_geo(metadata),
                    where=input_path,
                    verbose=verbose,
                )
            )
            query, reordered = build_query(con, source, key, verbose, working)
            if streaming:
                write_output(
                    con,
                    query,
                    output_path,
                    original_metadata=metadata,
                    geometry_column=primary,
                    preserve_wkb=True,
                    verbose=verbose,
                    custom_metadata=custom_metadata,
                    **write.funnel_kwargs(),
                )
            else:
                write_parquet_with_metadata(
                    con,
                    query,
                    output_path,
                    original_metadata=metadata,
                    verbose=verbose,
                    input_file=input_path,
                    geometry_info={"primary": primary} if primary else None,
                    custom_metadata=custom_metadata,
                    invalidate_derived_stats=is_partition_path(input_path),
                    **write.funnel_kwargs(),
                )
            if not stream_output:
                if success_message is not None:
                    success(f"{success_message} to: {output_path}")
                elif reordered:
                    success(f"Successfully reordered data using {label} to: {output_path}")
                else:
                    success(f"Wrote data without {label} ordering to: {output_path}")
    except duckdb.IOException as exc:
        if is_remote_url(input_path):
            hints = get_remote_error_hint(exc, input_path)
            raise RemoteAccessError(input_path, f"{hints}\n\nOriginal error: {exc}") from exc
        raise
    except Exception as exc:
        raise_for_schema_mismatch(exc, input_path)
        raise
