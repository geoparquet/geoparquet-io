"""Shared dispatch, staging and writing for spatial sorts."""

from contextlib import contextmanager

import duckdb

from geoparquet_io.core.add.bbox import prepare_streaming_bbox
from geoparquet_io.core.bbox_structure import get_bbox_advice
from geoparquet_io.core.common import add_bbox, get_parquet_metadata
from geoparquet_io.core.duckdb_utils import (
    describe_query,
    get_duckdb_connection,
    sql_path,
    uses_aws_profile,
)
from geoparquet_io.core.exceptions import RemoteAccessError
from geoparquet_io.core.file_utils import (
    handle_output_overwrite,
    resolve_file_url,
    scratch_path,
    unlink_released_file,
)
from geoparquet_io.core.geo_metadata import sanitized_carried_geo
from geoparquet_io.core.geometry_detection import resolve_geometry_column
from geoparquet_io.core.logging_config import debug, info, success, warn
from geoparquet_io.core.parquet_writer import resolve_output_geoparquet_version
from geoparquet_io.core.partition.reader import require_single_file
from geoparquet_io.core.remote import (
    get_remote_error_hint,
    is_remote_url,
    needs_httpfs,
    show_remote_read_message,
    validate_profile_for_urls,
)
from geoparquet_io.core.stream_io import open_input, write_output
from geoparquet_io.core.streaming import is_stdin, should_stream_output
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


def prepare_working_file(input_parquet, add_bbox_flag, verbose):
    """Prepare working file, adding bbox if needed.

    Returns:
        tuple: (working_parquet, temp_file_created, temp_file_path_or_none)
    """
    import shutil

    bbox_advice = get_bbox_advice(input_parquet, "bounds_calculation", verbose)

    if add_bbox_flag and not bbox_advice["has_bbox_column"]:
        info("\nAdding bbox column to enable fast bounds calculation...")
        temp_file = scratch_path()
        shutil.copy2(input_parquet, temp_file)
        add_bbox(temp_file, "bbox", verbose)
        success("Added bbox column for optimized processing")
        return temp_file, True, temp_file

    if bbox_advice["needs_warning"]:
        warn(f"\nWarning: {bbox_advice['message']}")
        if not add_bbox_flag:
            for suggestion in bbox_advice["suggestions"]:
                info(f"Tip: {suggestion}")

    return input_parquet, False, None


@contextmanager
def _file_source(input_path, output_path, add_bbox_flag, verbose, write, command):
    require_single_file(input_path, command)
    working, _, temp_file = prepare_working_file(input_path, add_bbox_flag, verbose)
    try:
        validate_profile_for_urls(write.remote_access().profile, input_path, output_path)
        show_remote_read_message(working, verbose)
        source = sql_path(resolve_file_url(working, verbose))
        metadata, _ = get_parquet_metadata(working, verbose)
        con = get_duckdb_connection(load_spatial=True, load_httpfs=needs_httpfs(working))
        try:
            yield source, metadata, con, working
        finally:
            con.close()
    finally:
        unlink_released_file(temp_file)


@uses_aws_profile
def run_sort(
    input_path,
    output_path,
    *,
    command,
    label,
    build_query,
    geometry_column,
    add_bbox_flag,
    verbose,
    write,
    overwrite,
):
    """Validate before dispatch, then build and write one ordering query.

    ``build_query`` owns only the ordering key; it returns (query, reordered).
    File queries retain their file witness for bounds and metadata policy.
    """
    streaming = is_stdin(input_path) or should_stream_output(output_path)
    if not should_stream_output(output_path):
        handle_output_overwrite(output_path, overwrite, input_path, remove=False)
    if not is_stdin(input_path):
        require_single_file(input_path, command)
    if should_stream_output(output_path):
        verbose = False
    context = (
        open_input(input_path, verbose=verbose, stream_output=should_stream_output(output_path))
        if streaming
        else _file_source(input_path, output_path, add_bbox_flag, verbose, write, command)
    )
    try:
        with context as opened:
            if streaming:
                source, metadata, _, con = opened
                described = describe_query(con, f"SELECT * FROM {source}")
                geom_col = resolve_geometry_column(
                    described,
                    geometry_column,
                    geo=sanitized_carried_geo(metadata),
                    where=input_path,
                )
                working = None
            else:
                source, metadata, con, working = opened
                geom_col = resolve_geometry_column(working, geometry_column, verbose=verbose)
            custom_metadata = None
            if streaming and add_bbox_flag:
                bbox_query, _, _, custom_metadata = prepare_streaming_bbox(
                    source,
                    con,
                    "bbox",
                    force=False,
                    geoparquet_version=write.geoparquet_version,
                    metadata=metadata,
                    geometry_column=geom_col,
                )
                source = f"({bbox_query})"
            query, reordered = build_query(con, source, geom_col, verbose, working)
            if streaming:
                write_output(
                    con,
                    query,
                    output_path,
                    original_metadata=metadata,
                    geometry_column=geom_col,
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
                    **write.funnel_kwargs(),
                )
            if not should_stream_output(output_path):
                if reordered:
                    success(f"Successfully reordered data using {label} to: {output_path}")
                else:
                    success(f"Wrote data without {label} ordering to: {output_path}")
    except duckdb.IOException as exc:
        if is_remote_url(input_path):
            hints = get_remote_error_hint(exc, input_path)
            raise RemoteAccessError(input_path, f"{hints}\n\nOriginal error: {exc}") from exc
        raise
