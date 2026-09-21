#!/usr/bin/env python3
"""Rewrite a Parquet file's footer key-value metadata without touching the data.

A Parquet footer sits at the end of the file and every offset it carries is
absolute from byte 0. So a footer key can be added, replaced or removed by
copying every byte below the footer verbatim and appending a new one: data
pages, dictionary pages, bloom filters, ``ColumnIndex`` and ``OffsetIndex``
all keep the positions the footer already names.

That is the difference between this module and a ``COPY`` rewrite, which is
what ``add bbox-metadata`` used to do to add one key (#1141). A rewrite
re-encodes every page, and a Parquet file does not record the compression
*level* it was written at, so the level cannot be carried over: a file written
at zstd 15 came back at DuckDB's default of 3, 11.6% bigger on the reported
fixture and 36% bigger on a real one. Nothing here decodes a page, so the
codec, the level, the encodings, the row-group boundaries, the row order and
the writer's identity are not gpio's to get wrong, and the work is
proportional to the footer rather than to the file.

Only the one field is re-encoded. ``FileMetaData`` is read far enough to find
field 5, ``key_value_metadata``, and its byte span; every other byte of the
footer is copied as it was. That needs a thrift *skip*, not a parser, which is
the short table of compact-protocol rules below.
"""

from __future__ import annotations

import os
import stat
import struct
from dataclasses import dataclass
from typing import BinaryIO

import pyarrow.parquet as pq

from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.logging_config import debug
from geoparquet_io.core.write_strategies import atomic_write

MAGIC = b"PAR1"
ENCRYPTED_MAGIC = b"PARE"

# The trailer: a 4-byte little-endian footer length, then the magic.
_TRAILER_SIZE = 8
_COPY_CHUNK = 8 * 1024 * 1024

# Thrift compact protocol type ids.
_BOOL_TRUE, _BOOL_FALSE, _BYTE, _I16, _I32, _I64 = 1, 2, 3, 4, 5, 6
_DOUBLE, _BINARY, _LIST, _SET, _MAP, _STRUCT, _UUID = 7, 8, 9, 10, 11, 12, 13

_BOOLEAN_TYPES = frozenset({_BOOL_TRUE, _BOOL_FALSE})
_VARINT_TYPES = frozenset({_I16, _I32, _I64})
# A boolean field carries its value in the type nibble, so it has no payload.
_FIXED_WIDTH = {_BOOL_TRUE: 0, _BOOL_FALSE: 0, _BYTE: 1, _DOUBLE: 8, _UUID: 16}

# FileMetaData.key_value_metadata, the only field this module rewrites.
_KV_FIELD_ID = 5
# KeyValue { 1: required string key; 2: optional string value }. Both fields
# are BINARY and each follows the previous id by one, so both headers are the
# same byte: delta 1 in the high nibble, type 8 in the low one.
_BINARY_DELTA_1 = (1 << 4) | _BINARY


class FooterPatchUnsupported(GeoParquetError):
    """This file's footer cannot be patched in place; a caller may fall back.

    Raised for a file that is not Parquet at all, for an encrypted footer, and
    for a footer whose thrift structure does not read cleanly. It never means
    the file was modified: the patch is staged beside the destination and only
    replaces it once it has been read back.
    """


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    """One unsigned LEB128 varint, and the position after it."""
    result = shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift > 63:
            raise FooterPatchUnsupported("a varint in the footer runs past 64 bits")


def _write_varint(value: int) -> bytes:
    """The unsigned LEB128 encoding of a non-negative integer."""
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if not value:
            out.append(byte)
            return bytes(out)
        out.append(byte | 0x80)


def _skip_value(buf: bytes, pos: int, type_id: int) -> int:
    """The position after a value of `type_id` written at `pos`."""
    if type_id in _FIXED_WIDTH:
        return pos + _FIXED_WIDTH[type_id]
    if type_id in _VARINT_TYPES:
        return _read_varint(buf, pos)[1]
    if type_id == _BINARY:
        length, pos = _read_varint(buf, pos)
        return pos + length
    if type_id in (_LIST, _SET):
        return _skip_collection(buf, pos)
    if type_id == _MAP:
        return _skip_map(buf, pos)
    if type_id == _STRUCT:
        return _skip_struct(buf, pos)
    raise FooterPatchUnsupported(f"unknown thrift type id {type_id} in the footer")


def _skip_element(buf: bytes, pos: int, type_id: int) -> int:
    """As `_skip_value`, for an element of a list, set or map.

    A boolean is the one type encoded differently inside a collection: there is
    no field header to carry it, so it takes a byte of its own.
    """
    if type_id in _BOOLEAN_TYPES:
        return pos + 1
    return _skip_value(buf, pos, type_id)


def _skip_collection(buf: bytes, pos: int) -> int:
    """The position after a list or set: a size/type byte, then the elements."""
    header = buf[pos]
    pos += 1
    count, element_type = header >> 4, header & 0x0F
    if count == 15:
        count, pos = _read_varint(buf, pos)
    for _ in range(count):
        pos = _skip_element(buf, pos, element_type)
    return pos


def _skip_map(buf: bytes, pos: int) -> int:
    """The position after a map: size, one key/value type byte, then pairs."""
    count, pos = _read_varint(buf, pos)
    if not count:
        return pos
    types = buf[pos]
    pos += 1
    key_type, value_type = types >> 4, types & 0x0F
    for _ in range(count):
        pos = _skip_element(buf, pos, key_type)
        pos = _skip_element(buf, pos, value_type)
    return pos


def _skip_struct(buf: bytes, pos: int) -> int:
    """The position after a nested struct, its STOP byte included."""
    return _struct_fields(buf, pos)[1] + 1


# One footer key and its value. The value is an optional thrift field, so a
# file may hold a key with none; that is kept as it was rather than filled in.
_Entry = tuple[bytes, bytes | None]


@dataclass(frozen=True)
class _Field:
    """One field of a thrift struct, located in the buffer that holds it."""

    field_id: int
    type_id: int
    start: int  # first byte of the field header
    value_start: int  # first byte of the value
    end: int  # one past the last byte of the value


def _struct_fields(buf: bytes, pos: int = 0) -> tuple[list[_Field], int]:
    """Every field of the struct at `pos`, and the offset *of* its STOP byte.

    The STOP byte is not consumed: `_rewrite_kv_field` inserts ahead of it, and
    `_skip_struct` is the one caller that steps over it.
    """
    fields: list[_Field] = []
    previous_id = 0
    while buf[pos]:
        start = pos
        header = buf[pos]
        pos += 1
        type_id, delta = header & 0x0F, header >> 4
        if delta:
            field_id = previous_id + delta
        else:
            # Long form: the field id follows as a zigzag varint.
            zigzag, pos = _read_varint(buf, pos)
            field_id = (zigzag >> 1) ^ -(zigzag & 1)
        value_start = pos
        pos = _skip_value(buf, pos, type_id)
        fields.append(_Field(field_id, type_id, start, value_start, pos))
        previous_id = field_id
    return fields, pos


def _field_header(field_id: int, previous_id: int, type_id: int) -> bytes:
    """A field header, short form where the delta fits in its nibble.

    `type_id` is passed through rather than derived, because for a boolean it
    is the value.
    """
    delta = field_id - previous_id
    if 1 <= delta <= 15:
        return bytes([(delta << 4) | type_id])
    return bytes([type_id]) + _write_varint((field_id << 1) ^ (field_id >> 15))


def _encode_binary_field(payload: bytes) -> bytes:
    """A BINARY field whose id is one past the previous one."""
    return bytes([_BINARY_DELTA_1]) + _write_varint(len(payload)) + payload


def _encode_key_value(entry: _Entry) -> bytes:
    """One `KeyValue` struct. The value is optional and stays absent if it was."""
    encoded = _encode_binary_field(entry[0])
    if entry[1] is not None:
        encoded += _encode_binary_field(entry[1])
    return encoded + b"\x00"


def _encode_kv_list(entries: list[_Entry]) -> bytes:
    """The `list<KeyValue>` value of `FileMetaData.key_value_metadata`."""
    items = b"".join(_encode_key_value(entry) for entry in entries)
    if len(entries) < 15:
        return bytes([(len(entries) << 4) | _STRUCT]) + items
    return bytes([0xF0 | _STRUCT]) + _write_varint(len(entries)) + items


def _decode_key_value(buf: bytes, pos: int) -> tuple[_Entry, int]:
    """One `KeyValue` struct, and the position after it."""
    fields, stop = _struct_fields(buf, pos)
    parts: dict[int, bytes] = {}
    for field in fields:
        if field.type_id != _BINARY:
            raise FooterPatchUnsupported(
                f"a key-value entry holds thrift type {field.type_id} where a string was expected"
            )
        length, start = _read_varint(buf, field.value_start)
        parts[field.field_id] = buf[start : start + length]
    return (parts.get(1, b""), parts.get(2)), stop + 1


def _decode_kv_list(buf: bytes, pos: int) -> list[_Entry]:
    """Every `KeyValue` the field holds, in file order, duplicates included."""
    header = buf[pos]
    pos += 1
    count, element_type = header >> 4, header & 0x0F
    if count == 15:
        count, pos = _read_varint(buf, pos)
    if count and element_type != _STRUCT:
        raise FooterPatchUnsupported(
            f"key_value_metadata holds thrift type {element_type} where structs were expected"
        )
    entries = []
    for _ in range(count):
        entry, pos = _decode_key_value(buf, pos)
        entries.append(entry)
    return entries


def _normalized(updates: dict[str, str | bytes | None]) -> dict[bytes, bytes | None]:
    """`updates` with str keys and values encoded, `None` left as the deletion."""
    return {
        (key.encode("utf-8") if isinstance(key, str) else key): (
            value.encode("utf-8") if isinstance(value, str) else value
        )
        for key, value in updates.items()
    }


def _apply_updates(entries: list[_Entry], updates: dict[str, str | bytes | None]) -> list[_Entry]:
    """`entries` with `updates` applied in place, then anything new appended.

    A key the file carries twice, which the format allows and a writer can
    produce by accident, is written once and its later copies dropped: leaving
    two values for a key this call has just set would make the result depend on
    which one the reader picks. Untouched keys keep their position, their bytes
    and their duplicates.
    """
    wanted = _normalized(updates)
    result: list[_Entry] = []
    written: set[bytes] = set()

    for key, value in entries:
        if key not in wanted:
            result.append((key, value))
            continue
        replacement = wanted[key]
        if replacement is None or key in written:
            continue
        result.append((key, replacement))
        written.add(key)

    present = {key for key, _ in entries}
    result.extend(
        (key, value) for key, value in wanted.items() if value is not None and key not in present
    )
    return result


def _insert_kv_field(footer: bytes, fields: list[_Field], index: int, value: bytes) -> bytes:
    """Splice the field in ahead of `fields[index]`, whose delta must be redone."""
    following = fields[index]
    previous_id = fields[index - 1].field_id if index else 0
    header = _field_header(_KV_FIELD_ID, previous_id, _LIST)
    following_header = _field_header(following.field_id, _KV_FIELD_ID, following.type_id)
    return (
        footer[: following.start]
        + header
        + value
        + following_header
        + footer[following.value_start :]
    )


def _rewrite_kv_field(footer: bytes, updates: dict[str, str | bytes | None]) -> bytes:
    """The footer with `key_value_metadata` rewritten, everything else verbatim."""
    fields, stop = _struct_fields(footer)

    for index, field in enumerate(fields):
        if field.field_id == _KV_FIELD_ID:
            value = _encode_kv_list(
                _apply_updates(_decode_kv_list(footer, field.value_start), updates)
            )
            # The id is unchanged, so the header byte and the next field's
            # delta both still hold: only the value moves.
            return footer[: field.value_start] + value + footer[field.end :]
        if field.field_id > _KV_FIELD_ID:
            entries = _apply_updates([], updates)
            if not entries:
                return footer
            return _insert_kv_field(footer, fields, index, _encode_kv_list(entries))

    # The field is absent: add it, unless there would be nothing in it. An
    # empty list is legal but it is not what the file said, and a call that
    # removes a key the file never had should leave the footer alone.
    entries = _apply_updates([], updates)
    if not entries:
        return footer
    previous_id = fields[-1].field_id if fields else 0
    header = _field_header(_KV_FIELD_ID, previous_id, _LIST)
    return footer[:stop] + header + _encode_kv_list(entries) + footer[stop:]


def _patch_footer(footer: bytes, updates: dict[str, str | bytes | None]) -> bytes:
    """`_rewrite_kv_field`, with a malformed footer named as unsupported."""
    try:
        return _rewrite_kv_field(footer, updates)
    except FooterPatchUnsupported:
        raise
    except (IndexError, ValueError, OverflowError) as exc:
        raise FooterPatchUnsupported(
            f"the footer's thrift structure could not be read: {exc}"
        ) from exc


def _footer_span(handle: BinaryIO, size: int, parquet_file: str) -> tuple[int, int]:
    """Where the footer starts and how long it is."""
    if size < len(MAGIC) + _TRAILER_SIZE:
        raise FooterPatchUnsupported(f"{parquet_file} is too short to be a Parquet file")

    handle.seek(size - _TRAILER_SIZE)
    trailer = handle.read(_TRAILER_SIZE)
    magic = trailer[4:]
    if magic == ENCRYPTED_MAGIC:
        raise FooterPatchUnsupported(
            f"{parquet_file} has an encrypted footer (PARE), which gpio cannot rewrite"
        )
    if magic != MAGIC:
        raise FooterPatchUnsupported(f"{parquet_file} does not end with the Parquet magic bytes")

    (footer_length,) = struct.unpack("<I", trailer[:4])
    footer_start = size - _TRAILER_SIZE - footer_length
    if footer_start < len(MAGIC):
        raise FooterPatchUnsupported(
            f"{parquet_file} declares a {footer_length}-byte footer, longer than the file"
        )
    return footer_start, footer_length


def _copy_below_footer(source: BinaryIO, sink: BinaryIO, footer_start: int) -> None:
    """Every byte up to the footer, unread and unchanged."""
    source.seek(0)
    remaining = footer_start
    while remaining:
        chunk = source.read(min(_COPY_CHUNK, remaining))
        if not chunk:
            raise FooterPatchUnsupported("the file ended before its footer began")
        sink.write(chunk)
        remaining -= len(chunk)


def _inherit_mode(staged: str, parquet_file: str, destination: str) -> None:
    """Give the staged file the mode it is about to replace.

    `atomic_write` stages through `mkstemp`, which creates 0600. Replacing a
    published file with one nobody else can read is not a metadata edit, so the
    destination's own mode wins, falling back to the input's when the
    destination is new.
    """
    source = destination if os.path.exists(destination) else parquet_file
    os.chmod(staged, stat.S_IMODE(os.stat(source).st_mode))


def _file_stamp(parquet_file: str) -> tuple[int, int, int]:
    """What identifies this version of the file: inode, size, modification time."""
    info = os.stat(parquet_file)
    return info.st_ino, info.st_size, info.st_mtime_ns


def _changed(parquet_file: str) -> FooterPatchUnsupported:
    return FooterPatchUnsupported(
        f"{parquet_file} changed while it was being copied; nothing was written"
    )


def _refuse_if_the_pages_changed(
    parquet_file: str,
    source: BinaryIO,
    footer_start: int,
    original: bytes,
) -> None:
    """Refuse a copy taken from pages that were rewritten under it.

    Another process rewriting the input in place, between the footer being read
    and the pages being copied, would leave the staged file pairing one
    version's footer with another version's data. This re-reads the footer
    through the handle the copy came from, so it sees that inode as the copy
    saw it, whatever the path now points at.
    """
    source.seek(footer_start)
    if source.read(len(original)) != original:
        raise _changed(parquet_file)


def _refuse_if_the_file_changed(parquet_file: str, stamp: tuple[int, int, int]) -> None:
    """Refuse to publish over an input that is no longer the one that was read.

    `os.replace` is atomic but it is not a compare-and-swap: it will happily
    overwrite a version of the file this call never saw, which for an in-place
    patch means publishing over another writer's work. gpio coordinates writers
    nowhere and an advisory lock taken here would bind only other calls to this
    function, not the rewrite paths or whatever else holds the file, so this
    checks instead of locking.

    It is the last thing done before the replace, which leaves a window one
    syscall wide. Closing that window needs a lock every writer takes, or a
    conditional rename the platform does not offer.
    """
    if _file_stamp(parquet_file) != stamp:
        raise _changed(parquet_file)


def _read_metadata_or_refuse(parquet_file: str) -> pq.FileMetaData:
    """The file's own metadata, as the baseline the patch is checked against."""
    try:
        return pq.read_metadata(parquet_file)
    except Exception as exc:  # pyarrow raises its own error types here
        raise FooterPatchUnsupported(
            f"{parquet_file}: its footer could not be read: {exc}"
        ) from exc


def _verify_data_untouched(before: pq.FileMetaData, patched_file: str) -> None:
    """Read the staged footer back and refuse it if it moved any data.

    The splice cannot move a page, so this is a guard on the thrift surgery
    itself rather than on the copy: a mis-skipped field would produce a footer
    that either fails to parse here or no longer describes the same row groups.
    """
    try:
        after = pq.read_metadata(patched_file)
    except Exception as exc:  # pyarrow raises its own error types here
        raise FooterPatchUnsupported(f"the patched footer could not be read back: {exc}") from exc

    shape_before = (before.num_rows, before.num_row_groups, before.num_columns)
    shape_after = (after.num_rows, after.num_row_groups, after.num_columns)
    if shape_before != shape_after:
        raise FooterPatchUnsupported(
            f"the patched footer describes {shape_after} where the input had {shape_before}"
        )

    for index in range(before.num_row_groups):
        group_before, group_after = before.row_group(index), after.row_group(index)
        if group_before.total_byte_size != group_after.total_byte_size:
            raise FooterPatchUnsupported(f"row group {index} changed size in the patched footer")
        if before.num_columns and (
            group_before.column(0).data_page_offset != group_after.column(0).data_page_offset
        ):
            raise FooterPatchUnsupported(f"row group {index} moved in the patched footer")


def patch_footer_kv(
    parquet_file: str,
    updates: dict[str, str | bytes | None],
    *,
    output_file: str | None = None,
    verbose: bool = False,
) -> None:
    """Add, replace or remove footer key-value metadata, keeping the data bytes.

    The result is the input's bytes up to the footer, unchanged, followed by a
    footer whose `key_value_metadata` carries `updates` and whose every other
    field is copied verbatim. Compression codec and level, encodings, bloom
    filters, the page index, row-group boundaries and row order are all part of
    the bytes that are copied, so none of them can change.

    The copy is staged beside the destination and moved into place, so the
    input is never edited where it lies and a failure leaves it alone. That
    needs room for a second copy of the file while the call runs. An in-place
    call that would change nothing writes nothing at all.

    An input that changes while it is being copied, or between the copy and the
    replace, is refused rather than published: see `_refuse_if_the_pages_changed`
    and `_refuse_if_the_file_changed`. Both are checks, not locks.

    Args:
        parquet_file: Path to a local Parquet file. Only read.
        updates: Keys to set, as `str` or `bytes`; a `None` value removes a key.
            Keys the file already carries and `updates` does not name are kept.
        output_file: Where to write the result. Defaults to the input, replaced
            atomically once the new footer has been read back.
        verbose: Report the footer sizes.

    Raises:
        FooterPatchUnsupported: If the file is not Parquet, has an encrypted
            footer, or has a footer this cannot read. The destination is left
            as it was.
        OSError: If the file cannot be read or the destination written.
    """
    destination = output_file or parquet_file
    size = os.path.getsize(parquet_file)

    with open(parquet_file, "rb") as source:
        # The span check comes first so a file that is not Parquet at all is
        # refused by name, rather than through whatever pyarrow says about it.
        footer_start, footer_length = _footer_span(source, size, parquet_file)
        source.seek(footer_start)
        original = source.read(footer_length)
    stamp = _file_stamp(parquet_file)

    footer = _patch_footer(original, updates)
    if footer == original and destination == parquet_file:
        # Nothing to write, and copying a file onto itself to say so would cost
        # a full pass over it.
        return

    before = _read_metadata_or_refuse(parquet_file)
    if verbose:
        debug(
            f"Footer: {footer_length:,} bytes in, {len(footer):,} out; "
            f"{footer_start:,} bytes of data copied unchanged"
        )

    with atomic_write(destination) as staged:
        # The input is opened for the copy and closed again before the staged
        # file is moved over it: Windows refuses to replace an open file.
        with open(parquet_file, "rb") as source, open(staged, "wb") as sink:
            _copy_below_footer(source, sink, footer_start)
            sink.write(footer)
            sink.write(struct.pack("<I", len(footer)))
            sink.write(MAGIC)
            _refuse_if_the_pages_changed(parquet_file, source, footer_start, original)
        _verify_data_untouched(before, staged)
        _inherit_mode(staged, parquet_file, destination)
        # Last, so that the gap between deciding the input is still the one
        # that was read and `atomic_write` replacing it is a single syscall.
        _refuse_if_the_file_changed(parquet_file, stamp)
