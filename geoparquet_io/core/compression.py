"""The one codec list and the one level policy every write path resolves through.

:func:`resolve_codec` turns what a caller asked for (a codec name in any case,
a level or ``None``, and whether this is an ordinary write or a ``gpio
process`` write) into a :class:`ResolvedCodec`: the canonical codec name, the
level that write uses, and the exact option form each encoder takes -- the
``COMPRESSION``/``COMPRESSION_LEVEL`` clauses of a DuckDB ``COPY`` and the
``compression``/``compression_level`` kwargs of a pyarrow writer. No write path
keeps its own codec map, allowlist or default level (#1367).

Before this owner, an unset level meant 15 in the Arrow funnel, 9 in ``gpio
process``, DuckDB's own 3 on every plain ``COPY`` (#1335) and pyarrow's
default on the rewrites nobody had patched; ``none`` became ``UNCOMPRESSED``
on one path and ``NONE`` on another, and one rewriter accepted ``LZ4_RAW``
that every validator refused.

This is deliberately *not* part of the write facade in ``core/parquet_writer.py``:
that module owns the decisions the write paths were disagreeing about, and
compression already had its own owner.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.logging_config import warn

#: The codecs gpio writes, by canonical name. The only allowed-codec list in
#: the package: the CLI's ``--compression`` choice is built from it. ``LZ4``
#: is written as Parquet's ``LZ4_RAW`` codec by both encoders (and reported as
#: ``LZ4`` in the footer), so ``LZ4_RAW`` itself is not a name gpio accepts.
ALLOWED_CODECS = ("ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED")

#: Spellings accepted for a canonical codec. ``none`` is pyarrow's name for
#: ``UNCOMPRESSED``; both mean the same codec everywhere.
CODEC_ALIASES = {"NONE": "UNCOMPRESSED"}

#: Every name :func:`resolve_codec` accepts, canonical names first.
ACCEPTED_CODEC_NAMES = ALLOWED_CODECS + tuple(CODEC_ALIASES)

#: The codec an unset name resolves to.
DEFAULT_CODEC = "ZSTD"

#: Footer codec names that are another spelling of a codec gpio writes:
#: DuckDB reports pyarrow's and its own ``LZ4`` output as ``LZ4_RAW``.
_FOOTER_ALIASES = {"LZ4_RAW": "LZ4"}

#: One ``(min, max, default)`` per codec that takes a level. An unset level
#: resolves from here, so this is where the global default is written down.
COMPRESSION_RANGES = {
    "GZIP": (1, 9, 6),
    "ZSTD": (1, 22, 15),
    "BROTLI": (1, 11, 11),
}

#: The widest level any codec takes; the CLI's first bound before the codec
#: is known.
MAX_COMPRESSION_LEVEL = max(hi for _, hi, _ in COMPRESSION_RANGES.values())

#: What the ``gpio process`` commands resolve an unset *ZSTD* level to, in
#: place of the global 15.
#:
#: Measured on their own output tables (#1299, benchmarks on #1195): level 9
#: writes 2.8-8.2x faster than 15 for at most 4.9% more bytes, and on a
#: 217k-row polygonize table it is both faster *and* smaller -- level 15's
#: long-range matching buys nothing on a column of short, high-entropy WKB
#: blobs. Below 9 the size cost stops being noise: level 3 is 15-20% larger on
#: geometry-heavy tables and level 1 is 34-35% larger, the wrong trade for
#: files people host and pay egress on.
#:
#: These commands compute a whole table and then write it, so the write is a
#: large share of their wall clock -- 67-90% on the raster two. Every other
#: write path keeps 15, the DuckDB ``COPY`` paths included (#1335).
PROCESS_ZSTD_LEVEL = 9

#: The level DuckDB's Parquet ``COPY`` writes for a codec it takes no level for:
#: miniz's ``MZ_DEFAULT_LEVEL`` for GZIP and ``BROTLI_DEFAULT_QUALITY`` for BROTLI
#: (``extension/parquet/column_writer.cpp`` in DuckDB 1.5.6 and 2.0.0). gpio's
#: GZIP and BROTLI defaults in :data:`COMPRESSION_RANGES` are these same levels,
#: so an unset level writes the same on a COPY and a pyarrow write.
DUCKDB_COPY_FIXED_LEVELS = {"GZIP": 6, "BROTLI": 11}

#: ``gpio process`` defaults, where they differ from :data:`COMPRESSION_RANGES`.
_PROCESS_DEFAULTS = {"ZSTD": PROCESS_ZSTD_LEVEL}

WriteContext = Literal["write", "process"]
_CONTEXTS = ("write", "process")


def _invalid_codec(name) -> InvalidParameterError:
    return InvalidParameterError(
        "compression",
        f"Invalid compression '{name}'. Must be one of: {', '.join(ALLOWED_CODECS)}",
    )


def _check_level(codec: str, level) -> None:
    """Refuse a level the codec cannot take; the level is formatted into SQL."""
    if codec not in COMPRESSION_RANGES:
        if level is not None:
            raise InvalidParameterError("compression_level", f"{codec} takes no level")
        return
    # ``bool`` is an ``int`` subclass, and ``COMPRESSION_LEVEL True`` is not
    # something a caller meant. Anything but an int is refused here, never
    # interpolated: ``COMPRESSION_LEVEL`` takes no parameter binding.
    if isinstance(level, bool) or not isinstance(level, int):
        raise InvalidParameterError(
            "compression_level", f"expected an integer level, got {level!r}"
        )
    min_level, max_level, _ = COMPRESSION_RANGES[codec]
    if not min_level <= level <= max_level:
        raise InvalidParameterError(
            "compression_level",
            f"{codec} compression level must be between {min_level} and {max_level}, got {level}",
        )


@dataclass(frozen=True)
class ResolvedCodec:
    """A validated codec and the level one write uses with it.

    ``level`` is ``None`` exactly when the codec takes no level. The
    constructor validates both, so no instance can carry a value that is not a
    codec gpio writes or a level outside its range -- the clauses below are
    interpolated into SQL. Build one with :func:`resolve_codec`.
    """

    name: str
    level: int | None

    def __post_init__(self) -> None:
        if self.name not in ALLOWED_CODECS:
            raise _invalid_codec(self.name)
        _check_level(self.name, self.level)

    @property
    def description(self) -> str:
        """``ZSTD:15`` for a levelled codec, the bare name otherwise."""
        return self.name if self.level is None else f"{self.name}:{self.level}"

    @property
    def copy_level(self) -> int | None:
        """The level a DuckDB ``COPY`` with these clauses actually writes.

        ZSTD writes :attr:`level`; GZIP and BROTLI write DuckDB's fixed level
        for them (:data:`DUCKDB_COPY_FIXED_LEVELS`), whatever was asked.
        """
        return DUCKDB_COPY_FIXED_LEVELS.get(self.name, self.level)

    def duckdb_copy_options(self) -> list[str]:
        """The ``COMPRESSION`` (and ``COMPRESSION_LEVEL``) clauses of a ``COPY``.

        DuckDB takes ``COMPRESSION_LEVEL`` for ZSTD only -- naming any other
        codec with a level is a binder error -- so GZIP and BROTLI write at
        DuckDB's fixed level for them, and a level that differs from it is
        warned about rather than silently dropped. A ZSTD level is always
        emitted: leaving it out is not "the default", it is DuckDB's 3 (#1335).
        """
        options = [f"COMPRESSION {self.name}"]
        if self.name == "ZSTD":
            options.append(f"COMPRESSION_LEVEL {self.level}")
        elif self.copy_level != self.level:
            warn(
                f"DuckDB writes {self.name} at level {self.copy_level} on this path, "
                f"not {self.level}: its COPY takes a level for ZSTD only."
            )
        return options

    def pyarrow_kwargs(self) -> dict:
        """``compression`` (and ``compression_level``) for a pyarrow writer.

        ``UNCOMPRESSED`` is pyarrow's ``None``. The level is always passed for
        a levelled codec: an omitted one is pyarrow's own default (ZSTD 1).
        """
        kwargs: dict = {"compression": None if self.name == "UNCOMPRESSED" else self.name.lower()}
        if self.level is not None:
            kwargs["compression_level"] = self.level
        return kwargs


def _canonical_codec(name: str | None) -> str:
    if name is None:
        return DEFAULT_CODEC
    if not isinstance(name, str) or name.upper() not in ACCEPTED_CODEC_NAMES:
        raise _invalid_codec(name)
    upper = name.upper()
    return CODEC_ALIASES.get(upper, upper)


def _default_level(codec: str, context: WriteContext) -> int | None:
    if codec not in COMPRESSION_RANGES:
        return None
    if context == "process":
        return _PROCESS_DEFAULTS.get(codec, COMPRESSION_RANGES[codec][2])
    return COMPRESSION_RANGES[codec][2]


def resolve_codec(
    name: str | None,
    level: int | None,
    *,
    context: WriteContext,
    verbose: bool = False,
) -> ResolvedCodec:
    """Validate a codec and level once and resolve them for one write.

    Args:
        name: Codec name in any case; ``none`` is ``UNCOMPRESSED``. ``None``
            means the default, ZSTD.
        level: The caller's level, or ``None`` for the codec's default. Ignored
            (and, with ``verbose``, reported) for codecs that take none.
        context: ``"write"`` for every ordinary write (ZSTD 15), ``"process"``
            for the ``gpio process`` commands (ZSTD :data:`PROCESS_ZSTD_LEVEL`).
        verbose: Report a level given to a codec that takes none.

    Raises:
        InvalidParameterError: an unknown codec (``LZ4_RAW`` included), or a
            level that is not an integer inside the codec's range.
    """
    if context not in _CONTEXTS:
        raise ValueError(f"context must be one of {_CONTEXTS}, got {context!r}")
    codec = _canonical_codec(name)
    if codec not in COMPRESSION_RANGES:
        if verbose and level is not None and codec != "UNCOMPRESSED":
            warn(f"Note: {codec} does not support compression levels. Ignoring level {level}.")
        return ResolvedCodec(codec, None)
    return ResolvedCodec(codec, _default_level(codec, context) if level is None else level)


def codec_from_footer(
    footer_codec: str | None, level: int | None, *, context: WriteContext = "write"
) -> ResolvedCodec:
    """The codec a rewrite keeps, given the name a Parquet footer reports.

    A rewrite that mirrors its input's codec reads the name off the footer
    (pyarrow and DuckDB spell ``LZ4`` as ``LZ4_RAW`` there). A codec gpio does
    not write (``LZO``), or no name at all, falls to the default. Parquet
    records no level, so ``level`` (or the codec's default) is the one used.
    """
    upper = (footer_codec or "").upper()
    upper = _FOOTER_ALIASES.get(upper, upper)
    return resolve_codec(upper if upper in ACCEPTED_CODEC_NAMES else None, level, context=context)


def resolve_process_compression_level(compression, compression_level):
    """The level a ``gpio process`` write uses: ``resolve_codec(..., context="process").level``.

    Kept for its callers in ``core/process/`` until they call
    :func:`resolve_codec` themselves (T2f).
    """
    return resolve_codec(compression, compression_level, context="process").level
