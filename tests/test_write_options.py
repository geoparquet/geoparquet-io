"""The write-options object (#1367, T5c): one value carries the write facts.

Every write path used to forward the same seven keywords (``compression``,
``compression_level``, ``row_group_size_mb``, ``row_group_rows``, ``profile``,
``geoparquet_version``, ``memory_limit``) from its public entry point through
each private driver to the funnel; pylint's duplicate-code check reported those
lists as 18 copies. ``parquet_writer.WriteOptions`` is the one value they travel
in now: built once at the entry point, expanded once at the funnel.
"""

from __future__ import annotations

import dataclasses
import inspect

import pytest

from geoparquet_io.core.parquet_writer import WriteOptions
from geoparquet_io.core.stream_io import execute_transform, write_output
from geoparquet_io.core.write_funnels import write_parquet_with_metadata

FIELDS = (
    "compression",
    "compression_level",
    "row_group_size_mb",
    "row_group_rows",
    "profile",
    "geoparquet_version",
    "memory_limit",
)


def test_the_fields_are_the_forwarded_write_keywords():
    assert tuple(f.name for f in dataclasses.fields(WriteOptions)) == FIELDS


def test_it_is_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        WriteOptions().compression = "GZIP"  # type: ignore[misc]


def test_every_default_is_the_funnels_own_default():
    """An unset field means what leaving the keyword out of the funnel means."""
    signature = inspect.signature(write_parquet_with_metadata)
    for name, value in WriteOptions().funnel_kwargs().items():
        assert value == signature.parameters[name].default, name


@pytest.mark.parametrize("funnel", [write_parquet_with_metadata, write_output, execute_transform])
def test_every_funnel_takes_every_field(funnel):
    assert set(FIELDS) <= set(inspect.signature(funnel).parameters)


def test_pick_reads_the_fields_by_name_and_ignores_the_rest():
    values = {
        "compression": "GZIP",
        "compression_level": 6,
        "row_group_size_mb": 64.0,
        "row_group_rows": None,
        "profile": "p",
        "geoparquet_version": "2.0",
        "memory_limit": "2GB",
        "verbose": True,
        "input_parquet": "in.parquet",
    }
    options = WriteOptions.pick(values)
    assert options == WriteOptions("GZIP", 6, 64.0, None, "p", "2.0", "2GB")
    assert options.funnel_kwargs() == {k: values[k] for k in FIELDS}


def test_pick_leaves_an_absent_field_at_its_default():
    assert WriteOptions.pick({"compression": "LZ4"}) == WriteOptions(compression="LZ4")
