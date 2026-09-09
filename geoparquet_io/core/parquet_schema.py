"""Walking the flat Parquet schema listing returned by ``get_schema_info()``.

The listing is depth first: a struct's children follow their parent, so telling
a root-level column from a nested field means skipping whole subtrees rather
than pattern-matching names. Nothing in either listing shape renders a dotted
path, which is why a ``"." not in name`` filter silently kept every struct child.

Two shapes reach these helpers. DuckDB's ``parquet_schema()`` prefixes the
listing with the file's root group element -- every row carries a ``file_name``
and the root alone has no ``type`` -- while the pyarrow fast path starts straight
at the first column and always renders a ``type`` string.

This module deliberately imports nothing from the rest of the package: it sits
below ``duckdb_metadata``, ``duckdb_utils``, ``metadata_utils`` and ``validate``,
all of which share it.
"""


def schema_subtree_end(schema_info: list, index: int) -> int:
    """Index just past the depth-first subtree rooted at ``schema_info[index]``."""
    end = index + 1
    for _ in range(schema_info[index].get("num_children") or 0):
        if end >= len(schema_info):
            break
        end = schema_subtree_end(schema_info, end)
    return end


def schema_root_offset(schema_info: list) -> int:
    """Index at which the root-level columns start: 0 or 1.

    Skips DuckDB's leading root group element; the pyarrow shape has none.
    """
    if not schema_info:
        return 0
    first = schema_info[0]
    return 1 if "file_name" in first and first.get("type") is None else 0


def root_schema_columns(schema_info: list) -> list[dict]:
    """Root-level schema entries, struct children and the root group excluded.

    Entries with an empty name are dropped: the schema root element renders that
    way in some listings and is never a user-facing column.
    """
    columns = []
    index = schema_root_offset(schema_info)
    while index < len(schema_info):
        if schema_info[index].get("name"):
            columns.append(schema_info[index])
        index = schema_subtree_end(schema_info, index)
    return columns


def root_schema_index(schema_info: list, name: str) -> int | None:
    """Index of the root-level schema entry called ``name``, else ``None``.

    Scanning the listing for the first entry of a given name can return a
    *nested* field (say ``meta.bbox``) and shadow the real root column of the
    same name. Skipping whole subtrees is also what enforces the GeoParquet
    v1.1.0 rule that the covering bbox column is at the root of the schema.
    """
    index = schema_root_offset(schema_info)
    while index < len(schema_info):
        if schema_info[index].get("name") == name:
            return index
        index = schema_subtree_end(schema_info, index)
    return None


def schema_direct_children(schema_info: list, index: int) -> list[dict]:
    """Direct children of ``schema_info[index]``, grandchildren excluded."""
    children = []
    child = index + 1
    for _ in range(schema_info[index].get("num_children") or 0):
        if child >= len(schema_info):
            break
        children.append(schema_info[child])
        child = schema_subtree_end(schema_info, child)
    return children
