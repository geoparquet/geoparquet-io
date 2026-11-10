"""The one source scan the tree guards share: each file parsed and walked once.

``tests/test_owner_guards.py`` (31 G1 arms, the size/cap/query guards) and
``tests/test_duckdb2_result_api_guard.py`` both read every module under
``geoparquet_io/`` (and the latter ``tests/``) as an AST. Each used to parse the
tree for itself and every arm walked it with ``ast.walk`` again; on CI that was
the bulk of the two files' ~400 worker-seconds. Here a file is parsed once per
process and walked once, on first use: the breadth-first node list and each
node's parent are kept, so a check iterates a list instead of re-walking the
tree.

Not a ``conftest.py``: plain helpers, imported by name. The leading underscore
keeps pytest from collecting it.
"""

from __future__ import annotations

import ast
import functools
from collections.abc import Iterable
from pathlib import Path

#: Attribute set on a parsed module's tree: the :class:`Source` it came from.
_SOURCE = "_guard_scan_source"
#: Attribute set on a scope by :func:`walk_scope`: its own nodes, cached.
_SCOPE_NODES = "_guard_scan_scope_nodes"

_FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)


class Source:
    """One parsed file: its text and tree, and (built on first use, then kept)
    its lines, its node list in ``ast.walk`` order and its parent map."""

    def __init__(self, path: Path, text: str, tree: ast.Module) -> None:
        self.path = path
        self.text = text
        self.tree = tree
        self._nodes: list[ast.AST] | None = None
        self._parents: dict[int, ast.AST] | None = None
        setattr(tree, _SOURCE, self)

    @functools.cached_property
    def lines(self) -> list[str]:
        return self.text.splitlines()

    @property
    def nodes(self) -> list[ast.AST]:
        """Every node in ``ast.walk``'s breadth-first order."""
        if self._nodes is None:
            self._index()
        return self._nodes

    @property
    def parents(self) -> dict[int, ast.AST]:
        """id(node) -> parent. The context and operator singletons
        (``ast.Load()`` and the like) are shared by every tree; theirs is the
        last parent seen, as with any map built this way."""
        if self._parents is None:
            self._index()
        return self._parents

    def _index(self) -> None:
        """The one walk: the node list and the parent map, built together."""
        nodes: list[ast.AST] = [self.tree]
        parents: dict[int, ast.AST] = {}
        append = nodes.append
        # ``ast.iter_child_nodes`` inlined (same fields, same order): the list
        # grows as it is read, which is ``ast.walk``'s breadth-first order.
        for node in nodes:
            for name in node._fields:
                value = getattr(node, name, None)
                if isinstance(value, ast.AST):
                    parents[id(value)] = node
                    append(value)
                elif value.__class__ is list:
                    for item in value:
                        if isinstance(item, ast.AST):
                            parents[id(item)] = node
                            append(item)
        self._nodes = nodes
        self._parents = parents


@functools.lru_cache(maxsize=4096)
def _parse(path: str, _mtime_ns: int, _size: int) -> Source:
    text = Path(path).read_text(encoding="utf-8")
    return Source(Path(path), text, ast.parse(text))


def parse(path: Path) -> Source:
    """*path* parsed, once per process while the file is unchanged."""
    stat = path.stat()
    return _parse(str(path), stat.st_mtime_ns, stat.st_size)


def python_files(*roots: Path) -> list[Path]:
    """Every ``*.py`` under *roots*, sorted per root."""
    return [p for root in roots for p in sorted(root.rglob("*.py"))]


def walk(tree: ast.AST) -> Iterable[ast.AST]:
    """``ast.walk(tree)``, from the node list the file's :class:`Source` keeps
    when *tree* is a parsed module (walked once, then reused by every check);
    a subtree is walked as usual."""
    source = getattr(tree, _SOURCE, None)
    return source.nodes if source is not None else ast.walk(tree)


def walk_scope(scope: ast.AST) -> list[ast.AST]:
    """``ast.walk`` of one scope's own code (nested functions and lambdas are
    scopes of their own), remembered on the scope: a checker asks for the same
    scope many times."""
    cached = getattr(scope, _SCOPE_NODES, None)
    if cached is not None:
        return cached
    found: list[ast.AST] = []
    stack = list(ast.iter_child_nodes(scope))
    while stack:
        node = stack.pop()
        found.append(node)
        if not isinstance(node, _FUNCTIONS):
            stack.extend(ast.iter_child_nodes(node))
    setattr(scope, _SCOPE_NODES, found)
    return found
