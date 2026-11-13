"""Deprecated name aliases on the Python API (#1436, decision 4).

A twin whose two legs were spelled differently collapses to one canonical name,
and the old spelling stays for one minor release as an alias built here. The
mechanism is the one the per-keyword deprecations already use --
:func:`~geoparquet_io.core.remote.remote_options_from_keywords` and
:func:`~geoparquet_io.core.check_options.check_options_from_keywords` -- raised
from a keyword to a whole name: one ``DeprecationWarning`` per alias per
process, naming the replacement and the horizon "the next minor release".

An alias carries :data:`ALIAS_ATTRIBUTE`, whose value is the canonical name it
forwards to. `tests/parity_registry.py` skips a callable that carries it, so the
parity census compares canonical names only, and its ``DEPRECATED_ALIASES``
records every one with its reason.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

from geoparquet_io.core.deprecation import warn_deprecated

#: The attribute an alias carries: the canonical name it forwards to. Read by
#: the parity census, so a new alias needs no second list to stay accounted for.
ALIAS_ATTRIBUTE = "__gpio_deprecated_alias__"


def deprecated_alias(canonical: Callable[..., Any], *, old: str, new: str) -> Callable[..., Any]:
    """*canonical*, wrapped as the deprecated spelling *old*.

    The alias forwards every argument and returns what *canonical* returns, so
    the two names are the same operation; the only difference is the
    ``DeprecationWarning`` the old one raises on its first call in a process.

    ``functools.wraps`` keeps the signature, the module and ``__wrapped__``, so
    ``inspect.signature`` and the public-API snapshot still see the alias as a
    function of its own module; only ``__name__``/``__qualname__`` and the
    docstring are re-pointed at the old name.

    Args:
        canonical: The function or method the alias forwards to.
        old: The deprecated dotted name, as a caller writes it
            (``"ops.read_bigquery"``), used in the warning and as the key of
            the warned-once ledger.
        new: The canonical dotted name, named in the warning as the replacement.

    Returns:
        The alias, carrying :data:`ALIAS_ATTRIBUTE` set to *new*.
    """

    @functools.wraps(canonical)
    def alias(*args: Any, **kwargs: Any) -> Any:
        warn_deprecated({old: (f"{old}()", f"call {new}() instead")})
        return canonical(*args, **kwargs)

    alias.__name__ = old.rsplit(".", 1)[-1]
    alias.__qualname__ = old
    alias.__doc__ = (
        f"Deprecated alias of ``{new}``, removed in the next minor release.\n\n"
        f"{canonical.__doc__ or ''}"
    )
    alias.__dict__[ALIAS_ATTRIBUTE] = new
    return alias


def alias_target(value: Any) -> str | None:
    """The canonical name *value* is a deprecated alias of, or None.

    Unwraps a ``classmethod``/``staticmethod`` descriptor's ``__func__`` first,
    so a marked classmethod read off a class is recognised too.
    """
    return getattr(getattr(value, "__func__", value), ALIAS_ATTRIBUTE, None)
