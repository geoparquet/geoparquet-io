"""The check-options object: what a check looks at and what its fix writes.

The third per-concern options object (#1436), after
:class:`~geoparquet_io.core.parquet_writer.WriteOptions` (#761, the write
concern) and :class:`~geoparquet_io.core.remote.RemoteOptions` (the
remote-access concern).

The check concern is spread across three doors that are supposed to be one
surface: the nine ``gpio check`` commands, the ``gpio.check_*`` path functions
in :mod:`geoparquet_io.api.check`, and ``Table.fix``. Each door spelled the
same facts for itself -- the size band to judge against, how many rows to
sample, and which repairs to write where -- so they drifted: ``--sample-rows``
and ``sample_rows=``, ``--random-sample-size`` and ``sample_size=``,
``--fix-output`` and ``output=``, and four copies of the ``--fix`` /
``--fix-output`` / ``--no-backup`` trio in the CLI alone. This module is the one
owner of those facts, and of the default for each one.

What it deliberately does **not** carry:

* ``--verbose`` and ``--strict`` are output formatting and exit-status policy,
  owned by ``cli/decorators.py``. The API returns a
  :class:`~geoparquet_io.api.check.CheckResult` and the caller reads
  ``verdict()``, so neither crosses to the Python side at all.
* ``--all-files`` and ``--sample-files`` select which files of a partitioned
  dataset to read. A ``Table`` is one table, so these have no API twin either.

Both are recorded as one-sided classes in ``tests/parity_registry.py``
(``cli_plumbing`` and ``multi_file_check``), which is the statement that they
are CLI-only on purpose.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from typing import TYPE_CHECKING, Any

from geoparquet_io.core.check_bbox_values import DEFAULT_BBOX_SAMPLE_ROWS
from geoparquet_io.core.remote import RemoteOptions

if TYPE_CHECKING:
    from collections.abc import Mapping

    from geoparquet_io.core.check_parquet_structure import CheckProfile

#: Random consecutive/paired distances the spatial-order check compares
#: (``--random-sample-size``).
DEFAULT_SPATIAL_SAMPLE_PAIRS = 100
#: Rows the spatial-order check reads at most (``--limit-rows``).
DEFAULT_SPATIAL_LIMIT_ROWS = 500_000
#: Rows the spec check samples for data validation (``check spec --sample-size``).
DEFAULT_SPEC_SAMPLE_ROWS = 1000

#: The repairs ``check all --fix`` applies, and the names ``Table.fix(checks=)`` takes.
FIXABLE_CHECKS = ("bbox", "covering", "compression", "row_groups", "spatial")

#: CLI parameter -> ``CheckOptions`` field, where the two spell it differently.
_CLI_SPELLINGS = {"sample_size": "spec_sample_size"}

#: The fields a Click ``is_flag=True`` option fills. An absent flag arrives as
#: ``False``, which is "not asked", not a request -- so it must not beat a value
#: set elsewhere, and must not read as a field named twice.
_FLAG_FIELDS = frozenset({"all_rows", "fix", "no_backup", "overwrite"})


@dataclass(frozen=True, kw_only=True)
class CheckOptions:
    """What a check looks at and what its fix writes, as one value (``gpio.CheckOptions``).

    Every ``gpio check`` command builds exactly one from its flags
    (:meth:`from_cli`, invoked by ``cli.decorators.pack_check_options``), and
    the Python API takes the same object by name. A flag therefore cannot be
    declared on a command and then forgotten in its body, which is how
    ``--sample-rows`` and friends came to be accepted and dropped (#627, #663).

    Every field defaults to ``None``, "not asked": the ``resolved_*`` methods
    answer with this module's default, so there is one owner for each number
    rather than a copy in the Click option, a copy in the ``ops`` signature and
    a copy in core.

    Attributes:
        profile: Use-case band to judge against, as ``--profile``
            (``CheckProfile.web``). Not an AWS profile -- that is ``remote``.
        sample_rows: Rows whose bbox is compared against their geometry, as
            ``check bbox --sample-rows``; 0 skips the comparison.
        all_rows: Compare every row's bbox, as ``check bbox --all-rows``.
        random_sample_size: Random pairs the spatial-order check compares, as
            ``--random-sample-size``.
        limit_rows: Rows the spatial-order check reads at most, as ``--limit-rows``.
        spec_sample_size: Rows the spec check samples for data validation, as
            ``check spec --sample-size``.
        fix: Apply the repairs the check offers, as ``--fix``.
        checks: Which repairs to apply, from :data:`FIXABLE_CHECKS`; None means
            all of them. This is what ``Table.fix(checks=)`` takes; on the CLI
            the selection is the command you ran, and ``check all --fix``
            applies them all.
        fix_output: Where the repaired file goes, as ``--fix-output`` (and
            ``Table.fix(output=)``). None overwrites in place, with a backup
            unless ``no_backup``.
        no_backup: Skip the ``.bak`` backup when repairing in place, as
            ``--no-backup``.
        overwrite: Replace an existing ``fix_output``, as ``--overwrite``.
        remote: How the file is reached, as one
            :class:`~geoparquet_io.core.remote.RemoteOptions`. The check
            concern does not restate an endpoint or a credential; it holds the
            one object that owns them (#1436).
    """

    profile: CheckProfile | None = None
    sample_rows: int | None = None
    all_rows: bool | None = None
    random_sample_size: int | None = None
    limit_rows: int | None = None
    spec_sample_size: int | None = None
    fix: bool | None = None
    checks: tuple[str, ...] | None = None
    fix_output: str | None = None
    no_backup: bool | None = None
    overwrite: bool | None = None
    remote: RemoteOptions = RemoteOptions()

    def __post_init__(self) -> None:
        """Normalise a forwarded ``remote=None`` and a ``checks`` given as a str or list."""
        if self.remote is None:  # a caller that forwards `remote=None` by name
            object.__setattr__(self, "remote", RemoteOptions())
        if self.checks is not None and not isinstance(self.checks, tuple):
            names = (self.checks,) if isinstance(self.checks, str) else tuple(self.checks)
            object.__setattr__(self, "checks", names)

    @classmethod
    def pick(cls, values: Mapping[str, Any]) -> CheckOptions:
        """The options named in *values*; other keys are ignored, absent fields stay unset."""
        return cls(
            **{
                f.name: values[f.name]
                for f in fields(cls)
                if f.name in values and values[f.name] is not None
            }
        )

    @classmethod
    def from_cli(cls, **params: Any) -> CheckOptions:
        """The one ``CheckOptions`` a ``gpio check`` command builds from its Click parameters.

        ``check spec --sample-size`` is ``spec_sample_size``, because the other
        commands' ``sample_size`` on the API side means the spatial pair count.
        An absent ``is_flag`` option arrives as ``False``, which is "not asked"
        and leaves the field unset -- asking would make a command's own unset
        flag beat a value set anywhere else. Every other parameter is ignored:
        ``--verbose``, ``--strict``, ``--all-files`` and ``--sample-files``
        above all (see this module's docstring for why).
        """
        values: dict[str, Any] = {}
        for name, value in params.items():
            field = _CLI_SPELLINGS.get(name, name)
            values[field] = None if field in _FLAG_FIELDS and not value else value
        return cls.pick(values)

    def merge(self, other: CheckOptions | None) -> CheckOptions:
        """This value with *other*'s asked-for fields on top."""
        if other is None:
            return self
        return replace(self, **{name: getattr(other, name) for name in other.asked()})

    def asked(self) -> frozenset[str]:
        """The names of the fields that were asked for (not None, and a non-empty ``remote``)."""
        names = {f.name for f in fields(self) if getattr(self, f.name) is not None}
        if not self.remote:
            names.discard("remote")
        return frozenset(names)

    def __bool__(self) -> bool:
        """Whether anything was asked for at all."""
        return bool(self.asked())

    # -- the resolved numbers: one owner per default -------------------------

    def resolved_sample_rows(self) -> int:
        """Rows whose bbox is compared against their geometry (``all_rows`` overrides it)."""
        return DEFAULT_BBOX_SAMPLE_ROWS if self.sample_rows is None else self.sample_rows

    def resolved_random_sample_size(self) -> int:
        """Random pairs the spatial-order check compares."""
        if self.random_sample_size is None:
            return DEFAULT_SPATIAL_SAMPLE_PAIRS
        return self.random_sample_size

    def resolved_limit_rows(self) -> int:
        """Rows the spatial-order check reads at most."""
        return DEFAULT_SPATIAL_LIMIT_ROWS if self.limit_rows is None else self.limit_rows

    def resolved_spec_sample_size(self) -> int:
        """Rows the spec check samples for data validation."""
        return DEFAULT_SPEC_SAMPLE_ROWS if self.spec_sample_size is None else self.spec_sample_size

    def resolved_checks(self) -> tuple[str, ...]:
        """Which repairs to apply; unset means all of :data:`FIXABLE_CHECKS`."""
        return FIXABLE_CHECKS if self.checks is None else self.checks


def check_options_from_keywords(
    where: str,
    options: CheckOptions | None,
    **legacy: Any,
) -> CheckOptions:
    """*options*, or one built from the deprecated per-keyword spellings.

    The migration path off the loose check keywords (#1436), the same shape
    :func:`~geoparquet_io.core.remote.remote_options_from_keywords` gives the
    remote concern. Each legacy keyword is folded into the returned
    ``CheckOptions`` and warned about once per process, under the horizon
    "removed in the next minor release". Naming a field both in *options* and as
    a keyword is a ``TypeError``.

    *legacy* is keyed by **field** name, not by the entry point's keyword, since
    ``sample_size`` means the spatial pair count on ``check_all`` and the spec
    row count on ``validate``. Each caller therefore names the field it means,
    and ``_SPELLINGS_BY_ENTRY_POINT`` maps the field back to the keyword the
    warning has to quote.

    Args:
        where: The entry point's name, for the warning and the error message.
        options: The new spelling, or None.
        **legacy: The old keywords, keyed by the field each one sets.

    Returns:
        The one ``CheckOptions`` the entry point should pass on.
    """
    asked = {name: value for name, value in legacy.items() if value is not None}
    base = options or CheckOptions()
    if not asked:
        return base

    both = sorted(asked.keys() & base.asked())
    if both:
        raise TypeError(
            f"{where}() got {', '.join(both)} both in options= and as a keyword; "
            "set each field in one place"
        )
    _warn_deprecated_spellings(where, asked)
    return base.merge(CheckOptions.pick(asked))


#: One warning per deprecated spelling per process, keyed ``"<entry point>.<field>"``.
_warned_spellings: set[str] = set()

#: Field -> the keyword an entry point spells it with, when the two differ.
_SPELLINGS_BY_ENTRY_POINT: dict[str, dict[str, str]] = {
    "check_all": {"random_sample_size": "sample_size"},
    "check_spatial": {"random_sample_size": "sample_size"},
    "Table.check_spatial": {"random_sample_size": "sample_size"},
    "validate": {"spec_sample_size": "sample_size"},
    "Table.fix": {"fix_output": "output"},
}


def _warn_deprecated_spellings(where: str, asked: Any) -> None:
    """Warn once per process for each deprecated keyword *where* was given."""
    import warnings

    keywords = _SPELLINGS_BY_ENTRY_POINT.get(where, {})
    for field in sorted(asked):
        key = f"{where}.{field}"
        if key in _warned_spellings:
            continue
        _warned_spellings.add(key)
        keyword = keywords.get(field, field)
        warnings.warn(
            f"{where}({keyword}=...) is deprecated and will be removed in the next "
            f"minor release; pass options=gpio.CheckOptions({field}=...) instead",
            DeprecationWarning,
            stacklevel=4,
        )
