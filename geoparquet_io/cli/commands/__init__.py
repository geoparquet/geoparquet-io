"""Per-group ``gpio`` command modules.

One module per top-level command group. Each module declares its group with
``@click.group()`` - passing ``cls=`` when the group needs a default subcommand
or custom argument parsing, as ``check``, ``convert``, ``extract`` and
``inspect`` do - and
its subcommands with ``@<group>.command(...)``. A group may also nest a subgroup
with ``@<group>.group(...)``, as ``process`` does for ``gpio process aggregate``.
``cli/main.py`` imports the group object and attaches it to the root group with
an explicit ``cli.add_command(<group>)``. Nothing here imports ``cli.main``, so
the dependency runs one way:

    cli/main.py -> cli/commands/<group>.py -> cli/_shared.py, cli/decorators.py

That direction is enforced, not just documented: the ``commands-no-cli-main``
import-linter contract forbids the back edge, and ``commands-independent``
forbids one group module importing another.

Shared helpers live in :mod:`geoparquet_io.cli._shared` (group-neutral runtime
plumbing) and :mod:`geoparquet_io.cli.decorators` (reusable Click options and
``cls=`` command classes). A helper used by exactly one group belongs in that
group's module.

Importing from here
-------------------
This package is not reachable by attribute traversal. ``geoparquet_io/__init__``
does ``from geoparquet_io.cli.main import cli``, which rebinds the
``geoparquet_io.cli`` *attribute* to the Click ``Group`` object - and a
``Group``'s ``.commands`` is a dict of its subcommands. So
``geoparquet_io.cli.commands.add`` resolves against that dict and fails with a
confusing ``'dict' object has no attribute 'add'``, even though
``import geoparquet_io.cli.commands.add`` itself succeeds. The re-export is
forced by the ``gpio = "geoparquet_io:cli"`` entry point, so the shadow stays
(tracked separately). Use the ``from`` form, as the tests do::

    from geoparquet_io.cli.commands import add as cli_add
"""
