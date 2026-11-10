"""Per-group ``gpio`` command modules.

One module per top-level command group. Each module declares its group with
``@click.group()`` - passing ``cls=`` when the group needs a default subcommand
or custom argument parsing, as ``check``, ``convert``, ``extract`` and
``inspect`` do - and
its subcommands with ``@<group>.command(...)``. A group may also nest a subgroup
with ``@<group>.group(...)``, as ``process`` does for ``gpio process aggregate``.
``cli/main.py`` loads each group on first selection or introspection. Nothing here imports ``cli.main``, so
the dependency runs one way:

    cli/main.py -> cli/commands/<group>.py -> cli/_shared.py, cli/decorators.py

That direction is enforced, not just documented: the ``commands-no-cli-main``
import-linter contract forbids the back edge, and ``commands-independent``
forbids one group module importing another.

Shared helpers live in :mod:`geoparquet_io.cli._shared` (group-neutral runtime
plumbing) and :mod:`geoparquet_io.cli.decorators` (reusable Click options and
``cls=`` command classes). A helper used by exactly one group belongs in that
group's module.

"""
