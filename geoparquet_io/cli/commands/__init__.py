"""Per-group ``gpio`` command modules.

One module per top-level command group. Each module declares its group with a
plain ``@click.group()`` and its subcommands with ``@<group>.command(...)``;
``cli/main.py`` imports the group object and attaches it to the root group with
an explicit ``cli.add_command(<group>)``. Nothing here imports ``cli.main``,
so the dependency runs one way:

    cli/main.py -> cli/commands/<group>.py -> cli/_shared.py, cli/decorators.py

Shared helpers live in :mod:`geoparquet_io.cli._shared` (group-neutral runtime
plumbing) and :mod:`geoparquet_io.cli.decorators` (reusable Click options and
``cls=`` command classes). A helper used by exactly one group belongs in that
group's module.
"""
