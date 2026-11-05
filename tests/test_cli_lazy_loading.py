"""Cold command selection must not import unrelated command groups."""

import json
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "args,groups",
    [(["--help"], []), (["inspect", "--help"], ["inspect"]), (["extract", "--help"], ["extract"])],
)
def test_cold_help_import_count(args, groups):
    code = """
import json
import sys
from click.testing import CliRunner
from geoparquet_io.cli.main import cli
result = CliRunner().invoke(cli, ARGS)
assert result.exit_code == 0, result.output
prefix = "geoparquet_io.cli.commands."
print(json.dumps(sorted(name.removeprefix(prefix) for name in sys.modules if name.startswith(prefix))))
assert "geoparquet_io.core.wfs" not in sys.modules
assert "botocore" not in sys.modules
assert "obstore" not in sys.modules
""".replace("ARGS", repr(args))
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert json.loads(result.stdout) == groups


def test_cli_commands_is_a_package():
    import types

    import geoparquet_io.cli.commands as commands
    import geoparquet_io.cli.commands.add as add

    assert isinstance(commands, types.ModuleType)
    assert isinstance(add, types.ModuleType)


def test_lazy_help_matches_loaded_commands_and_preserves_plugins():
    import click
    from click.testing import CliRunner

    from geoparquet_io.cli import main
    from geoparquet_io.cli.main import LazyGroup

    for name in main._GROUP_HELP:
        assert getattr(main, name) is main.cli.commands[name]
    with pytest.raises(AttributeError):
        main.missing_command

    group = LazyGroup("gpio")
    plugin = click.Command("plugin", help="A plugin command.", deprecated=True)
    group.add_command(plugin)
    group.add_command(click.Command("hidden-plugin", hidden=True))
    runner = CliRunner()
    cold = {
        width: runner.invoke(group, ["--help"], terminal_width=width).output
        for width in (40, 80, 120)
    }
    assert group.commands["plugin"] is plugin
    assert all(isinstance(command, click.Command) for command in group.commands.values())
    for width, output in cold.items():
        result = runner.invoke(group, ["--help"], terminal_width=width)
        assert result.exit_code == 0
        assert result.output == output
        assert "hidden-plugin" not in output
