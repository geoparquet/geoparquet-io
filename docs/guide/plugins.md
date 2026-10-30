# Plugins

gpio supports a plugin system that allows you to extend its functionality with specialized format support and additional commands. Plugins are distributed as separate Python packages and are automatically discovered when installed.

## Installing Plugins

### During Initial Installation

Install gpio with plugins in one command:

=== "uv"
    <!-- doctest: skip="illustrates authoring a plugin package" -->
    ```bash
    # Install gpio with a plugin
    uv tool install geoparquet-io --with gpio-myplugin

    # Install with multiple plugins
    uv tool install geoparquet-io --with gpio-myplugin --with gpio-other
    ```

=== "pipx"
    <!-- doctest: skip="illustrates authoring a plugin package" -->
    ```bash
    # Install gpio with a plugin
    pipx install geoparquet-io --preinstall gpio-myplugin

    # Install with multiple plugins (chain --preinstall flags)
    pipx install geoparquet-io --preinstall gpio-myplugin --preinstall gpio-other
    ```

### Adding to Existing Installation

Add plugins to an already-installed gpio tool:

=== "uv"
    <!-- doctest: skip="illustrates authoring a plugin package" -->
    ```bash
    # Add plugin to existing installation
    uv tool install --with gpio-myplugin geoparquet-io
    ```

=== "pipx"
    <!-- doctest: skip="illustrates authoring a plugin package" -->
    ```bash
    # Inject plugin into existing installation
    pipx inject geoparquet-io gpio-myplugin
    ```

### In Python Projects

For Python API usage, install plugins as regular dependencies:

=== "uv"
    <!-- doctest: skip="illustrates authoring a plugin package" -->
    ```bash
    uv add geoparquet-io gpio-myplugin
    ```

=== "pip"
    <!-- doctest: skip="illustrates authoring a plugin package" -->
    ```bash
    pip install geoparquet-io gpio-myplugin
    ```

## Available Plugins

!!! note "PMTiles is now built-in"
    The `gpio pmtiles` command is now part of geoparquet-io core. No plugin installation needed.
    See the [GeoJSON guide](geojson.md#using-gpio-pmtiles-built-in) for usage.

| Plugin | Package | Description |
|--------|---------|-------------|
| fiboa | `gpio-fiboa` | Field Boundaries for Agriculture — improve and describe fiboa datasets |

See the [fiboa guide](fiboa.md) for detailed usage.

## How Plugins Work

### Plugin Discovery

gpio uses Python's [entry points](https://packaging.python.org/en/latest/specifications/entry-points/) system to discover plugins. When you run a `gpio` command:

1. gpio loads its built-in commands
2. Searches for packages with `gpio.plugins` entry points
3. Automatically loads and registers plugin commands
4. Makes plugin commands available under the `gpio` CLI

### Entry Point Registration

Plugins register themselves by declaring an entry point in their `pyproject.toml`:

```toml
[project.entry-points."gpio.plugins"]
myplugin = "gpio_myplugin.cli:myplugin"
```

This tells gpio:
- The plugin provides a command group named `myplugin`
- The command implementation is at `gpio_myplugin.cli:myplugin`

### Verifying Plugin Installation

Check if a plugin is installed and discoverable:

<!-- doctest: skip="illustrates authoring a plugin package" -->
```bash
# List all installed tools (including plugins)
uv tool list
# or
pipx list

# Check if plugin command is available
gpio myplugin --help
```

## Removing Plugins

### From Tool Installation

=== "uv"
    <!-- doctest: skip="uninstalls and reinstalls the developer's global gpio" -->
    ```bash
    # Reinstall without the plugin
    uv tool uninstall geoparquet-io
    uv tool install geoparquet-io
    ```

=== "pipx"
    <!-- doctest: skip="illustrates authoring a plugin package" -->
    ```bash
    # Remove injected plugin
    pipx uninject geoparquet-io gpio-myplugin
    ```

### From Python Projects

=== "uv"
    <!-- doctest: skip="illustrates authoring a plugin package" -->
    ```bash
    uv remove gpio-myplugin
    ```

=== "pip"
    <!-- doctest: skip="illustrates authoring a plugin package" -->
    ```bash
    pip uninstall gpio-myplugin
    ```

## Developing Plugins

Want to create your own gpio plugin? Here's what you need:

### Plugin Structure

A minimal gpio plugin consists of:

```
gpio-myplugin/
├── pyproject.toml          # Plugin metadata and entry point
├── gpio_myplugin/
│   ├── __init__.py
│   └── cli.py              # Click command group
└── tests/
    └── test_myplugin.py
```

### Entry Point Setup

In `pyproject.toml`:

```toml
[project]
name = "gpio-myplugin"
version = "0.1.0"
dependencies = [
    "geoparquet-io>=0.9.0",
    "click>=8.0.0",
]

[project.entry-points."gpio.plugins"]
myplugin = "gpio_myplugin.cli:myplugin"
```

### Command Implementation

In `gpio_myplugin/cli.py`:

```python
import click

@click.group()
def myplugin():
    """My custom plugin commands."""
    pass

@myplugin.command()
@click.argument('input_file')
@click.argument('output_file')
def process(input_file, output_file):
    """Process a file with my plugin."""
    click.echo(f"Processing {input_file} -> {output_file}")
    # Your implementation here
```

### Best Practices

1. **Naming**: Use `gpio-<format>` pattern (e.g., `gpio-pmtiles`, `gpio-flatgeobuf`)
2. **Dependencies**: Declare `geoparquet-io` as a dependency
3. **Documentation**: Include README with installation and usage
4. **Testing**: Add tests for your plugin commands
5. **Entry Point**: Use `gpio.plugins` as the entry point group
6. **Command Group**: Make your top-level command a Click group for extensibility

### Publishing

Once your plugin is ready:

1. **Test locally**:
   <!-- doctest: skip="illustrates authoring a plugin package" -->
   ```bash
   cd gpio-myplugin
   uv tool install geoparquet-io --with .
   gpio myplugin --help
   ```

2. **Publish to PyPI**:
   <!-- doctest: skip="illustrates authoring a plugin package" -->
   ```bash
   uv build
   uv publish
   ```

3. **Create repository**: Follow the `gpio-<format>` naming convention
4. **Submit PR**: Add your plugin to this documentation

## Plugin Ideas

Looking for inspiration? Consider creating plugins for:

- **Formats**: FlatGeobuf, Shapefile, GeoJSON-seq, GeoPackage streaming
- **Spatial Operations**: Spatial joins, buffering, intersections
- **Cloud Services**: Specific cloud storage integrations
- **Visualization**: Map preview, quick visualization tools
- **Analytics**: Statistical analysis, spatial clustering

## Support

- **Questions**: [geoparquet-io Discussions](https://github.com/geoparquet/geoparquet-io/discussions)
- **Issues**: [geoparquet-io Issues](https://github.com/geoparquet/geoparquet-io/issues)
- **Plugin-specific**: Check each plugin's repository for support
