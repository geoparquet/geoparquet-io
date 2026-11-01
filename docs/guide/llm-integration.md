# LLM Integration

gpio includes a skill file that teaches Large Language Models (ChatGPT, Claude, Gemini, etc.) how to work with spatial data using gpio.

## Quick Start

```bash
# List available skills
gpio skills

# Print skill content to stdout
gpio skills --show

# Copy skill to current directory
gpio skills --copy .
```

## Using with LLMs

### Option 1: Paste skill content

Copy the skill content to your clipboard and paste it at the start of a conversation:

=== "macOS"
    <!-- doctest: skip="uses pbcopy, which only exists on macOS" -->
    ```bash
    gpio skills --show | pbcopy
    ```

=== "Linux (X11)"
    <!-- doctest: skip="uses xclip, which is not a gpio dependency" -->
    ```bash
    gpio skills --show | xclip -selection clipboard
    ```

=== "Windows (PowerShell)"
    <!-- doctest: skip="the PowerShell variant of the bash example above, which is the one executed" -->
    ```powershell
    gpio skills --show | Set-Clipboard
    ```

Then paste into your LLM conversation to give it context about gpio.

### Option 2: Reference the file

Tell your LLM to read the skill file:

```
Read geoparquet_io/skills/geoparquet.md and help me convert my shapefile to GeoParquet.
```

### Option 3: Claude Code

For [Claude Code](https://claude.ai/code) users, the skill is automatically available:

- Invoke via `/geoparquet` command
- Or ask Claude to help with GeoParquet conversions

## What the Skill Teaches

The skill teaches LLMs how to:

1. **Ingest** spatial data from URLs and local files
2. **Explore** data structure, CRS, and schema
3. **Convert** to optimized GeoParquet format
4. **Validate** against best practices
5. **Recommend** partitioning strategies based on size
6. **Publish** to cloud storage

## Customizing Skills

Copy the skill to customize it for your workflow:

```bash
gpio skills --copy .
# Edit geoparquet.md with your preferences
```

Custom skills can include:
- Your preferred compression settings
- Default cloud storage paths
- Organization-specific workflows
- Additional validation requirements

## Skill Content

The skill includes:

- Command reference table (auto-generated from CLI)
- Compression options and defaults
- Step-by-step workflow guides
- Remote file access patterns
- Example sessions

All generated sections stay in sync with the actual CLI via `scripts/doc_sync.py`.
