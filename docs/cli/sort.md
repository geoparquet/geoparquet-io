# sort Command

For detailed usage and examples, see the [Sort User Guide](../guide/sort.md).

## Quick Reference

```bash
gpio sort --help
```

This will show all available subcommands and options.

## Subcommands

### hilbert

Sort by Hilbert space-filling curve for optimal spatial ordering:

```bash
gpio sort hilbert input.parquet output.parquet [OPTIONS]
```

### column

Sort by any column(s):

```bash
gpio sort column input.parquet output.parquet COLUMNS [OPTIONS]
```

Arguments:
- `COLUMNS` - Comma-separated column names to sort by

Options:
- `--descending` - Sort in descending order
