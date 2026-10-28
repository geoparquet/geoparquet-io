# check Command

For detailed usage and examples, see the [Check User Guide](../guide/check.md).

## Quick Reference

```bash
gpio check --help
```

This will show all available subcommands and options.

## Subcommands

- `check all` - Run all validation checks
- `check spatial` - Check spatial ordering and filter pushdown readiness
- `check compression` - Validate compression settings
- `check bbox` - Verify bbox structure and metadata
- `check row-group` - Check row group optimization
- `check optimization` - Combined spatial query optimization score (5 factors)
- `check spec` - Validate against GeoParquet specification
- `check stac` - Validate STAC Item or Collection JSON

## Exit Codes

Every subcommand exits `1` when a check fails and `0` when it only warns; pass
`--strict` to make warnings exit `1` too. An input that matches no files exits
`1`. See [Exit Codes](../guide/check.md#exit-codes) in the user guide.
