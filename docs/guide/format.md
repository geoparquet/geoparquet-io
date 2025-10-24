# Formatting Files

The `format` command applies formatting best practices to GeoParquet files.

!!! warning "Work in Progress"
    The format command is still in development. Currently only bbox metadata formatting is available.

## Add Bbox Metadata

Add bbox covering metadata to the GeoParquet file:

```bash
gpio format bbox-metadata myfile.parquet
```

This updates the file's metadata to include proper bbox covering information following GeoParquet 1.1 spec.

## Future Commands

Planned format commands:

- `format all` - Apply all formatting best practices
- `format compression` - Optimize compression settings
- `format row-groups` - Optimize row group sizes

## Workflow with Check

The intended workflow:

```bash
# 1. Check current state
gpio check all myfile.parquet

# 2. Format to fix issues (future)
gpio format all myfile.parquet

# 3. Verify improvements
gpio check all myfile.parquet
```

## See Also

- [CLI Reference: format](../cli/format.md)
- [check command](check.md)
- [sort command](sort.md)
