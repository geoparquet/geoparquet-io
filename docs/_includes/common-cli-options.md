```bash
# Compression settings
--compression [ZSTD|GZIP|BROTLI|LZ4|SNAPPY|UNCOMPRESSED]
--compression-level [1-22]

# Row group sizing
--row-group-size [exact row count]
--row-group-size-mb [target size like '256MB' or '1GB']

# Workflow options
--dry-run          # Preview SQL without executing
--verbose          # Detailed output
--preview          # Preview results (partition commands)
--hive             # Use Hive-style partitioning
--overwrite        # Overwrite existing files
--profile NAME     # AWS profile for S3 operations
```
