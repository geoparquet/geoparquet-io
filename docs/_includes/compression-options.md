Supported formats:

- `ZSTD` (default) - Best balance, level 1-22, default 15
- `GZIP` - Wide compatibility, level 1-9, default 6
- `BROTLI` - High compression, level 1-11, default 6
- `LZ4` - Fastest
- `SNAPPY` - Fast, good compression
- `UNCOMPRESSED` - No compression

On a path that writes through a DuckDB `COPY`, DuckDB takes a level for ZSTD only:
GZIP is written at level 6 and BROTLI at level 11 whatever level is given, and gpio
warns when that differs from the level you asked for.
