# Cache Management

Remote admin boundary datasets are automatically cached locally for performance. This dramatically speeds up repeated operations.

## Performance

**Without Cache (first run):**
- Download and process: ~2-3 minutes

**With Cache (subsequent runs):**
- Use cached data: ~0.5 seconds

**Speedup: 300-400x faster**

## How It Works

When you use `partition admin` or `add admin-divisions` commands with remote datasets (GAUL, Overture), the tool:

1. **First checks** if the dataset is already cached
2. **If cached**: Uses the local copy instantly
3. **If not cached**: Downloads, caches, then uses it
4. **Cache persists** between sessions for future use

This happens automatically - no configuration needed.

## Cache Commands

### View Cached Datasets

```bash
# List all cached datasets
gpio cache list

# Show detailed information (paths, URLs)
gpio cache list --verbose
```

**Example output:**
```
Cached Admin Boundary Datasets (1 total):

  GAUL L2 Admin Boundaries
    Size: 482.0 MB
    Cached: 2025-10-24 15:37:50
```

### Cache Information

```bash
# Show cache directory and total size
gpio cache info
```

**Example output:**
```
Cache Directory: /home/user/.cache/geoparquet-io/admin-datasets
Exists: True
Total cached: 1 dataset(s), 482.0 MB
```

### Clear Cache

```bash
# Clear all cached datasets (with confirmation)
gpio cache clear

# Skip confirmation prompt
gpio cache clear --yes

# Clear specific dataset only
gpio cache clear --dataset gaul --yes
```

## Cache Location

Datasets are cached in a standard location following XDG Base Directory specification:

- **Linux/macOS**: `~/.cache/geoparquet-io/admin-datasets/`
- **Custom**: Set `$XDG_CACHE_HOME` to override

Each cached dataset includes:
- `.parquet` file: The actual dataset
- `.json` file: Metadata (URL, download time, size)

## When to Clear Cache

Consider clearing the cache when:

- **Disk space is needed**: GAUL is ~482 MB, Overture is ~500 MB
- **Dataset has been updated**: Remote datasets may get updates
- **Testing**: Want to verify download behavior

## Advanced

### Manual Cache Inspection

```bash
# View cache directory
ls -lh ~/.cache/geoparquet-io/admin-datasets/

# Check individual file
du -h ~/.cache/geoparquet-io/admin-datasets/GAUL*.parquet
```

### Cache Key Generation

Cache files are named using a hash of the source URL:
```
{dataset_name}_{url_hash}.parquet
GAUL L2 Admin Boundaries_ee009e4f76a7532d.parquet
```

This ensures different dataset versions/sources are cached separately.

## Troubleshooting

### Cache Not Working

If caching fails, the tool automatically falls back to direct remote access:

```bash
# Run with verbose to see what's happening
gpio partition admin input.parquet output/ --dataset gaul --levels continent --verbose
```

You'll see messages like:
- `"Downloading and caching GAUL L2 Admin Boundaries..."` (first run)
- `"Using cached dataset: /path/to/cache"` (subsequent runs)
- `"Warning: Caching failed, using direct remote access"` (fallback)

### Permission Issues

If you get permission errors:

```bash
# Check cache directory permissions
ls -ld ~/.cache/geoparquet-io/

# Recreate if needed
mkdir -p ~/.cache/geoparquet-io/admin-datasets
chmod 755 ~/.cache/geoparquet-io/admin-datasets
```

### Cache Corruption

If a cached file is corrupted:

```bash
# Clear the specific dataset
gpio cache clear --dataset gaul --yes

# Next operation will re-download
gpio partition admin input.parquet output/ --dataset gaul --levels continent
```

## See Also

- [Partition Guide](partition.md) - Partitioning by admin boundaries
- [Add Guide](add.md) - Adding admin division columns
