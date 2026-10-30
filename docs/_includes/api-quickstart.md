```python
import geoparquet_io as gpio

# Read, transform, and write in a fluent chain
gpio.read('input.parquet') \
    .add_bbox() \
    .sort_hilbert() \
    .write('optimized.parquet')

# Add multiple spatial indices
gpio.read('input.parquet') \
    .add_bbox() \
    .add_h3(resolution=9) \
    .add_quadkey(resolution=12) \
    .sort_hilbert() \
    .write('output.parquet')

# Partition by H3 cells
gpio.read('input.parquet') \
    .add_h3(resolution=9) \
    .partition_by_h3('output/', resolution=6)

# Convert from other formats
gpio.convert('data.gpkg') \
    .add_bbox() \
    .sort_hilbert() \
    .write('output.parquet')
```
