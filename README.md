# geoparquet-tools

A collection of tools for GeoParquet, using PyArrow and DuckDB.

Currently just a collection of python files, each with their own click cli. Goal is to bring them into one nice cli and make it installable with pip.

## Current Tools

* **check_spatial_order.py** attempts to check if a file has been ordered spatially. Not totally sure if it works right, but seems to.
* **hilbert_order.py** runs duckdb's st_hilbert function, as described in [this post](https://cholmes.medium.com/using-duckdbs-hilbert-function-with-geop-8ebc9137fb8a), properly using the bounds of the dataset. It also preserves the projection using pyarrow as DuckDB doesn't do that yet.
* **check_parquet_structure.py** checks row group size, compression and 1.1 / bbox compliance, in alignment with [in progress recommendations](https://github.com/opengeospatial/geoparquet/pull/254).

The aim is to try to make it so it's easy to run each check in the recommendations, and also test the whole set of things in one, and then to also provide the functionality to convert parquet files to be in line with the recommendations.

This ideally includes spatial partitioning, though there's more unknowns there. But hopefully can provide tools to break up large files with DuckDB, with both admin-partitioned strategies and at least a couple index-based ones (kd-tree, s2, etc). So a goal for these tools is to make it easy for anyone with a large dataset to break it up with a cli call or two.

## TODO's
 - try to drop pyarrow, just use duckdb
 - better handling of country file - download it automatically, test other country files, make a bit more generic.
 - spatial partitioning by kd-tree and s2
 - add tests
 - add docs
 - decompose checks so they can each be called individually, and added
 - call for country splitting to be all in one.
 - option for further partitioning within admin boundaries
 - admin level 2 splitting
 - ticket to admin extension in fiboa about putting source boundary file.
 
