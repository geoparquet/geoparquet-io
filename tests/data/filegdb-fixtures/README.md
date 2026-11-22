# FileGDB compatibility fixtures

These small GDAL test datasets come from GDAL commit
`6d7a6388745ef760b68d3ea460d700f4be5479c4`:

- [FileGDB 9.2](https://github.com/OSGeo/gdal/blob/6d7a6388745ef760b68d3ea460d700f4be5479c4/autotest/ogr/data/filegdb/testopenfilegdb92.gdb.zip)
- [FileGDB 9.3](https://github.com/OSGeo/gdal/blob/6d7a6388745ef760b68d3ea460d700f4be5479c4/autotest/ogr/data/filegdb/testopenfilegdb93.gdb.zip)
- [Signed-byte raster](https://github.com/OSGeo/gdal/tree/6d7a6388745ef760b68d3ea460d700f4be5479c4/autotest/gdrivers/data/filegdb/int8.gdb), zipped without changing the dataset files.

They exercise the older catalog layout and a dataset whose raster backing tables
must not be offered as vector layers. No external download is needed by the tests.
The GDAL/OGR General MIT-style permission notice is retained in `LICENSE.txt`;
see the upstream [licensing file](https://github.com/OSGeo/gdal/blob/6d7a6388745ef760b68d3ea460d700f4be5479c4/LICENSE.TXT).

The catalog row-to-file mapping and vector-open exclusions are also documented
by GDAL's [OpenFileGDB reader](https://github.com/OSGeo/gdal/blob/6d7a6388745ef760b68d3ea460d700f4be5479c4/ogr/ogrsf_frmts/openfilegdb/ogropenfilegdbdatasource.cpp).
