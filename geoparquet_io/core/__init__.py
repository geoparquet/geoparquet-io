# Core modules for geoparquet-io
from geoparquet_io.core.geoarrow_compat import install as _install_geoarrow_compat

# A srid:N type crs crashes a geoarrow.pyarrow read without this (#1276).
_install_geoarrow_compat()
