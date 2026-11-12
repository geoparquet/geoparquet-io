Two remote admin boundary datasets are supported:

| Dataset | Standard | Columns Added | Description |
|---------|----------|---------------|-------------|
| `gaul` (default) | GAUL naming + ISO 3166-1 alpha-3 | `admin:continent`, `admin:country`, `admin:department` | FAO Global Administrative Unit Layers (GAUL) L2 - worldwide coverage with standardized naming |
| `overture` | **Vecorel compliant** (ISO 3166-1/2) | `admin:country_code`, `admin:subdivision_code` | Overture Maps Divisions with ISO 3166 codes (219 countries + 53 dependent territories, 3,544 regions) - [docs](https://docs.overturemaps.org/guides/divisions/) |

### Vecorel Compliance (Overture Dataset Only)

The `overture` dataset follows the [Vecorel administrative division extension](https://vecorel.org/administrative-division-extension/v0.1.0/schema.yaml) specification with standardized ISO codes:

- **`admin:country_code`** (REQUIRED): ISO 3166-1 alpha-2 country code (e.g., "US", "AR", "DE")
- **`admin:subdivision_code`**: ISO 3166-2 subdivision code WITHOUT country prefix (e.g., "CA" not "US-CA")

The tool automatically transforms Overture's native region codes (e.g., "US-CA") to strip the country prefix for Vecorel compliance.

**Note:** The GAUL dataset uses FAO's standardized naming system but is NOT Vecorel compliant:
- Has ISO 3166-1 alpha-3 codes (e.g., "TZA"), but Vecorel requires alpha-2 (e.g., "TZ")
- Uses GAUL's standardized naming for subnational units, not ISO 3166-2 codes
- Columns: `admin:continent` (continent name), `admin:country` (GAUL country name), `admin:department` (GAUL L2 name)

### Notes

- **Overture dataset**: Vecorel compliant with ISO 3166-1 alpha-2 and ISO 3166-2 codes
- **GAUL dataset**: FAO standardized naming system - [source.coop GAUL L2](https://data.source.coop/nlebovits/gaul-l2-admin/)
- Performs spatial intersection to assign admin divisions based on geometry
- Requires internet connection to access remote datasets
- Uses spatial extent filtering and bbox columns for optimization

### Overture cold reads and interrupted downloads

`add admin-divisions` and `partition admin` prefer an existing complete level
cache. On a cold cache, they inspect the Parquet row-group bbox statistics before
reading geometry. If those statistics prove that at least one row group cannot
overlap the input extent, they read the bounded extent directly without creating
a global cache. Otherwise they build the complete level cache, which subsequent
runs reuse. This rule uses actual row-group overlap, with no geographic area
threshold; missing statistics cannot prove pruning. Only requested levels are built.

A complete level is built one source object at a time. Each object's filtered,
simplified rows publish atomically into a release- and level-specific parts
directory. A retry skips completed objects, retries the interrupted object, and
publishes the complete level only after all objects finish. Progress is reported
per object. Bytes within an unfinished object are not retained; bounded inline
reads also restart after an interruption.

Admin cache cleanup removes abandoned staging directories after 24 hours while
holding their ownership locks. A live writer remains protected regardless of its
age. Older staging directories created without an ownership identity are left
untouched. `--clear-cache` removes completed object parts as well as level caches.
