Two remote admin boundary datasets are supported:

| Dataset | Standard | Columns Added | Description |
|---------|----------|---------------|-------------|
| `gaul` (default) | GAUL naming + ISO 3166-1 alpha-3 | `admin:continent`, `admin:country`, `admin:department` | FAO Global Administrative Unit Layers (GAUL) L2 - worldwide coverage with standardized naming |
| `overture` | **Vecorel compliant** (ISO 3166-1/2) | `admin:country_code`, `admin:subdivision_code` | Overture Maps Divisions with ISO 3166 codes (219 countries, 3,544 regions) - [docs](https://docs.overturemaps.org/guides/divisions/) |

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
