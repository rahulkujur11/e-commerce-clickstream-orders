# Gold Layer quick run

## Prereqs
- Silver Parquet under `../data/silver/...` relative to this `dbt` folder.
- Python + pip.

## Run
```bash
export DBT_PROFILES_DIR="$(pwd)"
pip install --upgrade pip dbt-duckdb duckdb
dbt debug
dbt seed
dbt run
dbt test
```
