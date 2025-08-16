param([string]$ProfilesDir = ".\dbt")
$env:DBT_PROFILES_DIR = (Resolve-Path $ProfilesDir).Path
python -m venv .\.venv
.\.venv\Scripts\activate
pip install --upgrade pip
pip install dbt-duckdb duckdb
cd dbt
dbt debug
dbt seed
dbt run
dbt test
