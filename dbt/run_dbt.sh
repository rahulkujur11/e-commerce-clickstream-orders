#!/usr/bin/env bash
set -euo pipefail
export DBT_PROFILES_DIR="$(pwd)/dbt"
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install dbt-duckdb duckdb
cd dbt
dbt debug && dbt seed && dbt run && dbt test
