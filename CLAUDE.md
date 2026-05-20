# Weather Pipeline — Claude Context

## What this project is

A data engineering pipeline orchestrated with Apache Airflow that fetches historical weather data for multiple cities from the Visual Crossing API and stores it as partitioned Parquet files for analysis.

EL (Extract-Load) pattern — no transformation layer yet.

## Architecture

```
Visual Crossing API
    → src/fetch_data.py      fetch per city → concat → save as Parquet
    → dags/fetch_save_dag.py Airflow DAG, runs daily
    → data/weather_partitioned/city=X/year=Y/month=Z/
```

| File | Responsibility |
|---|---|
| `src/config.py` | API key, output path, list of cities |
| `src/fetch_data.py` | `fetch_weather` (single city), `fetch_historical_weather` (all cities), `save_data` |
| `src/utils/weather_utils.py` | `normalize_visual_crossing_data` — adds `city`, `date`, `year`, `month` columns |
| `dags/fetch_save_dag.py` | Airflow DAG definition — two tasks: fetch → save |

## Setup

```bash
python3 -m venv my-env
source my-env/bin/activate
pip install -r requirements.txt

cp .env.example .env   # add your VC_API_KEY
export AIRFLOW_HOME=$(pwd)/.airflow
airflow db init

# Terminal 1
airflow webserver --port 8080
# Terminal 2
airflow scheduler
```

## Running tests and lint

```bash
pytest tests/ -v
ruff check src/ tests/
```

All checks must pass before merging.

## Key design decisions

- **Partitioned Parquet** — `city/year/month` layout lets downstream tools (DuckDB, Spark, Athena) prune partitions without a full scan
- **`overwrite_or_ignore`** — re-running the DAG for the same date range is safe; existing files are not duplicated
- **`catchup=False`** — the DAG does not backfill missed runs automatically; backfilling must be triggered manually
- **XCom avoided** — `save_data` is called inside `fetch_historical_weather` task or via direct task dependency, not by serialising a DataFrame through XCom (DataFrames are too large for XCom)

## Known limitations / future work

- No transformation step — raw API fields are stored as-is
- No data quality checks on fetched records
- Cities list is static in `config.py`
