# Phase 1 — Repo Scaffold

**Status:** ✅ Complete  
**Commit:** `bc35f5a`, `ade3509`

## What was built

- Full directory structure as specified in CLAUDE.md
- `docker-compose.yml` — Postgres 15, Airflow 2.8.0 (webserver + scheduler + init), Metabase
- `requirements.txt` — all Python dependencies pinned
- `.gitignore` — excludes `data/raw/`, `data/warehouse.duckdb`, `.env`, venvs, dbt target/packages
- `.env.example` — template for local secrets
- `dbt/dbt_project.yml` — Bronze/Silver/Gold schema config
- `dbt/profiles.yml` — DuckDB dev profile
- `dbt/packages.yml` — dbt_utils 1.1.1
- `dbt/seeds/` — `dim_operator.csv`, `dim_population.csv`, `dim_period.csv` (stub)
- All stub SQL models and Python files created
- GitHub repo created and pushed: `https://github.com/SouhailBourhim/Moroccan-Telecom-Analytics.git`

## Problems encountered

### 1. `version:` key in docker-compose.yml
**Problem:** Docker Compose v2 ignores the `version: '3.8'` key and shows a warning.  
**Fix:** Removed the `version:` key entirely — not needed in Compose v2.

### 2. `airflow-init` running as root but `airflow` binary not on PATH
**Problem:** The init service used `user: "0:0"` (root) so pip could install packages without permission errors. But root's PATH doesn't include `/home/airflow/.local/bin/` where the `airflow` binary lives. The `airflow db migrate` and `airflow users create` commands were silently failing (the script used `|| true`), so the database was never initialized — the webserver and scheduler were hanging waiting for a migrated DB.

**Fix:** Removed `user: "0:0"` and the custom entrypoint entirely. Used the official Airflow entrypoint env vars instead:
```yaml
_AIRFLOW_DB_MIGRATE: 'true'
_AIRFLOW_WWW_USER_CREATE: 'true'
_AIRFLOW_WWW_USER_USERNAME: admin
_AIRFLOW_WWW_USER_PASSWORD: admin
```
The official image handles migration and user creation automatically before executing the given command.

### 3. Phase not tested before moving on
**Problem:** The docker-compose.yml was committed in Phase 1 but never run. The broken init wasn't caught until Phase 3 (two phases later).  
**Lesson saved:** Test every phase deliverable before declaring it done.

## Test result
After fix: `docker compose up -d` → all services healthy. Airflow UI accessible at `http://localhost:8080` (admin/admin), Metabase at `http://localhost:3000`.
