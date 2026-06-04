# Phase 4 — dbt Init

**Status:** ⚠️ Code written and locally tested — not validated inside Docker  
**Commit:** `8821741`

## What was built

- `dbt/profiles.yml` — updated with two targets:
  - `dev` — for Docker containers (`/opt/data/warehouse.duckdb`)
  - `local` — for local development (`WAREHOUSE_PATH` env var)
- `dbt/package-lock.yml` — locks dbt_utils 1.1.1
- `dbt/seeds/dim_period.csv` — completed to 68 rows covering 2006-Q1 → 2022-Q4 (was truncated to 8 rows)

## Problems encountered

### 1. dbt-core 1.7.0 incompatible with Python 3.14
**Problem:** The project's venv uses Python 3.14. dbt-core 1.7.0 requires `distutils` (removed in Python 3.12+) and uses protobuf features removed in protobuf 4.22+.

**Fix 1:** Installed `setuptools` to restore `distutils` compatibility.  
**Fix 2:** The protobuf `MessageToJson()` call (`including_default_value_fields` argument) was removed in newer protobuf. Pinned `protobuf>=3.20.0,<4.0.0` in the dbt venv.  
**Fix 3:** Created a separate `.dbt-venv` using Python 3.11 (`/opt/homebrew/bin/python3.11`) with its own pinned packages. All `dbt` commands now run via `/Users/apple/morocco-telecom-analytics/.dbt-venv/bin/dbt`.

### 2. `dim_period.csv` was truncated
**Problem:** The seed file only had 8 rows (2006 Q1–Q4, 2007 Q1–Q4) — the rest were never written.  
**Fix:** Regenerated programmatically: 68 rows covering every quarter from 2006-Q1 to 2022-Q4.

### 3. `dbt deps` exit code 0 despite showing "Updates available" warning
**Behaviour:** `dbt deps` prints an update notice for dbt_utils (1.1.1 → 1.3.3) but still exits 0 and installs the locked version correctly. Not an error — expected behaviour.

## Test result (local only — not run inside Docker)
```
dbt deps  → 0 (dbt_utils 1.1.1 installed)
dbt seed  → PASS=3 WARN=0 ERROR=0 TOTAL=3
            dim_operator: 3 rows
            dim_population: 17 rows
            dim_period: 68 rows
```

## What still needs to happen
- [ ] Restart Docker Desktop cleanly (was crashing at end of Phase 9 attempt)
- [ ] Bring up full stack: `docker compose up -d`
- [ ] Verify dbt is installed inside the Airflow container (`docker compose exec airflow-scheduler dbt --version`)
- [ ] Run `dbt deps && dbt seed` inside the container against `/opt/data/warehouse.duckdb`
