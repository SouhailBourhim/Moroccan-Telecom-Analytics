# Phase 7 — Airflow DAGs

**Status:** ⚠️ Code written and locally tested — not validated inside Docker  
**Commit:** `0c3c236`

## What was built

### `dag_ingest` (`@monthly`)
```
check_sources_available
    ↓
download_anrt_datasets ─┐
                         ├── (parallel)
download_itu_data ───────┘
    ↓
load_to_bronze       ← verifies row counts in DuckDB
    ↓
trigger_dag_transform
```

- `check_sources_available` — probes ANRT CKAN API and ITU DataHub; raises if either is unreachable
- `download_anrt_datasets` — calls `ingestion.anrt_extractor.run_anrt_extraction()`
- `download_itu_data` — calls `ingestion.itu_extractor.run_itu_extraction()`
- `load_to_bronze` — reads DuckDB to verify ANRT table count and ITU row count > 0

### `dag_transform` (triggered, `schedule=None`)
```
dbt_deps → dbt_run_staging → dbt_test_staging
  → dbt_run_intermediate → dbt_run_marts → dbt_test_marts
  → dbt_docs_generate → trigger_dag_quality
```

- Uses `BashOperator` with `dbt --profiles-dir /opt/dbt --project-dir /opt/dbt`
- `DBT_TARGET=dev` → uses `/opt/data/warehouse.duckdb` inside Docker
- `dbt_test_staging` and `dbt_test_marts` block pipeline on failure

### `dag_quality` (triggered, `schedule=None`)
```
ge_checkpoint_bronze → ge_checkpoint_silver → ge_checkpoint_gold
  → publish_quality_report
```

- Calls functions from `great_expectations/runner.py` via dynamic import
- Logs pass/fail for each check; non-blocking (failures logged, not raised)
- `publish_quality_report` writes a text summary to `data/ge_reports/`

## Problems encountered

### 1. Airflow 2.8.0 requires Python <3.12
**Problem:** Cannot install `apache-airflow==2.8.0` on the local venv (Python 3.14). No local import testing possible.

**Workaround:** Tested DAG imports inside the Airflow scheduler Docker container:
```bash
docker compose exec airflow-scheduler python -c "
import importlib.util
spec = importlib.util.spec_from_file_location('dag', '/opt/airflow/dags/dag_ingest.py')
..."
```
All 3 DAGs imported cleanly with correct task IDs.

### 2. `dbt` command location in Docker
**Issue:** The Airflow Docker image has dbt installed at the system Python level (via requirements.txt in the image). The `BashOperator` runs `dbt ...` which resolves to the correct binary without needing a venv path.

### 3. `reset_dag_run=True` on TriggerDagRunOperator
**Decision:** Added `reset_dag_run=True` to `TriggerDagRunOperator` calls so that re-triggering after a failure clears the previous state of the downstream DAG instead of skipping.

## Test result (DAG import only — no end-to-end run)
```
dag_ingest:    tasks=['check_sources_available', 'download_anrt_datasets',
                      'download_itu_data', 'load_to_bronze', 'trigger_dag_transform']
dag_transform: tasks=['dbt_deps', 'dbt_run_staging', 'dbt_test_staging',
                      'dbt_run_intermediate', 'dbt_run_marts', 'dbt_test_marts',
                      'dbt_docs_generate', 'trigger_dag_quality']
dag_quality:   tasks=['ge_checkpoint_bronze', 'ge_checkpoint_silver',
                      'ge_checkpoint_gold', 'publish_quality_report']
All 3 DAGs: OK (imported inside Airflow scheduler container)
```

## What still needs to happen
- [ ] Restart Docker Desktop cleanly and bring up full stack
- [ ] Trigger `dag_ingest` manually and confirm all tasks go green in Airflow UI
- [ ] Confirm `dag_transform` auto-triggers and dbt run/test tasks pass
- [ ] Confirm `dag_quality` auto-triggers and all GE checks pass
- [ ] Verify `data/ge_reports/` gets written inside the container
