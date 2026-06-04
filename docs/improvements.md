# Post-Phase Improvements

**Status:** ✅ Complete  
**Commit:** `3a2d324`

After completing all 9 phases a full project audit was performed. Findings were grouped into six fix phases (A–F) by severity. All 26 files changed in a single commit.

---

## Phase A — Critical Bugs

These caused silent data loss or runtime crashes.

### C1 — Missing `import time` in `anrt_extractor.py`

**Problem:** The DuckDB retry loop at the end of `run()` called `time.sleep(10)` but `time` was never imported. Any DuckDB lock-contention event (e.g., the ITU extractor still writing when ANRT tries to connect) raised `NameError: name 'time' is not defined`, crashing the entire ingestion task and losing all downloaded data.

**Fix:** Added `import time` to the imports block.

---

### C3 — dbt test failures did not fail the DAG

**Problem:** Both `dbt_test_staging` and `dbt_test_marts` in `dag_transform.py` ran without `--fail-fast`. dbt exits 0 even when individual tests fail (it collects all failures, prints them, and exits 0). This meant schema violations in Silver or Gold silently propagated downstream — Metabase dashboards showed bad data with no alert.

**Fix:** Added `--fail-fast` to both BashOperator commands. The DAG now marks `FAILED` immediately on the first test failure.

---

### C4 — PostgreSQL export had no transaction rollback

**Problem:** `export_to_postgres.py` called `pg.commit()` inside `export_table()` after each table. If the 3rd table failed mid-export, the first two were already committed — Metabase dashboards showed a mix of new and stale data with no way to know which tables were updated.

**Fix:** Moved all table exports inside a single `with pg.cursor()` block. `pg.commit()` is called once after all tables succeed. Any exception triggers `pg.rollback()`, leaving PostgreSQL in the previous consistent state.

---

## Phase B — Security

### H1 — Hardcoded credentials in `docker-compose.yml`

**Problem:** `POSTGRES_PASSWORD: airflow` and `_AIRFLOW_WWW_USER_PASSWORD: admin` were committed in plaintext. Anyone with read access to the repo could log into the database and the Airflow UI.

**Fix:** Both are now required from the `.env` file using the `${VAR:?error}` syntax — Docker Compose errors with a clear message if they are missing:
```yaml
POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set in .env}
_AIRFLOW_WWW_USER_PASSWORD: ${AIRFLOW_WWW_PASSWORD:?AIRFLOW_WWW_PASSWORD must be set in .env}
```
`.env.example` was expanded with all new variables and generation instructions.

---

### H2 — Hardcoded `PG_DSN` in `export_to_postgres.py`

**Problem:** `PG_DSN = "host=postgres port=5432 dbname=airflow user=airflow password=airflow"` was baked into source code. The password could not be rotated without editing code.

**Fix:** `PG_DSN = os.environ.get("POSTGRES_DSN", "...")`. The `docker-compose.yml` now injects `POSTGRES_DSN` via the environment block so the container value is always in sync with `POSTGRES_PASSWORD`.

---

### H3 — Metabase session token passed as CLI argument

**Problem:** `python scripts/create_metabase_dashboards.py <token> <db_id> <collection_id>` exposed the session token in `ps aux` output and shell history.

**Fix:** The script now reads `METABASE_TOKEN`, `METABASE_DB_ID`, and `METABASE_COLLECTION_ID` from environment variables. Positional arguments still work as a fallback for quick use.

---

### M9 — Metabase container ran as root

**Problem:** `Dockerfile.metabase` added `USER root` to run `apk add` but never switched back. The Metabase process ran as root inside the container.

**Fix:** Added `USER metabase` after the `apk add` line to restore the original non-root user.

---

## Phase C — Missing Staging Models

### H4 — 8 of 16 Bronze tables had no staging model

**Problem:** The ANRT extractor loaded 16 datasets into Bronze, but only 8 had staging models in Silver. The other 7 tables (portability, usage_avg, ip, data_links, tic_survey, domains, payphones) were ingested to Bronze but never promoted to Silver or Gold — they existed in the warehouse but were invisible to dbt and unused in any mart.

**Fix:** Created 7 new staging models following the existing pattern (cast types, rename, filter nulls):

| New model | Bronze source | Notes |
|---|---|---|
| `stg_anrt__portability` | `bronze_anrt_portability` | Mobile + fixed number portability per quarter |
| `stg_anrt__usage_avg` | `bronze_anrt_usage_avg` | Average monthly outgoing usage (minutes) |
| `stg_anrt__ip` | `bronze_anrt_ip` | IPv4 count and IPv6 prefixes — annual only, no quarter |
| `stg_anrt__data_links` | `bronze_anrt_data_links` | Enterprise data link circuits by type |
| `stg_anrt__tic_survey` | `bronze_anrt_tic_survey` | ICT survey results in long format (no quarter) |
| `stg_anrt__domains` | `bronze_anrt_domains` | Active `.ma` domain registrations per quarter |
| `stg_anrt__payphones` | `bronze_anrt_payphones` | Payphone count per quarter |

All 7 were added to `sources.yml` (with freshness checks) and `schema.yml` (with `not_null` tests).

---

## Phase D — Reliability

### H5 — No retry on CKAN API download calls

**Problem:** A single transient HTTP 429 or 503 from `data.gov.ma` caused the download to fail with an exception, silently skipping that dataset. The Bronze table was left with stale data from the previous run (or empty on first run) with no error visible in Airflow.

**Fix:** Introduced `_http_get_with_retry(url, stream, max_attempts=3)` which retries on 429/500/502/503/504 with exponential back-off (2s, 4s). Both `_ckan_download_url` and `_download_xlsx` use it.

---

### H6 — `publish_quality_report` re-ran all 3 GE checkpoints

**Problem:** `dag_quality.py` had individual tasks for Bronze, Silver, and Gold checkpoints, but `publish_quality_report` called all three checkpoint functions again internally to collect results. This meant every pipeline run performed 6 full DuckDB scans (3 per checkpoint task + 3 in the publish task).

**Fix:** Each checkpoint task now pushes its `CheckResult` to XCom (`ti.xcom_push`). `publish_quality_report` pulls from XCom (`ti.xcom_pull`) and publishes without re-running any checks.

---

### M4 — Seeds stopped at 2022; ITU data goes to 2024

**Problem:** `dim_period.csv` had 68 rows covering 2006-Q1 to 2022-Q4. `dim_population.csv` had 17 rows through 2022. ITU data includes 2023 and 2024. Any join involving `dim_period` or `dim_population` silently dropped 2023–2024 rows.

**Fix:** Extended both seeds through 2024:
- `dim_period.csv`: 76 rows (added 8 rows: 2023-Q1 through 2024-Q4)
- `dim_population.csv`: 19 rows (added 2023: 37,840,044 and 2024: 38,081,173 — World Bank estimates)

---

### M7 — NaN check used `v != v` instead of `pd.isna(v)`

**Problem:** `v != v` is the float IEEE 754 NaN trick — it works for `float('nan')` but returns `False` for string `"NaN"` values (which are equal to themselves). String NaN values passed through as text `"NaN"` instead of `NULL`, causing type errors in PostgreSQL.

**Fix:** Replaced with `pd.isna(v)` which correctly handles float NaN, `None`, `pd.NA`, and `pd.NaT`.

---

### M8 — No log rotation; Airflow log volume grew unbounded

**Problem:** Airflow writes one log file per task instance. Over months of `@monthly` runs plus manual triggers this fills the `airflow-logs` Docker volume with no automatic cleanup.

**Fix:** Added a `logging` block with `json-file` driver and `max-size: "10m" / max-file: "10"` to all services in `docker-compose.yml` using a YAML anchor for DRY configuration.

---

## Phase E — Test Coverage

### M1 — Sparse tests in staging `schema.yml`

**Problem:** `quarter`, `operator`, and `technology` columns in most staging models had only `not_null` tests. Invalid values (e.g., `quarter = 'Q5'` from a source format change) would pass into Silver and corrupt mart aggregations silently.

**Fix:** Added `accepted_values` tests across all applicable staging models:
- `quarter`: `['Q1', 'Q2', 'Q3', 'Q4']` in all quarterly models
- `operator`: `['Maroc Telecom', 'Orange Maroc', 'Inwi']` in mobile and QoS models
- `technology`: `['ADSL', 'FTTH', 'Mobile', 'Leased', 'Other']` in internet model

---

### M2 — No schema.yml for intermediate models

**Problem:** The 3 `int_` models (`int_market_share`, `int_penetration_rates`, `int_yoy_growth`) had zero quality gates. Bugs in business logic (wrong `LAG` window, division error) propagated undetected to Gold.

**Fix:** Created `dbt/models/intermediate/schema.yml` with:
- `not_null` on `year`, `quarter`, `operator`
- `accepted_values` on `operator`
- `dbt_utils.accepted_range` on `market_share_pct` [0, 100] and `mobile_penetration_per_100` [0, 200]

---

### L2 — No `accepted_values` on ITU `indicator_code`

**Problem:** A malformed indicator code from the ITU API (e.g., `i271_typo`) would be ingested, joined in `mart_benchmarks`, and silently produce `NULL` for the affected metric column with no test failure.

**Fix:** Added `accepted_values` test with the 8 known ITU codes (`i271`, `i112`, `i271p`, `i992b`, `i271mw`, `i99H`, `i4214`, `i741$`) in `dbt/models/staging/itu/schema.yml`.

---

### M5 — Bronze table list in GE runner was hardcoded

**Problem:** `ge_runner.py` had a hardcoded list of 9 Bronze tables. Adding a new dataset to the extractor required also editing the runner — easy to forget, leading to the new table being checked by dbt but skipped by GE.

**Fix:** Replaced the hardcoded list with a dynamic query:
```python
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'main' AND table_name LIKE 'bronze_%'
ORDER BY table_name
```
New Bronze tables are picked up automatically on the next GE run.

---

### M6 — Schema names hardcoded in GE runner

**Problem:** `main_silver` and `main_gold` were string literals in `ge_runner.py`. If dbt profile schema settings change, the GE runner silently queries the wrong schema.

**Fix:** Added two module-level constants driven by environment variables:
```python
_SILVER_SCHEMA = os.environ.get("DBT_SILVER_SCHEMA", "main_silver")
_GOLD_SCHEMA   = os.environ.get("DBT_GOLD_SCHEMA",   "main_gold")
```
All queries in the runner use these constants.

---

## Phase F — Polish

### L3 — `create_metabase_dashboards.py` was not idempotent

**Problem:** Running the script twice created duplicate dashboards and cards with the same names. Re-running after a failed partial setup required manual cleanup in the Metabase UI.

**Fix:** The script now calls `_existing_dashboards()` and `_existing_cards()` at startup to build a name→id map. `get_or_create_card()` and `get_or_create_dashboard()` update existing items instead of creating duplicates.

---

### L4 — `dbt_docs` used a bash retry loop instead of Airflow-native retries

**Problem:** `for i in 1 2 3 4 5; do dbt docs generate && break || sleep 10; done` is brittle — it swallows the exit code, doesn't surface retry attempts in the Airflow UI, and adds an arbitrary 5-attempt cap.

**Fix:** Replaced with `retries=4, retry_delay=timedelta(seconds=10)` on the `BashOperator`. Airflow natively tracks retry attempts, shows them in the task log, and marks the task `UP_FOR_RETRY` with proper state transitions.

---

### L5 — Only 2 custom singular dbt tests

**Problem:** The original test suite had `assert_mobile_subs_positive` and `assert_market_share_sums_to_100`. Missing: guard against empty Gold tables, time-series continuity, and a looser market share bounds check.

**Fix:** Added 3 new singular tests:
- `assert_no_empty_gold_tables.sql` — all 5 Gold marts must have > 0 rows; catches silent empty-load failures
- `assert_quarterly_time_continuity.sql` — detects gaps in the mobile quarterly series by comparing against `dim_period`
- `assert_market_share_bounds.sql` — sum of operator market shares per period must be in [95, 105] (allows for rounding tolerance)

---

### L6 — GE report filename collision when run twice in one day

**Problem:** Reports were named `quality_YYYYMMDD.txt`. Triggering `dag_quality` twice on the same day overwrote the first report with no warning.

**Fix:** Reports are now named `quality_YYYYMMDD_HHMMSS.txt` using a UTC timestamp at report generation time. Reports are never overwritten.
