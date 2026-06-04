# Phase 8 — Great Expectations

**Status:** ⚠️ Code written and locally tested — not validated inside Docker  
**Commit:** `9fd9cd1`  
**File:** `great_expectations/runner.py`

## What was built

A standalone Python module (`runner.py`) using the GE 0.18.x **fluent (ephemeral) API** — no YAML config files required. Loads DuckDB tables into pandas DataFrames and validates expectations. Results are non-blocking (failures are logged and reported but do not stop the pipeline).

### Bronze checkpoint — `run_bronze_checkpoint()`
- Checks all 9 Bronze tables: `expect_table_row_count_to_be_between(min_value=10)`
- Detects empty tables (loader failure, empty source file, etc.)

### Silver checkpoint — `run_silver_checkpoint()`
- `stg_anrt__mobile.total_subs >= 0` (no negative subscriber counts)
- `mart_internet_evol.mobile_penetration_per_100` in [0, 200] (detects impossible penetration rates)

### Gold checkpoint — `run_gold_checkpoint()`
- Maroc Telecom `market_share_pct` median in [30, 60] (`expect_column_quantile_values_to_be_between`)
- `mart_market_overview.mobile_total_subs` not null
- `mart_benchmarks` has at least 15 years of rows

### Report publisher — `publish_report()`
- Writes a plain-text summary to `data/ge_reports/quality_{YYYYMMDD}.txt`

### DAG integration (`dag_quality.py` updated)
- `dag_quality.py` was updated to import and call these functions directly via dynamic import from `/opt/airflow/great_expectations/runner.py`
- Replaces the earlier checkpoint-name-based approach which required YAML config files

## Problems encountered

### 1. GE 0.18.x YAML-based checkpoint approach too complex for DuckDB
**Problem:** The initial plan was to use YAML checkpoint config files (classic GE approach) with a SQLAlchemy DuckDB datasource. This requires `duckdb-engine` and a specific GE config hierarchy. DuckDB 0.10.0 + GE 0.18.0 SQLAlchemy integration has compatibility issues.

**Fix:** Switched to the **fluent ephemeral API**: read DuckDB tables into pandas DataFrames and pass them to GE in-memory. Simpler, no YAML files, works with any DuckDB version.

### 2. `expect_column_median_to_be_between` is not a standard GE expectation
**Problem:** CLAUDE.md specifies `expect_column_median_to_be_between` but this is not a built-in GE expectation.

**Fix:** Used `expect_column_quantile_values_to_be_between` with `quantiles: [0.5]` which gives the median. Equivalent result.

### 3. "Silver mobile_penetration" column doesn't exist in Silver layer
**Problem:** CLAUDE.md says "Silver: expect_column_values_to_be_between — mobile_penetration in [0, 200]". But `mobile_penetration_per_100` is only computed in `int_penetration_rates` (ephemeral) and `mart_internet_evol` (Gold). Silver staging views don't have this column.

**Fix:** Used `mart_internet_evol.mobile_penetration_per_100` (already computed in Gold) for this Silver-level conceptual check.

### 4. `result_format` UserWarning
**Behaviour:** GE 0.18.x logs `UserWarning: result_format configured at Validator-level will not be persisted`. This is a warning, not an error — ignored.

## Test result (local only — not run inside Docker)
```
Bronze: 9/9 checks passed
Silver: 2/2 checks passed
Gold:   3/3 checks passed
Total:  14/14 PASS

Report written to: data/ge_reports/quality_20260604.txt
```

## What still needs to happen
- [ ] Phases 3–7 must be completed first (pipeline must run end-to-end inside Docker)
- [ ] Trigger `dag_quality` inside Docker and confirm all 14 GE checks pass
- [ ] Verify `data/ge_reports/quality_{YYYYMMDD}.txt` is written to the mounted volume

Sample report:
```
Quality Report — 2026-06-04
==================================================
[PASS] BRONZE: 9/9 checks passed
  ✓ bronze_anrt_mobile  ✓ bronze_anrt_internet  ...
[PASS] SILVER: 2/2 checks passed
  ✓ mobile_subs_non_negative  ✓ mobile_penetration_in_range
[PASS] GOLD: 3/3 checks passed
  ✓ market_share_iam_median_30_60  ✓ overview_mobile_subs_not_null  ✓ benchmarks_min_15_years
```
