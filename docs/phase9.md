# Phase 9 — Metabase Dashboards

**Status:** ✅ Complete — 5 dashboards live via PostgreSQL export  
**Commit:** `1f3ea06`

## What was built

### Architecture decision: PostgreSQL bridge instead of DuckDB direct connection

The original plan was to connect Metabase directly to `warehouse.duckdb` via a community JDBC driver. This was blocked by two compatibility issues:
- DuckDB JDBC v0.10.0 cannot read DuckDB 1.5.3 database files (serialization format mismatch)
- The DuckDB community Metabase driver (v0.2.4) uses HoneySQL v1 but Metabase 0.49 requires HoneySQL v2

**Chosen approach:** Export the 5 Gold mart tables from DuckDB to the PostgreSQL instance already running for Airflow, then connect Metabase to PostgreSQL natively. This is reliable and production-grade.

### `ingestion/export_to_postgres.py`
Exports all 5 Gold marts from `main_gold` schema in DuckDB to the `gold` schema in PostgreSQL:
- `mart_market_overview` → `gold.mart_market_overview`
- `mart_operator_perf` → `gold.mart_operator_perf`
- `mart_qos_scorecard` → `gold.mart_qos_scorecard`
- `mart_internet_evol` → `gold.mart_internet_evol`
- `mart_benchmarks` → `gold.mart_benchmarks`

All column types are mapped from DuckDB to PostgreSQL equivalents.

### `scripts/create_metabase_dashboards.py`
Creates 5 Metabase dashboards via the Metabase REST API:
- Native SQL queries against the PostgreSQL `gold` schema
- 2–3 cards per dashboard (line/bar charts + full-table view)
- Uses `PUT /api/dashboard/:id` with `dashcards` array (Metabase 0.49 format)

### `Dockerfile.metabase`
Custom image based on `metabase/metabase:v0.49.7` with `libstdc++`, `libgcc`, and `libc6-compat` added for Alpine Linux compatibility.

### `docker-compose.yml` changes
- Metabase now builds from `Dockerfile.metabase` instead of pulling the stock image
- Added `MB_DB_FILE: /metabase-data/metabase.db` for H2 persistence across container restarts
- Added `metabase-data` volume

## Problems encountered

### 1. DuckDB JDBC driver incompatibility
**Problem:** The community DuckDB Metabase driver (v0.2.4) uses HoneySQL v1, while Metabase 0.49 requires HoneySQL v2. Driver load fails silently.

**Attempted:** Downloaded newer driver; hit DuckDB file format mismatch — JDBC 0.10.0 cannot open files written by DuckDB 1.5.3.

**Fix:** Abandoned DuckDB direct connection entirely. Exported Gold layer to PostgreSQL.

### 2. `libstdc++` missing on Alpine Linux
**Problem:** Metabase runs on Alpine Linux. The DuckDB native library requires `libstdc++.so.6` which is not present in Alpine by default.

**Fix:** Custom `Dockerfile.metabase` with `apk add --no-cache libstdc++ libgcc libc6-compat`.

### 3. Metabase H2 database resetting on container restart
**Problem:** Without `MB_DB_FILE`, Metabase writes its H2 state to the default container path (not on a volume), so admin setup was lost on every restart.

**Fix:** Added `MB_DB_FILE: /metabase-data/metabase.db` mapped to a named Docker volume.

### 4. `POST /api/dashboard/:id/cards` returns 404 in Metabase 0.49
**Problem:** The REST API changed in 0.49 — cards are added via `PUT /api/dashboard/:id` with a `dashcards` array, not via the old `POST .../cards` endpoint.

**Fix:** Updated `add_cards_to_dashboard()` to use `PUT` with `dashcards`.

### 5. `ROUND(double precision, integer)` fails in PostgreSQL
**Problem:** PostgreSQL's `ROUND(x, n)` function requires `numeric` type; `double precision` is not accepted.

**Fix:** Added explicit cast: `ROUND(market_share_pct::numeric, 1)`.

### 6. Column name mismatches between DuckDB Gold and dashboard queries
**Problem:** Several dashboard queries used column names that don't exist in the actual mart tables (e.g., `voice_qos_score`, `broadband_penetration_per_100`, `indicator_name`).

**Fix:** Corrected each query to use actual column names from the mart schemas.

## Test result

```
5 dashboards created in Metabase collection "Morocco Telecom Analytics":

Dashboard 1: Market Overview
  - Mobile Subscribers Over Time (line chart)
  - Fixed vs Mobile vs Internet Subscribers (bar chart)
  - Market Overview — Full Table

Dashboard 2: Operator Performance
  - Market Share by Operator (bar chart)
  - ARPM by Operator (line chart)
  - Operator Performance — Full Table

Dashboard 3: QoS Scorecard
  - QoS Voice Call Success Rate by Year (bar chart)
  - QoS Scorecard — Full Table

Dashboard 4: Internet Evolution
  - Mobile Penetration per 100 Inhabitants (line chart)
  - Mobile vs Fixed Broadband Subscribers (bar chart)
  - Internet Evolution — Full Table

Dashboard 5: Morocco vs MENA Benchmarks
  - Morocco Key ITU Indicators Over Time (line chart)
  - Benchmarks — Full Table

All 5 dashboards: accessible and rendering data at http://localhost:3000
```
