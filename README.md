# Morocco Telecom Analytics

End-to-end data engineering pipeline that ingests Moroccan telecom market data from **ANRT** (data.gov.ma) and **ITU DataHub** into a DuckDB warehouse, transforms it with dbt using a Bronze / Silver / Gold medallion architecture, orchestrates everything with Apache Airflow, and visualises results in Metabase — all running locally via Docker Compose.

---

## Architecture

```
ANRT (CKAN API)          ITU DataHub API
       │                        │
       └──────────┬─────────────┘
                  ▼
            dag_ingest  (@monthly)
                  │
           Bronze Layer (DuckDB)
           16 raw tables — exact copy of source data
                  │
            dag_transform  (triggered)
                  │
           Silver Layer — dbt staging views
           16 models: cast, rename, normalize
                  │
           Intermediate — 3 ephemeral models
           market share · penetration rates · YoY growth
                  │
           Gold Layer — 5 dbt mart tables
           mart_market_overview · mart_operator_perf
           mart_qos_scorecard · mart_internet_evol · mart_benchmarks
                  │
            dag_quality  (triggered)
                  │
           Great Expectations — checks across 3 layers
                  │
              Metabase  (5 dashboards)
```

---

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| Python | 3.11 | Extraction scripts + Airflow operators |
| Apache Airflow | 2.8.0 | Orchestration — 3 DAGs |
| dbt-core | 1.7.0 | Silver + Gold transformations |
| dbt-duckdb | 1.7.0 | dbt adapter for DuckDB |
| DuckDB | 1.5.3 | Local data warehouse |
| Great Expectations | 0.18.0 | Data quality checks |
| PostgreSQL | 15 | Airflow metadata + Metabase analytics export |
| Metabase | 0.49.7 | Dashboards |
| Docker Compose | — | Local orchestration of all services |

---

## Data Sources

### ANRT — data.gov.ma
16 XLSX datasets covering 2006–2022 downloaded via the CKAN API (no auth, ODbL licence):
mobile subscriptions, internet, fixed telephony, QoS, traffic, ARPM, bandwidth, complaints, portability (mobile + fixed), usage averages, IP addresses, data links, TIC surveys, domain names, payphones.

### ITU DataHub
8 ICT indicators for Morocco (mobile subscriptions, fixed broadband, internet users %, international bandwidth, mobile revenue) fetched from the ITU REST API — data available through 2024.

---

## Project Structure

```
morocco-telecom-analytics/
├── Dockerfile                  # Custom Airflow image (dbt + DuckDB + GE pre-installed)
├── Dockerfile.metabase         # Custom Metabase image (libstdc++ for Alpine)
├── docker-compose.yml
├── requirements.txt
├── .env.example                # Template — copy to .env and fill secrets before starting
├── ingestion/
│   ├── anrt_extractor.py       # CKAN API → 16 XLSX datasets → Bronze
│   ├── itu_extractor.py        # ITU API → Bronze
│   └── export_to_postgres.py   # DuckDB Gold → PostgreSQL (for Metabase)
├── airflow/
│   ├── dags/
│   │   ├── dag_ingest.py       # @monthly: sources → Bronze
│   │   ├── dag_transform.py    # triggered: dbt staging → marts
│   │   └── dag_quality.py      # triggered: Great Expectations checkpoints
│   └── plugins/
│       └── ge_runner.py        # GE validation runner (imported by dag_quality)
├── dbt/
│   ├── models/
│   │   ├── staging/            # 16 stg_ models (Silver layer, views)
│   │   ├── intermediate/       # 3 int_ models (ephemeral)
│   │   └── marts/              # 5 mart_ models (Gold layer, tables)
│   ├── seeds/                  # dim_operator, dim_population (through 2024), dim_period (through 2024)
│   └── tests/                  # 5 custom singular tests
├── scripts/
│   └── create_metabase_dashboards.py   # Metabase API — creates/updates 5 dashboards
└── data/
    ├── raw/                    # Downloaded XLSX + CSV files (gitignored)
    └── warehouse.duckdb        # DuckDB warehouse (gitignored)
```

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- ~4 GB RAM available for containers

### 1. Clone and configure

```bash
git clone https://github.com/SouhailBourhim/Moroccan-Telecom-Analytics.git
cd Moroccan-Telecom-Analytics
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD and AIRFLOW_WWW_PASSWORD at minimum
```

### 2. Start all services

```bash
docker compose up -d --build
```

This starts: PostgreSQL · Airflow (webserver + scheduler) · Metabase.
First build takes ~5 minutes as it installs dbt, DuckDB, and Great Expectations into the Airflow image.

### 3. Run the pipeline

```bash
# Trigger ingestion manually (runs @monthly in production)
docker compose exec airflow-scheduler \
  airflow dags trigger dag_ingest
```

The DAG chain runs automatically:
`dag_ingest` → `dag_transform` → `dag_quality`

### 4. Set up Metabase dashboards

Once the pipeline has run at least once:

```bash
# Export gold mart tables to PostgreSQL
docker compose exec airflow-scheduler \
  python3 /opt/airflow/ingestion/export_to_postgres.py

# Create/update dashboards (reads METABASE_TOKEN, METABASE_DB_ID, METABASE_COLLECTION_ID from env)
export METABASE_TOKEN=<session-token>
export METABASE_DB_ID=<db-id>
export METABASE_COLLECTION_ID=<collection-id>
python3 scripts/create_metabase_dashboards.py
```

---

## Services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / (AIRFLOW_WWW_PASSWORD from .env) |
| Metabase | http://localhost:3000 | admin@morocctelecom.local / Admin1234! |

---

## Dashboards

Five dashboards in the **Morocco Telecom Analytics** Metabase collection:

| Dashboard | Contents |
|---|---|
| Market Overview | Mobile, fixed, internet subscribers over time |
| Operator Performance | Market share, ARPM, traffic, complaints by operator |
| QoS Scorecard | Voice call success rate and network quality indicators |
| Internet Evolution | Broadband penetration, mobile vs fixed broadband |
| Morocco vs MENA Benchmarks | ITU indicators: subscriptions, bandwidth, internet users % |

---

## DAG Chain

```
dag_ingest  (@monthly)
  ├── check_sources_available
  ├── download_anrt_datasets  ┐ parallel
  ├── download_itu_data       ┘
  ├── load_to_bronze
  └── trigger → dag_transform

dag_transform  (triggered)
  ├── dbt_deps
  ├── dbt_run_staging → dbt_test_staging     ← --fail-fast: halts on any failure
  ├── dbt_run_intermediate
  ├── dbt_run_marts → dbt_test_marts         ← --fail-fast: halts on any failure
  ├── dbt_docs_generate                      ← retries=4 with 10s back-off
  └── trigger → dag_quality

dag_quality  (triggered)
  ├── ge_checkpoint_bronze   ┐
  ├── ge_checkpoint_silver   ├── each pushes results to XCom
  ├── ge_checkpoint_gold     ┘
  └── publish_quality_report  ← pulls XCom, writes timestamped report
```

---

## Medallion Layers

| Layer | Schema | Materialisation | Tables |
|---|---|---|---|
| Bronze | (default) | Tables | 16 raw tables, exact copy of source |
| Silver | `silver` | Views | 16 `stg_` models — cast, rename, normalize |
| Intermediate | — | Ephemeral | 3 `int_` models — business logic CTEs |
| Gold | `gold` | Tables | 5 `mart_` models — analytics-ready |

---

## Data Quality

### dbt tests (blocking — pipeline fails on error)
- `not_null` on all key columns across Bronze, Silver, Gold
- `accepted_values` on `quarter` (Q1–Q4), `operator` (3 names), `technology` (5 types), `indicator_code` (8 ITU codes)
- `unique_combination_of_columns` on `(year, quarter, operator)` in stg_mobile and stg_internet
- `dbt_utils.accepted_range` on `market_share_pct` [0, 100] and `mobile_penetration_per_100` [0, 200]
- Source freshness: warn > 90 days, error > 180 days
- **Custom singular tests:**
  - `assert_mobile_subs_positive` — no negative subscriber counts
  - `assert_market_share_sums_to_100` — `SUM(market_share_pct)` between 99 and 101 per period
  - `assert_market_share_bounds` — broader tolerance check (95–105%) for periods with rounding
  - `assert_no_empty_gold_tables` — all 5 Gold marts must have > 0 rows
  - `assert_quarterly_time_continuity` — no gaps in mobile quarterly time series

### Great Expectations (non-blocking — report written to `data/ge_reports/`)
- **Bronze**: row count ≥ 10 for all Bronze tables (auto-discovered)
- **Silver**: mobile subscribers non-negative, penetration in [0, 200]
- **Gold**: Maroc Telecom market share median in [30, 60], no null KPIs, benchmarks ≥ 15 years
- Reports use timestamped filenames (`quality_YYYYMMDD_HHMMSS.txt`) — no same-day collisions

---

## Implementation Phases

| Phase | What was built | Status |
|---|---|---|
| 1 | Repo scaffold: docker-compose, requirements, seeds, .env.example, directory structure | ✅ |
| 2 | `anrt_extractor.py`: CKAN API loop, universal XLSX parser, Bronze loader for 16 datasets | ✅ |
| 3 | `itu_extractor.py`: ITU REST API discovery, 8 indicators → `bronze_itu_morocco` | ✅ |
| 4 | dbt init: profiles.yml, dbt_project.yml, packages.yml, seeds, sources.yml | ✅ |
| 5 | 9 staging models + schema tests (Silver layer) | ✅ |
| 6 | 3 intermediate + 5 mart models + tests (Gold layer), 41 tests passing | ✅ |
| 7 | 3 Airflow DAGs (17 tasks total), TriggerDagRunOperator chain | ✅ |
| 8 | Great Expectations fluent API runner, 14 checks across 3 layers | ✅ |
| 9 | Metabase dashboards via PostgreSQL export; 5 dashboards live | ✅ |
| Improvements | Cross-cutting fixes: security, reliability, test coverage, missing models | ✅ |

---

## Post-Phase Improvements

After the initial 9-phase build, a full project audit was performed. The findings and fixes are documented in [`docs/improvements.md`](docs/improvements.md). Summary:

| Severity | Fixed |
|---|---|
| Critical bugs | `import time` crash, dbt test failures not blocking DAG, no-rollback PostgreSQL export |
| Security | Credentials moved to `.env`, secrets out of source code, container no longer runs as root |
| Data coverage | 7 missing staging models added (portability, usage_avg, ip, data_links, tic_survey, domains, payphones) |
| Reliability | CKAN retry logic, XCom-based quality report, seeds extended to 2024, log rotation |
| Test coverage | `accepted_values` on all categorical columns, intermediate schema.yml, 3 new singular tests |
| Polish | Idempotent dashboard script, Airflow-native retries, timestamped GE reports |
