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
           9 models: cast, rename, normalize operators
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
           Great Expectations — 14 checks across 3 layers
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
| Great Expectations | 0.18.0 | Data quality — 14 checks |
| PostgreSQL | 15 | Airflow metadata + Metabase analytics export |
| Metabase | 0.49.7 | Dashboards |
| Docker Compose | — | Local orchestration of all services |

---

## Data Sources

### ANRT — data.gov.ma
16 XLSX datasets covering 2006–2022 downloaded via the CKAN API (no auth, ODbL licence):
mobile subscriptions, internet, fixed telephony, QoS, traffic, ARPM, bandwidth, complaints, portability, usage, IP addresses, data links, TIC surveys, domain names, payphones.

### ITU DataHub
8 ICT indicators for Morocco (mobile subscriptions, fixed broadband, internet users %, international bandwidth, mobile revenue) fetched from the ITU REST API.

---

## Project Structure

```
morocco-telecom-analytics/
├── Dockerfile                  # Custom Airflow image (dbt + DuckDB + GE pre-installed)
├── Dockerfile.metabase         # Custom Metabase image (libstdc++ for DuckDB JDBC)
├── docker-compose.yml
├── requirements.txt
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
│   │   ├── staging/            # 9 stg_ models (Silver layer, views)
│   │   ├── intermediate/       # 3 int_ models (ephemeral)
│   │   └── marts/              # 5 mart_ models (Gold layer, tables)
│   ├── seeds/                  # dim_operator, dim_population, dim_period
│   └── tests/                  # Singular tests: positive subs, market share = 100
├── great_expectations/
│   └── runner.py               # Bronze / Silver / Gold checkpoint logic
├── scripts/
│   └── create_metabase_dashboards.py   # Metabase API — creates 5 dashboards
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
```

### 2. Start all services

```bash
docker compose up -d --build
```

This starts: PostgreSQL · Airflow (webserver + scheduler) · Metabase.
First build takes ~5 minutes as it installs dbt, DuckDB, and Great Expectations into the Airflow image.

### 3. Run the pipeline

```bash
# Trigger ingestion manually (runs monthly in production)
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

# Get a session token (Metabase must be set up first via http://localhost:3000)
python3 scripts/create_metabase_dashboards.py <session-token> <db-id> <collection-id>
```

---

## Services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
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

## Data Quality

### dbt tests (blocking — DAG fails on error)
- `not_null` on all key columns across Bronze, Silver, Gold
- `unique` combination tests on `(year, quarter, operator)` in staging
- Singular tests: `total_subs > 0`, `SUM(market_share) BETWEEN 99 AND 101`
- Source freshness: warn > 90 days, error > 180 days

### Great Expectations (non-blocking — report written to `data/ge_reports/`)
- **Bronze**: row count ≥ 10 for all 9 tables
- **Silver**: mobile subscribers non-negative, penetration in [0, 200]
- **Gold**: Maroc Telecom market share median in [30, 60], no null KPIs, benchmarks ≥ 15 years

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
  ├── dbt_run_staging → dbt_test_staging
  ├── dbt_run_intermediate
  ├── dbt_run_marts → dbt_test_marts
  ├── dbt_docs_generate
  └── trigger → dag_quality

dag_quality  (triggered)
  ├── ge_checkpoint_bronze
  ├── ge_checkpoint_silver
  ├── ge_checkpoint_gold
  └── publish_quality_report
```

---

## Medallion Layers

| Layer | Schema | Materialisation | Tables |
|---|---|---|---|
| Bronze | (default) | Tables | 16 raw tables, exact copy of source |
| Silver | `silver` | Views | 9 `stg_` models — cast, rename, normalize |
| Intermediate | — | Ephemeral | 3 `int_` models — business logic CTEs |
| Gold | `gold` | Tables | 5 `mart_` models — analytics-ready |
