# Morocco Telecom Analytics Pipeline

End-to-end data engineering pipeline. Ingests Moroccan telecom market data from
ANRT (data.gov.ma) and ITU DataHub into a DuckDB warehouse, transformed with dbt
using Bronze/Silver/Gold medallion architecture, orchestrated by Airflow, visualised
in Metabase. All services run locally via Docker Compose.

## Tech stack

| Tool | Version | Role |
|---|---|---|
| Python | 3.11 | Extraction scripts + Airflow operators |
| Apache Airflow | 2.8.0 | Orchestration (3 DAGs) |
| dbt-core | 1.7.0 | Transformations Silver + Gold |
| dbt-duckdb | 1.7.0 | dbt adapter for DuckDB |
| DuckDB | 1.5.3 | Data warehouse (single .duckdb file) |
| Great Expectations | 0.18.0 | Data quality checkpoints |
| pandas | 2.1.0 | Data manipulation in extractors |
| openpyxl | 3.1.2 | XLSX parsing |
| Metabase | latest | Dashboards connected to warehouse.duckdb |

## Directory structure

```
morocco-telecom-analytics/
├── CLAUDE.md
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
├── ingestion/
│   ├── __init__.py
│   ├── anrt_extractor.py      # CKAN API → downloads 16 XLSX datasets
│   └── itu_extractor.py       # HTTP download ITU Morocco data
├── airflow/
│   ├── dags/
│   │   ├── dag_ingest.py      # @monthly: sources → Bronze
│   │   ├── dag_transform.py   # triggered: dbt staging → marts
│   │   └── dag_quality.py     # triggered: GE checkpoints
│   └── plugins/
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── anrt/
│   │   │   │   ├── sources.yml
│   │   │   │   ├── stg_anrt__mobile.sql
│   │   │   │   ├── stg_anrt__internet.sql
│   │   │   │   ├── stg_anrt__fixed.sql
│   │   │   │   ├── stg_anrt__qos.sql
│   │   │   │   ├── stg_anrt__traffic.sql
│   │   │   │   ├── stg_anrt__arpm.sql
│   │   │   │   ├── stg_anrt__complaints.sql
│   │   │   │   └── stg_anrt__bandwidth.sql
│   │   │   └── itu/
│   │   │       ├── sources.yml
│   │   │       └── stg_itu__morocco.sql
│   │   ├── intermediate/
│   │   │   ├── int_market_share.sql
│   │   │   ├── int_penetration_rates.sql
│   │   │   └── int_yoy_growth.sql
│   │   └── marts/
│   │       ├── mart_market_overview.sql
│   │       ├── mart_operator_perf.sql
│   │       ├── mart_qos_scorecard.sql
│   │       ├── mart_internet_evol.sql
│   │       └── mart_benchmarks.sql
│   ├── seeds/
│   │   ├── dim_operator.csv
│   │   ├── dim_population.csv
│   │   └── dim_period.csv
│   └── tests/
│       └── generic/
├── great_expectations/
│   ├── checkpoints/
│   └── expectations/
└── data/
    ├── raw/                   # Downloaded XLSX files — gitignored
    └── warehouse.duckdb       # DuckDB file — gitignored
```

## Architecture: medallion Bronze / Silver / Gold

### Bronze — raw ingestion (16 tables)
Exact copy of XLSX source data. No transformations. Two system columns appended
to every table: `source_file VARCHAR` and `ingested_at TIMESTAMP`.

Key tables: `bronze_anrt_mobile`, `bronze_anrt_internet`, `bronze_anrt_fixed`,
`bronze_anrt_qos`, `bronze_anrt_traffic`, `bronze_anrt_arpm`, `bronze_anrt_bandwidth`,
`bronze_anrt_complaints`, `bronze_anrt_portability`, `bronze_anrt_usage_avg`,
`bronze_anrt_ip`, `bronze_anrt_data_links`, `bronze_anrt_tic_survey`,
`bronze_anrt_domains`, `bronze_anrt_payphones`, `bronze_itu_morocco`.

### Silver — dbt staging (9 models, materialized as views, schema: silver)
Each staging model does exactly: cast types, rename to snake_case, normalize
categorical values, filter nulls. Nothing else — no business logic here.

Operator name normalization: `IAM` → `Maroc Telecom`, keep `Orange Maroc`, `Inwi`.

### Intermediate — 3 models (ephemeral — no tables created)
Business logic only. Used as CTEs by mart models.
- `int_market_share`: `operator_share = operator_subs / total_subs` per period + segment
- `int_penetration_rates`: `penetration = subscribers / population` — uses seed `dim_population`
- `int_yoy_growth`: `growth = (val - LAG(val, 4)) / LAG(val, 4)` — 4-quarter rolling window

### Gold — 5 marts (materialized as tables, schema: gold)
These are the tables Metabase queries directly.
- `mart_market_overview`: top-level KPIs per year/quarter
- `mart_operator_perf`: per-operator breakdown (market share, ARPM, traffic, complaints)
- `mart_qos_scorecard`: QoS indicators per operator
- `mart_internet_evol`: technology mix + broadband penetration
- `mart_benchmarks`: Morocco vs MENA using ITU data

## Naming conventions — follow strictly

| Layer | Pattern | Example |
|---|---|---|
| Bronze | `bronze_{source}_{dataset}` | `bronze_anrt_mobile` |
| Staging | `stg_{source}__{dataset}` (double underscore) | `stg_anrt__mobile` |
| Intermediate | `int_{computation}` | `int_market_share` |
| Marts | `mart_{purpose}` | `mart_market_overview` |
| Seeds | `dim_{entity}` | `dim_operator` |
| DAGs | `dag_{function}` | `dag_ingest` |

## Data sources

### ANRT — data.gov.ma (CKAN API, no auth, ODbL licence)
Base URL: `https://data.gov.ma/data/api/3/action/`
Use `package_show?id={dataset_id}` to get resource download URL, then HTTP GET the XLSX.

Dataset IDs:
- `parc-de-la-telephonie-mobile-2006-2022`
- `parc-de-l-internet-2006-2022`
- `parc-de-la-telephonie-fixe-2006-2022`
- `qualite-de-service-des-reseaux-mobiles-des-telecommunications`
- `trafic-sortant-de-la-voix-et-des-sms-2006-2022`
- `bande-passante-internet-internationale-2006-2022`
- `revenu-moyen-par-minute-de-communication-arpm-facture-internet-2010-2022`
- `plaintes-des-consommateurs-2019-2022`
- `portabilites-des-numeros-mobiles-2016-2022`
- `portabilite-des-numeros-fixes`
- `usage-moyen-mensuel-sortant-de-la-telephonie-2010-2022`
- `usage-des-adresses-ip-2013-2022`
- `liaisons-data-entreprises-2018-2022`
- `resultats-des-enquetes-tic-2004-2021`
- `attribution-des-noms-de-domaines-en-ma-2013-2022`
- `parc-des-publiphones-2006-2022`

Important: ANRT XLSX files often have merged cells, multi-row headers, or headers
starting on row 2. Handle this in the extractor — do not assume row 0 is the header.

### ITU DataHub — datahub.itu.int (free since 2024, no auth)
Download Morocco slice manually or via HTTP. Store as CSV in `data/raw/itu/`.
Key indicators: mobile subscriptions per 100 inhabitants, fixed broadband,
internet users %, ICT Development Index.

## dbt configuration

### dbt_project.yml key settings
```yaml
name: morocco_telecom
version: '1.0.0'
profile: duckdb

models:
  morocco_telecom:
    staging:
      +materialized: view
      +schema: silver
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
      +schema: gold
```

### profiles.yml
```yaml
duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /opt/data/warehouse.duckdb
      threads: 4
```

### packages.yml
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

## Airflow DAG chain

```
dag_ingest (@monthly)
  → [check_sources_available]
  → [download_anrt_datasets ‖ download_itu_data]   (parallel)
  → [load_to_bronze]
  → [TriggerDagRunOperator → dag_transform]

dag_transform (triggered by dag_ingest)
  → [dbt_deps]
  → [dbt_run_staging]
  → [dbt_test_staging]           (blocks on failure)
  → [dbt_run_intermediate]
  → [dbt_run_marts]
  → [dbt_test_marts]             (blocks on failure)
  → [dbt_docs_generate]
  → [TriggerDagRunOperator → dag_quality]

dag_quality (triggered by dag_transform)
  → [ge_checkpoint_bronze]
  → [ge_checkpoint_silver]
  → [ge_checkpoint_gold]
  → [publish_quality_report]
```

Use `LocalExecutor` mode (no Celery). Set `AIRFLOW__CORE__EXECUTOR=LocalExecutor`.

## Data quality contracts

### dbt tests (blocking — DAG fails on error)
| Layer | Test | Scope |
|---|---|---|
| Bronze | `not_null` | `year`, `quarter`, `value` in all tables |
| Bronze | `accepted_values` | `operator` IN ('Maroc Telecom', 'Orange Maroc', 'Inwi') |
| Silver | `not_null` | all dimension columns |
| Silver | `unique` | `(year, quarter, operator)` in stg_mobile + stg_internet |
| Silver | singular test | `SELECT * WHERE total_subs <= 0` returns 0 rows |
| Gold | `not_null` | all KPI columns in marts |
| Gold | singular test | `SUM(market_share) BETWEEN 99 AND 101` per period |
| Gold | source freshness | warn > 90 days, error > 180 days |

### Great Expectations (non-blocking — generates HTML report in data/ge_reports/)
- Bronze: `expect_table_row_count_to_be_between` min=10 (detects empty files)
- Silver: `expect_column_values_to_be_between` — mobile_penetration in [0, 200]
- Gold: `expect_column_median_to_be_between` — market_share_iam in [30, 60]

## Common commands

```bash
# Start all services
docker-compose up -d

# Stop services
docker-compose down

# View Airflow UI
open http://localhost:8080          # credentials: admin / admin

# View Metabase UI
open http://localhost:3000

# Run dbt locally (if dbt installed)
cd dbt
dbt deps
dbt seed
dbt run
dbt test
dbt docs generate && dbt docs serve

# Run specific dbt layer
dbt run --select staging.*
dbt run --select intermediate.*
dbt run --select marts.*
dbt run --select mart_market_overview

# Inspect DuckDB
python -c "
import duckdb
con = duckdb.connect('data/warehouse.duckdb')
print(con.execute('SHOW TABLES').fetchall())
"

# Run ingestion manually
python -m ingestion.anrt_extractor
python -m ingestion.itu_extractor

# Trigger Airflow DAG manually
docker-compose exec airflow-scheduler \
  airflow dags trigger dag_ingest
```

## Implementation phases (check off as done)

- [ ] **Phase 1** — Repo scaffold: directory structure, docker-compose.yml, requirements.txt, .gitignore, .env.example
- [ ] **Phase 2** — `ingestion/anrt_extractor.py`: CKAN API loop, XLSX download, Bronze loader
- [ ] **Phase 3** — `ingestion/itu_extractor.py`: ITU CSV download, Bronze loader
- [ ] **Phase 4** — dbt init: dbt_project.yml, profiles.yml, packages.yml, seeds, sources.yml
- [ ] **Phase 5** — dbt staging: 9 `stg_` models + schema tests
- [ ] **Phase 6** — dbt intermediate + marts: 3 `int_` + 5 `mart_` models + tests
- [ ] **Phase 7** — Airflow: dag_ingest.py, dag_transform.py, dag_quality.py
- [ ] **Phase 8** — Great Expectations: expectations + checkpoints for 3 layers
- [ ] **Phase 9** — Metabase: DuckDB connection + 5 dashboards

## Coding conventions

- All Python files: type hints, docstrings, logging via `logging` module (not print)
- Error handling in extractors: catch HTTP errors, log and skip malformed XLSX rows
- dbt SQL: CTEs only (no subqueries), one CTE per logical step, final SELECT at end
- dbt models start with a `-- depends_on:` comment listing upstream models
- All column names: `snake_case`
- No hardcoded file paths — use env variables or constants from a `config.py`

## Reference docs
See `docs/` for:
- `docs/schema.md` — full column-level schema for all 16 Bronze tables
- `docs/quality.md` — complete GE expectation suite definitions


## KEEP IN MIND
create a github repo and push to it each phase