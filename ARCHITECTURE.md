# Architecture Reference

Technical reference for the Morocco Telecom Analytics pipeline — naming conventions, layer contracts, model structure, and data source details.

---

## Medallion Layers

### Bronze — raw ingestion
Exact copy of source data. No transformations. Every table appends two system columns: `source_file VARCHAR` and `ingested_at TIMESTAMP`. The loader is idempotent — it deletes rows for the current `source_file` before re-inserting, so re-runs are safe.

| Table | Source |
|---|---|
| `bronze_anrt_mobile` | Mobile subscriptions by operator and quarter |
| `bronze_anrt_internet` | Internet subscriptions by technology and quarter |
| `bronze_anrt_fixed` | Fixed-line subscriptions |
| `bronze_anrt_qos` | Quality-of-service indicators (aggregate, no operator breakdown) |
| `bronze_anrt_traffic` | Outgoing voice and SMS traffic |
| `bronze_anrt_arpm` | Average revenue per minute |
| `bronze_anrt_bandwidth` | International internet bandwidth (annual) |
| `bronze_anrt_complaints` | Consumer complaints by type |
| `bronze_anrt_portability` | Number portability (mobile + fixed) |
| `bronze_anrt_usage_avg` | Average monthly outgoing usage in minutes |
| `bronze_anrt_ip` | IPv4 count and IPv6 prefixes (annual, no quarter) |
| `bronze_anrt_data_links` | Enterprise data link circuits by type |
| `bronze_anrt_tic_survey` | ICT survey results — long format, one row per indicator |
| `bronze_anrt_domains` | Active .ma domain registrations |
| `bronze_anrt_payphones` | Payphone count |
| `bronze_itu_morocco` | ITU ICT indicators for Morocco (2000–2024) |

### Silver — dbt staging (schema: `silver`)
Each model does exactly: cast types → rename to snake_case → filter invalid rows. No business logic.

Key normalisation rules applied at this layer:
- Operator names: `IAM` / `Itissalat Al-Maghrib` / `MT` → `Maroc Telecom`; `Médi Telecom` → `Orange Maroc`; `Wana Corporate` → `Inwi`
- `'Total'` summary rows are dropped (present in mobile and internet Bronze tables as per-period aggregates)
- Null key-column rows are dropped (e.g., Inwi had no subscribers in 2006–2008)
- FTTH internet subscribers are divided by 1 000 to match the thousands unit used by all other internet technologies

### Intermediate — business logic (ephemeral)
Three CTEs used exclusively by mart models. No tables created.

| Model | Logic |
|---|---|
| `int_market_share` | `market_share_pct = operator_subs / SUM(operator_subs) × 100` per (year, quarter) |
| `int_penetration_rates` | `penetration = (total_subs_Q4 × 1 000) / population × 100` — annual Q4 snapshot joined to `dim_population` |
| `int_yoy_growth` | `(subs − LAG(subs, 4)) / LAG(subs, 4) × 100` — 4-quarter lag = same quarter previous year |

### Gold — mart tables (schema: `gold`)

| Mart | Key columns | Source models |
|---|---|---|
| `mart_market_overview` | year, quarter, mobile_total_subs, fixed_total_subs, internet_total_subs, mobile_yoy_growth_pct | stg_mobile, stg_internet, stg_fixed, int_yoy_growth |
| `mart_operator_perf` | year, quarter, operator, market_share_pct, arpm, voice_minutes, complaint_count | int_market_share, stg_arpm, stg_traffic, stg_complaints |
| `mart_qos_scorecard` | year, operator, indicator, value | stg_qos |
| `mart_internet_evol` | year, quarter, adsl_subs, ftth_subs, mobile_bb_subs, mobile_penetration_per_100 | stg_internet, stg_bandwidth, int_penetration_rates |
| `mart_benchmarks` | year, mobile_subs_total, fixed_bb_subs, internet_users_pct, intl_bandwidth_mbps | stg_itu__morocco |

---

## Naming Conventions

| Layer | Pattern | Example |
|---|---|---|
| Bronze | `bronze_{source}_{dataset}` | `bronze_anrt_mobile` |
| Staging | `stg_{source}__{dataset}` (double underscore) | `stg_anrt__mobile` |
| Intermediate | `int_{computation}` | `int_market_share` |
| Marts | `mart_{purpose}` | `mart_market_overview` |
| Seeds | `dim_{entity}` | `dim_operator` |
| DAGs | `dag_{function}` | `dag_ingest` |

---

## Data Sources

### ANRT — data.gov.ma
Base URL: `https://data.gov.ma/data/api/3/action/`

The extractor calls `package_show?id={dataset_id}` to resolve the XLSX download URL, then streams the file to `data/raw/anrt/`. XLSX files have merged cells and multi-row headers — a universal parser scans each file for the header row (identified by `"Période (à fin)"` or `"Donnée"` in column 0) and reads the legend block above it.

**Data ceiling:** ANRT's published open data stops at 2022. No 2023–2024 ANRT data is publicly available.

### ITU DataHub — api.datahub.itu.int
Base URL: `https://api.datahub.itu.int/v2`

Endpoint: `GET /v2/data/bycode/{codeID}/byiso/MAR` (ISO 3166-1 alpha-3, not the short name).

| codeID | code | Indicator |
|---|---|---|
| 178 | i271 | Mobile-cellular subscriptions |
| 15 | i112 | Fixed-telephone subscriptions |
| 193 | i271p | Mobile-cellular subscriptions: Prepaid |
| 19303 | i992b | Fixed-broadband subscriptions |
| 11632 | i271mw | Active mobile-broadband subscriptions |
| 11624 | i99H | Individuals using the Internet (% of population) |
| 242 | i4214 | International bandwidth usage (Mbit/s) |
| 331 | i741$ | Revenue from mobile networks (millions USD) |

---

## dbt Configuration

```yaml
# dbt_project.yml (key settings)
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

```yaml
# profiles.yml
duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /opt/data/warehouse.duckdb
      threads: 4
```

---

## Airflow DAG Chain

```
dag_ingest (@monthly)
  → check_sources_available
  → download_anrt_datasets ║ download_itu_data   (parallel)
  → load_to_bronze
  → TriggerDagRunOperator → dag_transform

dag_transform (triggered)
  → dbt_deps
  → dbt_run_staging → dbt_test_staging --fail-fast
  → dbt_run_intermediate
  → dbt_run_marts → dbt_test_marts --fail-fast
  → dbt_docs_generate                  (retries=4)
  → TriggerDagRunOperator → dag_quality

dag_quality (triggered)
  → ge_checkpoint_bronze   (pushes CheckResult to XCom)
  → ge_checkpoint_silver   (pushes CheckResult to XCom)
  → ge_checkpoint_gold     (pushes CheckResult to XCom)
  → publish_quality_report (pulls XCom, writes quality_YYYYMMDD_HHMMSS.txt)
```

---

## Metabase / PostgreSQL Export

Metabase connects to the PostgreSQL instance (already running for Airflow metadata) via a native driver. The Gold mart tables are exported from DuckDB to a `gold` schema in PostgreSQL by `ingestion/export_to_postgres.py`. The entire export runs inside a single transaction — any failure rolls back all tables, leaving PostgreSQL in the previous consistent state.

The DuckDB warehouse is **not** connected directly to Metabase: the DuckDB JDBC driver cannot read files written by DuckDB 1.5.3 without a matching driver version, and the community Metabase–DuckDB driver has HoneySQL v1/v2 compatibility issues with Metabase 0.49. PostgreSQL export is the production-grade solution.
