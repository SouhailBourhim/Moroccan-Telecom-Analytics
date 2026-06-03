# Data quality contracts

## Strategy

- **dbt tests** → blocking. DAG task fails immediately. Retries = 0.
- **GE checkpoints** → non-blocking. Failure = warning logged + HTML report written
  to `data/ge_reports/{layer}/{run_date}/`. Airflow marks as success with warning.
- Tests run **after each layer**, not only at the end of the pipeline.

---

## dbt tests — Bronze (in sources.yml)

```yaml
sources:
  - name: bronze
    database: warehouse
    schema: main
    tables:
      - name: bronze_anrt_mobile
        columns:
          - name: year
            tests: [not_null]
          - name: operator
            tests:
              - not_null
              - accepted_values:
                  values: ['Maroc Telecom', 'Orange Maroc', 'Inwi', 'Total']
          - name: total_subs
            tests: [not_null]
      - name: bronze_anrt_internet
        columns:
          - name: year
            tests: [not_null]
          - name: technology
            tests:
              - accepted_values:
                  values: ['ADSL', '4G', 'Fiber', 'FH', 'Total']
```

---

## dbt tests — Silver (in staging/anrt/schema.yml)

```yaml
models:
  - name: stg_anrt__mobile
    columns:
      - name: year
        tests: [not_null]
      - name: quarter
        tests:
          - accepted_values:
              values: ['Q1', 'Q2', 'Q3', 'Q4', null]
      - name: operator
        tests: [not_null]
      - name: total_subs
        tests: [not_null]
    tests:
      - unique:
          column_name: "year || '-' || coalesce(quarter, 'annual') || '-' || operator"

  - name: stg_anrt__internet
    tests:
      - unique:
          column_name: "year || '-' || coalesce(quarter, 'annual') || '-' || technology"
```

### Singular test: stg_anrt__mobile_no_negative_subs.sql
```sql
-- tests/generic/stg_anrt__mobile_no_negative_subs.sql
select *
from {{ ref('stg_anrt__mobile') }}
where total_subs <= 0
  and operator != 'Total'   -- Total rows can be 0 for granular slices
```

---

## dbt tests — Gold (in marts/schema.yml)

```yaml
models:
  - name: mart_market_overview
    columns:
      - name: year
        tests: [not_null]
      - name: total_mobile_subs
        tests: [not_null]
      - name: mobile_penetration_rate
        tests: [not_null]
```

### Singular test: market_share_sums_to_100.sql
```sql
-- tests/generic/market_share_sums_to_100.sql
select
  year,
  quarter,
  segment,
  sum(market_share_pct) as total_share
from {{ ref('mart_operator_perf') }}
where operator != 'Total'
group by 1, 2, 3
having total_share < 99 or total_share > 101
```

### Source freshness (in sources.yml)
```yaml
sources:
  - name: bronze
    freshness:
      warn_after: {count: 90, period: day}
      error_after: {count: 180, period: day}
    loaded_at_field: ingested_at
```

---

## Great Expectations — Bronze checkpoint

File: `great_expectations/checkpoints/bronze_checkpoint.yml`

Expectations per suite `bronze_suite`:
```python
# expect_table_row_count_to_be_between
# Per table: min_value=10, max_value=None
# Detects: empty files, truncated downloads

# expect_column_to_exist
# Check all required columns are present after ingestion

# expect_column_values_to_not_be_null
# year, quarter (where applicable), core value columns

# expect_column_values_to_be_of_type
# year: INTEGER, value columns: FLOAT or INTEGER
```

---

## Great Expectations — Silver checkpoint

File: `great_expectations/checkpoints/silver_checkpoint.yml`

Expectations per suite `silver_suite`:
```python
# expect_column_values_to_be_between
# mobile_penetration_rate: min=0, max=200
# arpm: min=0, max=10
# market_share_pct (intermediate): min=0, max=100

# expect_column_values_to_match_regex
# year: ^\d{4}$
# quarter: ^Q[1-4]$  (where not null)

# expect_column_values_to_be_in_set
# operator: {'Maroc Telecom', 'Orange Maroc', 'Inwi', 'Total'}
```

---

## Great Expectations — Gold checkpoint

File: `great_expectations/checkpoints/gold_checkpoint.yml`

Expectations per suite `gold_suite`:
```python
# expect_column_median_to_be_between
# mart_operator_perf.market_share_pct where operator='Maroc Telecom': min=30, max=65
# Detects: major data anomaly in dominant operator

# expect_column_values_to_be_between
# mart_market_overview.mobile_penetration_rate: min=50, max=180
# (Morocco mobile penetration is historically 80-140%)

# expect_table_row_count_to_be_between
# mart_market_overview: min=40  (at least 10 years × 4 quarters)
# mart_qos_scorecard: min=30
```

---

## Alerting

When a GE checkpoint fails critically (score < 80%), `publish_quality_report`
logs an Airflow warning and writes a summary to `data/ge_reports/FAILED_{date}.txt`.
No automatic email configured — check Airflow logs manually or add an
`EmailOperator` in dag_quality.py if needed.
