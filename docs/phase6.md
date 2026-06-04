# Phase 6 — dbt Intermediate + Mart Models

**Status:** ✅ Complete  
**Commit:** `091e531`

## What was built

### Intermediate models (ephemeral — no tables created)

| Model | Logic |
|---|---|
| `int_market_share` | `market_share_pct = operator_subs / SUM(operator_subs) per period × 100` |
| `int_penetration_rates` | `mobile_penetration_per_100 = (total_subs_Q4 × 1000) / population × 100` (annual, Q4 snapshot) |
| `int_yoy_growth` | `yoy_growth_pct = (subs - LAG(subs, 4)) / LAG(subs, 4) × 100` (4-quarter window = same quarter last year) |

### Gold mart tables (materialized as tables in `main_gold`)

| Mart | Source models | Description |
|---|---|---|
| `mart_market_overview` | mobile, internet, fixed, traffic, arpm, int_yoy_growth | Top-level KPIs per year/quarter |
| `mart_operator_perf` | int_market_share, arpm, traffic, complaints | Per-operator breakdown |
| `mart_qos_scorecard` | stg_anrt__qos | QoS indicators (no operator breakdown) |
| `mart_internet_evol` | stg_anrt__internet, stg_anrt__bandwidth, int_penetration_rates | Technology mix + penetration |
| `mart_benchmarks` | stg_itu__morocco | ITU indicators pivoted to one row per year |

### Schema tests
- `mart_operator_perf.operator` accepted_values test (Maroc Telecom, Orange Maroc, Inwi)
- `mart_operator_perf.market_share_pct` not_null
- `mart_benchmarks.mobile_subs_total` not_null
- `mart_market_overview.mobile_total_subs` not_null

## Key data facts confirmed

- **ANRT `total_subs` unit:** thousands (ANRT 2006-Q4 Total = 16,005 ✓ vs ITU 2006 = 16,004,731)
- **2022 Q4 market shares:** Maroc Telecom 36.3%, Orange Maroc 33.2%, Inwi 30.5% (sums to 100% ✓)
- **Morocco mobile penetration 2022:** 141.2 per 100 inhabitants (above 100 = multi-SIM, expected)
- **Internet users 2022:** 89.9% of population (per ITU)

## Problems encountered

### 1. `int_penetration_rates` needs unit conversion
**Problem:** `total_subs` from `stg_anrt__mobile` is in thousands. Dividing directly by population gives a rate 1000× too small.

**Fix:** Multiply by 1000 before dividing: `(total_subs_thousands × 1000) / population × 100`.

### 2. `mart_internet_evol` FTTH join issue (pre-fix)
**Problem:** Before the FTTH unit fix in Phase 5, `mart_internet_evol` showed `ftth_subs = 611,032` which implied 611 billion subscribers. After the staging fix (÷1000), it correctly shows 611K.

### 3. `int_penetration_rates` only produces annual values
**Design decision:** Penetration is computed from Q4 (end-of-year) snapshots and the annual population seed. In `mart_internet_evol`, the annual penetration value is joined onto all 4 quarters of that year (same value shown for Q1–Q4), which is acceptable for visualization.

## Test result
```
dbt run   → PASS=14 WARN=0 ERROR=0 TOTAL=14  (9 views + 5 tables)
dbt test  → PASS=41 WARN=0 ERROR=0 TOTAL=41
```
