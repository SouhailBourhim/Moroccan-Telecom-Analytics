# Phase 5 — dbt Staging Models

**Status:** ✅ Complete  
**Commit:** `2c55bfe`

## What was built

9 staging views in `main_silver` schema, each doing: cast types → rename to snake_case → filter nulls/invalid rows.

| Model | Source table | Key transformations |
|---|---|---|
| `stg_anrt__mobile` | `bronze_anrt_mobile` | Filter `operator != 'Total'`, cast all columns |
| `stg_anrt__internet` | `bronze_anrt_internet` | FTTH ÷ 1000 (unit normalization), filter `subscribers > 0` |
| `stg_anrt__fixed` | `bronze_anrt_fixed` | Cast, filter nulls |
| `stg_anrt__qos` | `bronze_anrt_qos` | Cast, filter `value IS NOT NULL` |
| `stg_anrt__traffic` | `bronze_anrt_traffic` | Cast, filter `voice_minutes IS NOT NULL` |
| `stg_anrt__arpm` | `bronze_anrt_arpm` | Cast, filter `arpm IS NOT NULL` |
| `stg_anrt__complaints` | `bronze_anrt_complaints` | Rename `count` → `complaint_count` |
| `stg_anrt__bandwidth` | `bronze_anrt_bandwidth` | Annual only (no quarter column) |
| `stg_itu__morocco` | `bronze_itu_morocco` | Cast all columns |

Schema tests added in `schema.yml` files for both ANRT and ITU staging models.

## Problems encountered

### 1. `accepted_values` test for `operator` failing
**Problem:** The test `accepted_values: ['Maroc Telecom', 'Orange Maroc', 'Inwi']` on `stg_anrt__mobile.operator` found 1 unexpected value: `'Total'`.

**Root cause:** The ANRT XLSX files include a "Total" summary row for each period. The Bronze extractor faithfully copies it (correct — Bronze is a raw copy). The staging model must filter it out.

**Fix:** Added `AND operator != 'Total'` to the WHERE clause in `stg_anrt__mobile.sql`.

### 2. FTTH stored in absolute units, all other internet technologies in thousands
**Problem (discovered during Phase 6 mart review):** `bronze_anrt_internet` stores FTTH subscriber counts in absolute numbers (e.g., 611,032 in 2022-Q4) while ADSL, Mobile, Leased, and Other are in thousands (e.g., ADSL=1,556 = 1,556,000 subs). The "Total" row is also in thousands (35,574 = 35.5M), so FTTH in thousands would be 611B — impossible.

**Proof:** ANRT Q4 2022 Total=35,574K. Sum of others: 1556+3+33204+202=34965K. Difference: 609K ≈ FTTH 611K (small rounding). Confirms FTTH is in absolute units.

**Fix:** In `stg_anrt__internet.sql`:
```sql
case
    when technology = 'FTTH'
        then cast(cast(subscribers as bigint) / 1000.0 as bigint)
    else cast(subscribers as bigint)
end as subscribers
```

### 3. QoS data has no operator breakdown
**Discovery:** All 60 rows in `bronze_anrt_qos` have `operator = NULL`. The ANRT QoS XLSX reports aggregate national indicators, not per-operator. The `mart_qos_scorecard` reflects this — no per-operator filtering possible.

## Test result
```
dbt run --select staging  → PASS=9 WARN=0 ERROR=0 TOTAL=9
dbt test --select staging → PASS=25 WARN=0 ERROR=0 TOTAL=25
```
