# Phase 2 — ANRT Extractor

**Status:** ✅ Complete  
**Commit:** `bc35f5a`, `7b767e7`  
**File:** `ingestion/anrt_extractor.py`

## What was built

Downloads 16 ANRT datasets from the CKAN API at `data.gov.ma`, parses the XLSX files, and loads them into Bronze DuckDB tables.

### Key components

**`DatasetConfig`** — dataclass holding for each dataset:
- `dataset_id` (CKAN package ID)
- `bronze_table` name
- `ddl` (CREATE TABLE IF NOT EXISTS SQL)
- `row_parser` (function: `(legend, row) → dict | None`)

**`_parse_anrt_xlsx(path)`** — universal XLSX reader:
- Detects the header row by scanning for a cell containing `"Période"` or `"Donnée"`
- Everything above the header row is treated as the legend block (letter_code → description)
- Data rows are filtered to only keep valid period strings: `T1-2006`, `S2-2013`, `2006`

**`_parse_period(period)`** — maps period strings to `(year, quarter)`:
- `T1-2006` → `(2006, 'Q1')`
- `S1-2013` → `(2013, 'S1')`
- `2006` → `(2006, None)`

**`_load_to_bronze(con, cfg, df, source_file, ingested_at)`** — idempotent loader:
- `DELETE WHERE source_file = ?` before INSERT (safe re-runs)

### Operator name normalization (done in row parsers)
| Raw value | Normalized |
|---|---|
| `ITISSALAT AL-MAGHRIB`, `IAM` | `Maroc Telecom` |
| `MEDI TELECOM` | `Orange Maroc` |
| `WANA CORPORATE` | `Inwi` |

### Result
15/15 Bronze tables loaded, 2,143 total rows, years 2004–2025.

## Problems encountered

### 1. First extractor returned 0 rows for every table
**Problem:** The original parsers searched for French column keywords (`"année"`, `"trimestre"`) across all cells. The real ANRT XLSX format uses:
1. A **legend block** at the top: two columns mapping a letter code to a description (e.g., `A → Maroc Telecom`)
2. A **header row** starting with `"Période (à fin)"` or just `"Donnée"`
3. **Data rows** using those letter codes as column headers, with period strings like `T1-2006`

The original parsers never found valid headers, returned empty DataFrames.

**Fix:** Complete rewrite of the parser using a universal `_parse_anrt_xlsx()` function that scans row-by-row for the header row, extracts the legend, then filters only rows matching the period regex.

### 2. `total_subs` values in thousands
**Discovery:** Comparing ANRT 2006-Q4 Total (16,005) against ITU 2006 value (16,004,731) confirmed that all ANRT subscriber counts are in **thousands**, not absolute units. This affects penetration rate calculations downstream.

### 3. FTTH internet data in different units
**Discovery (found in Phase 6):** In `bronze_anrt_internet`, the FTTH row is stored in **absolute subscriber counts** while ADSL, Mobile, Leased, and Other are in thousands. Fixed in `stg_anrt__internet.sql` by dividing FTTH by 1000.

### 4. `'Total'` aggregate rows included in mobile data
**Discovery (found in Phase 5 testing):** The `accepted_values` test for `operator` failed because `'Total'` rows were included in `bronze_anrt_mobile`. The extractor correctly stores them (faithful Bronze copy). Fixed in `stg_anrt__mobile.sql` with `WHERE operator != 'Total'`.

## Test result
```
15 tables loaded:
  bronze_anrt_mobile       320 rows
  bronze_anrt_internet     436 rows
  bronze_anrt_fixed         80 rows
  bronze_anrt_qos           60 rows
  bronze_anrt_traffic      160 rows
  bronze_anrt_arpm          63 rows
  bronze_anrt_complaints   196 rows
  bronze_anrt_bandwidth     20 rows
  ... (7 more tables)
Total: 2,143 rows, years 2004–2025
```
