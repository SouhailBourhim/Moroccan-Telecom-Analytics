# Phase 3 — ITU Extractor

**Status:** ✅ Complete — tested locally and inside Docker  
**Commit:** `b1b82ad`  
**File:** `ingestion/itu_extractor.py`

## What was built

Downloads 8 ICT indicators for Morocco from the ITU DataHub REST API and loads them into `bronze_itu_morocco`.

### API endpoint discovered
The ITU DataHub website uses a REST API at `https://api.datahub.itu.int/v2` (found by inspecting the Next.js JS bundle). The correct endpoint for Morocco data is:
```
GET /v2/data/bycode/{codeID}/byiso/MAR
```
- Uses ISO 3166-1 alpha-3 code `MAR` (not the short name "Morocco" which returns empty)
- Returns an array of records with `dataYear`, `answer[0].value`, `code`, `codeID`, `dataSource`

### Indicators fetched
| codeID | code | Indicator | Unit |
|---|---|---|---|
| 178 | i271 | Mobile-cellular subscriptions | subscriptions |
| 15 | i112 | Fixed-telephone subscriptions | subscriptions |
| 193 | i271p | Mobile-cellular subscriptions: Prepaid | subscriptions |
| 19303 | i992b | Fixed-broadband subscriptions | subscriptions |
| 11632 | i271mw | Active mobile-broadband subscriptions | subscriptions |
| 11624 | i99H | Individuals using the Internet | % of population |
| 242 | i4214 | International bandwidth usage | Mbit/s |
| 331 | i741$ | Revenue from mobile networks | millions USD |

### Bronze table schema
```sql
CREATE TABLE IF NOT EXISTS bronze_itu_morocco (
    year            INTEGER      NOT NULL,
    indicator_code  VARCHAR      NOT NULL,
    indicator_name  VARCHAR      NOT NULL,
    value           DOUBLE,
    unit            VARCHAR,
    data_source     VARCHAR,
    ingested_at     TIMESTAMP    NOT NULL
)
```

### Local test result
```
8 indicators fetched, 189 rows loaded into bronze_itu_morocco
Years: 2000–2024, 0 failures
```

## Problems encountered

### 1. ITU DataHub API is not the published SDMX endpoint
**Problem:** The CLAUDE.md ITU_BASE_URL `https://datahub.itu.int/` is the website, not the API. Standard SDMX endpoints (`/api/v1/data/...`) return 403 Forbidden. The download endpoint (`/v2/data/download`) also returns 403.

**Fix:** Inspected the Next.js JS bundle at `datahub.itu.int/_next/static/chunks/` to find the real API URL: `https://api.datahub.itu.int/v2`.

### 2. `/v2/data/bycode/{id}/byiso/Morocco` returns empty
**Problem:** Using the short name `"Morocco"` in the byiso parameter returns `[]`.  
**Fix:** Use the ISO 3-letter code `"MAR"` instead.

### 3. IDI (ICT Development Index) endpoint requires auth
**Problem:** `/v2/idi/data/bycountryid/155` returns IDI data but the label endpoint `/v2/idi/dictionary` returns 403. Cannot map IDI codeIDs (1–14) to indicator names without auth.  
**Decision:** Excluded IDI data from the extractor. The 8 standard indicators cover all mart requirements.

## Docker validation result

```
docker compose exec airflow-scheduler python -m ingestion.itu_extractor
→ 8 indicators fetched, 189 rows loaded, 0 failures
→ Raw CSV written to /opt/data/raw/itu/itu_morocco_20260604.csv
```

### Issue found and fixed: DuckDB version mismatch
The local `.venv` had DuckDB 1.5.3 (which wrote `warehouse.duckdb`) but `requirements.txt` pinned 0.10.0. DuckDB cannot read files written by a newer version — the container crashed with `SerializationException: expected end of object, field id: 100`.

**Fix:** Upgraded container to DuckDB 1.5.3 and updated `requirements.txt` and `CLAUDE.md` to pin `duckdb==1.5.3`.
