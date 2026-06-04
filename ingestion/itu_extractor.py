"""Phase 3 — ITU DataHub extractor.

Downloads Morocco ICT indicator data from the ITU DataHub REST API
(https://api.datahub.itu.int/v2) and loads it into the bronze_itu_morocco
DuckDB table.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_API_BASE = os.environ.get("ITU_API_BASE", "https://api.datahub.itu.int/v2")
_ISO_CODE = "MAR"  # Morocco ISO-3166-1 alpha-3
_WAREHOUSE = Path(os.environ.get("WAREHOUSE_PATH", "data/warehouse.duckdb"))
_RAW_DIR = Path(os.environ.get("RAW_DIR", "data/raw"))
_TIMEOUT = 30
_RETRY_WAITS = (2, 4, 8)

_BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS bronze_itu_morocco (
    year            INTEGER      NOT NULL,
    indicator_code  VARCHAR      NOT NULL,
    indicator_name  VARCHAR      NOT NULL,
    value           DOUBLE,
    unit            VARCHAR,
    data_source     VARCHAR,
    ingested_at     TIMESTAMP    NOT NULL
)
"""

# ── Indicator catalogue ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Indicator:
    code_id: int
    code: str          # ITU short code (e.g. "i271")
    name: str
    unit: str


_INDICATORS: list[_Indicator] = [
    _Indicator(178,   "i271",   "Mobile-cellular subscriptions",                  "subscriptions"),
    _Indicator(15,    "i112",   "Fixed-telephone subscriptions",                  "subscriptions"),
    _Indicator(193,   "i271p",  "Mobile-cellular subscriptions: Prepaid",         "subscriptions"),
    _Indicator(19303, "i992b",  "Fixed-broadband subscriptions",                  "subscriptions"),
    _Indicator(11632, "i271mw", "Active mobile-broadband subscriptions",          "subscriptions"),
    _Indicator(11624, "i99H",   "Individuals using the Internet",                 "% of population"),
    _Indicator(242,   "i4214",  "International bandwidth usage",                  "Mbit/s"),
    _Indicator(331,   "i741$",  "Revenue from mobile networks",                   "millions USD"),
]

# ── HTTP helpers ───────────────────────────────────────────────────────────────


def _get(session: requests.Session, url: str) -> list[dict]:
    """GET *url* with retry on 5xx; return parsed JSON list."""
    for attempt, wait in enumerate((*_RETRY_WAITS, None)):
        try:
            resp = session.get(url, timeout=_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code
            if status < 500 or wait is None:
                raise
            logger.warning("HTTP %d on attempt %d — retry in %ds", status, attempt + 1, wait)
            time.sleep(wait)
        except requests.exceptions.RequestException as exc:
            if wait is None:
                raise
            logger.warning("Request error on attempt %d — retry in %ds: %s", attempt + 1, wait, exc)
            time.sleep(wait)
    raise RuntimeError("Unreachable")  # pragma: no cover

# ── Fetch logic ────────────────────────────────────────────────────────────────


def _fetch_indicator(ind: _Indicator, session: requests.Session) -> list[dict]:
    """Return row dicts for one indicator × Morocco from the ITU API."""
    url = f"{_API_BASE}/data/bycode/{ind.code_id}/byiso/{_ISO_CODE}"
    logger.info("Fetching %-50s codeID=%d", ind.name, ind.code_id)
    records = _get(session, url)

    rows: list[dict] = []
    skipped = 0
    for rec in records:
        answer = rec.get("answer") or []
        if not answer:
            skipped += 1
            continue
        try:
            value = float(answer[0]["value"])
        except (KeyError, TypeError, ValueError):
            skipped += 1
            continue
        rows.append({
            "year":           int(rec["dataYear"]),
            "indicator_code": ind.code,
            "indicator_name": ind.name,
            "value":          value,
            "unit":           ind.unit,
            "data_source":    (rec.get("dataSource") or "").strip(),
        })

    logger.info("  → %d rows (%d skipped)", len(rows), skipped)
    return rows

# ── Raw CSV writer ─────────────────────────────────────────────────────────────


def _save_raw_csv(df: pd.DataFrame, ingested_at: datetime) -> Path:
    """Write raw rows to data/raw/itu/itu_morocco_<date>.csv."""
    itu_dir = _RAW_DIR / "itu"
    itu_dir.mkdir(parents=True, exist_ok=True)
    date_str = ingested_at.strftime("%Y%m%d")
    path = itu_dir / f"itu_morocco_{date_str}.csv"
    df.to_csv(path, index=False)
    logger.info("Raw ITU data saved → %s (%d rows)", path, len(df))
    return path


# ── Bronze loader ──────────────────────────────────────────────────────────────


def _load_bronze(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, ingested_at: datetime) -> int:
    """Full-refresh load into bronze_itu_morocco."""
    con.execute(_BRONZE_DDL)
    con.execute("DELETE FROM bronze_itu_morocco")
    if df.empty:
        logger.warning("No rows to load into bronze_itu_morocco")
        return 0
    df = df.copy()
    df["ingested_at"] = ingested_at
    con.execute("INSERT INTO bronze_itu_morocco SELECT * FROM df")
    return len(df)

# ── Entry-point ────────────────────────────────────────────────────────────────


def run_itu_extraction() -> None:
    """Download all configured ITU indicators for Morocco and load to Bronze."""
    _WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    failed: list[str] = []

    with requests.Session() as session:
        session.headers.update({"User-Agent": "morocco-telecom-analytics/1.0"})
        for ind in _INDICATORS:
            try:
                all_rows.extend(_fetch_indicator(ind, session))
            except Exception:
                logger.exception("Failed to fetch %s (codeID=%d) — skipping", ind.code, ind.code_id)
                failed.append(ind.code)

    df = pd.DataFrame(all_rows)
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

    if not df.empty:
        _save_raw_csv(df, ingested_at)

    con = duckdb.connect(str(_WAREHOUSE))
    try:
        n = _load_bronze(con, df, ingested_at)
    finally:
        con.close()

    logger.info(
        "bronze_itu_morocco: %d rows loaded across %d indicators (%d failed)",
        n,
        len(_INDICATORS) - len(failed),
        len(failed),
    )
    if failed:
        logger.warning("Failed indicators: %s", ", ".join(failed))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run_itu_extraction()
