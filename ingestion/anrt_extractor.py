"""
ANRT extractor — CKAN API → XLSX download → Bronze tables in DuckDB.

Flow per dataset:
  1. package_show CKAN call → resource download URL
  2. HTTP GET → save XLSX to data/raw/anrt/
  3. Parse XLSX using the universal ANRT format reader
  4. Map to Bronze schema (docs/schema.md)
  5. Load into DuckDB bronze_anrt_* table

All ANRT XLSX files share the same structure:
  - Top rows: legend block  [letter_code | description]
  - Blank separator rows
  - Header row: "Période (à fin)" in col-0, letter codes as remaining columns
  - Data rows: period string in col-0 ("T1-2006", "S2-2013", "2006"), values in remaining cols
  Exception: QoS file uses "Donnée" as the header key instead of "Période (à fin)".
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import time

import duckdb
import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
ANRT_BASE_URL = os.environ.get("ANRT_BASE_URL", "https://data.gov.ma/data/api/3/action/")
RAW_DIR = Path(os.environ.get("RAW_DIR", "data/raw"))
WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "data/warehouse.duckdb")
ANRT_RAW_DIR = RAW_DIR / "anrt"

REQUEST_TIMEOUT = 120  # seconds

# ── Operator name normalisation ────────────────────────────────────────────────
# Maps historical operator names (as they appear in legend descriptions) to
# canonical Bronze names.
_OPERATOR_MAP: dict[str, str] = {
    "iam": "Maroc Telecom",
    "itissalat al-maghrib": "Maroc Telecom",
    "maroc telecom": "Maroc Telecom",
    "mt": "Maroc Telecom",
    "orange maroc": "Orange Maroc",
    "medi telecom": "Orange Maroc",
    "orange": "Orange Maroc",
    "inwi": "Inwi",
    "wana corporate": "Inwi",
    "wana": "Inwi",
    "total": "Total",
}


def _norm_operator(val: object) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    key = str(val).strip().lower()
    return _OPERATOR_MAP.get(key, str(val).strip())


# ── Universal ANRT XLSX reader ────────────────────────────────────────────────
def _parse_anrt_xlsx(path: Path) -> tuple[dict[str, str], pd.DataFrame]:
    """Read any ANRT XLSX and return (legend, data).

    legend: {column_code → description}
    data:   DataFrame with columns ["period", code1, code2, ...]
            where period strings are e.g. "T1-2006", "S2-2013", "2017".
    """
    raw = pd.read_excel(path, sheet_name=0, header=None, dtype=str)

    # Find header row: first row where col-0 contains "Période" or equals "Donnée"
    header_row = None
    for i in range(len(raw)):
        cell = str(raw.iloc[i, 0]).strip()
        if "Période" in cell or cell == "Donnée":
            header_row = i
            break

    if header_row is None:
        raise ValueError(f"Cannot locate header row ('Période'/'Donnée') in {path.name}")

    # Build legend from rows above the header
    legend: dict[str, str] = {}
    for i in range(header_row):
        code = str(raw.iloc[i, 0]).strip()
        desc_cell = raw.iloc[i, 1]
        if code and code != "nan" and pd.notna(desc_cell):
            legend[code] = str(desc_cell).strip()

    # Slice out the data block
    col_names = [str(v).strip() if pd.notna(v) else None for v in raw.iloc[header_row]]
    data = raw.iloc[header_row + 1 :].copy()
    data.columns = col_names
    period_col = col_names[0]

    # Keep only rows that look like valid period strings and drop separator rows
    valid = data[period_col].apply(
        lambda v: bool(
            re.match(r"^(T[1-4]-\d{4}|S[12]-\d{4}|\d{4})$", str(v).strip())
        )
    )
    data = data[valid].reset_index(drop=True)
    data = data.rename(columns={period_col: "period"})

    return legend, data


def _parse_period(period: str) -> tuple[int | None, str | None]:
    """Return (year, quarter) from period strings.

    'T1-2006' → (2006, 'Q1')
    'S2-2013' → (2013, 'S2')
    '2017'    → (2017, None)
    """
    s = str(period).strip()
    m = re.match(r"^T([1-4])-(\d{4})$", s)
    if m:
        return int(m.group(2)), f"Q{m.group(1)}"
    m = re.match(r"^S([12])-(\d{4})$", s)
    if m:
        return int(m.group(2)), f"S{m.group(1)}"
    m = re.match(r"^(\d{4})$", s)
    if m:
        return int(m.group(1)), None
    return None, None


# ── Type coercion ──────────────────────────────────────────────────────────────
def _int(val: object) -> int | None:
    try:
        cleaned = re.sub(r"[\s\xa0,]", "", str(val))
        return int(float(cleaned))
    except (ValueError, TypeError):
        return None


def _float(val: object) -> float | None:
    try:
        cleaned = re.sub(r"[\s\xa0]", "", str(val)).replace(",", ".")
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _col(row: pd.Series, code: str) -> object:
    """Safely get a cell; return None when column is absent or NaN."""
    if code not in row.index:
        return None
    v = row[code]
    return None if (isinstance(v, float) and pd.isna(v)) or str(v).strip().upper() in {"NAN", "ND", "NM", ""} else v


# ── Per-dataset parsers ────────────────────────────────────────────────────────
def _parse_mobile(path: Path) -> pd.DataFrame:
    """bronze_anrt_mobile: year, quarter, operator, total_subs, prepaid_subs, postpaid_subs.

    Column layout (values in thousands):
      postpaid: A=IAM  B=Orange  C=Inwi  D=Total
      prepaid:  E=IAM  F=Orange  G=Inwi  H=Total
      total:    I=IAM  J=Orange  K=Inwi  L=Total
    """
    _, data = _parse_anrt_xlsx(path)

    op_cols = {
        "Maroc Telecom": ("I", "E", "A"),
        "Orange Maroc":  ("J", "F", "B"),
        "Inwi":          ("K", "G", "C"),
        "Total":         ("L", "H", "D"),
    }

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        for operator, (tot, pre, post) in op_cols.items():
            rows.append({
                "year": year,
                "quarter": quarter,
                "operator": operator,
                "total_subs": _int(_col(r, tot)),
                "prepaid_subs": _int(_col(r, pre)),
                "postpaid_subs": _int(_col(r, post)),
            })
    return pd.DataFrame(rows)


def _parse_internet(path: Path) -> pd.DataFrame:
    """bronze_anrt_internet: year, quarter, technology, subscribers.

    Aggregate columns only (global totals per technology):
      D=ADSL  H=Mobile  K=FTTH  L=Leased  M=Other  Q=Total
    """
    _, data = _parse_anrt_xlsx(path)

    tech_cols = {
        "ADSL":   "D",
        "Mobile": "H",
        "FTTH":   "K",
        "Leased": "L",
        "Other":  "M",
        "Total":  "Q",
    }

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        for technology, col in tech_cols.items():
            val = _int(_col(r, col))
            if val is None:
                continue
            rows.append({
                "year": year,
                "quarter": quarter,
                "technology": technology,
                "subscribers": val,
            })
    return pd.DataFrame(rows)


def _parse_fixed(path: Path) -> pd.DataFrame:
    """bronze_anrt_fixed: year, quarter, total_subs.

    Z = Parc fixe global (en milliers)
    """
    _, data = _parse_anrt_xlsx(path)

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        rows.append({
            "year": year,
            "quarter": quarter,
            "total_subs": _int(_col(r, "Z")),
        })
    return pd.DataFrame(rows)


def _parse_qos(path: Path) -> pd.DataFrame:
    """bronze_anrt_qos: year, quarter, operator, indicator, value, unit.

    QoS has a different header key ("Donnée") and annual-only data.
    Indicator codes: TR-CN, TR-CE, DD-CN, DD-CE, DU-CN, DU-CE, LM-CN, LM-CE.
    "NM" (Non mesuré) values are treated as NULL.
    """
    legend, data = _parse_anrt_xlsx(path)

    # Build indicator → unit mapping from legend descriptions
    def _extract_unit(desc: str) -> str | None:
        m = re.search(r"\(en ([^)]+)\)", desc)
        return m.group(1) if m else None

    metric_cols = [c for c in data.columns if c and c != "period" and c is not None]

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        for col in metric_cols:
            val = _float(_col(r, col))
            if val is None:
                continue
            desc = legend.get(col, col)
            unit = _extract_unit(desc)
            rows.append({
                "year": year,
                "quarter": quarter,
                "operator": None,
                "indicator": desc,
                "value": val,
                "unit": unit,
            })
    return pd.DataFrame(rows)


def _parse_traffic(path: Path) -> pd.DataFrame:
    """bronze_anrt_traffic: year, quarter, segment, voice_minutes, sms_count.

    A = voix mobile (millions de min)
    B = voix fixe   (millions de min)
    C = SMS mobile  (millions de SMS)
    """
    _, data = _parse_anrt_xlsx(path)

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        rows.append({
            "year": year,
            "quarter": quarter,
            "segment": "mobile",
            "voice_minutes": _int(_col(r, "A")),
            "sms_count": _int(_col(r, "C")),
        })
        rows.append({
            "year": year,
            "quarter": quarter,
            "segment": "fixed",
            "voice_minutes": _int(_col(r, "B")),
            "sms_count": None,
        })
    return pd.DataFrame(rows)


def _parse_bandwidth(path: Path) -> pd.DataFrame:
    """bronze_anrt_bandwidth: year, capacity_gbps.

    Bandwidth has no legend block — header is row 0 with full descriptions.
    Column index 1 = utilisée (Gb/s).
    """
    _, data = _parse_anrt_xlsx(path)

    # Pick the first numeric column (index 1 after period)
    value_col = [c for c in data.columns if c and c != "period"][0]

    rows = []
    for _, r in data.iterrows():
        year, _ = _parse_period(r["period"])
        if year is None:
            continue
        rows.append({
            "year": year,
            "capacity_gbps": _float(_col(r, value_col)),
        })
    return pd.DataFrame(rows)


def _parse_arpm(path: Path) -> pd.DataFrame:
    """bronze_anrt_arpm: year, quarter, arpm, internet_bill_avg.

    A = ARPM mobile global (DHHT/min)
    B = Facture mensuelle Internet global (DHHT)
    """
    _, data = _parse_anrt_xlsx(path)

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        rows.append({
            "year": year,
            "quarter": quarter,
            "arpm": _float(_col(r, "A")),
            "internet_bill_avg": _float(_col(r, "B")),
        })
    return pd.DataFrame(rows)


def _parse_complaints(path: Path) -> pd.DataFrame:
    """bronze_anrt_complaints: year, quarter, operator, complaint_type, count.

    Complaint data is aggregate (no operator breakdown). All metric columns
    are unpivoted to long form with complaint_type = legend description.
    """
    legend, data = _parse_anrt_xlsx(path)
    metric_cols = [c for c in data.columns if c and c != "period" and c is not None]

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        for col in metric_cols:
            val = _int(_col(r, col))
            if val is None:
                continue
            rows.append({
                "year": year,
                "quarter": quarter,
                "operator": None,
                "complaint_type": legend.get(col, col),
                "count": val,
            })
    return pd.DataFrame(rows)


def _parse_portability(path: Path, segment: str) -> pd.DataFrame:
    """bronze_anrt_portability: year, quarter, segment, ported_numbers.

    Mobile: B = demandes abouties (successful ports)
    Fixed:  D = demandes abouties (successful ports)
    """
    _, data = _parse_anrt_xlsx(path)
    # Use the last non-None column as the "successful" count
    code = "B" if segment == "mobile" else "D"

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        rows.append({
            "year": year,
            "quarter": quarter,
            "segment": segment,
            "ported_numbers": _int(_col(r, code)),
        })
    return pd.DataFrame(rows)


def _parse_usage_avg(path: Path) -> pd.DataFrame:
    """bronze_anrt_usage_avg: year, quarter, mobile_minutes, fixed_minutes.

    A = mobile global (en minutes)
    D = fixe (en minutes)
    """
    _, data = _parse_anrt_xlsx(path)

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        rows.append({
            "year": year,
            "quarter": quarter,
            "mobile_minutes": _float(_col(r, "A")),
            "fixed_minutes": _float(_col(r, "D")),
        })
    return pd.DataFrame(rows)


def _parse_ip(path: Path) -> pd.DataFrame:
    """bronze_anrt_ip: year, ipv4_count, ipv6_prefixes.

    Data is semi-annual (S1/S2). We keep both records per year — downstream
    Silver can pick end-of-period values.
    A = adresses IPv4 allouées (en milliers)
    IPv6 data not available in this source.
    """
    _, data = _parse_anrt_xlsx(path)

    rows = []
    for _, r in data.iterrows():
        year, _ = _parse_period(r["period"])
        if year is None:
            continue
        rows.append({
            "year": year,
            "ipv4_count": _int(_col(r, "A")),
            "ipv6_prefixes": None,
        })
    # Deduplicate: keep last record per year (end-of-year value)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["year"], keep="last")
    return df


def _parse_data_links(path: Path) -> pd.DataFrame:
    """bronze_anrt_data_links: year, quarter, link_type, count.

    All metric columns unpivoted to long form. link_type = legend description.
    """
    legend, data = _parse_anrt_xlsx(path)
    metric_cols = [c for c in data.columns if c and c != "period" and c is not None]

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        for col in metric_cols:
            val = _int(_col(r, col))
            if val is None:
                continue
            rows.append({
                "year": year,
                "quarter": quarter,
                "link_type": legend.get(col, col),
                "count": val,
            })
    return pd.DataFrame(rows)


def _parse_tic_survey(path: Path) -> pd.DataFrame:
    """bronze_anrt_tic_survey: year, indicator, value, unit.

    Annual data. All metric columns unpivoted to long form.
    indicator = legend description; unit extracted from "(en X)" in description.
    """
    legend, data = _parse_anrt_xlsx(path)
    metric_cols = [c for c in data.columns if c and c != "period" and c is not None]

    def _unit(desc: str) -> str | None:
        m = re.search(r"\(en ([^)]+)\)", desc)
        return m.group(1) if m else None

    rows = []
    for _, r in data.iterrows():
        year, _ = _parse_period(r["period"])
        if year is None:
            continue
        for col in metric_cols:
            val = _float(_col(r, col))
            if val is None:
                continue
            desc = legend.get(col, col)
            rows.append({
                "year": year,
                "indicator": desc,
                "value": val,
                "unit": _unit(desc),
            })
    return pd.DataFrame(rows)


def _parse_domains(path: Path) -> pd.DataFrame:
    """bronze_anrt_domains: year, quarter, active_domains.

    A = Parc des noms de domaine «.ma» (cumulative total)
    """
    _, data = _parse_anrt_xlsx(path)

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        rows.append({
            "year": year,
            "quarter": quarter,
            "active_domains": _int(_col(r, "A")),
        })
    return pd.DataFrame(rows)


def _parse_payphones(path: Path) -> pd.DataFrame:
    """bronze_anrt_payphones: year, quarter, total_payphones.

    AD = total lignes de publiphones (IAM + Orange)
    """
    _, data = _parse_anrt_xlsx(path)

    rows = []
    for _, r in data.iterrows():
        year, quarter = _parse_period(r["period"])
        if year is None:
            continue
        rows.append({
            "year": year,
            "quarter": quarter,
            "total_payphones": _int(_col(r, "AD")),
        })
    return pd.DataFrame(rows)


# ── Dataset registry ───────────────────────────────────────────────────────────
@dataclass
class DatasetConfig:
    dataset_id: str
    bronze_table: str
    parse_fn: Callable[[Path], pd.DataFrame]
    ddl: str


_DATASETS: list[DatasetConfig] = [
    DatasetConfig(
        dataset_id="parc-de-la-telephonie-mobile-2006-2022",
        bronze_table="bronze_anrt_mobile",
        parse_fn=_parse_mobile,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_mobile (
                year          INTEGER,
                quarter       VARCHAR,
                operator      VARCHAR,
                total_subs    BIGINT,
                prepaid_subs  BIGINT,
                postpaid_subs BIGINT,
                source_file   VARCHAR,
                ingested_at   TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="parc-de-l-internet-2006-2022",
        bronze_table="bronze_anrt_internet",
        parse_fn=_parse_internet,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_internet (
                year        INTEGER,
                quarter     VARCHAR,
                technology  VARCHAR,
                subscribers BIGINT,
                source_file VARCHAR,
                ingested_at TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="parc-de-la-telephonie-fixe-2006-2022",
        bronze_table="bronze_anrt_fixed",
        parse_fn=_parse_fixed,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_fixed (
                year        INTEGER,
                quarter     VARCHAR,
                total_subs  BIGINT,
                source_file VARCHAR,
                ingested_at TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="qualite-de-service-des-reseaux-mobiles-des-telecommunications",
        bronze_table="bronze_anrt_qos",
        parse_fn=_parse_qos,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_qos (
                year        INTEGER,
                quarter     VARCHAR,
                operator    VARCHAR,
                indicator   VARCHAR,
                value       DOUBLE,
                unit        VARCHAR,
                source_file VARCHAR,
                ingested_at TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="trafic-sortant-de-la-voix-et-des-sms-2006-2022",
        bronze_table="bronze_anrt_traffic",
        parse_fn=_parse_traffic,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_traffic (
                year          INTEGER,
                quarter       VARCHAR,
                segment       VARCHAR,
                voice_minutes BIGINT,
                sms_count     BIGINT,
                source_file   VARCHAR,
                ingested_at   TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="bande-passante-internet-internationale-2006-2022",
        bronze_table="bronze_anrt_bandwidth",
        parse_fn=_parse_bandwidth,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_bandwidth (
                year          INTEGER,
                capacity_gbps DOUBLE,
                source_file   VARCHAR,
                ingested_at   TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="revenu-moyen-par-minute-de-communication-arpm-facture-internet-2010-2022",
        bronze_table="bronze_anrt_arpm",
        parse_fn=_parse_arpm,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_arpm (
                year              INTEGER,
                quarter           VARCHAR,
                arpm              DOUBLE,
                internet_bill_avg DOUBLE,
                source_file       VARCHAR,
                ingested_at       TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="plaintes-des-consommateurs-2019-2022",
        bronze_table="bronze_anrt_complaints",
        parse_fn=_parse_complaints,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_complaints (
                year           INTEGER,
                quarter        VARCHAR,
                operator       VARCHAR,
                complaint_type VARCHAR,
                count          INTEGER,
                source_file    VARCHAR,
                ingested_at    TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="portabilites-des-numeros-mobiles-2016-2022",
        bronze_table="bronze_anrt_portability",
        parse_fn=lambda p: _parse_portability(p, "mobile"),
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_portability (
                year           INTEGER,
                quarter        VARCHAR,
                segment        VARCHAR,
                ported_numbers INTEGER,
                source_file    VARCHAR,
                ingested_at    TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="portabilite-des-numeros-fixes",
        bronze_table="bronze_anrt_portability",
        parse_fn=lambda p: _parse_portability(p, "fixed"),
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_portability (
                year           INTEGER,
                quarter        VARCHAR,
                segment        VARCHAR,
                ported_numbers INTEGER,
                source_file    VARCHAR,
                ingested_at    TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="usage-moyen-mensuel-sortant-de-la-telephonie-2010-2022",
        bronze_table="bronze_anrt_usage_avg",
        parse_fn=_parse_usage_avg,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_usage_avg (
                year           INTEGER,
                quarter        VARCHAR,
                mobile_minutes DOUBLE,
                fixed_minutes  DOUBLE,
                source_file    VARCHAR,
                ingested_at    TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="usage-des-adresses-ip-2013-2022",
        bronze_table="bronze_anrt_ip",
        parse_fn=_parse_ip,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_ip (
                year          INTEGER,
                ipv4_count    BIGINT,
                ipv6_prefixes INTEGER,
                source_file   VARCHAR,
                ingested_at   TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="liaisons-data-entreprises-2018-2022",
        bronze_table="bronze_anrt_data_links",
        parse_fn=_parse_data_links,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_data_links (
                year        INTEGER,
                quarter     VARCHAR,
                link_type   VARCHAR,
                count       INTEGER,
                source_file VARCHAR,
                ingested_at TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="resultats-des-enquetes-tic-2004-2021",
        bronze_table="bronze_anrt_tic_survey",
        parse_fn=_parse_tic_survey,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_tic_survey (
                year        INTEGER,
                indicator   VARCHAR,
                value       DOUBLE,
                unit        VARCHAR,
                source_file VARCHAR,
                ingested_at TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="attribution-des-noms-de-domaines-en-ma-2013-2022",
        bronze_table="bronze_anrt_domains",
        parse_fn=_parse_domains,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_domains (
                year           INTEGER,
                quarter        VARCHAR,
                active_domains INTEGER,
                source_file    VARCHAR,
                ingested_at    TIMESTAMP
            )
        """,
    ),
    DatasetConfig(
        dataset_id="parc-des-publiphones-2006-2022",
        bronze_table="bronze_anrt_payphones",
        parse_fn=_parse_payphones,
        ddl="""
            CREATE TABLE IF NOT EXISTS bronze_anrt_payphones (
                year            INTEGER,
                quarter         VARCHAR,
                total_payphones INTEGER,
                source_file     VARCHAR,
                ingested_at     TIMESTAMP
            )
        """,
    ),
]


# ── CKAN client ────────────────────────────────────────────────────────────────
def _http_get_with_retry(url: str, stream: bool = False, max_attempts: int = 3) -> requests.Response:
    """GET url with exponential-backoff retry on 429/5xx responses."""
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(url, stream=stream, timeout=REQUEST_TIMEOUT)
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            wait = 2 ** attempt
            logger.warning(
                "HTTP %s from %s — retry %d/%d in %ds",
                resp.status_code, url, attempt, max_attempts, wait,
            )
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()  # unreachable but satisfies type checkers
    return resp


def _ckan_download_url(dataset_id: str) -> str:
    """Return the first XLSX resource download URL for a CKAN dataset."""
    url = f"{ANRT_BASE_URL.rstrip('/')}/package_show?id={dataset_id}"
    resp = _http_get_with_retry(url)
    payload = resp.json()

    if not payload.get("success"):
        raise RuntimeError(f"CKAN API error for {dataset_id}: {payload.get('error')}")

    resources = payload["result"].get("resources", [])
    for res in resources:
        fmt = (res.get("format") or "").upper()
        name = (res.get("name") or "").lower()
        if fmt in {"XLSX", "XLS"} or name.endswith((".xlsx", ".xls")):
            return res["url"]
    for res in resources:
        if res.get("url"):
            return res["url"]

    raise RuntimeError(f"No downloadable resource found for dataset '{dataset_id}'")


# ── HTTP downloader ────────────────────────────────────────────────────────────
def _download_xlsx(url: str, dest: Path) -> Path:
    """Download url to dest. Skip if already present."""
    if dest.exists():
        logger.info("Already downloaded: %s", dest.name)
        return dest

    logger.info("Downloading %s → %s", url, dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with _http_get_with_retry(url, stream=True) as r:
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65_536):
                f.write(chunk)
    logger.info("Saved %s (%.1f KB)", dest.name, dest.stat().st_size / 1024)
    return dest


# ── DuckDB Bronze loader ───────────────────────────────────────────────────────
def _load_to_bronze(
    con: duckdb.DuckDBPyConnection,
    cfg: DatasetConfig,
    df: pd.DataFrame,
    source_file: str,
    ingested_at: datetime,
) -> int:
    """Create table if needed, delete stale rows for this source_file, insert df."""
    con.execute(cfg.ddl)
    con.execute(f"DELETE FROM {cfg.bronze_table} WHERE source_file = ?", [source_file])

    if df.empty:
        logger.warning("%s produced 0 rows — skipping insert", cfg.bronze_table)
        return 0

    df = df.copy()
    df["source_file"] = source_file
    df["ingested_at"] = ingested_at

    con.execute(f"INSERT INTO {cfg.bronze_table} SELECT * FROM df")
    n = len(df)
    logger.info("Loaded %d rows → %s", n, cfg.bronze_table)
    return n


# ── Public entry point ─────────────────────────────────────────────────────────
def run(dataset_ids: list[str] | None = None) -> dict[str, int]:
    """Download and ingest ANRT datasets into Bronze tables.

    Args:
        dataset_ids: Subset of dataset IDs to process; None processes all.

    Returns:
        Mapping of bronze_table → row count inserted.
    """
    ANRT_RAW_DIR.mkdir(parents=True, exist_ok=True)
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    results: dict[str, int] = {}

    # Retry loop: DuckDB allows only one writer; ITU task may still hold the lock.
    for _attempt in range(12):
        try:
            con = duckdb.connect(WAREHOUSE_PATH)
            break
        except duckdb.IOException:
            if _attempt == 11:
                raise
            logger.warning("DuckDB lock held — retry %d/12 in 10s", _attempt + 1)
            time.sleep(10)
    try:
        for cfg in _DATASETS:
            if dataset_ids and cfg.dataset_id not in dataset_ids:
                continue

            logger.info("Processing dataset: %s", cfg.dataset_id)
            xlsx_path = ANRT_RAW_DIR / f"{cfg.dataset_id}.xlsx"

            try:
                download_url = _ckan_download_url(cfg.dataset_id)
                xlsx_path = _download_xlsx(download_url, xlsx_path)
            except Exception:
                logger.exception("Failed to download %s — skipping", cfg.dataset_id)
                continue

            try:
                df = cfg.parse_fn(xlsx_path)
            except Exception:
                logger.exception("Failed to parse %s — skipping", xlsx_path.name)
                continue

            try:
                n = _load_to_bronze(con, cfg, df, xlsx_path.name, ingested_at)
                results[cfg.bronze_table] = results.get(cfg.bronze_table, 0) + n
            except Exception:
                logger.exception("Failed to load %s into %s", xlsx_path.name, cfg.bronze_table)
                continue
    finally:
        con.close()

    return results


def main() -> None:
    """CLI entry point: python -m ingestion.anrt_extractor."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    totals = run()
    for table, n in sorted(totals.items()):
        logger.info("  %-35s %d rows", table, n)


if __name__ == "__main__":
    main()
