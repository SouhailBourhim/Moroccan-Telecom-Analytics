"""
ANRT extractor — CKAN API → XLSX download → Bronze tables in DuckDB.

Flow per dataset:
  1. package_show CKAN call → resource download URL
  2. HTTP GET → save XLSX to data/raw/anrt/
  3. Parse XLSX (merged cells, multi-row headers handled)
  4. Map to Bronze schema (docs/schema.md)
  5. Load into DuckDB bronze_anrt_* table
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

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
_OPERATOR_MAP: dict[str, str] = {
    "iam": "Maroc Telecom",
    "maroc telecom": "Maroc Telecom",
    "mt": "Maroc Telecom",
    "orange maroc": "Orange Maroc",
    "orange": "Orange Maroc",
    "inwi": "Inwi",
    "total": "Total",
}


def _norm_operator(val: object) -> str | None:
    """Return canonical operator name or None for unrecognised values."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    key = str(val).strip().lower()
    return _OPERATOR_MAP.get(key, str(val).strip())


# ── XLSX reading helpers ───────────────────────────────────────────────────────
def _read_raw(path: Path, sheet: int | str = 0) -> pd.DataFrame:
    """Read a sheet with no header, forward-filling to expand merged cells."""
    df = pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)
    return df.ffill(axis=0).ffill(axis=1)


def _find_header_row(raw: pd.DataFrame, keywords: list[str]) -> int:
    """Return the 0-based index of the first row that contains any keyword."""
    for i in range(len(raw)):
        row_str = " ".join(str(v).lower() for v in raw.iloc[i] if pd.notna(v))
        if any(kw.lower() in row_str for kw in keywords):
            return i
    logger.warning("Header row not found for keywords %s; assuming row 0", keywords)
    return 0


def _make_df(raw: pd.DataFrame, header_row: int) -> pd.DataFrame:
    """Slice raw sheet into (header, data) pair and clean column names."""
    headers = [str(v).strip() for v in raw.iloc[header_row]]
    df = raw.iloc[header_row + 1 :].copy()
    df.columns = headers
    df = df.dropna(how="all").reset_index(drop=True)
    df.columns = [c.lower().strip() for c in df.columns]
    return df


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


def _quarter(val: object) -> str | None:
    """Normalise quarter values to 'Q1'/'Q2'/'Q3'/'Q4' or None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().upper()
    if re.match(r"^T[1-4]$", s):
        return "Q" + s[1]
    if re.match(r"^Q[1-4]$", s):
        return s
    if s in {"1", "2", "3", "4"}:
        return "Q" + s
    return None


# ── Per-dataset parsers ────────────────────────────────────────────────────────
def _parse_mobile(path: Path) -> pd.DataFrame:
    """bronze_anrt_mobile: year, quarter, operator, total_subs, prepaid_subs, postpaid_subs."""
    raw = _read_raw(path)
    hr = _find_header_row(raw, ["année", "annee", "year", "operateur", "opérateur", "trimestre"])
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    op_col = _first_col(df, ["operateur", "opérateur", "operator"])
    total_col = _first_col(df, ["total", "parc total", "total abonnés", "total abonnes"])
    prepaid_col = _first_col(df, ["prépayé", "prepaye", "prepaid", "prépayé"])
    postpaid_col = _first_col(df, ["postpayé", "postpaye", "postpaid", "post-payé"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "operator": _norm_operator(r.get(op_col)),
                "total_subs": _int(r.get(total_col)),
                "prepaid_subs": _int(r.get(prepaid_col)),
                "postpaid_subs": _int(r.get(postpaid_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_internet(path: Path) -> pd.DataFrame:
    """bronze_anrt_internet: year, quarter, technology, subscribers."""
    raw = _read_raw(path)
    hr = _find_header_row(raw, ["année", "annee", "year", "technologie", "technology", "trimestre"])
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    tech_col = _first_col(df, ["technologie", "technology", "type"])
    subs_col = _first_col(df, ["abonnés", "abonnes", "subscribers", "nombre"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "technology": str(r.get(tech_col, "")).strip() or None,
                "subscribers": _int(r.get(subs_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_fixed(path: Path) -> pd.DataFrame:
    """bronze_anrt_fixed: year, quarter, total_subs."""
    raw = _read_raw(path)
    hr = _find_header_row(raw, ["année", "annee", "year", "trimestre", "fixe"])
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    total_col = _first_col(df, ["total", "parc total", "abonnés", "abonnes"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "total_subs": _int(r.get(total_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_qos(path: Path) -> pd.DataFrame:
    """bronze_anrt_qos: year, quarter, operator, indicator, value, unit.

    QoS files are wide (one column per metric) — we unpivot to long form.
    """
    raw = _read_raw(path)
    hr = _find_header_row(raw, ["année", "annee", "year", "indicateur", "indicator", "opérateur"])
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    op_col = _first_col(df, ["operateur", "opérateur", "operator"])

    dim_cols = [c for c in [year_col, qtr_col, op_col] if c]
    metric_cols = [c for c in df.columns if c not in dim_cols and c]

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        base = {
            "year": year,
            "quarter": _quarter(r.get(qtr_col)),
            "operator": _norm_operator(r.get(op_col)),
        }
        for col in metric_cols:
            val = _float(r.get(col))
            if val is None:
                continue
            # Try to split "indicator (unit)" patterns
            m = re.match(r"^(.+?)\s*\(([^)]+)\)\s*$", str(col))
            indicator = m.group(1).strip() if m else str(col).strip()
            unit = m.group(2).strip() if m else None
            rows.append({**base, "indicator": indicator, "value": val, "unit": unit})
    return pd.DataFrame(rows)


def _parse_traffic(path: Path) -> pd.DataFrame:
    """bronze_anrt_traffic: year, quarter, segment, voice_minutes, sms_count.

    Traffic files may have segment as row label or column header.
    We try both wide and long layouts.
    """
    raw = _read_raw(path)
    hr = _find_header_row(raw, ["année", "annee", "year", "trafic", "traffic", "sms", "voix"])
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    seg_col = _first_col(df, ["segment", "type", "sens", "destination"])
    voice_col = _first_col(df, ["voix", "voice", "minutes", "trafic voix", "mn"])
    sms_col = _first_col(df, ["sms", "nombre sms", "sms sortant"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "segment": str(r.get(seg_col, "")).strip() or None,
                "voice_minutes": _int(r.get(voice_col)),
                "sms_count": _int(r.get(sms_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_bandwidth(path: Path) -> pd.DataFrame:
    """bronze_anrt_bandwidth: year, capacity_gbps."""
    raw = _read_raw(path)
    hr = _find_header_row(raw, ["année", "annee", "year", "bande passante", "gbps", "capacité"])
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    cap_col = _first_col(df, ["capacité", "capacity", "bande passante", "gbps", "gbit", "valeur"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append({"year": year, "capacity_gbps": _float(r.get(cap_col))})
    return pd.DataFrame(rows)


def _parse_arpm(path: Path) -> pd.DataFrame:
    """bronze_anrt_arpm: year, quarter, arpm, internet_bill_avg."""
    raw = _read_raw(path)
    hr = _find_header_row(raw, ["année", "annee", "year", "arpm", "facture", "trimestre"])
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    arpm_col = _first_col(df, ["arpm", "revenu moyen", "revenue par minute"])
    bill_col = _first_col(df, ["facture", "facture internet", "bill", "internet bill"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "arpm": _float(r.get(arpm_col)),
                "internet_bill_avg": _float(r.get(bill_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_complaints(path: Path) -> pd.DataFrame:
    """bronze_anrt_complaints: year, quarter, operator, complaint_type, count."""
    raw = _read_raw(path)
    hr = _find_header_row(
        raw, ["année", "annee", "year", "plainte", "complaint", "opérateur", "trimestre"]
    )
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    op_col = _first_col(df, ["operateur", "opérateur", "operator"])
    type_col = _first_col(df, ["type", "motif", "catégorie", "categorie", "complaint"])
    count_col = _first_col(df, ["nombre", "count", "total", "plaintes"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "operator": _norm_operator(r.get(op_col)),
                "complaint_type": str(r.get(type_col, "")).strip() or None,
                "count": _int(r.get(count_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_portability(path: Path, segment: str) -> pd.DataFrame:
    """bronze_anrt_portability: year, quarter, segment, ported_numbers."""
    raw = _read_raw(path)
    hr = _find_header_row(
        raw, ["année", "annee", "year", "portabilité", "portabilite", "numéros", "trimestre"]
    )
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    ported_col = _first_col(
        df, ["portés", "portes", "ported", "numéros portés", "nombre", "total"]
    )

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "segment": segment,
                "ported_numbers": _int(r.get(ported_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_usage_avg(path: Path) -> pd.DataFrame:
    """bronze_anrt_usage_avg: year, quarter, mobile_minutes, fixed_minutes."""
    raw = _read_raw(path)
    hr = _find_header_row(
        raw, ["année", "annee", "year", "usage", "minutes", "mobile", "fixe", "trimestre"]
    )
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    mob_col = _first_col(df, ["mobile", "usage mobile", "minutes mobile"])
    fix_col = _first_col(df, ["fixe", "usage fixe", "minutes fixe", "minutes fixes"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "mobile_minutes": _float(r.get(mob_col)),
                "fixed_minutes": _float(r.get(fix_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_ip(path: Path) -> pd.DataFrame:
    """bronze_anrt_ip: year, ipv4_count, ipv6_prefixes."""
    raw = _read_raw(path)
    hr = _find_header_row(raw, ["année", "annee", "year", "ipv4", "ipv6", "adresses"])
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    ipv4_col = _first_col(df, ["ipv4", "adresses ipv4", "nombre ipv4"])
    ipv6_col = _first_col(df, ["ipv6", "préfixes ipv6", "prefixes ipv6"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "ipv4_count": _int(r.get(ipv4_col)),
                "ipv6_prefixes": _int(r.get(ipv6_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_data_links(path: Path) -> pd.DataFrame:
    """bronze_anrt_data_links: year, quarter, link_type, count."""
    raw = _read_raw(path)
    hr = _find_header_row(
        raw, ["année", "annee", "year", "liaison", "link", "type", "trimestre"]
    )
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    type_col = _first_col(df, ["type", "liaison", "link type"])
    count_col = _first_col(df, ["nombre", "count", "total", "liaisons"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "link_type": str(r.get(type_col, "")).strip() or None,
                "count": _int(r.get(count_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_tic_survey(path: Path) -> pd.DataFrame:
    """bronze_anrt_tic_survey: year, indicator, value, unit.

    TIC files are wide — each column is an indicator. We unpivot to long.
    """
    raw = _read_raw(path)
    hr = _find_header_row(raw, ["année", "annee", "year", "indicateur", "indicator", "enquête"])
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    ind_col = _first_col(df, ["indicateur", "indicator", "libellé", "libelle"])
    val_col = _first_col(df, ["valeur", "value", "résultat", "resultat"])
    unit_col = _first_col(df, ["unité", "unite", "unit"])

    rows = []
    # If there's an explicit indicator column, use long form directly
    if ind_col and val_col:
        for _, r in df.iterrows():
            year = _int(r.get(year_col))
            if year is None:
                continue
            rows.append(
                {
                    "year": year,
                    "indicator": str(r.get(ind_col, "")).strip() or None,
                    "value": _float(r.get(val_col)),
                    "unit": str(r.get(unit_col, "")).strip() or None if unit_col else None,
                }
            )
    else:
        # Wide layout — unpivot metric columns
        dim_cols = [c for c in [year_col] if c]
        metric_cols = [c for c in df.columns if c not in dim_cols and c]
        for _, r in df.iterrows():
            year = _int(r.get(year_col))
            if year is None:
                continue
            for col in metric_cols:
                val = _float(r.get(col))
                if val is None:
                    continue
                m = re.match(r"^(.+?)\s*\(([^)]+)\)\s*$", str(col))
                indicator = m.group(1).strip() if m else str(col).strip()
                unit = m.group(2).strip() if m else None
                rows.append({"year": year, "indicator": indicator, "value": val, "unit": unit})
    return pd.DataFrame(rows)


def _parse_domains(path: Path) -> pd.DataFrame:
    """bronze_anrt_domains: year, quarter, active_domains."""
    raw = _read_raw(path)
    hr = _find_header_row(
        raw, ["année", "annee", "year", "domaine", "domain", ".ma", "trimestre"]
    )
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    dom_col = _first_col(df, ["domaines", "domains", "nombre", "total", "actifs", ".ma"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "active_domains": _int(r.get(dom_col)),
            }
        )
    return pd.DataFrame(rows)


def _parse_payphones(path: Path) -> pd.DataFrame:
    """bronze_anrt_payphones: year, quarter, total_payphones."""
    raw = _read_raw(path)
    hr = _find_header_row(
        raw, ["année", "annee", "year", "publiphone", "payphone", "cabine", "trimestre"]
    )
    df = _make_df(raw, hr)

    year_col = _first_col(df, ["année", "annee", "year", "an"])
    qtr_col = _first_col(df, ["trimestre", "quarter", "trim"])
    count_col = _first_col(df, ["publiphones", "payphones", "cabines", "nombre", "total"])

    rows = []
    for _, r in df.iterrows():
        year = _int(r.get(year_col))
        if year is None:
            continue
        rows.append(
            {
                "year": year,
                "quarter": _quarter(r.get(qtr_col)),
                "total_payphones": _int(r.get(count_col)),
            }
        )
    return pd.DataFrame(rows)


def _first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column name that matches any candidate (substring, case-insensitive)."""
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        cand_l = cand.lower()
        # Exact match first
        if cand_l in cols_lower:
            return cols_lower[cand_l]
        # Substring match
        for col_l, col in cols_lower.items():
            if cand_l in col_l:
                return col
    return None


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
    # Portability mobile + fixed share the same table; loaded in two passes
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
def _ckan_download_url(dataset_id: str) -> str:
    """Return the first XLSX resource download URL for a CKAN dataset."""
    url = f"{ANRT_BASE_URL.rstrip('/')}/package_show?id={dataset_id}"
    logger.debug("CKAN package_show: %s", url)
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()

    if not payload.get("success"):
        raise RuntimeError(f"CKAN API error for {dataset_id}: {payload.get('error')}")

    resources = payload["result"].get("resources", [])
    for res in resources:
        fmt = (res.get("format") or "").upper()
        name = (res.get("name") or "").lower()
        if fmt in {"XLSX", "XLS"} or name.endswith((".xlsx", ".xls")):
            return res["url"]

    # Fall back to first resource with a download URL
    for res in resources:
        if res.get("url"):
            return res["url"]

    raise RuntimeError(f"No downloadable resource found for dataset '{dataset_id}'")


# ── HTTP downloader ────────────────────────────────────────────────────────────
def _download_xlsx(url: str, dest: Path) -> Path:
    """Download url to dest; return dest path. Skip if already present."""
    if dest.exists():
        logger.info("Already downloaded: %s", dest.name)
        return dest

    logger.info("Downloading %s → %s", url, dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
        r.raise_for_status()
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

    # Idempotent: remove any previous load from this exact file
    con.execute(
        f"DELETE FROM {cfg.bronze_table} WHERE source_file = ?", [source_file]
    )

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

    con = duckdb.connect(WAREHOUSE_PATH)
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
