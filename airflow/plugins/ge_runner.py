"""Great Expectations validation runner for Bronze / Silver / Gold layers.

Uses GE 0.18.x fluent (ephemeral) API: loads DuckDB tables into pandas DataFrames
and validates expectations. Results are non-blocking — failures are logged and
reported but do not halt the DAG.
"""

from __future__ import annotations

import logging
import os
import pathlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import duckdb
import great_expectations as ge
import pandas as pd

logger = logging.getLogger(__name__)

_WAREHOUSE = os.environ.get("WAREHOUSE_PATH", "/opt/data/warehouse.duckdb")
_REPORTS_DIR = pathlib.Path(os.environ.get("DATA_DIR", "/opt/data")) / "ge_reports"

# Schema prefixes — match dbt profiles (+schema: silver / gold)
_SILVER_SCHEMA = os.environ.get("DBT_SILVER_SCHEMA", "main_silver")
_GOLD_SCHEMA = os.environ.get("DBT_GOLD_SCHEMA", "main_gold")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_table(query: str) -> pd.DataFrame:
    con = duckdb.connect(_WAREHOUSE, read_only=True)
    try:
        return con.execute(query).df()
    finally:
        con.close()


def _discover_bronze_tables() -> list[str]:
    """Return all Bronze table names from the warehouse information schema."""
    con = duckdb.connect(_WAREHOUSE, read_only=True)
    try:
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name LIKE 'bronze_%' "
            "ORDER BY table_name"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


def _make_validator(
    context: ge.data_context.AbstractDataContext,
    df: pd.DataFrame,
    ds_name: str,
    asset_name: str,
    suite_name: str,
) -> Any:
    ds = context.sources.add_pandas(ds_name)
    asset = ds.add_dataframe_asset(name=asset_name, dataframe=df)
    br = asset.build_batch_request()
    return context.get_validator(
        batch_request=br,
        create_expectation_suite_with_name=suite_name,
    )


@dataclass
class CheckResult:
    layer: str
    passed: int
    failed: int
    total: int
    details: list[dict]

    @property
    def success(self) -> bool:
        return self.failed == 0


# ── Bronze checkpoint ─────────────────────────────────────────────────────────

def run_bronze_checkpoint() -> CheckResult:
    """Validate all Bronze tables (auto-discovered) have at least 10 rows."""
    bronze_tables = _discover_bronze_tables()

    context = ge.get_context()
    details: list[dict] = []
    passed = failed = 0

    for table in bronze_tables:
        df = _load_table(f"SELECT * FROM {table}")
        validator = _make_validator(
            context, df, f"bronze_{table}", table, f"bronze_{table}_suite"
        )
        result = validator.expect_table_row_count_to_be_between(min_value=10)
        ok = result.success
        details.append(
            {"table": table, "rows": result.result.get("observed_value"), "passed": ok}
        )
        if ok:
            passed += 1
        else:
            failed += 1
            logger.warning("BRONZE FAIL %s: row_count=%s", table, result.result)
        logger.info("Bronze %s: rows=%d, passed=%s", table, len(df), ok)

    return CheckResult("bronze", passed, failed, len(bronze_tables), details)


# ── Silver checkpoint ─────────────────────────────────────────────────────────

def run_silver_checkpoint() -> CheckResult:
    """Validate Silver layer quality."""
    context = ge.get_context()
    details: list[dict] = []
    passed = failed = 0

    # 1. Mobile staging: total_subs must be non-negative
    df_mobile = _load_table(
        f"SELECT total_subs FROM {_SILVER_SCHEMA}.stg_anrt__mobile WHERE total_subs IS NOT NULL"
    )
    v = _make_validator(
        context, df_mobile, "silver_mobile", "stg_mobile", "silver_mobile_suite"
    )
    r = v.expect_column_values_to_be_between(
        column="total_subs", min_value=0, mostly=1.0
    )
    ok = r.success
    details.append({"check": "mobile_subs_non_negative", "passed": ok})
    if ok:
        passed += 1
    else:
        failed += 1
        logger.warning("SILVER FAIL mobile_subs_non_negative: %s", r.result)

    # 2. Penetration: mobile_penetration_per_100 in [0, 200]
    df_pen = _load_table(
        f"SELECT mobile_penetration_per_100 "
        f"FROM {_GOLD_SCHEMA}.mart_internet_evol "
        f"WHERE mobile_penetration_per_100 IS NOT NULL"
    )
    v2 = _make_validator(
        context, df_pen, "silver_penetration", "stg_penetration", "silver_penetration_suite"
    )
    r2 = v2.expect_column_values_to_be_between(
        column="mobile_penetration_per_100", min_value=0, max_value=200, mostly=1.0
    )
    ok2 = r2.success
    details.append({
        "check": "mobile_penetration_in_range",
        "observed_min": df_pen["mobile_penetration_per_100"].min(),
        "observed_max": df_pen["mobile_penetration_per_100"].max(),
        "passed": ok2,
    })
    if ok2:
        passed += 1
    else:
        failed += 1
        logger.warning("SILVER FAIL mobile_penetration_in_range: %s", r2.result)

    return CheckResult("silver", passed, failed, 2, details)


# ── Gold checkpoint ───────────────────────────────────────────────────────────

def run_gold_checkpoint() -> CheckResult:
    """Validate Gold layer quality."""
    context = ge.get_context()
    details: list[dict] = []
    passed = failed = 0

    # 1. Maroc Telecom market share median in [30, 60]
    df_ms = _load_table(
        f"SELECT market_share_pct "
        f"FROM {_GOLD_SCHEMA}.mart_operator_perf "
        f"WHERE operator = 'Maroc Telecom' AND market_share_pct IS NOT NULL"
    )
    v = _make_validator(
        context, df_ms, "gold_ms", "market_share", "gold_market_share_suite"
    )
    r = v.expect_column_quantile_values_to_be_between(
        column="market_share_pct",
        quantile_ranges={"quantiles": [0.5], "value_ranges": [[30, 60]]},
    )
    ok = r.success
    details.append({
        "check": "market_share_iam_median_30_60",
        "median": r.result.get("observed_value", {}).get("values", [None])[0],
        "passed": ok,
    })
    if ok:
        passed += 1
    else:
        failed += 1
        logger.warning("GOLD FAIL market_share_iam_median: %s", r.result)

    # 2. mart_market_overview: no null mobile_total_subs
    df_ov = _load_table(f"SELECT mobile_total_subs FROM {_GOLD_SCHEMA}.mart_market_overview")
    v2 = _make_validator(
        context, df_ov, "gold_overview", "market_overview", "gold_overview_suite"
    )
    r2 = v2.expect_column_values_to_not_be_null(column="mobile_total_subs")
    ok2 = r2.success
    details.append({"check": "overview_mobile_subs_not_null", "passed": ok2})
    if ok2:
        passed += 1
    else:
        failed += 1
        logger.warning("GOLD FAIL overview_mobile_subs_not_null: %s", r2.result)

    # 3. mart_benchmarks: at least 15 years of data
    df_bm = _load_table(f"SELECT year FROM {_GOLD_SCHEMA}.mart_benchmarks")
    v3 = _make_validator(
        context, df_bm, "gold_benchmarks", "benchmarks", "gold_benchmarks_suite"
    )
    r3 = v3.expect_table_row_count_to_be_between(min_value=15)
    ok3 = r3.success
    details.append({
        "check": "benchmarks_min_15_years",
        "rows": r3.result.get("observed_value"),
        "passed": ok3,
    })
    if ok3:
        passed += 1
    else:
        failed += 1
        logger.warning("GOLD FAIL benchmarks_min_15_years: %s", r3.result)

    return CheckResult("gold", passed, failed, 3, details)


# ── Report publisher ──────────────────────────────────────────────────────────

def publish_report(results: list[CheckResult], run_date: str) -> str:
    """Write a text summary of validation results to data/ge_reports/."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = _REPORTS_DIR / f"quality_{timestamp}.txt"

    lines = [f"Quality Report — {run_date} (generated {timestamp})", "=" * 60]
    for res in results:
        status = "PASS" if res.success else "FAIL"
        lines.append(
            f"[{status}] {res.layer.upper()}: {res.passed}/{res.total} checks passed"
        )
        for d in res.details:
            check = d.get("check") or d.get("table", "?")
            ok = d.get("passed", False)
            lines.append(f"  {'✓' if ok else '✗'} {check}")
    lines.append("")

    report_text = "\n".join(lines)
    report_path.write_text(report_text)
    logger.info("Quality report written to %s", report_path)
    return str(report_path)


# ── CLI entry-point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    from datetime import date

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    today = date.today().isoformat()
    all_results = []

    logger.info("=== Bronze checkpoint ===")
    br = run_bronze_checkpoint()
    all_results.append(br)
    logger.info("Bronze: %d/%d passed", br.passed, br.total)

    logger.info("=== Silver checkpoint ===")
    sv = run_silver_checkpoint()
    all_results.append(sv)
    logger.info("Silver: %d/%d passed", sv.passed, sv.total)

    logger.info("=== Gold checkpoint ===")
    gd = run_gold_checkpoint()
    all_results.append(gd)
    logger.info("Gold: %d/%d passed", gd.passed, gd.total)

    path = publish_report(all_results, today)
    logger.info("Report: %s", path)

    overall = all(r.success for r in all_results)
    sys.exit(0 if overall else 1)
