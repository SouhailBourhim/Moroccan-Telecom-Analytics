"""Take screenshots of Airflow and Metabase for the README docs/screenshots/ folder."""

from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path("docs/screenshots")
OUT.mkdir(parents=True, exist_ok=True)

MB_EMAIL = "admin@morocctelecom.local"
MB_PASS = "Admin1234!"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin"

DASHBOARD_IDS = {
    "dashboard_market_overview": 2,
    "dashboard_operator_perf": 3,
    "dashboard_internet_evol": 5,
    "dashboard_qos_scorecard": 4,
    "dashboard_benchmarks": 6,
}


def shot(page, name: str, wait: int = 3) -> None:
    time.sleep(wait)
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=False)
    print(f"  saved {path}")


with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})

    # ── Airflow ───────────────────────────────────────────────────────────────
    print("Airflow: logging in …")
    page = ctx.new_page()
    page.goto("http://localhost:8080/login")
    page.fill('input[name="username"]', AIRFLOW_USER)
    page.fill('input[name="password"]', AIRFLOW_PASS)
    page.click('input[type="submit"]')
    time.sleep(3)

    for dag_id, name in [
        ("dag_transform", "airflow_dag_transform"),
        ("dag_ingest",    "airflow_dag_ingest"),
        ("dag_quality",   "airflow_dag_quality"),
    ]:
        print(f"Airflow: {dag_id} grid view …")
        page.goto(f"http://localhost:8080/dags/{dag_id}/grid")
        time.sleep(4)
        shot(page, name, wait=0)

    page.close()

    # ── Metabase ──────────────────────────────────────────────────────────────
    print("Metabase: logging in …")
    page = ctx.new_page()
    page.goto("http://localhost:3000/auth/login")
    page.wait_for_selector('input[name="username"]', timeout=30000)
    page.fill('input[name="username"]', MB_EMAIL)
    page.fill('input[name="password"]', MB_PASS)
    page.click('button[type="submit"]')
    time.sleep(4)

    for name, dash_id in DASHBOARD_IDS.items():
        print(f"Metabase: dashboard {dash_id} ({name}) …")
        page.goto(f"http://localhost:3000/dashboard/{dash_id}")
        time.sleep(6)
        shot(page, name, wait=0)

    page.close()
    browser.close()

print("\nDone — screenshots saved to docs/screenshots/")
