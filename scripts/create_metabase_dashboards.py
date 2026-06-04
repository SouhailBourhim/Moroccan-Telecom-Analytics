"""Create 5 Metabase dashboards for Morocco Telecom Analytics gold marts.

Reads credentials from environment variables (set in .env):
  METABASE_TOKEN       — session token from POST /api/session
  METABASE_DB_ID       — numeric ID of the PostgreSQL database in Metabase
  METABASE_COLLECTION_ID — numeric ID of the target collection

Idempotent: if a dashboard with the same name already exists in the collection
it is reused (cards are replaced), not duplicated.
"""

from __future__ import annotations

import os
import sys

import requests

BASE = os.environ.get("METABASE_URL", "http://localhost:3000")
TOKEN = os.environ.get("METABASE_TOKEN") or (sys.argv[1] if len(sys.argv) > 1 else None)
DB_ID = int(os.environ.get("METABASE_DB_ID") or (sys.argv[2] if len(sys.argv) > 2 else 0))
COLLECTION_ID = int(
    os.environ.get("METABASE_COLLECTION_ID") or (sys.argv[3] if len(sys.argv) > 3 else 0)
)

if not TOKEN or not DB_ID or not COLLECTION_ID:
    sys.exit(
        "Usage: METABASE_TOKEN=<tok> METABASE_DB_ID=<id> METABASE_COLLECTION_ID=<id> "
        "python scripts/create_metabase_dashboards.py\n"
        "  (or pass positional args: <token> <db_id> <collection_id>)"
    )

HEADERS = {"X-Metabase-Session": TOKEN, "Content-Type": "application/json"}


def get(path: str) -> dict | list:
    r = requests.get(f"{BASE}{path}", headers=HEADERS)
    r.raise_for_status()
    return r.json()


def post(path: str, body: dict) -> dict:
    r = requests.post(f"{BASE}{path}", headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json()


def put(path: str, body: dict) -> dict:
    r = requests.put(f"{BASE}{path}", headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json()


def _existing_dashboards() -> dict[str, int]:
    """Return {name: dashboard_id} for dashboards in our collection."""
    items = get(f"/api/collection/{COLLECTION_ID}/items?models=dashboard")
    data = items.get("data", []) if isinstance(items, dict) else []
    return {d["name"]: d["id"] for d in data if d.get("model") == "dashboard"}


def _existing_cards() -> dict[str, int]:
    """Return {name: card_id} for cards in our collection."""
    items = get(f"/api/collection/{COLLECTION_ID}/items?models=card")
    data = items.get("data", []) if isinstance(items, dict) else []
    return {c["name"]: c["id"] for c in data if c.get("model") == "card"}


def get_or_create_card(
    name: str,
    sql: str,
    existing: dict[str, int],
    display: str = "table",
    viz_settings: dict | None = None,
) -> int:
    if name in existing:
        card_id = existing[name]
        put(f"/api/card/{card_id}", {
            "dataset_query": {
                "type": "native",
                "native": {"query": sql},
                "database": DB_ID,
            },
            "display": display,
            "visualization_settings": viz_settings or {},
        })
        return card_id
    body = {
        "name": name,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql},
            "database": DB_ID,
        },
        "display": display,
        "visualization_settings": viz_settings or {},
        "collection_id": COLLECTION_ID,
    }
    return post("/api/card", body)["id"]


def get_or_create_dashboard(name: str, description: str, existing: dict[str, int]) -> int:
    if name in existing:
        return existing[name]
    return post("/api/dashboard", {
        "name": name,
        "description": description,
        "collection_id": COLLECTION_ID,
    })["id"]


def add_cards_to_dashboard(dash_id: int, cards: list[dict]) -> None:
    dashcards = [
        {
            "id": -(i + 1),
            "card_id": c["card_id"],
            "row": c["row"],
            "col": c["col"],
            "size_x": c.get("size_x", 18),
            "size_y": c.get("size_y", 8),
            "parameter_mappings": [],
            "visualization_settings": {},
        }
        for i, c in enumerate(cards)
    ]
    put(f"/api/dashboard/{dash_id}", {"dashcards": dashcards})


# ── Pre-load existing items ───────────────────────────────────────────────────
existing_dashboards = _existing_dashboards()
existing_cards = _existing_cards()

# ── Dashboard 1: Market Overview ──────────────────────────────────────────────
print("Creating Dashboard 1: Market Overview …")
c1a = get_or_create_card(
    "Mobile Subscribers Over Time",
    "SELECT year, quarter, mobile_total_subs FROM gold.mart_market_overview ORDER BY year, quarter",
    existing_cards,
    display="line",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["mobile_total_subs"]},
)
c1b = get_or_create_card(
    "Fixed vs Mobile vs Internet Subscribers",
    "SELECT year, mobile_total_subs, fixed_total_subs, internet_total_subs FROM gold.mart_market_overview ORDER BY year",
    existing_cards,
    display="bar",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["mobile_total_subs", "fixed_total_subs", "internet_total_subs"]},
)
c1c = get_or_create_card(
    "Market Overview — Full Table",
    "SELECT * FROM gold.mart_market_overview ORDER BY year DESC, quarter DESC LIMIT 40",
    existing_cards,
)
d1 = get_or_create_dashboard(
    "Market Overview",
    "Top-level KPIs: mobile, fixed, internet subscribers per year/quarter",
    existing_dashboards,
)
add_cards_to_dashboard(d1, [
    {"card_id": c1a, "row": 0, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c1b, "row": 8, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c1c, "row": 16, "col": 0, "size_x": 18, "size_y": 10},
])
print(f"  Dashboard ID: {d1}")

# ── Dashboard 2: Operator Performance ─────────────────────────────────────────
print("Creating Dashboard 2: Operator Performance …")
c2a = get_or_create_card(
    "Market Share by Operator",
    "SELECT year, operator, ROUND(market_share_pct::numeric, 1) AS market_share_pct FROM gold.mart_operator_perf ORDER BY year, operator",
    existing_cards,
    display="bar",
    viz_settings={"graph.dimensions": ["year", "operator"], "graph.metrics": ["market_share_pct"]},
)
c2b = get_or_create_card(
    "ARPM by Operator",
    "SELECT year, operator, arpm FROM gold.mart_operator_perf WHERE arpm IS NOT NULL ORDER BY year, operator",
    existing_cards,
    display="line",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["arpm"]},
)
c2c = get_or_create_card(
    "Operator Performance — Full Table",
    "SELECT * FROM gold.mart_operator_perf ORDER BY year DESC, quarter DESC LIMIT 50",
    existing_cards,
)
d2 = get_or_create_dashboard(
    "Operator Performance",
    "Per-operator breakdown: market share, ARPM, traffic, complaints",
    existing_dashboards,
)
add_cards_to_dashboard(d2, [
    {"card_id": c2a, "row": 0, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c2b, "row": 8, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c2c, "row": 16, "col": 0, "size_x": 18, "size_y": 10},
])
print(f"  Dashboard ID: {d2}")

# ── Dashboard 3: QoS Scorecard ────────────────────────────────────────────────
print("Creating Dashboard 3: QoS Scorecard …")
c3a = get_or_create_card(
    "QoS Voice Call Success Rate by Year",
    "SELECT year, indicator, ROUND(AVG(value)::numeric, 4) AS avg_value\nFROM gold.mart_qos_scorecard\nWHERE indicator ILIKE '%appels voix%'\nGROUP BY year, indicator\nORDER BY year",
    existing_cards,
    display="bar",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["avg_value"]},
)
c3b = get_or_create_card(
    "QoS Scorecard — Full Table",
    "SELECT * FROM gold.mart_qos_scorecard ORDER BY year DESC, operator LIMIT 60",
    existing_cards,
)
d3 = get_or_create_dashboard(
    "QoS Scorecard",
    "Quality of Service indicators per operator",
    existing_dashboards,
)
add_cards_to_dashboard(d3, [
    {"card_id": c3a, "row": 0, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c3b, "row": 8, "col": 0, "size_x": 18, "size_y": 10},
])
print(f"  Dashboard ID: {d3}")

# ── Dashboard 4: Internet Evolution ───────────────────────────────────────────
print("Creating Dashboard 4: Internet Evolution …")
c4a = get_or_create_card(
    "Mobile Penetration per 100 Inhabitants",
    "SELECT year, quarter, ROUND(mobile_penetration_per_100::numeric, 2) AS mobile_penetration_per_100\nFROM gold.mart_internet_evol\nORDER BY year, quarter",
    existing_cards,
    display="line",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["mobile_penetration_per_100"]},
)
c4b = get_or_create_card(
    "Mobile vs Fixed Broadband Subscribers",
    "SELECT year, SUM(mobile_bb_subs) AS mobile_broadband_subs, SUM(adsl_subs + ftth_subs) AS fixed_broadband_subs\nFROM gold.mart_internet_evol WHERE mobile_bb_subs IS NOT NULL\nGROUP BY year ORDER BY year",
    existing_cards,
    display="bar",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["mobile_broadband_subs", "fixed_broadband_subs"]},
)
c4c = get_or_create_card(
    "Internet Evolution — Full Table",
    "SELECT * FROM gold.mart_internet_evol ORDER BY year DESC, quarter DESC LIMIT 40",
    existing_cards,
)
d4 = get_or_create_dashboard(
    "Internet Evolution",
    "Technology mix and broadband penetration trends",
    existing_dashboards,
)
add_cards_to_dashboard(d4, [
    {"card_id": c4a, "row": 0, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c4b, "row": 8, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c4c, "row": 16, "col": 0, "size_x": 18, "size_y": 10},
])
print(f"  Dashboard ID: {d4}")

# ── Dashboard 5: Morocco vs MENA Benchmarks ───────────────────────────────────
print("Creating Dashboard 5: Morocco vs MENA Benchmarks …")
c5a = get_or_create_card(
    "Morocco Key ITU Indicators Over Time",
    "SELECT year, mobile_subs_total, fixed_bb_subs, internet_users_pct, intl_bandwidth_mbps\nFROM gold.mart_benchmarks\nORDER BY year",
    existing_cards,
    display="line",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["mobile_subs_total", "internet_users_pct"]},
)
c5b = get_or_create_card(
    "Benchmarks — Full Table",
    "SELECT * FROM gold.mart_benchmarks ORDER BY year",
    existing_cards,
)
d5 = get_or_create_dashboard(
    "Morocco vs MENA Benchmarks",
    "Morocco telecom indicators vs ITU / MENA data",
    existing_dashboards,
)
add_cards_to_dashboard(d5, [
    {"card_id": c5a, "row": 0, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c5b, "row": 8, "col": 0, "size_x": 18, "size_y": 10},
])
print(f"  Dashboard ID: {d5}")

print("\nAll 5 dashboards created/updated successfully!")
print(f"Open: {BASE}/collection/{COLLECTION_ID}")
