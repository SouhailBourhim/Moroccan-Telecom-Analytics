"""Create 5 Metabase dashboards for Morocco Telecom Analytics gold marts."""

from __future__ import annotations

import json
import sys
import requests

BASE = "http://localhost:3000"
TOKEN = sys.argv[1]
DB_ID = int(sys.argv[2])
COLLECTION_ID = int(sys.argv[3])

HEADERS = {"X-Metabase-Session": TOKEN, "Content-Type": "application/json"}


def post(path: str, body: dict) -> dict:
    r = requests.post(f"{BASE}{path}", headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json()


def put(path: str, body: dict) -> dict:
    r = requests.put(f"{BASE}{path}", headers=HEADERS, json=body)
    r.raise_for_status()
    return r.json()


def create_card(name: str, sql: str, display: str = "table", viz_settings: dict | None = None) -> int:
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


def create_dashboard(name: str, description: str) -> int:
    return post("/api/dashboard", {
        "name": name,
        "description": description,
        "collection_id": COLLECTION_ID,
    })["id"]


def add_cards_to_dashboard(dash_id: int, cards: list[dict]) -> None:
    """cards: list of {card_id, row, col, size_x, size_y}"""
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


# ── Dashboard 1: Market Overview ─────────────────────────────────────────────
print("Creating Dashboard 1: Market Overview …")
c1a = create_card(
    "Mobile Subscribers Over Time",
    "SELECT year, quarter, mobile_total_subs FROM gold.mart_market_overview ORDER BY year, quarter",
    display="line",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["mobile_total_subs"]},
)
c1b = create_card(
    "Fixed vs Mobile vs Internet Subscribers",
    "SELECT year, mobile_total_subs, fixed_total_subs, internet_total_subs FROM gold.mart_market_overview ORDER BY year",
    display="bar",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["mobile_total_subs", "fixed_total_subs", "internet_total_subs"]},
)
c1c = create_card(
    "Market Overview — Full Table",
    "SELECT * FROM gold.mart_market_overview ORDER BY year DESC, quarter DESC LIMIT 40",
)
d1 = create_dashboard("Market Overview", "Top-level KPIs: mobile, fixed, internet subscribers per year/quarter")
add_cards_to_dashboard(d1, [
    {"card_id": c1a, "row": 0, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c1b, "row": 8, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c1c, "row": 16, "col": 0, "size_x": 18, "size_y": 10},
])
print(f"  Dashboard ID: {d1}")

# ── Dashboard 2: Operator Performance ────────────────────────────────────────
print("Creating Dashboard 2: Operator Performance …")
c2a = create_card(
    "Market Share by Operator",
    "SELECT year, operator, ROUND(market_share_pct, 1) AS market_share_pct FROM gold.mart_operator_perf ORDER BY year, operator",
    display="bar",
    viz_settings={"graph.dimensions": ["year", "operator"], "graph.metrics": ["market_share_pct"]},
)
c2b = create_card(
    "ARPM by Operator",
    "SELECT year, operator, arpm FROM gold.mart_operator_perf WHERE arpm IS NOT NULL ORDER BY year, operator",
    display="line",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["arpm"]},
)
c2c = create_card(
    "Operator Performance — Full Table",
    "SELECT * FROM gold.mart_operator_perf ORDER BY year DESC, quarter DESC LIMIT 50",
)
d2 = create_dashboard("Operator Performance", "Per-operator breakdown: market share, ARPM, traffic, complaints")
add_cards_to_dashboard(d2, [
    {"card_id": c2a, "row": 0, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c2b, "row": 8, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c2c, "row": 16, "col": 0, "size_x": 18, "size_y": 10},
])
print(f"  Dashboard ID: {d2}")

# ── Dashboard 3: QoS Scorecard ────────────────────────────────────────────────
print("Creating Dashboard 3: QoS Scorecard …")
c3a = create_card(
    "QoS Voice Call Success Rate by Year",
    "SELECT year, indicator, ROUND(AVG(value)::numeric, 4) AS avg_value\nFROM gold.mart_qos_scorecard\nWHERE indicator ILIKE '%réussite%voix%' OR indicator ILIKE '%appels voix%'\nGROUP BY year, indicator\nORDER BY year",
    display="bar",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["avg_value"]},
)
c3b = create_card(
    "QoS Scorecard — Full Table",
    "SELECT * FROM gold.mart_qos_scorecard ORDER BY year DESC, operator LIMIT 60",
)
d3 = create_dashboard("QoS Scorecard", "Quality of Service indicators per operator")
add_cards_to_dashboard(d3, [
    {"card_id": c3a, "row": 0, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c3b, "row": 8, "col": 0, "size_x": 18, "size_y": 10},
])
print(f"  Dashboard ID: {d3}")

# ── Dashboard 4: Internet Evolution ──────────────────────────────────────────
print("Creating Dashboard 4: Internet Evolution …")
c4a = create_card(
    "Mobile Penetration per 100 Inhabitants",
    "SELECT year, quarter, ROUND(mobile_penetration_per_100::numeric, 2) AS mobile_penetration_per_100\nFROM gold.mart_internet_evol\nORDER BY year, quarter",
    display="line",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["mobile_penetration_per_100"]},
)
c4b = create_card(
    "Mobile vs Fixed Broadband Subscribers",
    "SELECT year, SUM(mobile_bb_subs) AS mobile_broadband_subs, SUM(adsl_subs + ftth_subs) AS fixed_broadband_subs\nFROM gold.mart_internet_evol WHERE mobile_bb_subs IS NOT NULL\nGROUP BY year ORDER BY year",
    display="bar",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["mobile_broadband_subs", "fixed_broadband_subs"]},
)
c4c = create_card(
    "Internet Evolution — Full Table",
    "SELECT * FROM gold.mart_internet_evol ORDER BY year DESC, quarter DESC LIMIT 40",
)
d4 = create_dashboard("Internet Evolution", "Technology mix and broadband penetration trends")
add_cards_to_dashboard(d4, [
    {"card_id": c4a, "row": 0, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c4b, "row": 8, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c4c, "row": 16, "col": 0, "size_x": 18, "size_y": 10},
])
print(f"  Dashboard ID: {d4}")

# ── Dashboard 5: Morocco vs MENA Benchmarks ──────────────────────────────────
print("Creating Dashboard 5: Morocco vs MENA Benchmarks …")
c5a = create_card(
    "Morocco Key ITU Indicators Over Time",
    "SELECT year, mobile_subs_total, fixed_bb_subs, internet_users_pct, intl_bandwidth_mbps\nFROM gold.mart_benchmarks\nORDER BY year",
    display="line",
    viz_settings={"graph.dimensions": ["year"], "graph.metrics": ["mobile_subs_total", "internet_users_pct"]},
)
c5b = create_card(
    "Benchmarks — Full Table",
    "SELECT * FROM gold.mart_benchmarks ORDER BY indicator_name, year",
)
d5 = create_dashboard("Morocco vs MENA Benchmarks", "Morocco telecom indicators vs ITU / MENA data")
add_cards_to_dashboard(d5, [
    {"card_id": c5a, "row": 0, "col": 0, "size_x": 18, "size_y": 8},
    {"card_id": c5b, "row": 8, "col": 0, "size_x": 18, "size_y": 10},
])
print(f"  Dashboard ID: {d5}")

print("\nAll 5 dashboards created successfully!")
print(f"Open: http://localhost:3000/collection/{COLLECTION_ID}")
