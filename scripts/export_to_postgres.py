"""Export DuckDB gold mart tables to PostgreSQL for Metabase dashboards."""

from __future__ import annotations

import logging
import os

import duckdb
import psycopg2
from psycopg2 import sql

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

WAREHOUSE = os.environ.get("WAREHOUSE_PATH", "/opt/data/warehouse.duckdb")
PG_DSN = "host=postgres port=5432 dbname=airflow user=airflow password=airflow"
SCHEMA = "gold"

MART_TABLES = [
    "mart_market_overview",
    "mart_operator_perf",
    "mart_qos_scorecard",
    "mart_internet_evol",
    "mart_benchmarks",
]


def pg_type(duck_type: str) -> str:
    t = duck_type.upper()
    if t in ("INTEGER", "INT", "INT4", "SIGNED"):
        return "INTEGER"
    if t in ("BIGINT", "INT8", "LONG", "HUGEINT", "UBIGINT"):
        return "BIGINT"
    if t in ("DOUBLE", "FLOAT8", "NUMERIC", "REAL", "FLOAT4", "FLOAT", "DECIMAL"):
        return "DOUBLE PRECISION"
    if t in ("BOOLEAN", "BOOL", "LOGICAL"):
        return "BOOLEAN"
    if t in ("DATE",):
        return "DATE"
    if "TIMESTAMP" in t:
        return "TIMESTAMP"
    return "TEXT"


def export_table(duck: duckdb.DuckDBPyConnection, pg: psycopg2.extensions.connection, table: str) -> int:
    logger.info("Exporting %s …", table)
    df = duck.execute(f"SELECT * FROM main_gold.{table}").df()

    col_defs = ", ".join(
        f'"{col}" {pg_type(str(duck.execute(f"DESCRIBE SELECT {col} FROM main_gold.{table}").fetchone()[1]))}'
        for col in df.columns
    )

    with pg.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}')
        cur.execute(f'DROP TABLE IF EXISTS {SCHEMA}.{table} CASCADE')
        cur.execute(f'CREATE TABLE {SCHEMA}.{table} ({col_defs})')

        rows = [tuple(None if (v != v) else v for v in row) for row in df.itertuples(index=False)]
        cols = ", ".join(f'"{c}"' for c in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        cur.executemany(f'INSERT INTO {SCHEMA}.{table} ({cols}) VALUES ({placeholders})', rows)

    pg.commit()
    logger.info("  → %d rows exported to %s.%s", len(df), SCHEMA, table)
    return len(df)


def main() -> None:
    duck = duckdb.connect(WAREHOUSE, read_only=True)
    pg = psycopg2.connect(PG_DSN)

    total = 0
    for table in MART_TABLES:
        try:
            n = export_table(duck, pg, table)
            total += n
        except Exception:
            logger.exception("Failed to export %s", table)

    duck.close()
    pg.close()
    logger.info("Done — %d total rows exported to PostgreSQL schema '%s'", total, SCHEMA)


if __name__ == "__main__":
    main()
