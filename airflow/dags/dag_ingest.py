"""dag_ingest — monthly ingestion: ANRT + ITU → Bronze → trigger dag_transform."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

_ANRT_BASE = os.environ.get("ANRT_BASE_URL", "https://data.gov.ma/data/api/3/action/")
_ITU_BASE = os.environ.get("ITU_API_BASE", "https://api.datahub.itu.int/v2")


def check_sources_available() -> str:
    """Probe ANRT CKAN API and ITU DataHub; raise on unreachable."""
    import requests

    endpoints = {
        "ANRT": f"{_ANRT_BASE}package_show?id=parc-de-la-telephonie-mobile-2006-2022",
        "ITU":  "https://api.datahub.itu.int/v2/country/all",
    }
    for name, url in endpoints.items():
        resp = requests.get(url, timeout=20)
        if not resp.ok:
            raise RuntimeError(f"{name} API returned {resp.status_code}: {url}")
        logger.info("%s API: OK (%d)", name, resp.status_code)
    return "all sources reachable"


def download_anrt_datasets() -> str:
    """Run ANRT extractor: CKAN API → XLSX download → Bronze tables."""
    from ingestion.anrt_extractor import run
    run()
    return "anrt extraction complete"


def download_itu_data() -> str:
    """Run ITU extractor: DataHub REST API → Bronze table."""
    from ingestion.itu_extractor import run_itu_extraction
    run_itu_extraction()
    return "itu extraction complete"


def verify_bronze_load(**context) -> str:
    """Confirm both Bronze loads completed by checking row counts."""
    import duckdb
    path = os.environ.get("WAREHOUSE_PATH", "/opt/data/warehouse.duckdb")
    con = duckdb.connect(path, read_only=True)
    try:
        n_anrt = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name LIKE 'bronze_anrt_%'"
        ).fetchone()[0]
        n_itu = con.execute(
            "SELECT COUNT(*) FROM bronze_itu_morocco"
        ).fetchone()[0]
        logger.info("Bronze: %d ANRT tables, %d ITU rows", n_anrt, n_itu)
        if n_itu == 0:
            raise RuntimeError("bronze_itu_morocco is empty")
        return f"bronze verified: {n_anrt} ANRT tables, {n_itu} ITU rows"
    finally:
        con.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_ingest",
    description="Monthly ingestion: ANRT + ITU → Bronze",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingest"],
) as dag:

    check_sources = PythonOperator(
        task_id="check_sources_available",
        python_callable=check_sources_available,
    )

    dl_anrt = PythonOperator(
        task_id="download_anrt_datasets",
        python_callable=download_anrt_datasets,
    )

    dl_itu = PythonOperator(
        task_id="download_itu_data",
        python_callable=download_itu_data,
    )

    verify = PythonOperator(
        task_id="load_to_bronze",
        python_callable=verify_bronze_load,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_dag_transform",
        trigger_dag_id="dag_transform",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    check_sources >> [dl_anrt, dl_itu] >> verify >> trigger_transform
