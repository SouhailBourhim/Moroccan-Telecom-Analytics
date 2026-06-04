"""dag_quality — triggered by dag_transform: GE validations → publish report."""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)


def run_checkpoint_bronze() -> str:
    from ge_runner import run_bronze_checkpoint
    result = run_bronze_checkpoint()
    logger.info("BRONZE: %d/%d checks passed", result.passed, result.total)
    for d in result.details:
        check = d.get("check") or d.get("table", "?")
        logger.info("  %s %s", "✓" if d.get("passed") else "✗", check)
    return f"bronze: {result.passed}/{result.total} passed"


def run_checkpoint_silver() -> str:
    from ge_runner import run_silver_checkpoint
    result = run_silver_checkpoint()
    logger.info("SILVER: %d/%d checks passed", result.passed, result.total)
    for d in result.details:
        check = d.get("check") or d.get("table", "?")
        logger.info("  %s %s", "✓" if d.get("passed") else "✗", check)
    return f"silver: {result.passed}/{result.total} passed"


def run_checkpoint_gold() -> str:
    from ge_runner import run_gold_checkpoint
    result = run_gold_checkpoint()
    logger.info("GOLD: %d/%d checks passed", result.passed, result.total)
    for d in result.details:
        check = d.get("check") or d.get("table", "?")
        logger.info("  %s %s", "✓" if d.get("passed") else "✗", check)
    return f"gold: {result.passed}/{result.total} passed"


def publish_quality_report(**context) -> str:
    from ge_runner import (
        publish_report,
        run_bronze_checkpoint,
        run_gold_checkpoint,
        run_silver_checkpoint,
    )
    run_date = context.get("ds", "unknown")
    results = [run_bronze_checkpoint(), run_silver_checkpoint(), run_gold_checkpoint()]
    path = publish_report(results, run_date)
    logger.info("Quality report published: %s", path)
    return f"report: {path}"


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_quality",
    description="Great Expectations checkpoints (triggered by dag_transform)",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["quality", "great-expectations"],
) as dag:

    ge_bronze = PythonOperator(
        task_id="ge_checkpoint_bronze",
        python_callable=run_checkpoint_bronze,
    )

    ge_silver = PythonOperator(
        task_id="ge_checkpoint_silver",
        python_callable=run_checkpoint_silver,
    )

    ge_gold = PythonOperator(
        task_id="ge_checkpoint_gold",
        python_callable=run_checkpoint_gold,
    )

    publish = PythonOperator(
        task_id="publish_quality_report",
        python_callable=publish_quality_report,
    )

    ge_bronze >> ge_silver >> ge_gold >> publish
