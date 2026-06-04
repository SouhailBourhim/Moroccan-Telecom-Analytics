"""dag_quality — triggered by dag_transform: GE validations → publish report."""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)


def run_checkpoint_bronze(**context) -> str:
    from ge_runner import run_bronze_checkpoint
    result = run_bronze_checkpoint()
    logger.info("BRONZE: %d/%d checks passed", result.passed, result.total)
    for d in result.details:
        check = d.get("check") or d.get("table", "?")
        logger.info("  %s %s", "✓" if d.get("passed") else "✗", check)
    context["ti"].xcom_push(key="bronze_result", value={
        "layer": result.layer,
        "passed": result.passed,
        "failed": result.failed,
        "total": result.total,
        "details": result.details,
    })
    return f"bronze: {result.passed}/{result.total} passed"


def run_checkpoint_silver(**context) -> str:
    from ge_runner import run_silver_checkpoint
    result = run_silver_checkpoint()
    logger.info("SILVER: %d/%d checks passed", result.passed, result.total)
    for d in result.details:
        check = d.get("check") or d.get("table", "?")
        logger.info("  %s %s", "✓" if d.get("passed") else "✗", check)
    context["ti"].xcom_push(key="silver_result", value={
        "layer": result.layer,
        "passed": result.passed,
        "failed": result.failed,
        "total": result.total,
        "details": result.details,
    })
    return f"silver: {result.passed}/{result.total} passed"


def run_checkpoint_gold(**context) -> str:
    from ge_runner import run_gold_checkpoint
    result = run_gold_checkpoint()
    logger.info("GOLD: %d/%d checks passed", result.passed, result.total)
    for d in result.details:
        check = d.get("check") or d.get("table", "?")
        logger.info("  %s %s", "✓" if d.get("passed") else "✗", check)
    context["ti"].xcom_push(key="gold_result", value={
        "layer": result.layer,
        "passed": result.passed,
        "failed": result.failed,
        "total": result.total,
        "details": result.details,
    })
    return f"gold: {result.passed}/{result.total} passed"


def publish_quality_report(**context) -> str:
    from ge_runner import CheckResult, publish_report

    ti = context["ti"]
    run_date = context.get("ds", "unknown")

    results = []
    for task_id, key in [
        ("ge_checkpoint_bronze", "bronze_result"),
        ("ge_checkpoint_silver", "silver_result"),
        ("ge_checkpoint_gold", "gold_result"),
    ]:
        raw = ti.xcom_pull(task_ids=task_id, key=key)
        if raw:
            results.append(CheckResult(**raw))

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
