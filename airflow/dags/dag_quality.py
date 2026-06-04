"""dag_quality — triggered by dag_transform: GE validations → publish report."""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

_GE_RUNNER = "/opt/airflow/great_expectations/runner.py"


def _call_runner(fn_name: str) -> str:
    """Import and call a function from great_expectations/runner.py."""
    import importlib.util
    import sys

    if _GE_RUNNER not in [getattr(s, "__file__", None) for s in sys.modules.values()]:
        spec = importlib.util.spec_from_file_location("ge_runner", _GE_RUNNER)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        sys.modules["ge_runner"] = mod
    else:
        mod = sys.modules["ge_runner"]

    fn = getattr(mod, fn_name)
    result = fn()
    logger.info(
        "%s: %d/%d checks passed", result.layer.upper(), result.passed, result.total
    )
    for detail in result.details:
        check = detail.get("check") or detail.get("table", "?")
        ok = detail.get("passed", False)
        logger.info("  %s %s", "✓" if ok else "✗", check)
    return f"{result.layer}: {result.passed}/{result.total} passed"


def run_checkpoint_bronze() -> str:
    return _call_runner("run_bronze_checkpoint")


def run_checkpoint_silver() -> str:
    return _call_runner("run_silver_checkpoint")


def run_checkpoint_gold() -> str:
    return _call_runner("run_gold_checkpoint")


def publish_quality_report(**context) -> str:
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location("ge_runner", _GE_RUNNER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    run_date = context.get("ds", "unknown")
    results = [
        mod.run_bronze_checkpoint(),
        mod.run_silver_checkpoint(),
        mod.run_gold_checkpoint(),
    ]
    path = mod.publish_report(results, run_date)
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
