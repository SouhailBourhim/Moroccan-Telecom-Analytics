"""dag_quality — triggered by dag_transform: GE checkpoints → publish HTML report."""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

_GE_DIR = os.environ.get("GE_DIR", "/opt/airflow/great_expectations")
_DATA_DIR = os.environ.get("DATA_DIR", "/opt/data")
_GE_REPORTS_DIR = os.path.join(_DATA_DIR, "ge_reports")


def _run_checkpoint(checkpoint_name: str) -> str:
    """Run a single Great Expectations checkpoint by name."""
    try:
        import great_expectations as ge
    except ImportError:
        logger.warning("great_expectations not installed — skipping %s", checkpoint_name)
        return f"skipped (no GE): {checkpoint_name}"

    context = ge.get_context(context_root_dir=_GE_DIR)
    result = context.run_checkpoint(checkpoint_name=checkpoint_name)

    if result.success:
        logger.info("Checkpoint %s: PASSED", checkpoint_name)
        return f"passed: {checkpoint_name}"
    else:
        failed = [
            v["expectation_config"]["expectation_type"]
            for v in result.run_results.values()
            for v in v.get("validation_result", {}).get("results", [])
            if not v.get("success")
        ]
        logger.warning("Checkpoint %s: FAILED — %s", checkpoint_name, failed[:5])
        return f"failed: {checkpoint_name}"


def run_checkpoint_bronze() -> str:
    return _run_checkpoint("bronze_checkpoint")


def run_checkpoint_silver() -> str:
    return _run_checkpoint("silver_checkpoint")


def run_checkpoint_gold() -> str:
    return _run_checkpoint("gold_checkpoint")


def publish_quality_report(**context) -> str:
    """Collect GE HTML reports from the run and log their paths."""
    import glob
    import pathlib

    pathlib.Path(_GE_REPORTS_DIR).mkdir(parents=True, exist_ok=True)

    # GE writes HTML under <GE_DIR>/uncommitted/data_docs/
    html_files = glob.glob(
        os.path.join(_GE_DIR, "uncommitted", "data_docs", "**", "*.html"),
        recursive=True,
    )
    logger.info("Quality report HTML files generated: %d", len(html_files))
    for f in html_files:
        logger.info("  %s", f)

    run_date = context["ds"]
    logger.info("Quality check run complete for %s", run_date)
    return f"report published for {run_date}: {len(html_files)} HTML file(s)"


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
