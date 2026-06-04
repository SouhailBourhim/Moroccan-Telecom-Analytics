"""dag_transform — triggered by dag_ingest: dbt run staging → marts → trigger dag_quality."""

from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

_DBT_DIR = "/opt/dbt"
_DBT_PROFILES_DIR = "/opt/dbt"
_DBT_TARGET = "dev"  # maps to /opt/data/warehouse.duckdb in profiles.yml

_DBT_ENV = {
    "DBT_TARGET": _DBT_TARGET,
    "WAREHOUSE_PATH": os.environ.get("WAREHOUSE_PATH", "/opt/data/warehouse.duckdb"),
    "HOME": "/home/airflow",
}

_DBT_CMD = f"dbt {{cmd}} --profiles-dir {_DBT_PROFILES_DIR} --project-dir {_DBT_DIR}"

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_transform",
    description="dbt: staging → intermediate → marts (triggered by dag_ingest)",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["transform", "dbt"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=_DBT_CMD.format(cmd="deps"),
        env=_DBT_ENV,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=_DBT_CMD.format(cmd="run --select staging"),
        env=_DBT_ENV,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=_DBT_CMD.format(cmd="test --select staging"),
        env=_DBT_ENV,
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=_DBT_CMD.format(cmd="run --select intermediate"),
        env=_DBT_ENV,
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=_DBT_CMD.format(cmd="run --select marts"),
        env=_DBT_ENV,
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=_DBT_CMD.format(cmd="test --select marts"),
        env=_DBT_ENV,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=_DBT_CMD.format(cmd="docs generate"),
        env=_DBT_ENV,
    )

    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_dag_quality",
        trigger_dag_id="dag_quality",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    (
        dbt_deps
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test_marts
        >> dbt_docs
        >> trigger_quality
    )
