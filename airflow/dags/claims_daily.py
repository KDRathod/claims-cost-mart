"""Daily claims pipeline.

    land_feed -> check_warehouse -> load_raw -> source_freshness -> dbt_build -> audit_run

Design rules
------------
* The DAG only orchestrates. Every task is a thin call to a script or to dbt
  that can also be run by hand, so nothing is locked inside Airflow.
* Every task is idempotent and keyed on the run's logical date ({{ ds }}).
  Re-running any date, or clearing a failed task, cannot double load:
  the loader skips files it already loaded and dbt merges on business keys.
* max_active_runs=1: two loads at once could interleave, so runs queue.
* catchup=False: a late scheduler start should run once and load everything
  that is waiting, not fire one run per missed day.
* Retries with exponential backoff cover brief warehouse or network blips.
  A real failure (a failed dbt test, a bad file) alerts and stops the run,
  so bad numbers are never published.

Where it runs: this project has no always-on server (free tier), so GitHub
Actions starts the DAG on a schedule with `airflow dags test`. On a managed
Airflow (MWAA, Astronomer, Composer) the same file runs unchanged.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT_ROOT = Path(os.environ.get("CLAIMS_PROJECT_ROOT", Path(__file__).resolve().parents[2]))
PYTHON = os.environ.get("PIPELINE_PYTHON", "python")  # interpreter that has dbt, duckdb, snowflake
DBT = os.environ.get("PIPELINE_DBT", "dbt")
TARGET = os.environ.get("DBT_TARGET", "duckdb")


def alert_on_failure(context) -> None:
    """Open (or update) a GitHub issue so a failed run reaches a person."""
    ti = context["task_instance"]
    subprocess.run(
        [PYTHON, "scripts/alert.py", "--task", ti.task_id, "--run-date", context["ds"],
         "--message", str(context.get("exception", "task failed"))[:1500]],
        cwd=PROJECT_ROOT, check=False,
    )


default_args = {
    "owner": "analytics-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="claims_daily",
    description="Load the claims feed and rebuild the cost marts",
    schedule="17 11 * * *",  # 05:17 US Central; off the hour to avoid the top-of-hour rush
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["claims", "finance", "dbt"],
    doc_md=__doc__,
) as dag:

    def step(task_id: str, command: str, **kwargs) -> BashOperator:
        return BashOperator(task_id=task_id, bash_command=command, cwd=str(PROJECT_ROOT), **kwargs)

    # Stands in for the upstream claims system dropping files. In production
    # this task would be a sensor waiting for the day's file to arrive.
    land_feed = step(
        "land_feed",
        f"FEED_AS_OF_DATE={{{{ ds }}}} {PYTHON} generator/simulate_adjudication.py",
    )

    # Fail fast on a bad credential or a suspended warehouse, before any work.
    check_warehouse = step(
        "check_warehouse",
        f"{PYTHON} scripts/snowflake_check.py" if TARGET == "snowflake" else "echo 'local DuckDB target: nothing to check'",
        retries=1,
    )

    load_raw = step(
        "load_raw",
        f"{PYTHON} ingestion/load_raw.py --target {TARGET} --through-date {{{{ ds }}}}",
    )

    # Stops the run if the feed has gone quiet, instead of publishing stale marts.
    source_freshness = step(
        "source_freshness",
        f"cd dbt && {DBT} deps && {DBT} source freshness",
        retries=0,  # a stale feed does not fix itself in two minutes
    )

    # Models, snapshot, data tests and unit tests in dependency order. A
    # failed test fails the task and skips everything downstream of the model.
    dbt_build = step(
        "dbt_build",
        f"cd dbt && {DBT} build",
        retries=0,  # a failed data test is not transient: do not retry it, alert
    )

    audit_run = step(
        "audit_run",
        f"{PYTHON} scripts/audit_run.py --target {TARGET} --run-date {{{{ ds }}}}",
        retries=0,
    )

    land_feed >> check_warehouse >> load_raw >> source_freshness >> dbt_build >> audit_run


if __name__ == "__main__":
    dag.test(logical_date=datetime.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None)
