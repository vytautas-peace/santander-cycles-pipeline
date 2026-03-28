"""
airflow/dags/santander_cycles_dag.py
─────────────────────────────────────
Main Airflow DAG for the Santander Cycles batch pipeline.

Schedule: Weekly on Mondays at 06:00 UTC
  TfL releases new usage files weekly, typically on Fridays.

DAG steps:
  1. check_new_files        — Discover new CSVs in TfL S3 bucket
  2. ingest_to_data_lake    — Download, normalise, upload Parquet to GCS
  3. load_to_bigquery       — Refresh BigQuery external table metadata
  4. run_dbt_staging        — dbt run --select staging.*
  5. run_dbt_marts          — dbt run --select marts.*
  6. run_dbt_tests          — dbt test
  7. notify_success         — Log completion metrics
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ─── Config (set these as Airflow Variables in the UI or Composer) ────────────
GCP_PROJECT    = Variable.get("gcp_project_id", default_var="your-gcp-project")
GCS_BUCKET     = Variable.get("gcs_bucket",     default_var="your-project-santander-cycles-lake")
BQ_LOCATION    = Variable.get("bq_location",    default_var="EU")
DBT_DIR        = Variable.get("dbt_dir",        default_var="/opt/airflow/dbt")
DBT_TARGET     = Variable.get("dbt_target",     default_var="prod")

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ─── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="santander_cycles_pipeline",
    description="End-to-end batch pipeline for TfL Santander Cycles data",
    schedule_interval="0 6 * * 1",   # Every Monday at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["santander", "cycles", "tfl", "batch"],
    doc_md=__doc__,
) as dag:

    # ── Task 1: Check for new files ──────────────────────────────────────────
    @task(task_id="check_new_files")
    def check_new_files(**context) -> dict:
        """
        Discover CSV files in TfL S3 bucket that haven't been ingested yet.
        Returns metadata about new files to process.
        """
        import sys
        sys.path.insert(0, "/opt/airflow/scripts")
        from ingest import list_tfl_csv_files, load_manifest
        from google.cloud import storage

        gcs_client = storage.Client()
        ingested = load_manifest(gcs_client, GCS_BUCKET)

        # Find files from the last 14 days (2 weeks buffer)
        all_files = list(list_tfl_csv_files())
        new_files = [f for f in all_files if f not in ingested]

        logger.info("Total files in TfL: %d, Already ingested: %d, New: %d",
                    len(all_files), len(ingested), len(new_files))

        context["ti"].xcom_push(key="new_file_count", value=len(new_files))
        context["ti"].xcom_push(key="new_files", value=new_files[:100])  # XCom limit

        return {"new_file_count": len(new_files), "sample": new_files[:5]}

    # ── Task 2: Ingest to Data Lake ──────────────────────────────────────────
    @task(task_id="ingest_to_data_lake")
    def ingest_to_data_lake(file_info: dict, **context) -> dict:
        """
        Download new CSVs from TfL S3, normalise schema, upload Parquet to GCS.
        This is the main ETL step — handles all historical schema drift.
        """
        import sys
        sys.path.insert(0, "/opt/airflow/scripts")
        from ingest import run_ingestion

        if file_info["new_file_count"] == 0:
            logger.info("No new files to ingest. Skipping.")
            return {"uploaded": 0, "paths": []}

        # Run ingestion for current year only on scheduled runs
        # Use year=None for historical backfills
        execution_year = context["execution_date"].year
        uploaded_paths = run_ingestion(
            gcs_bucket=GCS_BUCKET,
            year=execution_year,
            max_files=50,  # Safety cap per run
        )

        return {"uploaded": len(uploaded_paths), "paths": uploaded_paths[:10]}

    # ── Task 3: Refresh BigQuery External Table ──────────────────────────────
    refresh_bq_external_table = BigQueryInsertJobOperator(
        task_id="refresh_bq_metadata",
        configuration={
            "query": {
                "query": f"""
                    -- Refresh external table metadata so BQ picks up new Parquet files
                    SELECT COUNT(*) as row_count
                    FROM `{GCP_PROJECT}.santander_cycles_raw.raw_journeys`
                    WHERE start_year = EXTRACT(YEAR FROM CURRENT_DATE())
                """,
                "useLegacySql": False,
                "location": BQ_LOCATION,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # ── Task 4: dbt staging models ───────────────────────────────────────────
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"""
            cd {DBT_DIR} && \
            dbt run \
                --select staging \
                --target {DBT_TARGET} \
                --vars '{{"execution_date": "{{{{ ds }}}}"}}'
        """,
        env={
            "DBT_PROFILES_DIR": f"{DBT_DIR}",
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/credentials/pipeline-sa-key.json",
        },
    )

    # ── Task 5: dbt marts models ─────────────────────────────────────────────
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"""
            cd {DBT_DIR} && \
            dbt run \
                --select marts \
                --target {DBT_TARGET} \
                --vars '{{"execution_date": "{{{{ ds }}}}"}}'
        """,
        env={
            "DBT_PROFILES_DIR": f"{DBT_DIR}",
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/credentials/pipeline-sa-key.json",
        },
    )

    # ── Task 6: dbt tests ────────────────────────────────────────────────────
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"""
            cd {DBT_DIR} && \
            dbt test \
                --target {DBT_TARGET}
        """,
        env={
            "DBT_PROFILES_DIR": f"{DBT_DIR}",
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/credentials/pipeline-sa-key.json",
        },
    )

    # ── Task 7: Success notification ─────────────────────────────────────────
    @task(task_id="notify_success", trigger_rule=TriggerRule.ALL_SUCCESS)
    def notify_success(**context):
        """Log pipeline completion metrics."""
        ti = context["ti"]
        ingestion_result = ti.xcom_pull(task_ids="ingest_to_data_lake")
        file_info = ti.xcom_pull(task_ids="check_new_files")

        summary = {
            "dag_run_id":      context["run_id"],
            "execution_date":  context["ds"],
            "new_files_found": file_info.get("new_file_count", 0),
            "parquet_uploaded":ingestion_result.get("uploaded", 0) if ingestion_result else 0,
            "status":          "SUCCESS",
        }
        logger.info("Pipeline complete: %s", json.dumps(summary, indent=2))
        # In production: send to Slack/PagerDuty/monitoring here

    # ── Task 8: Failure handler ──────────────────────────────────────────────
    @task(task_id="handle_failure", trigger_rule=TriggerRule.ONE_FAILED)
    def handle_failure(**context):
        """Log failure details for alerting."""
        logger.error(
            "Pipeline FAILED for run_id=%s execution_date=%s",
            context["run_id"], context["ds"],
        )
        # In production: page on-call, send Slack alert, create incident

    # ── Wire the DAG ─────────────────────────────────────────────────────────
    file_info_result   = check_new_files()
    ingest_result      = ingest_to_data_lake(file_info_result)

    (
        file_info_result
        >> ingest_result
        >> refresh_bq_external_table
        >> run_dbt_staging
        >> run_dbt_marts
        >> run_dbt_tests
        >> [notify_success(), handle_failure()]
    )
