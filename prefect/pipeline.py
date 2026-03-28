"""
Santander Cycles - Batch Ingestion Pipeline (Prefect)
=====================================================
DAG steps:
  1. discover_files   - Scrape TfL index page for available CSV links
  2. download_file    - Download each CSV to local temp directory
  3. parse_and_clean  - Normalise columns, cast types, add metadata
  4. upload_to_gcs    - Write Parquet to GCS data lake (raw/ prefix)
  5. load_to_bigquery - Append partition to BigQuery raw_rides table
"""

from __future__ import annotations

import io
import os
import re
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery, storage
from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

# Configuration
TFL_BASE_URL   = "https://cycling.data.tfl.gov.uk"
TFL_INDEX_URL  = f"{TFL_BASE_URL}/"
GCP_PROJECT    = os.environ["GCP_PROJECT"]
GCS_BUCKET     = os.environ["GCS_BUCKET"]
BQ_DATASET_RAW = os.environ.get("BQ_DATASET_RAW", "santander_cycles_raw")
BQ_TABLE_RAW   = os.environ.get("BQ_TABLE_RAW", "raw_rides")

COLUMN_MAP = {
    "rental id": "rental_id",
    "duration": "duration",
    "duration_seconds": "duration",
    "bike id": "bike_id",
    "bikeid": "bike_id",
    "end date": "end_date",
    "enddate": "end_date",
    "end station id": "end_station_id",
    "endstationid": "end_station_id",
    "end station name": "end_station_name",
    "endstationname": "end_station_name",
    "start date": "start_date",
    "startdate": "start_date",
    "start station id": "start_station_id",
    "startstationid": "start_station_id",
    "start station name": "start_station_name",
    "startstationname": "start_station_name",
    "end station logical terminal": "end_station_logical_terminal",
    "start station logical terminal": "start_station_logical_terminal",
    "end station priority id": "end_station_priority_id",
}

DATETIME_COLS = ["start_date", "end_date"]
INT_COLS      = ["duration"]
STR_COLS      = [
    "rental_id", "bike_id",
    "start_station_id", "start_station_name",
    "end_station_id", "end_station_name",
    "start_station_logical_terminal",
    "end_station_logical_terminal",
    "end_station_priority_id",
]


@task(
    name="discover_files",
    retries=3,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=6),
)
def discover_files(limit: int | None = None) -> list[dict]:
    """Scrape TfL cycling data index and return list of CSV file metadata."""
    logger = get_run_logger()
    logger.info("Fetching TfL index: %s", TFL_INDEX_URL)
    resp = requests.get(TFL_INDEX_URL, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    files: list[dict] = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "usage-stats" in href and href.lower().endswith(".csv"):
            filename = href.split("/")[-1]
            full_url = href if href.startswith("http") else f"{TFL_BASE_URL}/{href.lstrip('/')}"
            match = re.search(r"(\d{1,2}\w{3}\d{4})", filename)
            period = match.group(1) if match else "unknown"
            files.append({"filename": filename, "url": full_url, "period": period})

    logger.info("Discovered %d CSV files", len(files))
    return files[:limit] if limit else files


@task(name="download_file", retries=3, retry_delay_seconds=60)
def download_file(file_meta: dict, tmp_dir: str = "/tmp/tfl") -> str:
    """Download a CSV file and return the local path."""
    logger = get_run_logger()
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)
    local_path = os.path.join(tmp_dir, file_meta["filename"])

    if os.path.exists(local_path):
        logger.info("Cache hit: %s", local_path)
        return local_path

    logger.info("Downloading: %s", file_meta["url"])
    with requests.get(file_meta["url"], stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)

    size_mb = os.path.getsize(local_path) / 1e6
    logger.info("Downloaded %s (%.1f MB)", file_meta["filename"], size_mb)
    return local_path


@task(name="parse_and_clean")
def parse_and_clean(local_path: str, source_filename: str) -> pd.DataFrame:
    """Parse CSV, normalise columns, cast types, add metadata."""
    logger = get_run_logger()
    logger.info("Parsing: %s", local_path)

    try:
        df = pd.read_csv(local_path, encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(local_path, encoding="latin-1", low_memory=False)

    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
    df = df.dropna(how="all")

    expected = STR_COLS + INT_COLS + DATETIME_COLS
    for col in expected:
        if col not in df.columns:
            df[col] = None

    for col in DATETIME_COLS:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce", utc=True)

    for col in INT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in STR_COLS:
        df[col] = df[col].astype(str).where(df[col].notna(), None)

    df = df[[c for c in expected if c in df.columns]].copy()
    df["_source_file"] = source_filename
    df["_ingested_at"] = datetime.now(timezone.utc)

    if "rental_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["rental_id"], keep="last")
        logger.info("Deduplication: %d -> %d rows", before, len(df))

    logger.info("Parsed %d rows from %s", len(df), source_filename)
    return df


@task(name="upload_to_gcs", retries=2)
def upload_to_gcs(df: pd.DataFrame, filename: str) -> str:
    """Write DataFrame as Snappy Parquet to GCS raw/ prefix. Returns GCS URI."""
    logger = get_run_logger()
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    parquet_name = filename.replace(".csv", ".parquet")
    gcs_path = f"raw/{parquet_name}"
    gcs_uri  = f"gs://{GCS_BUCKET}/{gcs_path}"

    buffer = io.BytesIO()
    table  = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    blob = bucket.blob(gcs_path)
    blob.upload_from_file(buffer, content_type="application/octet-stream")
    logger.info("Uploaded %s", gcs_uri)
    return gcs_uri


@task(name="load_to_bigquery", retries=2)
def load_to_bigquery(gcs_uri: str) -> None:
    """Load GCS Parquet into BigQuery raw_rides (append, monthly partition)."""
    logger = get_run_logger()
    client = bigquery.Client(project=GCP_PROJECT)
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET_RAW}.{BQ_TABLE_RAW}"

    job_config = bigquery.LoadJobConfig(
        source_format     = bigquery.SourceFormat.PARQUET,
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        time_partitioning = bigquery.TimePartitioning(
            type_  = bigquery.TimePartitioningType.MONTH,
            field  = "start_date",
        ),
        clustering_fields = ["start_station_id"],
    )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()
    logger.info("Loaded %s -> %s (%d rows)", gcs_uri, table_ref, load_job.output_rows)


@flow(
    name="santander-cycles-ingestion",
    description="Batch ingestion of TfL Santander Cycles usage CSVs to GCS + BigQuery",
    log_prints=True,
)
def ingestion_flow(
    file_limit: int | None = None,
    tmp_dir: str = "/tmp/tfl",
) -> None:
    """
    Main orchestration flow.
    Set file_limit=5 for a quick smoke test; None processes all historical data.
    """
    logger = get_run_logger()
    logger.info("Starting Santander Cycles ingestion pipeline")

    files = discover_files(limit=file_limit)
    logger.info("Processing %d files", len(files))

    results: list[dict[str, Any]] = []
    for file_meta in files:
        try:
            local_path = download_file(file_meta, tmp_dir=tmp_dir)
            df         = parse_and_clean(local_path, file_meta["filename"])
            gcs_uri    = upload_to_gcs(df, file_meta["filename"])
            load_to_bigquery(gcs_uri)
            results.append({"file": file_meta["filename"], "rows": len(df), "status": "ok"})
        except Exception as exc:
            logger.error("Failed %s: %s", file_meta["filename"], exc)
            results.append({"file": file_meta["filename"], "status": "error", "error": str(exc)})

    ok    = [r for r in results if r["status"] == "ok"]
    fails = [r for r in results if r["status"] == "error"]
    total = sum(r.get("rows", 0) for r in ok)
    logger.info("Done. Success: %d | Failed: %d | Rows loaded: %d",
                len(ok), len(fails), total)
    if fails:
        logger.warning("Failed files: %s", [f["file"] for f in fails])


if __name__ == "__main__":
    ingestion_flow(file_limit=3)
