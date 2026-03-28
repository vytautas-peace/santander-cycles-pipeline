"""
scripts/ingest.py
─────────────────
Batch ingestion script for TfL Santander Cycles data.

Flow:
  1. Discover CSV files in TfL's public S3 bucket via XML bucket listing
  2. Filter to files not yet ingested (idempotent via GCS manifest)
  3. Download each CSV (stream to memory)
  4. Normalise schema (handle multi-era drift)
  5. Write as Parquet (partitioned by year/month) to GCS
  6. Update GCS manifest

Run standalone:
    python ingest.py --bucket your-gcs-bucket --year 2024

Or imported by Airflow DAG tasks.
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
from datetime import date
from pathlib import Path
from typing import Generator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import storage
from xml.etree import ElementTree as ET

from normalise import normalise_csv

logger = logging.getLogger(__name__)

# ─── TfL S3 public bucket ─────────────────────────────────────────────────────
TFL_BUCKET_XML = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk"
USAGE_STATS_PREFIX = "usage-stats/"

# ─── Parquet schema (enforced on write) ──────────────────────────────────────
PARQUET_SCHEMA = pa.schema([
    pa.field("journey_id",        pa.int64()),
    pa.field("duration_seconds",  pa.int32()),
    pa.field("bike_id",           pa.int32()),
    pa.field("start_datetime",    pa.timestamp("us")),
    pa.field("end_datetime",      pa.timestamp("us")),
    pa.field("start_station_id",  pa.int32()),
    pa.field("end_station_id",    pa.int32()),
    pa.field("start_station_name",pa.string()),
    pa.field("end_station_name",  pa.string()),
    pa.field("bike_model",        pa.string()),
    pa.field("start_date",        pa.date32()),
    pa.field("start_year",        pa.int16()),
    pa.field("start_month",       pa.int8()),
    pa.field("source_file",       pa.string()),
])


# ─── Discover available files ─────────────────────────────────────────────────
def list_tfl_csv_files(year: int | None = None) -> Generator[str, None, None]:
    """
    Fetch the TfL S3 bucket XML listing and yield CSV file keys.

    The bucket listing is paginated (max 1000 keys per response).
    We follow 'NextMarker' tokens until exhausted.
    """
    marker = ""
    seen = 0
    pattern = re.compile(r"usage-stats/.*\.csv$", re.IGNORECASE)
    year_pattern = re.compile(rf"/{year}/" if year else r".*") if year else None

    while True:
        url = f"{TFL_BUCKET_XML}?prefix={USAGE_STATS_PREFIX}&max-keys=1000"
        if marker:
            url += f"&marker={marker}"

        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Failed to fetch bucket listing: %s", exc)
            break

        root = ET.fromstring(resp.text)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        keys = [el.text for el in root.findall(".//s3:Key", ns) if el.text]
        for key in keys:
            if pattern.match(key):
                if year_pattern is None or year_pattern.search(key):
                    seen += 1
                    yield key

        # Pagination
        truncated = root.findtext("s3:IsTruncated", namespaces=ns)
        if truncated and truncated.lower() == "true":
            marker = keys[-1] if keys else ""
        else:
            break

    logger.info("Discovered %d CSV files", seen)


# ─── GCS manifest (idempotency) ───────────────────────────────────────────────
MANIFEST_BLOB = "parquet/.manifest.json"

def load_manifest(gcs_client: storage.Client, bucket_name: str) -> set[str]:
    """Load set of already-ingested source file keys from GCS manifest."""
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)
    if blob.exists():
        data = json.loads(blob.download_as_text())
        return set(data.get("ingested", []))
    return set()


def save_manifest(gcs_client: storage.Client, bucket_name: str, ingested: set[str]) -> None:
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(MANIFEST_BLOB)
    blob.upload_from_string(
        json.dumps({"ingested": sorted(ingested), "updated_at": date.today().isoformat()}),
        content_type="application/json",
    )


# ─── Download one CSV from TfL ────────────────────────────────────────────────
def download_csv(key: str) -> bytes:
    url = f"{TFL_BUCKET_XML}/{key}"
    logger.info("Downloading: %s", url)
    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    chunks = []
    for chunk in resp.iter_content(chunk_size=8192):
        chunks.append(chunk)
    return b"".join(chunks)


# ─── Upload Parquet to GCS ────────────────────────────────────────────────────
def upload_parquet(
    gcs_client: storage.Client,
    bucket_name: str,
    df: pd.DataFrame,
    year: int,
    month: int,
    source_file: str,
) -> str:
    """Write DataFrame to Parquet and upload to GCS with hive partitioning."""
    # Hive-style partition path: parquet/journeys/start_year=2024/start_month=3/
    safe_name = Path(source_file).stem.replace(" ", "_")
    blob_path = (
        f"parquet/journeys/start_year={year}/start_month={month:02d}/{safe_name}.parquet"
    )

    # Cast to schema (coerce mismatches)
    table = pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, safe=False)

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_file(buf, content_type="application/octet-stream")

    logger.info("Uploaded %d rows → gs://%s/%s", len(df), bucket_name, blob_path)
    return blob_path


# ─── Main ingestion loop ──────────────────────────────────────────────────────
def run_ingestion(
    gcs_bucket: str,
    year: int | None = None,
    max_files: int | None = None,
    dry_run: bool = False,
) -> list[str]:
    """
    Run the full ingestion pipeline.

    Parameters
    ----------
    gcs_bucket  : GCS bucket name (without gs:// prefix)
    year        : Only ingest files from this year (None = all)
    max_files   : Cap on number of files to process (for testing)
    dry_run     : If True, discover files but don't download/upload

    Returns
    -------
    List of GCS blob paths written
    """
    gcs_client = storage.Client()
    ingested = load_manifest(gcs_client, gcs_bucket)
    logger.info("Manifest: %d files already ingested", len(ingested))

    uploaded_paths: list[str] = []
    processed = 0

    for key in list_tfl_csv_files(year=year):
        if key in ingested:
            logger.debug("Skipping already-ingested: %s", key)
            continue

        if max_files and processed >= max_files:
            logger.info("Reached max_files=%d, stopping", max_files)
            break

        if dry_run:
            logger.info("[DRY RUN] Would ingest: %s", key)
            processed += 1
            continue

        try:
            # 1. Download
            raw_bytes = download_csv(key)

            # 2. Normalise
            df = normalise_csv(raw_bytes, filename=key)
            if df.empty:
                logger.warning("Empty DataFrame after normalisation: %s", key)
                ingested.add(key)
                continue

            # 3. Group by year/month and upload (a single CSV may span months)
            for (yr, mo), group in df.groupby(["start_year", "start_month"]):
                if pd.isna(yr) or pd.isna(mo):
                    continue
                path = upload_parquet(gcs_client, gcs_bucket, group, int(yr), int(mo), key)
                uploaded_paths.append(path)

            ingested.add(key)
            processed += 1

        except Exception as exc:
            logger.error("Failed to process %s: %s", key, exc, exc_info=True)
            # Continue with next file — don't add to manifest so it's retried

    # 4. Save manifest
    if not dry_run:
        save_manifest(gcs_client, gcs_bucket, ingested)

    logger.info("Ingestion complete. Processed %d files, uploaded %d Parquet blobs",
                processed, len(uploaded_paths))
    return uploaded_paths


# ─── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Ingest TfL Santander Cycles data to GCS")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--year", type=int, default=None, help="Filter to specific year")
    parser.add_argument("--max-files", type=int, default=None, help="Max files to ingest")
    parser.add_argument("--dry-run", action="store_true", help="Discover without downloading")
    args = parser.parse_args()

    paths = run_ingestion(
        gcs_bucket=args.bucket,
        year=args.year,
        max_files=args.max_files,
        dry_run=args.dry_run,
    )
    print(f"\nUploaded {len(paths)} Parquet files")
    for p in paths[:10]:
        print(f"  {p}")
    if len(paths) > 10:
        print(f"  ... and {len(paths) - 10} more")
