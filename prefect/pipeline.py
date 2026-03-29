"""
Santander Cycles - Batch Ingestion Pipeline (Prefect)
=====================================================
DAG steps:
  1. discover_files   - Find all CSV file URLs from TfL (tries S3 XML listing,
                        then HTML scrape, then known URL pattern as fallback)
  2. download_file    - Download each CSV to local temp directory
  3. parse_and_clean  - Normalise columns, cast types, add metadata
  4. upload_to_gcs    - Write Parquet to GCS data lake (raw/ prefix)
  5. load_to_bigquery - Append partition to BigQuery raw_rides table
"""

from __future__ import annotations

import io
import os
import re
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

# ── Configuration ──────────────────────────────────────────────────────────────
TFL_BASE_URL   = "https://cycling.data.tfl.gov.uk"
TFL_S3_URL     = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk"
GCP_PROJECT    = os.environ["GCP_PROJECT"]
GCS_BUCKET     = os.environ["GCS_BKT"]
BQ_DATASET_RAW = os.environ["BQ_DS_RAW"]
BQ_TABLE_RAW   = os.environ["BQ_TBL_RAW"]

# TfL changed column names several times across years — map all variants
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
    "end station logical terminal": "end_station_id",
    "start station logical terminal": "start_station_id",
    "end station priority id": "end_station_priority_id",
}

DATETIME_COLS = ["start_date", "end_date"]
INT_COLS      = [
    "duration",
    "rental_id",
    "bike_id",
    "end_station_id",
    "start_station_id",
    "end_station_priority_id"
]

STR_COLS      = [
    "start_station_name",
    "end_station_name"
]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; data-pipeline/1.0)"})


# ── Year filtering helpers ─────────────────────────────────────────────────────

# Zip files cover 2012-2016 inclusive. CSVs cover 2017+.
ZIP_YEARS = {
    2012: "usage-stats/cyclehireusagestats-2012.zip",
    2013: "usage-stats/cyclehireusagestats-2013.zip",
    2014: "usage-stats/cyclehireusagestats-2014.zip",
    2015: "usage-stats/2015TripDatazip.zip",
    2016: "usage-stats/2016TripDataZip.zip",
}

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_year_from_csv_filename(filename: str) -> set[int]:
    """
    Extract years from a CSV filename containing one or two dates.
    Handles formats like:
      - 01Jan2023-07Jan2023  → {2023}
      - 29Dec2022-04Jan2023  → {2022, 2023}
      - 18  Aug12            → {2012}  (early TfL madness)
    Returns a set of years found.
    """
    years = set()

    # Standard format: DDMonYYYY
    for match in re.finditer(r"\d{1,2}\s*[A-Za-z]{3}\s*(\d{2,4})", filename):
        year_str = match.group(1)
        year = int(year_str) + 2000 if len(year_str) == 2 else int(year_str)
        if 2012 <= year <= 2030:
            years.add(year)

    return years


def _csv_matches_years(filename: str, years: list[int]) -> bool:
    """Return True if the CSV filename overlaps with any of the requested years."""
    file_years = _parse_year_from_csv_filename(filename)
    if not file_years:
        return False
    return bool(file_years & set(years))


# ── File discovery strategies ──────────────────────────────────────────────────

def _fetch_all_keys() -> list[str]:
    """
    Fetch all keys from the TfL S3 bucket.
    Returns raw key strings (e.g. 'usage-stats/foo.csv').
    """
    resp = SESSION.get(TFL_S3_URL + "/", timeout=15,
                       params={"prefix": "usage-stats/"})
    resp.raise_for_status()

    # Strategy A: lxml with recovery mode
    try:
        from lxml import etree
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(resp.content, parser=parser)
        ns = "http://s3.amazonaws.com/doc/2006-03-01/"
        keys = [
            (el.text or "").strip()
            for el in root.findall(f".//{{{ns}}}Key")
            if (el.text or "").strip()
        ]
        if keys:
            return keys
    except Exception:
        pass

    # Strategy B: regex on raw text
    return re.findall(
        r"<Key>(usage-stats/[^<]+)</Key>",
        resp.text,
        re.IGNORECASE,
    )


def _make_file_meta(key: str) -> dict:
    """Build a file metadata dict from an S3 key."""
    filename = key.split("/")[-1]
    return {
        "filename": filename,
        "url": f"{TFL_BASE_URL}/{key}",
        "key": key,
        "is_zip": filename.lower().endswith(".zip"),
    }


# ── Tasks ──────────────────────────────────────────────────────────────────────

@task(
    name="discover_files",
    retries=3,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=6),
)
def discover_files(years: list[int] | None = None) -> list[dict]:
    """
    Discover files to process based on requested years.

    Year → source mapping (hard rule):
      2012–2016  →  zip files only
      2017+      →  CSV files only
      years=None →  everything (zips 2012-2016 + CSVs 2017+)

    Args:
        years: List of integer years e.g. [2022, 2023].
               None means all years.
    """
    logger = get_run_logger()

    all_keys = _fetch_all_keys()
    logger.info("Fetched %d keys from S3", len(all_keys))

    zip_years  = sorted(ZIP_YEARS.keys()) if years is None else [
        y for y in years if 2012 <= y <= 2016
    ]
    csv_years  = list(range(2017, 2031)) if years is None else [
        y for y in years if y >= 2017
    ]

    files: list[dict] = []

    # ── Zips (2012-2016) ──────────────────────────────────────────────────────
    for year in zip_years:
        expected_key = ZIP_YEARS[year]
        if expected_key in all_keys:
            files.append(_make_file_meta(expected_key))
            logger.info("Zip found for year %d: %s", year, expected_key)
        else:
            logger.warning("Zip not found for year %d (expected %s)", year, expected_key)

    # ── CSVs (2017+) ──────────────────────────────────────────────────────────
    if csv_years:
        csv_keys = [k for k in all_keys if k.lower().endswith(".csv")]
        for key in csv_keys:
            filename = key.split("/")[-1]
            if _csv_matches_years(filename, csv_years):
                files.append(_make_file_meta(key))

        logger.info("CSVs matched for years %s: %d files", csv_years, len(files) - len(zip_years))

    logger.info("Total files to process: %d", len(files))
    return files


@task(name="download_file", retries=3, retry_delay_seconds=60)
def download_file(file_meta: dict, tmp_dir: str = "/tmp/tfl") -> list[str]:
    """
    Download a CSV or ZIP file. Returns list of local CSV paths.
    Zips are extracted; CSVs are returned as-is.
    """
    import zipfile

    logger = get_run_logger()
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)
    local_path = os.path.join(tmp_dir, file_meta["filename"])

    if not os.path.exists(local_path):
        logger.info("Downloading: %s", file_meta["url"])
        with SESSION.get(file_meta["url"], stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
        logger.info("Downloaded %s (%.1f MB)", file_meta["filename"],
                    os.path.getsize(local_path) / 1e6)
    else:
        logger.info("Cache hit: %s", local_path)

    # Extract zip → return list of extracted CSV paths
    if file_meta.get("is_zip"):
        extract_dir = os.path.join(tmp_dir, file_meta["filename"].replace(".zip", ""))
        Path(extract_dir).mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(local_path, "r") as zf:
            zf.extractall(extract_dir)
        csv_paths = [
            os.path.join(extract_dir, name)
            for name in os.listdir(extract_dir)
            if name.lower().endswith(".csv")
        ]
        logger.info("Extracted %d CSVs from %s", len(csv_paths), file_meta["filename"])
        return csv_paths

    return [local_path]


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
    
    logger.info("Columns after rename: %s", list(df.columns))

    df = df.dropna(how="all")

    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    expected = STR_COLS + INT_COLS + DATETIME_COLS
    for col in expected:
        if col not in df.columns:
            df[col] = None

    for col in DATETIME_COLS:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce", utc=True)

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
    """Write DataFrame as Snappy Parquet to GCS raw/ prefix."""
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


# ── Flow ───────────────────────────────────────────────────────────────────────

@flow(
    name="santander-cycles-ingestion",
    description="Batch ingestion of TfL Santander Cycles usage CSVs to GCS + BigQuery",
    log_prints=True,
)

def ingestion_flow( \
    years: list[int] | None = None, \
    tmp_dir: str = "/tmp/tfl", \
) -> None:
    """
    Main orchestration flow.

    Args:
        years: List of years to process e.g. [2022, 2023].
               None means all years (zips 2012-2016 + CSVs 2017+).
        tmp_dir: Local directory for downloaded files.
    """
    logger = get_run_logger()
    year_str = "all" if years is None else str(years)
    logger.info("Starting Santander Cycles ingestion — years: %s", year_str)

    files = discover_files(years=years)
    logger.info("Processing %d files", len(files))

    results: list[dict[str, Any]] = []
    for file_meta in files:
        try:
            local_paths = download_file(file_meta, tmp_dir=tmp_dir)
            for local_path in local_paths:
                csv_filename = os.path.basename(local_path)
                df       = parse_and_clean(local_path, csv_filename)
                gcs_uri  = upload_to_gcs(df, csv_filename)
                load_to_bigquery(gcs_uri)
                results.append({
                    "file": csv_filename,
                    "rows": len(df),
                    "status": "ok",
                })
        except Exception as exc:
            logger.error("Failed %s: %s", file_meta["filename"], exc)
            results.append({
                "file": file_meta["filename"],
                "status": "error",
                "error": str(exc),
            })

    ok    = [r for r in results if r["status"] == "ok"]
    fails = [r for r in results if r["status"] == "error"]
    total = sum(r.get("rows", 0) for r in ok)
    logger.info("Done. Success: %d | Failed: %d | Rows loaded: %d",
                len(ok), len(fails), total)
    if fails:
        logger.warning("Failed files: %s", [f["file"] for f in fails])


if __name__ == "__main__":
    years_env = os.environ.get("YEARS", "").strip()
    years = [int(y) for y in years_env.split()] if years_env else None
    ingestion_flow(years=years)