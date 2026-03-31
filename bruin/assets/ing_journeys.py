""" @bruin
name: san_cycles_ing.journeys
type: python
image: python:.12
connection: san_cycles

materialization:
    type: table
    strategy: append

columns:
  - name: rental_id
    type: integer
    checks:
      - name: not_null
  - name: start_date
    type: timestamp
    checks:
      - name: not_null
  - name: start_station_id
    type: integer
  - name: end_station_id
    type: integer
  - name: total_duration
    type: string
  - name: bike_model
    type: string

description: |
  Ingests TfL Santander Cycles journey data from S3 into BigQuery.
  
  Source rules:
    - 2012-2016: zip files
    - 2017+:     weekly CSV files
  
  Business logic:
    - Filters to requested years only
    - Newer CSVs (from ~late 2022): Number, Total duration, Total duration (ms), Bike model, etc.

@bruin"""

from __future__ import annotations

import io
import json
import os
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import bigquery, storage

# ── Configuration ──────────────────────────────────────────────────────────────
TFL_BASE_URL = "https://cycling.data.tfl.gov.uk"
TFL_S3_URL   = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk"
GCP_PROJECT  = os.environ["GCP_PROJECT"]
GCS_BUCKET   = os.environ["GCS_BKT"]
BQ_DATASET   = os.environ["BQ_DS_ING"]
BQ_TABLE     = os.environ["BQ_TBL_ING"]
TMP_DIR      = "/tmp/tfl"

ZIP_YEARS = {
    2012: "usage-stats/cyclehireusagestats-2012.zip",
    2013: "usage-stats/cyclehireusagestats-2013.zip",
    2014: "usage-stats/cyclehireusagestats-2014.zip",
    2015: "usage-stats/2015TripDatazip.zip",
    2016: "usage-stats/2016TripDataZip.zip",
}

COLUMN_MAP = {
    "rental id":                      "rental_id",
    "number":                         "rental_id",
    "start date":                     "start_date",
    "startdate":                      "start_date",
    "end date":                       "end_date",
    "enddate":                        "end_date",
    "start station id":               "start_station_id",
    "startstationid":                 "start_station_id",
    "startstation id":                "start_station_id",
    "start station logical terminal": "start_station_id",
    "start station number":           "start_station_id",
    "start station name":             "start_station_name",
    "start station":                  "start_station_name",
    "startstationname":               "start_station_name",
    "startstation name":              "start_station_name",
    "end station id":                 "end_station_id",
    "endstationid":                   "end_station_id",
    "endstation id":                  "end_station_id",
    "end station logical terminal":   "end_station_id",
    "end station number":             "end_station_id",
    "end station name":               "end_station_name",
    "end station":                    "end_station_name",
    "endstationname":                 "end_station_name",
    "endstation name":                "end_station_name",
    "duration":                       "duration",
    "duration_seconds":               "duration",
    "bike id":                        "bike_id",
    "bikeid":                         "bike_id",
    "bike number":                    "bike_id",
    "bike model":                     "bike_model",
    "end station priority id":        "end_station_priority_id",
    "total duration":                 "total_duration",
}

# Aliases for duration in milliseconds (handled before COLUMN_MAP rename)
DURATION_MS_ALIASES = (
    "duration (ms)",
    "total duration (ms)",
    "total duration ms",
)

INT_COLS = [
    "rental_id", "bike_id",
    "start_station_id", "end_station_id",
    "end_station_priority_id",
]

STR_COLS      = [
    "start_station_name", "end_station_name",
    "total_duration", "bike_model",
]

DATETIME_COLS = ["start_date", "end_date"]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (compatible; bruin-pipeline/1.0)"})


def _dataframe_for_bruin_upload(df: pd.DataFrame) -> pd.DataFrame:
    """Bruin serializes via Arrow/ingestr; nullable pandas Int64 can error."""
    out = df.copy()
    for col in out.columns:
        s = out[col]
        if not pd.api.types.is_extension_array_dtype(s):
            continue
        name = str(s.dtype)
        if name in ("Int64", "Int32", "Int16", "Int8"):
            out[col] = pd.to_numeric(s, errors="coerce").fillna(0).astype("int64")
        elif name == "boolean":
            out[col] = s.astype("bool")
    return out


# ── Helpers ────────────────────────────────────────────────────────────────────

def fetch_all_keys() -> list[str]:
    resp = SESSION.get(TFL_S3_URL + "/", timeout=15,
                       params={"prefix": "usage-stats/"})
    resp.raise_for_status()

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

    return re.findall(r"<Key>(usage-stats/[^<]+)</Key>",
                      resp.text, re.IGNORECASE)


def parse_year_from_filename(filename: str) -> set[int]:
    years = set()
    for match in re.finditer(r"\d{1,2}\s*[A-Za-z]{3}\s*(\d{2,4})", filename):
        year_str = match.group(1)
        year = int(year_str) + 2000 if len(year_str) == 2 else int(year_str)
        if 2012 <= year <= 2030:
            years.add(year)
    return years


def discover_files(years: list[int] | None) -> list[dict]:
    all_keys = fetch_all_keys()
    print(f"Fetched {len(all_keys)} keys from S3")

    zip_years = sorted(ZIP_YEARS.keys()) if years is None else [
        y for y in years if 2012 <= y <= 2016
    ]
    csv_years = list(range(2017, 2031)) if years is None else [
        y for y in years if y >= 2017
    ]

    files: list[dict] = []

    for year in zip_years:
        key = ZIP_YEARS[year]
        if key in all_keys:
            files.append({"filename": key.split("/")[-1],
                          "url": f"{TFL_BASE_URL}/{key}",
                          "is_zip": True})
            print(f"Zip queued for {year}: {key}")
        else:
            print(f"WARNING: zip not found for {year}")

    if csv_years:
        csv_keys = [k for k in all_keys if k.lower().endswith(".csv")]
        for key in csv_keys:
            filename = key.split("/")[-1]
            file_years = parse_year_from_filename(filename)
            if file_years & set(csv_years):
                files.append({"filename": filename,
                              "url": f"{TFL_BASE_URL}/{key}",
                              "is_zip": False})

    print(f"Total files to process: {len(files)}")
    return files


def download_file(file_meta: dict) -> list[str]:
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)
    local_path = os.path.join(TMP_DIR, file_meta["filename"])

    if not os.path.exists(local_path):
        print(f"Downloading: {file_meta['url']}")
        with SESSION.get(file_meta["url"], stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
        size_mb = os.path.getsize(local_path) / 1e6
        print(f"Downloaded {file_meta['filename']} ({size_mb:.1f} MB)")
    else:
        print(f"Cache hit: {local_path}")

    if file_meta["is_zip"]:
        extract_dir = local_path.replace(".zip", "")
        Path(extract_dir).mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(local_path, "r") as zf:
            zf.extractall(extract_dir)
        csv_paths = [
            os.path.join(extract_dir, name)
            for name in os.listdir(extract_dir)
            if name.lower().endswith(".csv")
        ]
        print(f"Extracted {len(csv_paths)} CSVs from {file_meta['filename']}")
        return csv_paths

    return [local_path]


def parse_and_clean(local_path: str, source_filename: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(local_path, encoding="utf-8", low_memory=False,
                         dtype=str)  # read everything as string first
    except UnicodeDecodeError:
        df = pd.read_csv(local_path, encoding="latin-1", low_memory=False,
                         dtype=str)

    df.columns = [c.strip().lower() for c in df.columns]

    # Milliseconds: legacy "Duration (ms)" or newer "Total duration (ms)" / "Total duration ms"
    ms_present = [c for c in DURATION_MS_ALIASES if c in df.columns]
    if ms_present:
        acc = pd.to_numeric(df[ms_present[0]], errors="coerce")
        for c in ms_present[1:]:
            acc = acc.fillna(pd.to_numeric(df[c], errors="coerce"))
        df = df.drop(columns=ms_present)
        df["duration_ms"] = acc

    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
    df = df.dropna(how="all")

    # Duration: prefer duration_ms (ms → seconds), fall back to duration column
    if "duration_ms" in df.columns:
        df["duration"] = pd.to_numeric(df["duration_ms"], errors="coerce") // 1000
        df = df.drop(columns=["duration_ms"])
    
    df["duration"] = pd.to_numeric(
        df.get("duration"), errors="coerce"
    ).astype("Int64")

    # Parse timestamps
    for col in DATETIME_COLS:
        if col in df.columns:
            s = df[col]
            result = None
            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M",
            ):
                parsed = pd.to_datetime(s, format=fmt, errors="coerce", utc=True)
                if result is None:
                    result = parsed
                else:
                    result = result.fillna(parsed)
            df[col] = result

    # Cast IDs to integer
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # String columns
    for col in STR_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).where(df[col].notna(), None)

    # Ensure all expected columns exist
    expected = INT_COLS + STR_COLS + DATETIME_COLS + ["duration"]
    expected = list(dict.fromkeys(expected))  # deduplicate preserving order
    for col in expected:
        if col not in df.columns:
            df[col] = None

    df = df[[c for c in expected if c in df.columns]].copy()

    # ── Deduplication ──────────────────────────────────────────────────────────
    if "rental_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["rental_id"], keep="last")
        if before != len(df):
            print(f"Deduplication: {before} -> {len(df)} rows")

    # Metadata
    df["_source_file"] = source_filename
    df["_ingested_at"] = datetime.now(timezone.utc)

    print(f"Parsed {len(df)} rows from {source_filename}")
    return _dataframe_for_bruin_upload(df)


# ── Entry point ────────────────────────────────────────────────────────────────

def materialize():
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    years_raw = bruin_vars.get("years") or os.environ.get("YEARS")
    if isinstance(years_raw, list):
        years = years_raw
    elif years_raw:
        years = json.loads(years_raw)
    else:
        years = None
    
    year_str = "all" if years is None else str(years)
    print(f"Starting ingestion — years: {year_str}")
    all_dfs = []

    files = discover_files(years)
    ok, failed, total_rows = 0, 0, 0

    for file_meta in files:
        try:
            local_paths = download_file(file_meta)
            for local_path in local_paths:
                csv_filename = os.path.basename(local_path)
                df = parse_and_clean(local_path, csv_filename)
                all_dfs.append(df)
        except Exception as exc:
            print(f"ERROR: {file_meta['filename']}: {exc}")

    if not all_dfs:
        raise RuntimeError("No data loaded")

    print(f"Done. Success: {ok} | Failed: {failed} | Rows loaded: {total_rows}")

    print(f"Concatenating {len(all_dfs)} DataFrames...")
    merged = pd.concat(all_dfs, ignore_index=True)
    print(f"Merged shape: {merged.shape} | dtypes: {merged.dtypes.to_dict()}")
    result = _dataframe_for_bruin_upload(merged)
    print(f"Ready to return {len(result)} rows to Bruin")
    return result