"""
Single-script replacement for discover + download + make-parquet + upload-parquet.

Pipeline (overlapping phases):
  1. List TfL S3 bucket  ──────────────────────────────────────────► metadata dict
  2. BQ write (background thread) ◄── metadata dict                  (runs during phase 3-4)
  3. Concurrent HTTP downloads ◄─── metadata dict
  4. Concurrent Polars process + GCS upload ◄── download bytes

Result: BQ metadata write overlaps with file processing, eliminating ~3s of sequential overhead.
"""

import argparse
import io
import json
import os
import re
import struct
import threading
import zlib
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


# ── Config ─────────────────────────────────────────────────────────────

TFL_CDN  = "https://cycling.data.tfl.gov.uk"
TFL_S3   = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk"
UA       = {"User-Agent": "Mozilla/5.0 (compatible; kestra-pipeline/1.0)"}
PROJECT  = os.environ.get("GCLOUD_PROJECT", "san-cycles-data-pipe")
LOCATION = os.environ.get("LOCATION", "asia-southeast1")
BUCKET   = os.environ.get("GCS_BUCKET",  "san-cycles-data-pipe-bkt")
GCS_PFX  = "parquet"

# At most 2 zip files processed concurrently — each annual zip can be 200-400 MB;
# holding more simultaneously risks OOM on the 16 GB Codespace.
_ZIP_SEM = threading.Semaphore(2)

MONTH_MAP = {
    "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"june":6,
    "jul":7,"july":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
}
DATE_RE = re.compile(
    r"(\d{1,2})\s*(Jan|Feb|Mar|Apr|May|June?|July?|Aug|Sep|Oct|Nov|Dec)\s*(\d{2,4})?\s*"
    r"[-–]\s*(\d{1,2})\s*(Jan|Feb|Mar|Apr|May|June?|July?|Aug|Sep|Oct|Nov|Dec)\s*(\d{2,4})",
    re.IGNORECASE,
)

DEST_SCHEMA = {
    "rental_id":               pl.Int32,
    "start_datetime":          pl.Datetime("ms"),
    "end_datetime":            pl.Datetime("ms"),
    "duration_s":              pl.Int32,
    "start_station_id":        pl.Int32,
    "end_station_id":          pl.Int32,
    "bike_id":                 pl.Int32,
    "end_station_priority_id": pl.Int8,
    "start_station_name":      pl.String,
    "bike_model":              pl.String,
    "_ingested_at":            pl.Datetime("ms"),
}

COL_MAP = {
    "rental_id":               ["Rental Id", "Number"],
    "bike_id":                 ["Bike Id", "Bike number"],
    "bike_model":              ["Bike model"],
    "start_datetime":          ["Start Date", "Start date"],
    "end_datetime":            ["End Date", "End date"],
    "duration_s":              ["Duration", "Duration_Seconds"],
    "duration_ms":             ["Total duration (ms)"],
    "start_station_id":        ["StartStation Id", "StartStation Logical Terminal",
                                "Start Station Id", "Start station number"],
    "end_station_id":          ["EndStation Id", "EndStation Logical Terminal",
                                "End Station Id", "End station number"],
    "start_station_name":      ["StartStation Name", "Start Station Name", "Start station"],
    "end_station_priority_id": ["endStationPriority_id"],
}

DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M",
)

# ── Helpers ────────────────────────────────────────────────────────────

def _yr(y): n = int(y); return 2000+n if n < 100 else n

def parse_dates(name):
    m = DATE_RE.search(Path(name).stem)
    if not m: return None
    s_mon = MONTH_MAP[m.group(2).lower()]
    e_mon = MONTH_MAP[m.group(5).lower()]
    e_yr  = _yr(m.group(6))
    s_yr  = _yr(m.group(3)) if m.group(3) else e_yr - (1 if s_mon > e_mon else 0)
    return date(s_yr, s_mon, int(m.group(1))), date(e_yr, e_mon, int(m.group(4)))

def overlaps(start_str, end_str, window_start, window_end):
    if not start_str or not end_str: return False
    return date.fromisoformat(start_str) <= window_end and date.fromisoformat(end_str) >= window_start

def peek_zip(url):
    head = requests.head(url, headers=UA, timeout=15, allow_redirects=True)
    size = int(head.headers.get("Content-Length", 0))
    if not size: return []
    r = requests.get(url, headers={**UA, "Range": f"bytes={max(0,size-65536)}-{size-1}"}, timeout=30)
    if r.status_code != 206: return []
    return [n for n in zipfile.ZipFile(io.BytesIO(r.content)).namelist()
            if n.lower().endswith((".csv", ".xlsx"))]


def _zip_year(key):
    """Extract 4-digit year from a zip filename, e.g. '2015TripDatazip.zip' → 2015."""
    m = re.search(r"(20\d{2})", Path(key).name)
    return int(m.group(1)) if m else None


# ── Phase 1: discover ──────────────────────────────────────────────────

def discover(start_date, end_date):
    print("Listing TfL S3...")
    resp = requests.get(f"{TFL_S3}/", params={"prefix": "usage-stats/"}, headers=UA, timeout=15)
    resp.raise_for_status()
    keys_and_dates = re.findall(
        r"<Key>(usage-stats/[^<]+)</Key>\s*<LastModified>([^<]+)</LastModified>", resp.text
    )
    print(f"  {len(keys_and_dates)} items on S3")

    serial  = 0
    results = []

    # Zip files — skip peeking when the zip's year is clearly outside the date window
    zip_entries = [
        (k, lm) for k, lm in keys_and_dates if k.lower().endswith(".zip")
        and (_zip_year(k) is None or start_date.year <= _zip_year(k) <= end_date.year)
    ]
    with ThreadPoolExecutor(max_workers=len(zip_entries) or 1) as pool:
        peek_futures = {pool.submit(peek_zip, f"{TFL_CDN}/{k}"): (k, lm) for k, lm in zip_entries}
        for fut in as_completed(peek_futures):
            k, lm = peek_futures[fut]
            contained_names = fut.result()
            contained = []
            for fn in contained_names:
                d = parse_dates(fn)
                if not d: continue
                serial += 1
                contained.append({"filename_source": Path(fn).name,
                                   "filename_parquet": f"{d[0]:%Y%m%d}_{d[1]:%Y%m%d}_{serial:04d}",
                                   "start": str(d[0]), "end": str(d[1])})
            if not contained: continue
            starts = [c["start"] for c in contained]
            ends   = [c["end"]   for c in contained]
            results.append({
                "root_file":       Path(k).name,
                "url":             f"{TFL_CDN}/{k}",
                "file_type":       "zip",
                "last_modified":   lm,
                "start_date":      min(starts),
                "end_date":        max(ends),
                "contained_files": contained,
            })

    # Direct CSV/XLSX
    for k, lm in keys_and_dates:
        if k.lower().endswith(".zip"): continue
        if not k.lower().endswith((".csv", ".xlsx")): continue
        fn = k.split("/")[-1]
        d  = parse_dates(fn)
        if not d: continue
        serial += 1
        results.append({
            "root_file":       fn,
            "url":             f"{TFL_CDN}/{k}",
            "file_type":       Path(fn).suffix.lstrip("."),
            "last_modified":   lm,
            "start_date":      str(d[0]),
            "end_date":        str(d[1]),
            "contained_files": [{"filename_source": fn,
                                  "filename_parquet": f"{d[0]:%Y%m%d}_{d[1]:%Y%m%d}_{serial:04d}",
                                  "start": str(d[0]), "end": str(d[1])}],
        })

    # Filter by date window
    filtered = []
    for r in results:
        if not overlaps(r["start_date"], r["end_date"], start_date, end_date): continue
        kept = [c for c in r["contained_files"] if overlaps(c["start"], c["end"], start_date, end_date)]
        if not kept: continue
        r["contained_files"] = kept
        filtered.append(r)

    print(f"  {len(filtered)} root files in window {start_date}..{end_date}")
    return filtered


# ── Phase 2: BQ write (runs in background) ────────────────────────────

def write_bq(results, creds):
    rows = []
    for r in results:
        for cf in r["contained_files"]:
            rows.append({
                "root_file":        r["root_file"],
                "url":              r["url"],
                "file_type":        r["file_type"],
                "last_modified":    r["last_modified"],
                "filename_source":  cf["filename_source"],
                "filename_parquet": cf["filename_parquet"],
                "start_date":       cf["start"],
                "end_date":         cf["end"],
                "record_count":     None,
            })
    try:
        client = bigquery.Client(project=PROJECT, credentials=creds, location=LOCATION)
        # DDL — two statements as a script (~1-1.5s)
        client.query(f"""
            CREATE SCHEMA IF NOT EXISTS `{PROJECT}.source` OPTIONS (location='{LOCATION}');
            CREATE OR REPLACE TABLE `{PROJECT}.source.metadata` (
                root_file        STRING NOT NULL,
                url              STRING NOT NULL,
                file_type        STRING NOT NULL,
                last_modified    TIMESTAMP,
                filename_source  STRING NOT NULL,
                filename_parquet STRING NOT NULL,
                start_date       DATE,
                end_date         DATE,
                record_count     INT64,
                PRIMARY KEY (root_file, filename_source) NOT ENFORCED
            );
        """).result()
        # Batch load — no race condition with CREATE OR REPLACE TABLE (unlike streaming inserts).
        # Takes ~2-3s; for the full range this finishes well before the ~200s processing phase.
        client.load_table_from_json(
            rows,
            f"{PROJECT}.source.metadata",
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ),
        ).result()
        print(f"  BQ: wrote {len(rows)} rows to source.metadata")
    except Exception as e:
        print(f"  BQ write error (non-fatal): {e}")


# ── Phase 3-4: download + process + upload ────────────────────────────

def _read_csv_bytes(data):
    try:
        return pl.read_csv(io.BytesIO(data), infer_schema=False)
    except Exception:
        return pl.read_csv(io.BytesIO(data.decode("latin-1").encode()), infer_schema=False)

def process_bytes(data, filename_parquet, filename_source):
    raw = pl.read_excel(io.BytesIO(data), infer_schema_length=0) \
          if filename_source.lower().endswith(".xlsx") else _read_csv_bytes(data)
    print(f"    {filename_source}: {raw.height:,} rows")
    source_cols = set(raw.columns)
    rename = []
    for dest, aliases in COL_MAP.items():
        match = next((a for a in aliases if a in source_cols), None)
        rename.append(pl.col(match).alias(dest) if match
                      else pl.lit(None).cast(DEST_SCHEMA.get(dest, pl.String)).alias(dest))
    df = (raw.lazy().with_columns(rename).select(COL_MAP.keys()).with_columns(
        pl.coalesce([pl.col("start_datetime").str.to_datetime(f, strict=False, ambiguous="earliest")
                     for f in DATE_FORMATS]).alias("start_datetime"),
        pl.coalesce([pl.col("end_datetime").str.to_datetime(f, strict=False, ambiguous="earliest")
                     for f in DATE_FORMATS]).alias("end_datetime"),
        pl.coalesce((pl.col("duration_ms").cast(pl.Int64, strict=False)//1000).cast(pl.Int32, strict=False),
                    pl.col("duration_s").cast(pl.Int32, strict=False)).alias("duration_s"),
        pl.col("rental_id").cast(pl.Int32, strict=False),
        pl.col("bike_id").cast(pl.Int32, strict=False),
        pl.col("start_station_id").cast(pl.Int32, strict=False),
        pl.col("end_station_id").cast(pl.Int32, strict=False),
        pl.col("end_station_priority_id").cast(pl.Int8, strict=False),
        pl.col("start_station_name").cast(pl.String),
        pl.col("bike_model").cast(pl.String),
        pl.lit(datetime.now(timezone.utc)).cast(pl.Datetime("ms")).alias("_ingested_at"),
    ).drop("duration_ms").with_columns(
        pl.lit(filename_source).alias("filename_source"),
        pl.lit(filename_parquet).alias("filename_parquet"),
    ).collect())
    buf = io.BytesIO(); df.write_parquet(buf); return buf.getvalue()

def _zip_member_offsets(src_bytes: bytes) -> dict:
    """Parse zip central directory + local headers once, return each member's
    (data_start, comp_size, compress_type) so threads can slice bytes directly."""
    offsets = {}
    with zipfile.ZipFile(io.BytesIO(src_bytes)) as zf:
        for info in zf.infolist():
            if not info.filename.lower().endswith((".csv", ".xlsx")):
                continue
            # Local file header layout (30 fixed bytes):
            #   signature(4) version(2) flags(2) method(2) mod_time(2) mod_date(2)
            #   crc(4) comp_size(4) uncomp_size(4) fname_len(2) extra_len(2)
            fname_len, extra_len = struct.unpack_from("<HH", src_bytes, info.header_offset + 26)
            data_start = info.header_offset + 30 + fname_len + extra_len
            offsets[Path(info.filename).name] = (data_start, info.compress_size, info.compress_type)
    return offsets


def _decompress_member(src_bytes: bytes, data_start: int, comp_size: int, compress_type: int) -> bytes:
    """Extract and decompress one zip member directly from the raw bytes.
    zlib.decompress releases the GIL → multiple threads run truly in parallel."""
    compressed = src_bytes[data_start: data_start + comp_size]  # C-level slice, no full copy
    if compress_type == 8:          # deflated
        return zlib.decompress(compressed, -15)
    elif compress_type == 0:        # stored
        return compressed
    else:
        raise ValueError(f"Unsupported zip compress_type: {compress_type}")


def process_root(root, sa_info):
    """Download one root file, process all contained CSVs, upload parquets. Thread-safe."""
    print(f"  Downloading {root['root_file']}...")
    resp = requests.get(root["url"], headers=UA, timeout=600)
    resp.raise_for_status()
    src_bytes = resp.content
    print(f"  Downloaded {root['root_file']} ({len(src_bytes)//1024} KB)")

    def _make_gcs():
        creds = service_account.Credentials.from_service_account_info(sa_info)
        return storage.Client(project=PROJECT, credentials=creds)

    def upload_member(data, fn_src, fn_pq):
        gcs  = _make_gcs()
        pq   = process_bytes(data, fn_pq, fn_src)
        gcs.bucket(BUCKET).blob(f"{GCS_PFX}/{fn_pq}.parquet").upload_from_string(
            pq, content_type="application/octet-stream"
        )
        print(f"    Uploaded {fn_pq}.parquet ({len(pq)//1024} KB)")

    if root["file_type"] == "zip":
        # Semaphore caps concurrent zip processing to 2 — prevents OOM when
        # multiple 200-400 MB zips + their decompressed members all live in
        # memory simultaneously. Direct CSVs are unaffected.
        with _ZIP_SEM:
            offsets = _zip_member_offsets(src_bytes)

            def process_zip_member(fn_src, fn_pq):
                if fn_src not in offsets:
                    print(f"    SKIP (not in zip): {fn_src}")
                    return
                data_start, comp_size, compress_type = offsets[fn_src]
                data = _decompress_member(src_bytes, data_start, comp_size, compress_type)
                upload_member(data, fn_src, fn_pq)

            tasks = [(cf["filename_source"], cf["filename_parquet"])
                     for cf in root["contained_files"]]
            with ThreadPoolExecutor(max_workers=8) as pool:
                futs = [pool.submit(process_zip_member, s, p) for s, p in tasks]
                for f in as_completed(futs): f.result()
    else:
        cf = root["contained_files"][0]
        upload_member(src_bytes, cf["filename_source"], cf["filename_parquet"])


# ── Main ───────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser()
parser.add_argument("--start-date", type=date.fromisoformat, required=True)
parser.add_argument("--end-date",   type=date.fromisoformat, required=True)
parser.add_argument("--workers",    type=int, default=16)
args = parser.parse_args()

sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
creds   = service_account.Credentials.from_service_account_info(sa_info)

# Phase 1: discover
results = discover(args.start_date, args.end_date)

# Phase 2: BQ write — starts in background immediately
bq_thread = threading.Thread(target=write_bq, args=(results, creds), daemon=False)
bq_thread.start()

# Phase 3-4: concurrent download + process + upload
print(f"Processing {len(results)} root files (workers={args.workers})...")
with ThreadPoolExecutor(max_workers=args.workers) as pool:
    futs = {pool.submit(process_root, r, sa_info): r["root_file"] for r in results}
    for fut in as_completed(futs):
        fut.result()  # re-raises exceptions

# Wait for BQ write to complete (usually already done)
bq_thread.join(timeout=60)
if bq_thread.is_alive():
    print("WARNING: BQ write did not complete within timeout")

print("Done.")
