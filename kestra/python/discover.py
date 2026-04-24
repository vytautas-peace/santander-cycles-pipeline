# ────────────────────────────────────────────────────────────────────────
# This script looks at Transport for London's file server and writes
# down every bike ride file it finds — like making a shopping list.
# ────────────────────────────────────────────────────────────────────────

import argparse
import io
import json
import os
import re
import zipfile
from datetime import date
from pathlib import Path

import requests
from google.cloud import bigquery
from google.oauth2 import service_account


# ── Where to look ──────────────────────────────────────────────────────
# TfL keeps all the bike ride files in an Amazon S3 bucket.
# We also have a faster CDN link for downloading.

TFL_CDN = "https://cycling.data.tfl.gov.uk"
TFL_S3  = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk"
PROJECT  = os.environ.get("GCLOUD_PROJECT", "san-cycles-data-pipe")
DATASET = "source"
LOCATION = os.environ.get("LOCATION", "asia-southeast1")
UA      = {"User-Agent": "Mozilla/5.0 (compatible; kestra-pipeline/1.0)"}


# ── Month names → numbers ──────────────────────────────────────────────
# The file names use month names like "Jan" or "June".
# We need to turn those into numbers so the computer understands them.

parser = argparse.ArgumentParser()
parser.add_argument("--start-date", type=date.fromisoformat, required=True)
parser.add_argument("--end-date",   type=date.fromisoformat, required=True)
args = parser.parse_args()

MONTH_MAP = {
    "jan": 1,  "feb": 2,  "mar": 3,  "apr": 4,
    "may": 5,  "jun": 6,  "june": 6, "jul": 7,
    "july": 7, "aug": 8,  "sep": 9,  "oct": 10,
    "nov": 11, "dec": 12,
}

# ── Date finder ────────────────────────────────────────────────────────
# File names look like "10Jan2012-23Jan2012.csv".
# This pattern finds the two dates hiding in the name.

DATE_RE = re.compile(
    r"(\d{1,2})\s*"
    r"(Jan|Feb|Mar|Apr|May|June?|July?|Aug|Sep|Oct|Nov|Dec)\s*"
    r"(\d{2,4})?\s*"
    r"[-\u2013]\s*"
    r"(\d{1,2})\s*"
    r"(Jan|Feb|Mar|Apr|May|June?|July?|Aug|Sep|Oct|Nov|Dec)\s*"
    r"(\d{2,4})",
    re.IGNORECASE,
)


def _yr(y):
    """Turn '12' into 2012, leave '2012' alone."""
    n = int(y)
    return 2000 + n if n < 100 else n


def parse_dates(filename_source):
    """Pull start and end dates out of a filename_source. Returns None if no dates found."""
    m = DATE_RE.search(Path(filename_source).stem)
    if not m:
        return None
    s_mon  = MONTH_MAP[m.group(2).lower()]
    e_mon  = MONTH_MAP[m.group(5).lower()]
    e_year = _yr(m.group(6))
    s_year = _yr(m.group(3)) if m.group(3) else e_year - (1 if s_mon > e_mon else 0)
    return date(s_year, s_mon, int(m.group(1))), date(e_year, e_mon, int(m.group(4)))


# ── Step 1: Ask S3 for every file it has ───────────────────────────────

print("Asking TfL's server for the file list...")
resp = requests.get(
    f"{TFL_S3}/", params={"prefix": "usage-stats/"}, headers=UA, timeout=15
)
resp.raise_for_status()

# The server replies with XML. We grab <Key> (filename_source) and
# <LastModified> (when it was last uploaded) for each file.
keys_and_dates = re.findall(
    r"<Key>(usage-stats/[^<]+)</Key>\s*<LastModified>([^<]+)</LastModified>",
    resp.text,
)
print(f"Found {len(keys_and_dates)} items on the server.")


# ── Step 2: Look inside zip files without downloading them ─────────────

def peek_inside_zip(url):
    """Read just the last 64KB of a zip to see what's inside."""
    head = requests.head(url, headers=UA, timeout=15, allow_redirects=True)
    size = int(head.headers.get("Content-Length", 0))
    if size == 0:
        return []
    tail = 65_536
    r = requests.get(
        url,
        headers={**UA, "Range": f"bytes={max(0, size - tail)}-{size - 1}"},
        timeout=30,
    )
    if r.status_code != 206:
        return []
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    return [n for n in zf.namelist() if n.lower().endswith((".csv", ".xlsx"))]


# ── Step 3: Build the big list ─────────────────────────────────────────

serial = 0      # Counts every single file — makes filename_parquet names unique
results = []    # Our shopping list of files

# --- Zip files first ---
zip_entries = [(k, lm) for k, lm in keys_and_dates if k.lower().endswith(".zip")]
for key, last_modified in zip_entries:
    url = f"{TFL_CDN}/{key}"
    contained = []
    for filename_source in peek_inside_zip(url):
        dates = parse_dates(filename_source)
        if not dates:
            continue
        serial += 1
        contained.append({
            "filename_source":     Path(filename_source).name,
            "filename_parquet":    f"{dates[0]:%Y%m%d}_{dates[1]:%Y%m%d}_{serial:04d}",
            "record_count": None,
        })
    if contained:
        all_starts = []
        all_ends   = []
        for cf in contained:
            d = parse_dates(cf["filename_source"])
            if d:
                all_starts.append(d[0])
                all_ends.append(d[1])
        results.append({
            "root_file":      Path(key).name,
            "url":            url,
            "file_type":      "zip",
            "start_date":     str(min(all_starts)) if all_starts else None,
            "end_date":       str(max(all_ends))   if all_ends   else None,
            "last_modified":  last_modified,
            "contained_files": contained,
        })

# --- Direct CSV/XLSX files ---
direct_entries = [
    (k, lm) for k, lm in keys_and_dates
    if k.lower().endswith((".csv", ".xlsx"))
]
for key, last_modified in direct_entries:
    filename_source = key.split("/")[-1]
    dates = parse_dates(filename_source)
    if not dates:
        continue
    serial += 1
    results.append({
        "root_file":          filename_source,
        "url":                f"{TFL_CDN}/{key}",
        "file_type":          Path(filename_source).suffix.lstrip("."),
        "start_date":         str(dates[0]),
        "end_date":           str(dates[1]),
        "last_modified":      last_modified,
        "contained_files":    [{
            "filename_source":  filename_source,
            "filename_parquet": f"{dates[0]:%Y%m%d}_{dates[1]:%Y%m%d}_{serial:04d}",
            "record_count":     None,
        }],
    })


# ── Step 4: Filter by date range ──────────────────────────────────────
# A file overlaps the window when its dates intersect [start_date, end_date].
# For zips, also filter contained files individually.

def overlaps(start_str, end_str):
    """Check if a file's date range overlaps the requested window."""
    if not start_str or not end_str:
        return False
    return date.fromisoformat(start_str) <= args.end_date \
       and date.fromisoformat(end_str)   >= args.start_date

total_before = len(results)
filtered = []
for r in results:
    if not overlaps(r["start_date"], r["end_date"]):
        continue
    # For zips, keep only contained files that overlap
    kept = []
    for cf in r["contained_files"]:
        d = parse_dates(cf["filename_source"])
        if d and overlaps(str(d[0]), str(d[1])):
            kept.append(cf)
    if kept:
        r["contained_files"] = kept
        # Update root-level dates to reflect the filtered contents
        all_dates = [parse_dates(cf["filename_source"]) for cf in kept]
        all_dates = [d for d in all_dates if d]
        if all_dates:
            r["start_date"] = str(min(d[0] for d in all_dates))
            r["end_date"]   = str(max(d[1] for d in all_dates))
        filtered.append(r)
results = filtered
print(f"Filtered {total_before} → {len(results)} files overlapping {args.start_date}..{args.end_date}")


# ── Step 5: Write to BigQuery ─────────────────────────────────────────
# One row per contained file, root fields denormalised. DROP + CREATE so
# this step is idempotent.

rows = []
for r in results:
    for cf in r["contained_files"]:
        d = parse_dates(cf["filename_source"])
        cf_start, cf_end = (str(d[0]), str(d[1])) if d else (None, None)
        rows.append({
            "root_file":        r["root_file"],
            "url":              r["url"],
            "file_type":        r["file_type"],
            "last_modified":    r["last_modified"],
            "filename_source":  cf["filename_source"],
            "filename_parquet": cf["filename_parquet"],
            "start_date":       cf_start,
            "end_date":         cf_end,
            "record_count":     None,
        })

sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
creds   = service_account.Credentials.from_service_account_info(sa_info)
client  = bigquery.Client(project=PROJECT, credentials=creds, location=LOCATION)

client.query(f"""
    CREATE SCHEMA IF NOT EXISTS `{PROJECT}.{DATASET}`
      OPTIONS (location = '{LOCATION}');

    CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.metadata` (
        root_file           STRING NOT NULL,
        url                 STRING NOT NULL,
        file_type           STRING NOT NULL,
        last_modified       TIMESTAMP,
        filename_source     STRING NOT NULL,
        filename_parquet    STRING NOT NULL,
        start_date          DATE,
        end_date            DATE,
        record_count        INT64,
        PRIMARY KEY (root_file, filename_source) NOT ENFORCED
    );
""").result()

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)
client.load_table_from_json(rows, f"{PROJECT}.{DATASET}.metadata", job_config=job_config).result()

print(f"Inserted {len(rows)} rows into {DATASET}.metadata ({len(results)} root files).")