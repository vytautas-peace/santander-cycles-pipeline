# ────────────────────────────────────────────────────────────────────────
# This script takes every bike ride file belonging to one root_file
# (a single CSV, or all CSVs extracted from a zip), fixes up the messy
# column names and date formats, saves each as a clean parquet file,
# then deletes the originals.
#
# Think of it like sorting a pile of LEGO bricks into the right boxes —
# even though different sets call the same brick different names.
# ────────────────────────────────────────────────────────────────────────

import argparse
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account


# ── Paths ──────────────────────────────────────────────────────────────

SOURCE_NEW      = Path("/workspace/kestra/data/source")
PARQUET_OUT_DIR = Path("/workspace/kestra/data/parquet")
PROJECT         = os.environ.get("GCLOUD_PROJECT", "san-cycles-data-pipe")
LOCATION        = os.environ.get("LOCATION", "asia-southeast1")


# ── The blueprint: what the clean data should look like ─────────────────

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


# ── The dictionary: old messy names → clean names ──────────────────────
# TfL used 9 different sets of column names over the years.
# This maps every name they ever used to our clean name.

COL_MAP = {
    "rental_id":              ["Rental Id", "Number"],
    "bike_id":                ["Bike Id", "Bike number"],
    "bike_model":             ["Bike model"],
    "start_datetime":         ["Start Date", "Start date"],
    "end_datetime":           ["End Date", "End date"],
    "duration_s":             ["Duration", "Duration_Seconds"],
    "duration_ms":            ["Total duration (ms)"],
    "start_station_id":       ["StartStation Id", "StartStation Logical Terminal",
                                "Start Station Id", "Start station number"],
    "end_station_id":         ["EndStation Id", "EndStation Logical Terminal",
                                "End Station Id", "End station number"],
    "start_station_name":     ["StartStation Name", "Start Station Name", "Start station"],
    "end_station_priority_id": ["endStationPriority_id"],
}


# ── The 4 date formats TfL has used ────────────────────────────────────

DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
)


# ══════════════════════════════════════════════════════════════════════
# Per-file pipeline: read → rename → clean → write parquet
# ══════════════════════════════════════════════════════════════════════

def process_one(csv_path: Path, filename_parquet: str) -> int:
    """Read one CSV/XLSX, write its parquet, return row count."""
    filename_source = csv_path.name
    print(f"Reading {csv_path}...")

    if csv_path.suffix.lower() == ".xlsx":
        raw = pl.read_excel(csv_path, infer_schema_length=0)
    else:
        try:
            raw = pl.read_csv(csv_path, infer_schema=False)
        except Exception:
            with open(csv_path, encoding="latin-1") as f:
                raw = pl.read_csv(io.StringIO(f.read()), infer_schema=False)

    record_count = raw.height
    print(f"  {record_count} rows, {len(raw.columns)} columns")

    # -- Build rename expressions; fill missing columns with NULL --
    source_cols = set(raw.columns)
    rename_exprs = []
    for dest_name, aliases in COL_MAP.items():
        match = next((a for a in aliases if a in source_cols), None)
        if match:
            rename_exprs.append(pl.col(match).alias(dest_name))
        else:
            data_type = DEST_SCHEMA.get(dest_name, pl.String)
            rename_exprs.append(pl.lit(None).cast(data_type).alias(dest_name))

    cave = (
        raw.lazy()
        .with_columns(rename_exprs)
        .select(COL_MAP.keys())
        .with_columns(
            pl.coalesce([
                pl.col("start_datetime").str.to_datetime(
                    fmt, strict=False, ambiguous="earliest"
                )
                for fmt in DATE_FORMATS
            ]).alias("start_datetime"),

            pl.coalesce([
                pl.col("end_datetime").str.to_datetime(
                    fmt, strict=False, ambiguous="earliest"
                )
                for fmt in DATE_FORMATS
            ]).alias("end_datetime"),

            pl.coalesce(
                (pl.col("duration_ms").cast(pl.Int64, strict=False) // 1000)
                    .cast(pl.Int32, strict=False),
                pl.col("duration_s").cast(pl.Int32, strict=False),
            ).alias("duration_s"),

            pl.col("rental_id").cast(pl.Int32, strict=False),
            pl.col("bike_id").cast(pl.Int32, strict=False),
            pl.col("start_station_id").cast(pl.Int32, strict=False),
            pl.col("end_station_id").cast(pl.Int32, strict=False),
            pl.col("end_station_priority_id").cast(pl.Int8, strict=False),

            pl.col("start_station_name").cast(pl.String),
            pl.col("bike_model").cast(pl.String),

            pl.lit(datetime.now(timezone.utc)).cast(pl.Datetime("ms")).alias("_ingested_at"),
        )
        .drop("duration_ms")
        .with_columns(
            pl.lit(filename_source).alias("filename_source"),
            pl.lit(filename_parquet).alias("filename_parquet"),
        )
    )

    out = PARQUET_OUT_DIR / f"{filename_parquet}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    cave.collect().write_parquet(out)
    print(f"  Saved {out} ({record_count} rows)")

    return record_count


# ══════════════════════════════════════════════════════════════════════
# Main: resolve the root's contained files, process each, update metadata
# ══════════════════════════════════════════════════════════════════════

parser = argparse.ArgumentParser()
parser.add_argument("--root-file", type=str, required=True)
args = parser.parse_args()

sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
creds   = service_account.Credentials.from_service_account_info(sa_info)
client  = bigquery.Client(project=PROJECT, credentials=creds, location=LOCATION)

meta_rows = [
    (r.file_type, r.filename_source, r.filename_parquet)
    for r in client.query(f"""
        SELECT file_type, filename_source, filename_parquet
        FROM `{PROJECT}.source.metadata`
        WHERE root_file = '{args.root_file}'
    """).result()
]

if not meta_rows:
    raise ValueError(f"No metadata entry for root_file={args.root_file!r}")

# Where does each contained file live on disk?
#   Direct CSV/XLSX: source/new/<root_file>
#   From zip:        source/new/<root_stem>/<filename_source>
file_type = meta_rows[0][0]
is_zip = file_type == "zip"
extract_dir = SOURCE_NEW / Path(args.root_file).stem if is_zip else None

updates: dict[str, int] = {}
for _, filename_source, filename_parquet in meta_rows:
    csv_path = (extract_dir / filename_source) if is_zip else (SOURCE_NEW / filename_source)

    if not csv_path.exists():
        print(f"  SKIP (missing on disk): {csv_path}")
        continue

    updates[filename_source] = process_one(csv_path, filename_parquet)

    # -- Delete source file --
    csv_path.unlink()
    print(f"  Deleted → {csv_path}")

# -- Remove the empty extract dir (housekeeping) --
if is_zip and extract_dir.exists() and not any(extract_dir.iterdir()):
    extract_dir.rmdir()


print(f"Done {args.root_file}: {len(updates)} file(s) processed")
