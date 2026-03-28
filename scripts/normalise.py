"""
scripts/normalise.py
────────────────────
Handles TfL Santander Cycles CSV schema drift across all historical files.

TfL changed the schema multiple times:
  Era 1 (2015–2017):  Rental Id, Duration, Bike Id, End Date, EndStation Id,
                       EndStation Name, Start Date, StartStation Id, StartStation Name
  Era 2 (2018–2021):  Duration_Seconds added (renamed from Duration), Total Duration
  Era 3 (2022+):      Bike model column added, further column renames

This module detects the era from the header and normalises to a canonical schema.
"""
from __future__ import annotations

import re
import io
import logging
from datetime import datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ─── Canonical output schema ─────────────────────────────────────────────────
CANONICAL_COLUMNS = [
    "journey_id",
    "duration_seconds",
    "bike_id",
    "end_datetime",
    "end_station_id",
    "end_station_name",
    "start_datetime",
    "start_station_id",
    "start_station_name",
    "bike_model",       # Nullable for older records
]

# ─── Column alias map (normalise all known variants → canonical name) ─────────
COLUMN_ALIASES: dict[str, str] = {
    # journey_id
    "rental id":            "journey_id",
    "number":               "journey_id",
    "rentalid":             "journey_id",

    # duration_seconds
    "duration":             "duration_seconds",
    "duration_seconds":     "duration_seconds",
    "total duration":       "duration_seconds",
    "total_duration":       "duration_seconds",

    # bike_id
    "bike id":              "bike_id",
    "bikeid":               "bike_id",
    "bike number":          "bike_id",
    "bike_id":              "bike_id",

    # end_datetime
    "end date":             "end_datetime",
    "end_date":             "end_datetime",
    "enddate":              "end_datetime",

    # end_station_id
    "endstation id":        "end_station_id",
    "end station id":       "end_station_id",
    "endstationid":         "end_station_id",
    "end_station_id":       "end_station_id",

    # end_station_name
    "endstation name":      "end_station_name",
    "end station name":     "end_station_name",
    "endstationname":       "end_station_name",
    "end_station_name":     "end_station_name",

    # start_datetime
    "start date":           "start_datetime",
    "start_date":           "start_datetime",
    "startdate":            "start_datetime",

    # start_station_id
    "startstation id":      "start_station_id",
    "start station id":     "start_station_id",
    "startstationid":       "start_station_id",
    "start_station_id":     "start_station_id",

    # start_station_name
    "startstation name":    "start_station_name",
    "start station name":   "start_station_name",
    "startstationname":     "start_station_name",
    "start_station_name":   "start_station_name",

    # bike_model (era 3+)
    "bike model":           "bike_model",
    "bike_model":           "bike_model",
    "bikemodel":            "bike_model",
}

# ─── Date format patterns TfL has used ───────────────────────────────────────
DATE_FORMATS = [
    "%d/%m/%Y %H:%M",       # 01/01/2023 08:30
    "%d/%m/%Y %H:%M:%S",    # 01/01/2023 08:30:00
    "%Y-%m-%d %H:%M:%S",    # 2023-01-01 08:30:00
    "%d %b %Y %H:%M",       # 01 Jan 2023 08:30
    "%d-%m-%Y %H:%M",       # 01-01-2023 08:30
]


def _parse_datetime(series: pd.Series) -> pd.Series:
    """Try multiple date formats; return NaT for unparseable values."""
    result = pd.Series([pd.NaT] * len(series), index=series.index)
    remaining = series.copy()

    for fmt in DATE_FORMATS:
        mask = result.isna() & remaining.notna()
        if not mask.any():
            break
        try:
            parsed = pd.to_datetime(remaining[mask], format=fmt, errors="coerce")
            result[mask] = parsed
        except Exception:
            pass

    # Final fallback — let pandas guess
    still_missing = result.isna() & remaining.notna()
    if still_missing.any():
        result[still_missing] = pd.to_datetime(
            remaining[still_missing], infer_datetime_format=True, errors="coerce"
        )

    return result


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all columns to canonical names using the alias map."""
    rename_map: dict[str, str] = {}
    for col in df.columns:
        normalised = col.strip().lower().replace("_", " ")
        canonical = COLUMN_ALIASES.get(normalised) or COLUMN_ALIASES.get(col.strip().lower())
        if canonical:
            rename_map[col] = canonical
        else:
            logger.warning("Unknown column '%s' — dropping", col)

    df = df.rename(columns=rename_map)
    # Keep only canonical columns present in this file
    present = [c for c in CANONICAL_COLUMNS if c in df.columns]
    df = df[present]
    return df


def _add_missing_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add canonical columns that are absent in older schema eras."""
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[CANONICAL_COLUMNS]


def _clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce numeric columns, strip stray quotes and whitespace."""
    for col in ["journey_id", "duration_seconds", "bike_id", "start_station_id", "end_station_id"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.replace(r"[\"']", "", regex=True)
                .pipe(pd.to_numeric, errors="coerce")
            )
    return df


def _drop_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that are clearly bad data."""
    n_before = len(df)
    # Must have a start datetime
    df = df[df["start_datetime"].notna()]
    # Duration must be positive (or missing for era 3 where it's sometimes omitted)
    mask_dur = df["duration_seconds"].notna()
    df = df[~mask_dur | (df.loc[mask_dur, "duration_seconds"] > 0)]
    n_after = len(df)
    if n_before != n_after:
        logger.info("Dropped %d invalid rows (%d → %d)", n_before - n_after, n_before, n_after)
    return df


def normalise_csv(raw_bytes: bytes, filename: str = "") -> pd.DataFrame:
    """
    Parse a raw TfL cycles CSV (any schema era) and return a normalised DataFrame.

    Parameters
    ----------
    raw_bytes : bytes
        Raw content of the CSV file
    filename : str
        Original filename (used for logging)

    Returns
    -------
    pd.DataFrame with CANONICAL_COLUMNS schema
    """
    logger.info("Normalising file: %s (%d bytes)", filename, len(raw_bytes))

    # ── 1. Detect encoding ──────────────────────────────────────────────────
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1")

    # ── 2. Load CSV, try multiple separators ────────────────────────────────
    for sep in [",", ";"]:
        try:
            df = pd.read_csv(io.StringIO(text), sep=sep, low_memory=False, dtype=str)
            if df.shape[1] > 3:
                break
        except Exception as exc:
            logger.warning("Failed to parse with sep='%s': %s", sep, exc)
    else:
        raise ValueError(f"Could not parse CSV: {filename}")

    logger.info("Raw shape: %s, columns: %s", df.shape, list(df.columns))

    # ── 3. Normalise columns ─────────────────────────────────────────────────
    df = _normalise_columns(df)
    df = _add_missing_columns(df)

    # ── 4. Parse datetimes ───────────────────────────────────────────────────
    for dt_col in ["start_datetime", "end_datetime"]:
        df[dt_col] = _parse_datetime(df[dt_col])

    # ── 5. Clean numerics ────────────────────────────────────────────────────
    df = _clean_numeric(df)

    # ── 6. Clean strings ─────────────────────────────────────────────────────
    for str_col in ["start_station_name", "end_station_name", "bike_model"]:
        if str_col in df.columns:
            df[str_col] = df[str_col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
            df[str_col] = df[str_col].where(df[str_col].str.len() > 0, pd.NA)

    # ── 7. Add derived columns ───────────────────────────────────────────────
    df["start_date"] = df["start_datetime"].dt.date
    df["start_year"]  = df["start_datetime"].dt.year
    df["start_month"] = df["start_datetime"].dt.month
    df["source_file"] = filename

    # ── 8. Drop invalid ──────────────────────────────────────────────────────
    df = _drop_invalid_rows(df)

    logger.info("Normalised shape: %s", df.shape)
    return df


def normalise_csv_file(path: str) -> pd.DataFrame:
    """Convenience wrapper to normalise a local CSV file path."""
    with open(path, "rb") as f:
        return normalise_csv(f.read(), filename=path)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 2:
        print("Usage: python normalise.py <csv_file>")
        sys.exit(1)
    df = normalise_csv_file(sys.argv[1])
    print(df.dtypes)
    print(df.head())
    print(f"\nTotal rows: {len(df)}")
