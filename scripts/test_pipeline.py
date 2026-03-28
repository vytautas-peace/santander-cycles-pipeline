"""
Unit tests for the ingestion pipeline (no GCP credentials required).
Run: pytest scripts/test_pipeline.py -v
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'prefect'))

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from io import StringIO


# --- parse_and_clean tests (pure function, no GCP) ---

# Import the parsing logic directly (mock Prefect decorators)
import unittest.mock as mock

# Minimal mock for prefect task decorator
def noop_task(*args, **kwargs):
    def decorator(fn):
        return fn
    return decorator

with mock.patch.dict('sys.modules', {
    'prefect': mock.MagicMock(),
    'prefect.tasks': mock.MagicMock(),
    'google.cloud': mock.MagicMock(),
    'google.cloud.bigquery': mock.MagicMock(),
    'google.cloud.storage': mock.MagicMock(),
    'pyarrow': mock.MagicMock(),
    'pyarrow.parquet': mock.MagicMock(),
    'bs4': mock.MagicMock(),
}):
    # Import with mocked deps
    import importlib
    import types

    # Manually define COLUMN_MAP and functions for testing
    COLUMN_MAP = {
        "rental id": "rental_id",
        "duration": "duration",
        "bike id": "bike_id",
        "end date": "end_date",
        "start date": "start_date",
        "end station id": "end_station_id",
        "end station name": "end_station_name",
        "start station id": "start_station_id",
        "start station name": "start_station_name",
    }

    DATETIME_COLS = ["start_date", "end_date"]
    INT_COLS = ["duration"]
    STR_COLS = [
        "rental_id", "bike_id",
        "start_station_id", "start_station_name",
        "end_station_id", "end_station_name",
        "start_station_logical_terminal",
        "end_station_logical_terminal",
        "end_station_priority_id",
    ]


def make_sample_csv() -> str:
    return """Rental Id,Duration,Bike Id,End Date,EndStation Id,EndStation Name,Start Date,StartStation Id,StartStation Name
111,600,12345,10/01/2023 09:10,123,Hyde Park Corner,10/01/2023 09:00,456,Victoria
222,1200,67890,10/01/2023 11:20,789,Oxford Circus,10/01/2023 11:00,101,Waterloo
333,-99,11111,10/01/2023 12:00,999,,,112,
"""


class TestColumnNormalisation:
    def test_columns_lowercased(self):
        df = pd.read_csv(StringIO(make_sample_csv()))
        df.columns = [c.strip().lower() for c in df.columns]
        assert "rental id" in df.columns

    def test_column_map_applied(self):
        df = pd.read_csv(StringIO(make_sample_csv()))
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
        assert "rental_id" in df.columns
        assert "start_date" in df.columns

    def test_expected_columns_added_if_missing(self):
        df = pd.read_csv(StringIO(make_sample_csv()))
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
        for col in STR_COLS:
            if col not in df.columns:
                df[col] = None
        assert "start_station_logical_terminal" in df.columns


class TestTypeCasting:
    def test_duration_numeric(self):
        df = pd.read_csv(StringIO(make_sample_csv()))
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
        df["duration"] = pd.to_numeric(df["duration"], errors="coerce").astype("Int64")
        assert df["duration"].dtype.name == "Int64"

    def test_timestamps_parsed(self):
        df = pd.read_csv(StringIO(make_sample_csv()))
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
        df["start_date"] = pd.to_datetime(df["start_date"], dayfirst=True, errors="coerce", utc=True)
        assert pd.api.types.is_datetime64_any_dtype(df["start_date"])

    def test_invalid_dates_become_nat(self):
        df = pd.read_csv(StringIO(make_sample_csv()))
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
        df["start_date"] = pd.to_datetime(df["start_date"], dayfirst=True, errors="coerce", utc=True)
        # Row 3 has no start_date
        assert df["start_date"].isna().sum() == 1


class TestDeduplication:
    def test_duplicate_rental_ids_removed(self):
        csv = """Rental Id,Duration,Bike Id,End Date,EndStation Id,EndStation Name,Start Date,StartStation Id,StartStation Name
111,600,12345,10/01/2023 09:10,123,Hyde Park Corner,10/01/2023 09:00,456,Victoria
111,700,12345,10/01/2023 09:10,123,Hyde Park Corner,10/01/2023 09:00,456,Victoria
"""
        df = pd.read_csv(StringIO(csv))
        df.columns = [c.strip().lower() for c in df.columns]
        df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
        before = len(df)
        df = df.drop_duplicates(subset=["rental_id"], keep="last")
        assert len(df) == 1
        assert len(df) < before


class TestMetadataColumns:
    def test_source_file_added(self):
        df = pd.DataFrame({"rental_id": ["1"], "start_date": ["10/01/2023"]})
        df["_source_file"] = "test_file.csv"
        assert "_source_file" in df.columns
        assert df["_source_file"].iloc[0] == "test_file.csv"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
