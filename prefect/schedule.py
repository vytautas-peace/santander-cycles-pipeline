"""
Prefect deployment definition + weekly schedule.
Run: python schedule.py to register the deployment in Prefect Cloud.
"""

import os

from pipeline import ingestion_flow
from prefect.schedules import Cron

GCP_REGION_TIMEZONES = {
    "asia-southeast1":    "Asia/Singapore",
    "asia-southeast2":    "Asia/Jakarta",
    "asia-east1":         "Asia/Taipei",
    "asia-east2":         "Asia/Hong_Kong",
    "asia-northeast1":    "Asia/Tokyo",
    "asia-northeast2":    "Asia/Tokyo",
    "asia-northeast3":    "Asia/Seoul",
    "asia-south1":        "Asia/Kolkata",
    "asia-south2":        "Asia/Kolkata",
    "australia-southeast1": "Australia/Sydney",
    "australia-southeast2": "Australia/Melbourne",
    "europe-west1":       "Europe/Brussels",
    "europe-west2":       "Europe/London",
    "europe-west3":       "Europe/Berlin",
    "europe-west4":       "Europe/Amsterdam",
    "europe-west6":       "Europe/Zurich",
    "europe-north1":      "Europe/Helsinki",
    "europe-central2":    "Europe/Warsaw",
    "us-central1":        "America/Chicago",
    "us-east1":           "America/New_York",
    "us-east4":           "America/New_York",
    "us-west1":           "America/Los_Angeles",
    "us-west2":           "America/Los_Angeles",
    "northamerica-northeast1": "America/Toronto",
    "southamerica-east1": "America/Sao_Paulo",
}

if __name__ == "__main__":
    location = os.environ["LOCATION"]
    timezone = GCP_REGION_TIMEZONES.get(location)
    if not timezone:
        raise ValueError(
            f"No timezone mapping for LOCATION={location}. "
            "Add it to GCP_REGION_TIMEZONES in schedule.py."
        )
    ingestion_flow.serve(
        name="weekly-ingestion",
        schedules=[Cron("0 3 * * 1", timezone=timezone)],
        parameters={"file_limit": None, "tmp_dir": "/tmp/tfl"},
        tags=["santander-cycles", "ingestion"],
    )
