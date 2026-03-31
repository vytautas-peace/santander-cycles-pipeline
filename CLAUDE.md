# Santander Cycles Data Pipeline — Project Handover

This file is for Claude Code to pick up the project from the root directory.

---

## Project Overview

End-to-end data pipeline ingesting TfL Santander Cycles journey data (2012–2025) into BigQuery, with staging transforms and a Streamlit dashboard.

**Stack:** Terraform (GCP infra) · Bruin (orchestration + ingestion) · BigQuery (ing + stg + mrt layers) · Streamlit (dashboard)

---

## Directory Structure

```
santander-cycles-pipeline/
├── .env                          # Environment variables (never commit)
├── .env.init                     # Template — copied to .env on setup
├── .bruin.yml                    # Bruin connection config (never commit)
├── .bruin.yml.init               # Template — copied to .bruin.yml on setup
├── .python-version               # Pins Python to 3.11 — must match Bruin's interpreter
├── .streamlit/
│   └── config.toml               # Streamlit theme + toolbar config
├── Makefile                      # All runnable commands
├── keys/
│   ├── gcp-sa-key.json           # Pipeline SA key (GOOGLE_APPLICATION_CREDENTIALS)
│   └── terraform-sa-key.json     # Terraform SA key (TF_VAR_credentials)
├── terraform/
│   ├── main.tf                   # GCP resources
│   └── variables.tf              # Terraform variables
├── bruin/
│   ├── .bruin.yml                # Bruin connection config
│   ├── pipeline.yml              # Pipeline definition + variables
│   └── assets/
│       ├── ing_journeys.py       # Ingestion asset (Python) → san_cycles_ing.journeys
│       ├── stg_journeys.py       # Staging asset (SQL)     → san_cycles_stg.journeys
│       ├── mrt_dim_stations.py
│       ├── mrt_fct_journeys.py
│       ├── mrt_station_stats.py
│       ├── mrt_bike_stats.py
│       └── mrt_kpis_monthly.py
└── streamlit/
    ├── app.py                    # Streamlit dashboard
    └── assets/
        └── santander_logo.svg    # Santander flame logo (extracted from santander.co.uk)
```

---

## Environment Variables (.env)

```bash
# User configuration
export GCP_PROJECT=san-cycles-data-pipe
export LOCATION=asia-southeast1
export YEARS="[2021,2022,2023,2024,2025]"
export TF_VAR_credentials=../keys/terraform-sa-key.json

# Pre-set — don't change
export GCS_BKT="${GCP_PROJECT}-bkt"    # derives from GCP_PROJECT
export BQ_DS_ING=san_cycles_ing
export BQ_DS_STG=san_cycles_stg
export BQ_DS_MRT=san_cycles_mrt
export BQ_TBL_ING=journeys

# Relative path required for Bruin (absolute paths break)
export GOOGLE_APPLICATION_CREDENTIALS=keys/gcp-sa-key.json
```

**Important:** All vars must have `export` prefix for Bruin. Source with `source .env` before running Bruin commands directly.

---

## GCP Infrastructure

**Project:** `san-cycles-data-pipe`

**Service Accounts:**
- `santander-cycles-pipeline@san-cycles-data-pipe.iam.gserviceaccount.com` — pipeline SA, used by Bruin. Roles: `bigquery.admin`, `storage.admin`
- `terraform@san-cycles-data-pipe.iam.gserviceaccount.com` — Terraform SA. Roles: `bigquery.dataOwner`, `storage.admin`, `iam.serviceAccountAdmin`, `iam.serviceAccountKeyAdmin`, `resourcemanager.projectIamAdmin`

**BigQuery Datasets:**
- `san_cycles_ing` — raw append-only journey data (ingestion layer)
- `san_cycles_stg` — cleaned staging table
- `san_cycles_mrt` — mart tables

**GCS Bucket:** `san-cycles-data-pipe-bkt` (asia-southeast1, STANDARD, versioned)

---

## Data Source

**TfL S3 bucket:** `s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk`
**CDN:** `cycling.data.tfl.gov.uk`

**File rules:**
- 2012–2016: zip files (one per year)
- 2017+: weekly CSV files

**Zip paths:**
```python
2012: usage-stats/cyclehireusagestats-2012.zip
2013: usage-stats/cyclehireusagestats-2013.zip
2014: usage-stats/cyclehireusagestats-2014.zip
2015: usage-stats/2015TripDatazip.zip
2016: usage-stats/2016TripDataZip.zip
```

---

## Schema Variants (7 formats across years)

TfL changed column names multiple times. All handled via `COLUMN_MAP` in `ing_journeys.py`.

| Schema | Key columns |
|--------|-------------|
| 1 | `Rental Id, Duration, Bike Id, End Date, EndStation Id, EndStation Name, Start Date, StartStation Id, StartStation Name` |
| 2 | `Rental Id, Duration, Bike Id, End Date, EndStation Logical Terminal, EndStation Name, endStationPriority_id, Start Date, StartStation Logical Terminal, StartStation Name` |
| 3 | `Rental Id, Duration, Bike Id, End Date, EndStation Name, Start Date, StartStation Id, StartStation Name` (missing EndStation Id) |
| 4 | `Rental Id, Duration_Seconds, Bike Id, End Date, End Station Id, End Station Name, Start Date, Start Station Id, Start Station Name` |
| 5 | `Number, Start date, Start station number, Start station, End date, End station number, End station, Bike number, Bike model, Total duration, Total duration (ms)` |
| 6 | `Number, Start date, Start station, Start station number, End date, End station, End station number, Bike number, Bike model, Total duration, Total duration (ms)` |
| 7 | `Number, Start date, Start station number, End date, End station, End station number, Start station, Bike number, Bike model, Total duration, Total duration (ms)` |

Schemas 1–4 use `dd/mm/yyyy` date formats. Schemas 5–7 are post-2022 with bike model and total duration fields.

**Date formats (4 variants):** `yyyy-mm-dd hh:mm:ss`, `yyyy-mm-dd hh:mm`, `dd/mm/yyyy hh:mm:ss`, `dd/mm/yyyy hh:mm`

---

## BigQuery Schema (san_cycles_ing.journeys)

| Column | Type | Notes |
|--------|------|-------|
| `rental_id` | INTEGER | |
| `bike_id` | INTEGER | |
| `start_station_id` | INTEGER | |
| `end_station_id` | INTEGER | |
| `end_station_priority_id` | INTEGER | |
| `start_station_name` | STRING | |
| `end_station_name` | STRING | |
| `total_duration` | STRING | Human-readable e.g. "5m 32s" (2022+ only) |
| `bike_model` | STRING | CLASSIC or PBSC_EBIKE (2022+ only) |
| `start_date` | TIMESTAMP | Partitioned by MONTH |
| `end_date` | TIMESTAMP | |
| `duration` | INTEGER | Seconds |
| `_source_file` | STRING | |
| `_ingested_at` | TIMESTAMP | |

Partitioned by `start_date` (MONTH), clustered by `start_station_id`.

**Current data:** 135M+ rows covering 2012–2025.

---

## Bruin Pipeline

**Connection:** `san_cycles` (defined in `bruin/.bruin.yml`)

**`.bruin.yml`:**
```yaml
default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "san_cycles"
          project_id: ${GCP_PROJECT}
          location: ${LOCATION}
          service_account_file: ${GOOGLE_APPLICATION_CREDENTIALS}
```

**`pipeline.yml`:** schedule `@weekly`, `years` variable declared with `default: "[2023,2024,2025]"`.

**Running ingestion:**
```bash
# Single year
bruin run bruin/assets/ing_journeys.py --var years='[2024]'

# Multiple years
bruin run bruin/assets/ing_journeys.py --var years='[2022,2023,2024]'

# Via Makefile (uses YEARS from .env)
make bruin-ingest   # ingestion only
make bruin-stage    # staging asset
make bruin-marts    # all mart assets
make bruin-run      # full pipeline
```

**Batch size limit:** run up to ~5 years at a time — Bruin loads the full DataFrame in memory before handing to ingestr.

---

## Ingestion Logic (ing_journeys.py)

**`materialize()` returns a concatenated DataFrame** — Bruin handles BQ loading via ingestr/dlt with `strategy: append`.

**Year resolution order:**
1. `--var years=` passed to Bruin (may arrive as list or JSON string — both handled)
2. `$YEARS` environment variable (from `.env`)
3. `None` → ingest all years

**Key business logic:**
- Reads all CSVs as `dtype=str` first, then casts explicitly
- `COLUMN_MAP` normalises all column name variants
- Duration: prefers `duration_ms` (ms→seconds), falls back to `duration`
- Deduplication on `rental_id` within each file (keep last)
- `_dataframe_for_bruin_upload()` converts pandas `Int64` → numpy `int64` with `fillna(0)` to avoid Arrow serialization errors

**Local cache:** Downloaded files cached at `/tmp/tfl/`. Delete to force re-download.

---

## Staging Logic (stg_journeys.py)

**Type:** `bq.sql`, `strategy: create+replace`
**Depends on:** `san_cycles_ing.journeys`

**Transforms:**
- `TIMESTAMP_TRUNC` on dates
- Derived fields: `ride_date`, `journey_month`, `start_hour`, `start_day_of_week`, `is_round_trip`
- `QUALIFY ROW_NUMBER()` deduplication (keep latest `_ingested_at` per `rental_id`)
- Backfills missing `start_station_id` / `end_station_id` via name→id lookup built from all rows where both fields are present
- Filters: `start_date IS NOT NULL`, `end_date IS NOT NULL`, `duration IS NOT NULL`, `duration > 0`
- Drops pre-2012 rows (erroneous 1901–1902 timestamps)
- Drops journeys with no end location at all

**Quality checks:**
- `rental_id`: not_null, unique
- `start_date_utc`: not_null
- `duration_seconds`: not_null, positive
- `start_station_id`: not_null
- `end_station_id`: not_null

---

## Mart Assets

All in `san_cycles_mrt`. Depend on `san_cycles_stg.journeys`.

| Asset | Description |
|-------|-------------|
| `mrt_dim_stations.py` | Dimension of all known stations; most-used name per station_id |
| `mrt_fct_journeys.py` | Fact table of all journeys |
| `mrt_station_stats.py` | Monthly station-level pickups + dropoffs |
| `mrt_bike_stats.py` | Per-bike ride counts and duration by month |
| `mrt_kpis_monthly.py` | Monthly KPIs: total rides, top station, top bike, longest ride |

---

## Streamlit Dashboard

**Run:** `make stream-dash`

**Config:** `.streamlit/config.toml` — light theme, Santander red primary, `toolbarMode = "viewer"` (hides Deploy button)

**Tile 1 — Most loved bike of the year:** year selector, top bike by ride count with total rides + hours metrics

**Tile 2 — Rides by season:** stacked bar chart, years on x-axis, trips on y-axis, stacked Winter/Autumn/Summer/Spring bottom to top

Logo: official Santander flame path extracted from santander.co.uk SVG, embedded as base64 inline.

---

## Makefile Commands

```bash
make setup             # Install uv, bruin + Python deps + copy init files
make infra-plan        # Terraform plan
make infra-apply       # Terraform apply
make infra-destroy     # Terraform destroy
make bruin-ingest      # Run ingestion (uses YEARS from .env)
make bruin-stage       # Run staging asset
make bruin-marts       # Run all mart assets
make bruin-run         # Run full pipeline
make stream-dash       # Launch Streamlit dashboard
make clean             # Remove .venv and /tmp/tfl cache
```

---

## Known Issues / Gotchas

- **Bruin doesn't expand env vars from `.env`** — must `source .env` first or use `export` prefix on all vars
- **`GOOGLE_APPLICATION_CREDENTIALS` must be relative path** for Bruin (`keys/gcp-sa-key.json` not absolute)
- **Terraform uses absolute path** for credentials via `TF_VAR_credentials`
- **`Int64` dtype breaks Arrow serialization** — `_dataframe_for_bruin_upload()` converts to `int64` with `fillna(0)`
- **Cross-year files** (e.g. `38JourneyDataExtract28Dec2016-03Jan2017.csv`) appear in both 2016 zip and 2017 CSV run — staging deduplication handles this
- **File 335 onwards (Sep 2022)** uses new schema with `Number`, `Start date`, `Total duration (ms)` etc.
- **Python version must match between Bruin and uv** — both must use 3.11 (pinned in `.python-version`). Mismatch causes `os error 66` venv conflict. Fix: `rm -rf .venv` and ensure `.python-version` is `3.11`
- **GCP SA key** — if you get `invalid_grant: Invalid JWT Signature`, the key has been revoked. Generate a new one from GCP Console → IAM → Service Accounts → Keys, or via `gcloud iam service-accounts keys create`
- **`--var years` arrives as list or string** depending on how Bruin passes it — `materialize()` handles both

---

## Next Steps

1. **Schedule** — set up weekly Cloud Scheduler or cron trigger for new TfL files
2. **GCS staging layer** — land raw CSVs in GCS before BigQuery for a proper data lake pattern
3. **VM deployment** — test fresh `make setup` on a Linux e2-highmem-4 for reproducibility