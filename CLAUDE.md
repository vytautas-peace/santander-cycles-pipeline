# Santander Cycles Data Pipeline — Project Handover

This file is for Claude Code to pick up the project from the root directory.

---

## Project Overview

End-to-end data pipeline ingesting TfL Santander Cycles journey data (2012–2026) into BigQuery, with staging transforms and a Streamlit dashboard.

**Stack:** Terraform (GCP infra) · Bruin (orchestration + ingestion) · BigQuery (raw + staging + mart) · Streamlit (dashboard)

---

## Directory Structure

```
santander-cycles-pipeline/
├── .env                          # Environment variables (never commit)
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
│       ├── journeys_raw.py       # Ingestion asset (Python)
│       └── journeys_stg.py       # Staging asset (SQL)
└── dashboard/
    └── app.py                    # Streamlit dashboard
```

---

## Environment Variables (.env)

```bash
export GCP_PROJECT=san-cycles-data-pipe
export GCS_BKT=san-cycles-data-pipe-bkt
export BQ_DS_RAW=san_cycles_raw
export BQ_TBL_RAW=journeys_raw
export BQ_DS_STG=san_cycles_stg
export BQ_DS_MRT=san_cycles_mrt
export LOCATION=asia-southeast1
export YEARS="2024"
export GOOGLE_APPLICATION_CREDENTIALS=keys/gcp-sa-key.json
export TF_VAR_credentials=keys/terraform-sa-key.json
export TF_VAR_project_id=san-cycles-data-pipe
export TF_VAR_location=asia-southeast1
export TF_VAR_environment=dev
```

**Important:** All variables must have `export` prefix for Bruin to pick them up. Source with `source .env` before running Bruin commands directly.

---

## GCP Infrastructure

**Project:** `san-cycles-data-pipe`

**Service Accounts:**
- `santander-cycles-pipeline@san-cycles-data-pipe.iam.gserviceaccount.com` — pipeline SA, used by Bruin. Roles: `bigquery.admin`, `storage.admin`
- `terraform@san-cycles-data-pipe.iam.gserviceaccount.com` — Terraform SA. Roles: `bigquery.dataOwner`, `storage.admin`, `iam.serviceAccountAdmin`, `iam.serviceAccountKeyAdmin`, `resourcemanager.projectIamAdmin`

**BigQuery Datasets:**
- `san_cycles_raw` — raw append-only journey data
- `san_cycles_stg` — cleaned staging table
- `san_cycles_mrt` — mart tables (not yet built)

**GCS Bucket:** `san-cycles-data-pipe-bkt` (asia-southeast1, STANDARD, versioned)

**Terraform commands:**
```bash
make infra-plan
make infra-apply
make infra-destroy
```

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


## Schema Variants (9 formats across years)

TfL changed column names multiple times. All are handled via `COLUMN_MAP` in `journeys_raw.py`.

| Schema | Key columns |
|--------|-------------|
| 1 | `Rental Id, Duration, Bike Id, End Date, EndStation Id, EndStation Name, Start Date, StartStation Id, StartStation Name` |
| 2 | `Rental Id, Duration, Bike Id, End Date, EndStation Logical Terminal, EndStation Name, endStationPriority_id, Start Date, StartStation Logical Terminal, StartStation Name` |
| 3 | `Rental Id, Duration, Bike Id, End Date, EndStation Name, Start Date, StartStation Id, StartStation Name` (missing EndStation Id) |
| 4 | `Rental Id, Duration_Seconds, Bike Id, End Date, End Station Id, End Station Name, Start Date, Start Station Id, Start Station Name` |
| 5 | `Number, Start date, Start station number, Start station, End date, End station number, End station, Bike number, Bike model, Total duration, Total duration (ms)` |
| 6 | `Number, Start date, Start station, Start station number, End date, End station, End station number, Bike number, Bike model, Total duration, Total duration (ms)` |
| 7 | `Number, Start date, Start station number, End date, End station, End station number, Start station, Bike number, Bike model, Total duration, Total duration (ms)` |

Schemas 1–4 use `dd/mm/yyyy` or `dd/mm/yyyy hh:mm:ss` date formats. Schemas 5–7 use `yyyy-mm-dd hh:mm` or `yyyy-mm-dd hh:mm:ss`. The key distinctions: schemas 2 and 3 are edge cases in early data where station ID columns are missing or renamed to logical terminal format; schemas 5–7 are the post-2022 new format with bike model and total duration fields, differing only in column order.

**Date formats (4 variants):**
- `yyyy-mm-dd hh:mm:ss`
- `yyyy-mm-dd hh:mm`
- `dd/mm/yyyy hh:mm:ss`
- `dd/mm/yyyy hh:mm`

All handled by trying each format sequentially with `fillna` chaining in `parse_and_clean`.

---

## BigQuery Schema (journeys_raw)

| Column | Type | Notes |
|--------|------|-------|
| `rental_id` | INTEGER | Primary identifier |
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

**`pipeline.yml` variables:**
```yaml
variables:
  years:
    type: string
    default: ""
```

This variable setting doesn't seem to be working as intended. Setting years="2021 2022 2023", Bruin picked up only the first year. So in reality, an array is being passed.


**Running ingestion:**
```bash
# Single year
bruin run bruin/assets/journeys_raw.py --var years='[2024]'

# Multiple years
bruin run bruin/assets/journeys_raw.py --var years='[2022,2023,2024]'

# Via Makefile
make bruin-ingest   # uses YEARS from .env
make bruin-stg      # runs staging asset
```

**Year array syntax:** must be JSON array e.g. `'[2023]'` not `"2023"`.

---

## Ingestion Logic (journeys_raw.py)

**`materialize()` returns a concatenated DataFrame** — Bruin handles BQ loading via ingestr/dlt with `strategy: append`.

**Key business logic:**
- Reads all CSVs as `dtype=str` first, then casts explicitly
- `COLUMN_MAP` normalises all column name variants
- Duration: prefers `duration_ms` (ms→seconds), falls back to `duration`
- Deduplication on `rental_id` within each file (keep last)
- `_dataframe_for_bruin_upload()` converts pandas `Int64` → numpy `int64` with `fillna(0)` to avoid Arrow serialization errors

**Local cache:** Downloaded files cached at `/tmp/tfl/`. Delete to force re-download.

---

## Staging Logic (journeys_stg.py) (work in progress)

**Type:** `bq.sql`, `strategy: create+replace`
**Depends on:** `san_cycles_raw.journeys_raw`

**Transforms:**
- `TIMESTAMP_TRUNC` on dates
- Derived fields: `ride_date`, `ride_month`, `start_hour`, `start_day_of_week`, `is_round_trip`
- `QUALIFY ROW_NUMBER()` deduplication (keep latest `_ingested_at` per `rental_id`)
- Filters: `start_date IS NOT NULL`, `end_date IS NOT NULL`, `duration IS NOT NULL`, `duration > 0`

**Quality checks:**
- `rental_id`: not_null, unique
- `start_date_utc`: not_null
- `duration_seconds`: not_null, positive
- `start_station_id`: not_null ⚠️ currently failing (~229k nulls from pre-fix 2016 ingestion)
- `end_station_id`: not_null ⚠️ currently failing (~543k nulls from pre-fix 2016/2022 ingestion)

---

## Data Status

Currently ingesting 2021-2025.

The last time data was in BigQuery, it showed up like this:

| Year | Raw Status | Notes |
|------|------------|-------|
| 2012 | ✅ | |
| 2013 | ✅ | |
| 2014 | ✅ | |
| 2015 | ✅ | |
| 2016 | ⚠️ | Re-ingested but 229k null start_station_id remain from pre-fix rows |
| 2017 | ⚠️ | Some null end_station_id from boundary files |
| 2018–2021 | ✅ | |
| 2022 | ⚠️ | 312k null end_station_id in file 325 (schema transition) |
| 2023 | ✅ | Re-ingested with 4-format date parser |
| 2024–2025 | ✅ | |

**Pending:** 323,991 rows with null `start_date` in raw — source files not yet identified. Run:
```sql
SELECT _source_file, COUNT(*) as n
FROM `san-cycles-data-pipe.san_cycles_raw.journeys_raw`
WHERE start_date IS NULL
GROUP BY 1 ORDER BY n DESC LIMIT 10
```

**Root cause of most nulls:** rows with null `start_date` survived the year-based DELETE because `DATE_TRUNC(NULL, YEAR)` returns NULL, not the target year.

After this, the journeys_raw table was truncated for fresh ingestion.

---

## Next Steps

1. **Work through any ingestion issues** until all years flush out error-free into BQ dataset.
2. **Re-ingest some data** to start workinng on journeys_stg
3. **Get staging to 7/7 checks passing**
4. **Build mart assets** — equivalent to: `fct_rides`, `dim_stations`, `monthly_summary`, `station_stats`, `bike_stats`, `yearly_stats`
5. **Update Streamlit dashboard** to read from `san_cycles_mrt`
6. **Schedule** weekly Bruin run for new TfL files
7. **Run the pipeline in GCP VM** to trouble-shoot fresh install / deployment issues. I need the project to run smoothly so that it works for fellow data engineers in the course.

---

## Makefile Commands

```bash
make infra-plan        # Terraform plan
make infra-apply       # Terraform apply
make infra-destroy     # Terraform destroy
make bruin-ingest      # Run ingestion (uses YEARS from .env)
make bruin-stg         # Run staging asset
make bruin-run         # Run full pipeline
make stream-dash       # Launch Streamlit dashboard
```

---

## Known Issues / Gotchas

- **Bruin doesn't expand env vars from `.env`** — must `source .env` first or use `export` prefix on all vars.
- **`GOOGLE_APPLICATION_CREDENTIALS` must be relative path** for Bruin (`keys/gcp-sa-key.json` not absolute)
- **Terraform uses absolute path** for credentials via `TF_VAR_credentials`
- **`Int64` dtype breaks Arrow serialization** — `_dataframe_for_bruin_upload()` converts to `int64` with `fillna(0)`
- **Cross-year files** (e.g. `38JourneyDataExtract28Dec2016-03Jan2017.csv`) appear in both 2016 zip and 2017 CSV run — staging deduplication handles this
- **File 335 onwards (Sep 2022)** uses new schema with `Number`, `Start date`, `Total duration (ms)` etc.
- **`duration_matches_timestamps` quality check** references wrong table name in SQL — needs fixing to `san_cycles_stg.journeys_stg`
