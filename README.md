# 🚲 Santander Cycles Analytics Pipeline

An end-to-end batch data pipeline that ingests **TfL Santander Cycles** journey data from the [cycling.data.tfl.gov.uk](https://cycling.data.tfl.gov.uk) open data portal, transforms it in BigQuery, and surfaces insights in a Looker Studio dashboard.

---

## Problem Statement

TfL publishes weekly CSV files of every Santander Cycles journey in London — over 100 million rows spanning 10+ years. The raw data is:

- **Messy**: inconsistent column names across years, mixed encodings, duplicate records, invalid durations
- **Fragmented**: hundreds of individual CSV files with no aggregation
- **Unanalysable at scale**: direct CSV analysis is slow and error-prone

This pipeline solves those problems by building a production-grade data warehouse that answers:

1. **How has cycling demand changed over time?** (seasonality, COVID impact, growth trends)
2. **Which stations are the busiest?** (origin-destination analysis, geographic distribution)
3. **What does a typical ride look like?** (duration distributions, round-trip rates)

---

## Architecture

```
TfL Website          Orchestration        Data Lake            Warehouse         Dashboard
──────────           ─────────────        ─────────            ─────────         ─────────
CSV files    ──►    Prefect DAG   ──►    GCS (Parquet)  ──►  BigQuery    ──►  Looker Studio
(weekly)            (batch)              raw/ prefix          dbt models        2 tiles
```

**Technology choices:**

| Layer | Tool | Why |
|---|---|---|
| IaC | Terraform | Reproducible GCP provisioning; state managed in code |
| Orchestration | Prefect | Python-native DAGs, built-in retries, Prefect Cloud scheduling |
| Data Lake | Google Cloud Storage | Cheap durable Parquet storage; direct BQ external table support |
| Warehouse | BigQuery | Serverless, columnar, handles 100M+ rows efficiently; partitioning + clustering |
| Transformation | dbt | SQL-based, version-controlled models; built-in testing; lineage |
| Dashboard | Looker Studio | Free, native BigQuery connector, shareable |
| CI/CD | GitHub Actions | Automated lint, compile, and deploy on push |

---

## Repository Structure

```
santander-cycles-pipeline/
├── terraform/               # GCP infrastructure (GCS, BigQuery, SA)
│   ├── main.tf
│   └── variables.tf
├── prefect/                 # Batch ingestion pipeline
│   ├── pipeline.py          # Main Prefect flow (5-step DAG)
│   ├── schedule.py          # Prefect Cloud deployment + weekly schedule
│   └── requirements.txt
├── dbt/                     # SQL transformations
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_rides.sql         # Clean + type raw data
│   │   └── marts/
│   │       ├── fct_rides.sql         # Fact table (partitioned + clustered)
│   │       ├── dim_stations.sql      # Station dimension
│   │       ├── mart_monthly_summary.sql  # Dashboard Tile 1
│   │       └── mart_station_stats.sql    # Dashboard Tile 2
│   ├── tests/               # Custom dbt data tests
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── packages.yml
├── scripts/
│   ├── test_pipeline.py     # Unit tests (pytest)
│   └── looker_studio_setup.md
├── .github/workflows/
│   └── ci.yml               # CI: lint → dbt compile → deploy
├── Makefile                 # All commands in one place
├── .env.example
└── README.md
```

---

## Data Model

### Partitioning & Clustering Strategy

**`fct_rides`** — the central fact table:
- **Partitioned by `ride_month` (DATE, monthly granularity)**
  Dashboards and queries almost always filter by date range. Monthly partitioning means a query for "rides in 2023" only scans 12 partitions instead of the entire table. At ~10M rows/month, this reduces query costs by ~90%.
- **Clustered by `[start_station_id, end_station_id]`**
  Origin-destination queries (e.g. "all rides from station X to station Y") are the second most common access pattern. Clustering sorts data within each partition by these fields, enabling BigQuery to skip irrelevant blocks without a full partition scan.

**`mart_monthly_summary`** — dashboard pre-aggregation:
- **Partitioned by `ride_month`**
  Looker Studio date range filters translate directly to partition pruning.

**`dim_stations`** — station lookup:
- **Clustered by `station_id`**
  Geographic filtering (e.g. "all stations in Hackney") is the primary access pattern. No partitioning needed — the table is small (~800 rows).

**`mart_station_stats`** — station aggregations:
- **Clustered by `station_id`**
  Same rationale as `dim_stations`.

### dbt Lineage

```
raw_rides (BigQuery)
    └── stg_rides (VIEW)
            ├── fct_rides (TABLE, partitioned)
            │       ├── mart_monthly_summary (TABLE, partitioned)  ← Dashboard Tile 1
            │       └── mart_station_stats (TABLE, clustered)      ← Dashboard Tile 2
            └── dim_stations (TABLE, clustered)
                    └── mart_station_stats
```

---

## Prerequisites

- GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth login`)
- Terraform ≥ 1.3
- `uv` (replaces pip/virtualenv — one tool for everything)
- `make`

Install uv if you don't have it:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# restart your terminal after
```

---

## Quick Start

### 1. Install uv (if you haven't already)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# restart your terminal
```

### 2. Clone and configure

```bash
git clone https://github.com/your-username/santander-cycles-pipeline.git
cd santander-cycles-pipeline
make setup        # creates .env from template and runs uv sync
# Edit .env — fill in GCP_PROJECT (the rest comes from Terraform output)
```

### 2. Provision GCP infrastructure

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars with your project_id
make infra-apply
```

This creates:
- GCS bucket: `<project>-santander-cycles-lake`
- BigQuery datasets: `santander_cycles_raw`, `santander_cycles_staging`, `santander_cycles_mart`
- Service account with BigQuery Admin + Storage Admin roles
- SA key written to `keys/gcp-sa-key.json`

### 3. Run the ingestion pipeline (smoke test — 3 files)

```bash
make run-pipeline-test
```

### 4. Run the full historical ingestion

```bash
make run-pipeline   # downloads all 300+ CSV files (~10–20 min)
```

### 5. Run dbt transformations

```bash
make dbt-deps
make dbt-run
make dbt-test
```

### 6. Set up the dashboard

See [`scripts/looker_studio_setup.md`](scripts/looker_studio_setup.md) for step-by-step Looker Studio configuration.

### 7. Schedule weekly ingestion (optional)

```bash
# Set PREFECT_API_URL and PREFECT_API_KEY in .env first
make register-schedule
```

---

## Running Tests

```bash
make test    # Python unit tests (no GCP needed)
make lint    # Ruff linting
make dbt-test  # dbt schema + custom tests (requires GCP)
```

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `GCP_PROJECT` | Yes | GCP project ID |
| `GCS_BUCKET` | Yes | GCS bucket name (output of `make infra-apply`) |
| `GOOGLE_APPLICATION_CREDENTIALS` | Yes | Path to SA key JSON |
| `BQ_DATASET_RAW` | No | Defaults to `santander_cycles_raw` |
| `PREFECT_API_URL` | No | Prefect Cloud workspace URL |
| `PREFECT_API_KEY` | No | Prefect Cloud API key |

---

## Dashboard

The Looker Studio dashboard includes two tiles:

**Tile 1 — Rides Over Time**
Monthly ride count and average duration from 2012 to present. Shows seasonal peaks (summer), COVID-19 dip in 2020, and long-term growth trend.

**Tile 2 — Top Stations**
Horizontal bar chart of the 20 busiest docking stations, coloured by London borough. Reveals that central/Zone 1 stations (Waterloo, Hyde Park Corner, King's Cross) dominate activity.

---

## CI/CD

GitHub Actions runs on every push to `main` or `develop`:

1. **Lint** — `ruff check prefect/`
2. **Unit tests** — `pytest scripts/test_pipeline.py`
3. **dbt compile** — validates SQL without executing
4. **Deploy** (main only) — registers updated Prefect deployment

Configure secrets in GitHub: `GCP_PROJECT`, `GCS_BUCKET`, `GCP_SA_KEY`, `PREFECT_API_URL`, `PREFECT_API_KEY`.

---

## Data Source

TfL Santander Cycles usage statistics:
- URL: https://cycling.data.tfl.gov.uk
- Licence: [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
- Coverage: 2012 – present, updated weekly
- Format: CSV, ~300+ files, ~100M+ total rows




# Dev notes

## Create a service account for Terraform

1. Login with gcloud auth login.

2. Create Terraform service account.

```shell
gcloud iam service-accounts create terraform \
  --display-name="Terraform SA" \
  --project=san-cycles-data-pipe
```

3. Create and download the key.

```shell
gcloud iam service-accounts keys create \
  keys/terraform-sa-key.json \
  --iam-account=terraform@san-cycles-data-pipe.iam.gserviceaccount.com \
  --project=san-cycles-data-pipe
```

4. Assign permissions to create / destry infrastructure

```shell
for role in \
  roles/bigquery.dataOwner \
  roles/storage.admin \
  roles/iam.serviceAccountAdmin \
  roles/iam.serviceAccountKeyAdmin \
  roles/resourcemanager.projectIamAdmin; do
  gcloud projects add-iam-policy-binding san-cycles-data-pipe \
    --member="serviceAccount:terraform@san-cycles-data-pipe.iam.gserviceaccount.com" \
    --role="$role"
done
```

5. Enable Cloud Resource Manager API.

```shell
gcloud services enable cloudresourcemanager.googleapis.com --project san-cycles-data-pipe
```


## CSV knowledge bank

What we know about the TfL data:

File sources:

- 2012–2016: zip files on S3 (cyclehireusagestats-2012.zip etc, 2016TripDataZip.zip)
- 2017+: individual weekly CSV files on S3
- S3 bucket: s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk
- CDN for downloads: cycling.data.tfl.gov.uk

Column name chaos across years:

- Rental Id / rental id → rental_id
- StartStation Id / Start Station Id / startstationid / startstation id → - start_station_id
- Same mess for end station, bike id, dates
- Start Station Logical Terminal was an alias for station ID in some years
- Duration was seconds in early files, Duration (ms) appeared later alongside a string Duration

Data types:

- All IDs (rental_id, bike_id, start_station_id, end_station_id, end_station_priority_id) → INTEGER
- duration → INTEGER (seconds) early years, then STRING
- start_date, end_date → TIMESTAMP (dayfirst, UTC)
- Station names → STRING
- Early 2012 files have dates like "18  Aug12" (two spaces, 2-digit year)

Business logic:

- Deduplicate on rental_id within each file
- Filter out rides with no start_date
- Duration valid range: fail on records where (end_date - start_date - duration) > 300 seconds
- Zips: extract all CSVs inside, process each individually
- YEARS parameter controls which files are downloaded — zips for 2012–2016, CSVs for 2017+

BigQuery target:

- Dataset: san_cycles_raw
- Table: journeys_raw
- Partitioned by start_date (monthly)
- Clustered by start_station_id


