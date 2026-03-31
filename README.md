# 🚲 Santander Cycles Analytics Pipeline

An end-to-end batch data pipeline that ingests **TfL Santander Cycles** journey data from the [cycling.data.tfl.gov.uk](https://cycling.data.tfl.gov.uk) open data portal into BigQuery, transforms it through staging and mart layers, and surfaces insights in a Streamlit dashboard.

---

## Problem Statement

TfL publishes weekly CSV files of every Santander Cycles journey in London — over 135 million rows spanning 2012 to present. The raw data is:

- **Messy**: 9 different column name schemas across years, mixed date formats, duplicate records.
- **Fragmented**: hundreds of individual CSV and zip files with no aggregation.
- **Unanalysable at scale**: direct CSV analysis is slow, error-prone and hard to handle >1m rows.

This pipeline solves those problems by building a production-grade data warehouse that answers:

This pipeline shows the beauty of data engineering by:

1. **Keeping it light** - 3 tools only, 2 more for IaC and venv management. All tools are open source. The project could be ported to DuckDB for more lightness.

2. **Working at scale** - reads data from 450+ files with creatively disparate naming schemas and 9 column header schemas, resolving a bunch of errors and typical CSV data issues along the way.

3. **Producing a beautiful dashboard** - credit to Streamlit and Claude Code for tech skills; credit to the heart for the feel and taste.


---

## Architecture

```
TfL Website          Orchestration        Warehouse                  Dashboard
───────────          ─────────────        ─────────                  ─────────
CSV / zip    ──►     Bruin (Python/SQL) ──►   BigQuery                   Streamlit
(weekly)             append strategy      ing → stg → mrt    ──►    (local)
```

**Stack:**

| Layer | Tool | Why |
|---|---|---|
| IaC | Terraform | Reproducible GCP provisioning |
| Orchestration | Bruin | Python-native assets, built-in quality checks, ingestr for BQ loading |
| Warehouse | BigQuery | Serverless, columnar, handles 135M+ rows; partitioning + clustering |
| Dashboard | Streamlit | Python-native, runs locally against BigQuery mart tables |

---

## Repository Structure

```
santander-cycles-pipeline/
├── .env                          # Environment variables (never commit)
├── .env.init                     # Template — copied to .env on setup
├── .bruin.yml                    # Bruin connection config (never commit)
├── .bruin.yml.init               # Template — copied to .bruin.yml on setup
├── .streamlit/
│   └── config.toml               # Streamlit theme config
├── Makefile                      # All runnable commands
├── keys/
│   ├── gcp-sa-key.json           # Pipeline SA key (never commit)
│   └── terraform-sa-key.json     # Terraform SA key (never commit)
├── terraform/
│   ├── main.tf
│   └── variables.tf              # Variables are sourced from .env file
├── bruin/
│   ├── pipeline.yml              # Pipeline definition + variables
│   └── assets/
│       ├── ing_journeys.py       # Ingestion → san_cycles_ing.journeys
│       ├── stg_journeys.py       # Staging  → san_cycles_stg.journeys
│       ├── mrt_dim_stations.py   # Staging  → san_cycles_mrt dataset
│       ├── mrt_fct_journeys.py
│       ├── mrt_station_stats.py
│       ├── mrt_bike_stats.py
│       └── mrt_kpis_monthly.py
└── streamlit/
    └── app.py
```

---

## Setup

The project runs on GCP and requires a GCP project and a Terraform service account created manually upfront. Everything else is managed through Terraform and `make`.

**Recommended: GitHub Codespaces.** Setup takes under 10 minutes in a fresh Codespace. Any environment with `git`, `make`, `uv`, `bruin`, and `terraform` should be enough.

### 1. Open Codespace and clone the repo

### 2. Run make setup

```bash
make setup
```

This installs Bruin, syncs uv dependencies, and initialises `.env` and `.bruin.yml` from their templates.

### 3. Install Terraform

The script below comes from individual components on https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli .


```bash
sudo apt-get update && sudo apt-get install -y gnupg software-properties-common

wget -O- https://apt.releases.hashicorp.com/gpg | \
gpg --dearmor | \
sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null

gpg --no-default-keyring \
--keyring /usr/share/keyrings/hashicorp-archive-keyring.gpg \
--fingerprint

echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
https://apt.releases.hashicorp.com $(grep -oP '(?<=UBUNTU_CODENAME=).*' /etc/os-release || lsb_release -cs) main" | \
sudo tee /etc/apt/sources.list.d/hashicorp.list

sudo apt update && sudo apt-get install terraform
```

### 4. Create a GCP project

Create a GCP project at [console.cloud.google.com](https://console.cloud.google.com). Note the **project ID** and your preferred **region** — you'll need these in `.env`.

### 5. Create the Terraform service account

- [ ] Create service account
- [ ] Grant permissions
  - [ ] bigquery.dataOwner
  - [ ] storage.admin
  - [ ] iam.serviceAccountAdmin
  - [ ] iam.serviceAccountKeyAdmin
  - [ ] resourcemanager.projectIamAdmin
- [ ] Download key → save to keys/terraform-sa-key.json
- [ ] Troubleshoot any setup issues. These would come up at `make infra-apply`


### 6. Configure .env

Edit `.env` and fill in:

```bash
export GCP_PROJECT=<your-project-id>
export LOCATION=<your-region>        # e.g. europe-west2
export YEARS="[2023,2024,2025]"      # years to ingest (JSON array)
export TF_VAR_credentials=../keys/terraform-sa-key.json
```

### 7. Provision GCP infrastructure

```bash
make infra-apply
```

This creates the GCS bucket, BigQuery datasets (`san_cycles_ing`, `san_cycles_stg`, `san_cycles_mrt`), pipeline service account, and writes `keys/gcp-sa-key.json`.

### 8. Run the pipeline

```bash
make bruin-run          # full pipeline: ingest → stage → marts
make stream-dash        # launch Streamlit dashboard at localhost:8501
```

> **Note:** Currently this project is limited to running a few years at a time. Bruin requires a dataframe to be passed after asset executes, and tends to choke on 100M records at once. The pipeline runs best processing up to 5 years at a time due to in-memory DataFrame concatenation. For full historical load, run in batches via `YEARS="[2012,2013,2014,2015]"` etc. Also go ahead and brake it!

---

## Makefile Commands

```bash
make setup             # Install uv, bruin + Python deps + copy init files
make infra-plan        # Terraform plan
make infra-apply       # Terraform apply (creates GCP resources + writes SA key)
make infra-destroy     # Terraform destroy
make bruin-ingest      # Run ingestion (uses YEARS from .env)
make bruin-stage       # Run staging asset
make bruin-marts       # Run all mart assets
make bruin-run         # Run full pipeline
make stream-dash       # Launch Streamlit dashboard
make clean             # Remove .venv and /tmp/tfl cache
```

---

## Data Model

Three BigQuery layers:

**`san_cycles_ing`** — raw append-only data as ingested from TfL. Partitioned by `start_date` (monthly), clustered by `start_station_id`.

**`san_cycles_stg`** — cleaned and deduplicated. Adds derived fields (`ride_date`, `journey_month`, `start_hour`, `is_round_trip`), backfills missing station IDs via name lookup, and enforces quality checks.

**`san_cycles_mrt`** — mart tables for the dashboard:

| Table | Description |
|---|---|
| `mrt_dim_stations` | All known stations; canonical name per station ID |
| `mrt_fct_journeys` | Full journey fact table |
| `mrt_station_stats` | Monthly pickups + dropoffs per station |
| `mrt_bike_stats` | Per-bike ride counts and duration by month |
| `mrt_kpis_monthly` | Monthly KPIs: total rides, top station, top bike, longest ride |

---

## Dashboard

Run with `make stream-dash` (opens at `localhost:8501`).

**Tile 1 — Most loved bike of the year:** year selector, top bike by ride count with total rides and hours ridden.

**Tile 2 — Rides by season:** stacked bar chart, years on x-axis, trips stacked by Winter / Autumn / Summer / Spring.

---

## Data Source

- **URL:** https://cycling.data.tfl.gov.uk
- **Licence:** [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
- **Coverage:** 2012 – present, updated weekly
- **Format:** CSV (weekly files 2017+), zip (annual files 2012–2016)
- **Volume:** 135M+ rows across 300+ files

---

## Known Issues

- `GOOGLE_APPLICATION_CREDENTIALS` must be a **relative path** for Bruin (`keys/gcp-sa-key.json`). Absolute paths break Bruin's env resolution.
- Bruin and `uv run` both manage `.venv` in the project root. They must use the same Python version (pinned via `.python-version`) or they'll conflict. If you hit `os error 66`, run `rm -rf .venv` and retry.
- Cross-year files (e.g. `38JourneyDataExtract28Dec2016-03Jan2017.csv`) appear in both the 2016 zip and the 2017 CSV run. Staging deduplication handles this.

---

## Self-Evaluation

| Criterion | Score | Notes |
|---|---|---|
| Problem description | 4 / 4 | Problem, scale, and what the pipeline answers are clearly stated |
| Cloud | 4 / 4 | Runs on GCP; all infrastructure provisioned via Terraform |
| Data ingestion (batch) | 2 / 4 | Multi-step Bruin DAG with quality checks; however data lands directly in BigQuery rather than a data lake (no GCS staging layer) |
| Data warehouse | 4 / 4 | Ingestion table partitioned by `start_date` (monthly) and clustered by `start_station_id`; mart tables pre-aggregated for dashboard query patterns |
| Transformations | 4 / 4 | Three-layer SQL pipeline (ing → stg → mrt) with deduplication, derived fields, and quality checks, orchestrated via Bruin — a lightweight alternative to dbt/Spark purpose-built for this kind of pipeline |
| Dashboard | 4 / 4 | Two tiles: most-loved bike of the year, and rides by season |
| Reproducibility | 4 / 4 | Step-by-step setup from zero, single `make` entrypoints, templated config files |
| **Total** | **26 / 28** | |
