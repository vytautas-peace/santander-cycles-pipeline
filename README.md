# Santander Cycles Data Pipeline

An end-to-end batch data pipeline that ingests **TfL Santander Cycles** journey data from the [cycling.data.tfl.gov.uk](https://cycling.data.tfl.gov.uk) open data portal, transforms it through the transform and serve layers, and surfaces insights in a Streamlit dashboard.

---

## Problem Statement

TfL publishes weekly CSV files of every Santander Cycles journey in London — over 138 million rows spanning 2012 to present. The raw data is:

- **Messy**: 9 different column-name schemas across years, 4 date formats, duplicate records across cross-year files.
- **Fragmented**: 500+ individual CSV and zip files with no aggregation.
- **Unanalysable at scale**: direct CSV analysis is slow and error-prone beyond a few million rows.

This pipeline shows the beauty of data engineering by:

1. **Keeping it elegant** — infrastructure is managed by Terraform; flows orchestrated by Kestra; data processing done in BigQuery; containerisation done by Docker; Streamlit for dashboard-as-code. All tools are open source.

2. **Working at scale** — reads data from 500+ files with creatively disparate naming schemas and 9 column header schemas, resolving encoding issues and typical CSV data problems along the way.

3. **Producing a beautiful dashboard** — credit to Streamlit and Claude Code for tech skills; credit to the heart for the feel and taste.


---

## Architecture

Kestra orchestrates the code that moves data from the TfL portal through the **source**, **ingest**, **transform**, and **serve** layers. Docker runs Kestra and Streamlit as containers.

```
                ┌──────────────────────────┐
                │   TfL open data portal   │   CSV + zip, weekly
                │ cycling.data.tfl.gov.uk  │
                └────────────┬─────────────┘
                             │
┌────────────────────────────┼─────────────────────────────────┐
│ Docker                     │                                 │
│                            │                                 │
│  ┌─────────────────────────┼───────────────────────────────┐ │
│  │ Kestra                  ▼                               │ │
│  │  ┌──────────────────────────┐                           │ │
│  │  │      source layer        │  Python + Polars:         │ │
│  │  │                          │  discover ▸ download ▸    │ │
│  │  │                          │  unzip ▸ parquet ▸ GCS    │ │
│  │  └────────────┬─────────────┘                           │ │
│  │               │                                         │ │
│  │               ▼                                         │ │
│  │  ┌──────────────────────────┐                           │ │
│  │  │      ingest layer        │  BigQuery external table  │ │
│  │  │                          │  over gs://<bucket>/      │ │
│  │  │                          │  parquet/ + dedup view    │ │
│  │  └────────────┬─────────────┘                           │ │
│  │               │                                         │ │
│  │               ▼                                         │ │
│  │  ┌──────────────────────────┐                           │ │
│  │  │     transform layer      │  BigQuery views:          │ │
│  │  │                          │  journeys, dim_stations   │ │
│  │  └────────────┬─────────────┘                           │ │
│  │               │                                         │ │
│  │               ▼                                         │ │
│  │  ┌──────────────────────────┐                           │ │
│  │  │       serve layer        │  BigQuery tables:         │ │
│  │  │                          │  fct_journeys, dashboard  │ │
│  │  └────────────┬─────────────┘                           │ │
│  └───────────────┼─────────────────────────────────────────┘ │
│                  │                                           │
│                  ▼                                           │
│    ┌──────────────────────────┐                              │
│    │        Streamlit         │  reads serve.dashboard       │
│    │     localhost:8501       │                              │
│    └──────────────────────────┘                              │
└──────────────────────────────────────────────────────────────┘
```

**Stack:**

| Layer | Tool | Why |
|---|---|---|
| IaC | Terraform | Light, clear, does one thing well. |
| Orchestration | Kestra | Declarative YAML flows, web UI, subflow composition, runs locally via Docker. |
| Extraction | Polars (Python) | Fast CSV parsing, handles 9 schema variants and 4 date formats in lazy plans. |
| Lake | GCS | Parquet landing zone — cheap, decoupled from the warehouse. |
| Warehouse | BigQuery | Serverless, columnar, handles 138M+ rows; partitioning + clustering. |
| Dashboard | Streamlit | Python-native, looks ace! |
| AI assistant | Claude Code | Gets me through the mental loops, reliable, wild tech! |


---

## Repository Structure

```
santander-cycles-pipeline/
├── docker/
│   └── docker-compose.yml           # Kestra + Postgres + Streamlit stack
├── kestra/
│   ├── data/                        # Kestra directory for data processing (gitignored)
│   ├── flows/                       # Kestra flow definitions
│   │   ├── main_prod_guide.yml      # guide: top-level flow
│   │   ├── main_prod_source_.yml    # source: discover ▸ download ▸ unzip ▸ parquet ▸ GCS
│   │   ├── main_prod_ingest.yml     # ingest: GCS parquet → BQ external table + dedup view
│   │   ├── main_prod_transform.yml  # transform: BQ views journeys, dim_stations
│   │   └── main_prod_serve.yml      # serve: BQ tables fct_journeys, dashboard
│   ├── python/                      # Polars-based ETL scripts
│   │   ├── discover.py              # List TfL files, write metadata table
│   │   ├── download.py              # Fetch zip/csv, cache locally
│   │   ├── unzip.py                 # Extract zip archives
│   │   ├── make-parquet.py          # Normalise CSV → Parquet (9 schemas, 4 date formats)
│   │   └── upload-parquet.py        # Upload to GCS
│   ├── pyproject.toml
│   └── uv.lock
├── secrets/                         # SA keys + encoded env (gitignored)
│   ├── terraform-sa-key.json        # Manually downloaded — Terraform SA key
│   ├── gcp-sa-key.json              # Written by `make infra-apply` — pipeline SA key
│   └── .env_encoded                 # Written by `make docker-up` — base64 SA key for Kestra
├── streamlit/
│   ├── dashboard.py                 # Reads serve.dashboard from BigQuery
│   ├── main.py
│   ├── santander_logo.svg
│   ├── pyproject.toml
│   └── uv.lock
├── terraform/                       # GCP infra: GCS bucket, BQ datasets, pipeline SA
│   ├── main.tf
│   └── variables.tf
├── .env                             # Local environment variables (gitignored)
├── .env_example                     # Template — copy to .env via `make env`
├── .gitignore
├── Makefile                         # env, infra-apply, docker-up, kestra-lets-flow, ...
└── README.md
```

---

## Setup

The project targets GCP (BigQuery + GCS). Setup takes under 10 minutes in a fresh GitHub Codespace — any environment with `git`, `docker`, `make`, and `terraform` is enough.

**Recommended Codespace spec:** 4-core / 16 GB RAM. From the repo's **Code → Codespaces → ⋯ (three dots) → New with options**, choose the 4-core / 16 GB machine type before launching.


### 1. Create a GCP project

Create a GCP project at [console.cloud.google.com](https://console.cloud.google.com). Note the **project ID** and your preferred **region** — you'll need these for `.env`.


### 2. Create the Terraform service account

- [ ] Create service account
- [ ] Grant permissions
  - [ ] bigquery.dataOwner
  - [ ] storage.admin
  - [ ] iam.serviceAccountAdmin
  - [ ] iam.serviceAccountKeyAdmin
  - [ ] resourcemanager.projectIamAdmin
- [ ] Download key → save to `secrets/terraform-sa-key.json`
- [ ] Troubleshoot any setup issues. These would come up at `terraform apply`.


### 3. Open a GitHub Codespace

From the repo page, **Code → Codespaces → ⋯ → New with options**, then pick the 4-core / 16 GB machine. The Codespace comes with `git` and `docker` pre-installed.


### 4. Create `.env`

Rename the template and fill in your values:

```bash
make env
```

Edit `.env`:

```bash
export GCLOUD_PROJECT=<your-project-id>
export LOCATION=<your-region>                    # e.g. asia-southeast1
export GCS_BKT="${GCLOUD_PROJECT}-bkt"
export START_DATE="2012-01-01"                   # The project can be run for any selected date range.
export END_DATE="2025-12-31"                     # Recommendation: run full years.
export GOOGLE_APPLICATION_CREDENTIALS=secrets/gcp-sa-key.json
export TF_VAR_credentials=../secrets/terraform-sa-key.json
```


### 5. Install Terraform

The script below comes from the individual components on [developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli).

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


### 6. Provision GCP infrastructure

```bash
make infra-apply
```

This runs `terraform init && terraform apply` and creates the GCS bucket, BigQuery datasets, and the pipeline service account (key written to `secrets/gcp-sa-key.json`).


---

## Running the Pipeline

### 1. Start Kestra and Streamlit

```bash
make docker-up
```

Encodes the GCP service account key into `secrets/.env_encoded` (consumed by Kestra as a secret) and brings up Kestra, its Postgres, and the Streamlit dashboard.

It may take a bit of time to pull the Kestra image.

### 2. Trigger the `guide` flow

Give Kestra 20s to start before executing the command below.

```bash
make kestra-lets-flow
```

Triggers the `prod.guide` flow via the Kestra API with the date range and project from `.env`. You will know that the command has worked if terminal output ends with something like `http://localhost:8080/ui/main/executions/prod/guide/5BBjy0dQFMNxDyVDl3OmpQ"}`. Otherwise, try again.


You can also trigger manually and monitor status from the Kestra UI under the `prod` namespace.



### 3. Monitor progress and view the dashboard

Kestra process dashboard, Gantt charts and logs are available at [http://localhost:8080](http://localhost:8080) with login details:

```
username: admin@kestra.io
password: Admin1234!
```

After the completion of the `serve` flow, the dashboard becomes available at [http://localhost:8501](http://localhost:8501).

### 4. Cleanup

When you're done, tear everything down in this order:

```bash
make docker-down-v    # stop containers AND remove Kestra volumes (Postgres + app storage)
make infra-destroy    # terraform destroy — removes GCS bucket, BigQuery datasets, pipeline SA
```

Then on the web:

- **GitHub Codespace:** github.com/codespaces → stop → delete the Codespace.
- **GCP:** delete the Terraform service account, then shut down (and schedule deletion of) the GCP project itself. Deleting the project is the cleanest way to guarantee no residual resources keep billing.

---

## Pipelines

The `guide` flow orchestrates four sequential layers:

### Source layer

Python tasks running on the Kestra worker:

1. `discover.py` lists TfL files and writes a `source.metadata` table in BigQuery.
2. For each root file (parallelised, concurrency 8): `download.py` → `unzip.py` → `make-parquet.py`. Polars normalises 9 column schemas and 4 date formats into a unified Parquet schema.
3. `upload-parquet.py` uploads each Parquet to `gs://<bucket>/parquet/`. Parquet files are an efficient and elegant way to store source data, with 10x smaller size than CSV.

### Ingest layer

- Creates `ingest` schema in BigQuery.
- Registers an external table `ingest.journeys_ext` over `gs://<bucket>/parquet/*.parquet`.
- Creates `ingest.journeys` view: joins the external table with `source.metadata` on `filename_parquet` and deduplicates by `rental_id` keeping the most recent row based on the `last_modified` file attribute from the TfL website.

### Transform layer

- `transform.journeys`: applies `duration_s` sanity check (positive only), derives `duration_s_derived` from timestamp diff, filters by `start_date` / `end_date`.
- `transform.dim_stations`: one row per station id.

### Serve layer

- `serve.fct_journeys`: partitioned by `start_datetime` (month), clustered by `bike_id, start_station_id`. Resolves `end_station_name` via `dim_stations` and coalesces `duration_s` with the derived fallback.
- `serve.dashboard`: pre-aggregated long-format table powering the three dashboard tiles (`top_bike`, `seasonal`, `monthly`).


---

## Streamlit Dashboard

Runs inside the Docker stack on `localhost:8501`, reading from `serve.dashboard`.

**Tile 1 — Most loved bike of the year:** year selector, top bike by hours ridden.

**Tile 2 — Rides by season:** grouped bar chart by year, Spring / Summer / Autumn / Winter with matching colours.

**Tile 3 — Total journeys by month:** line chart showing monthly journey volume across all years.


---

## Data Source

- **URL:** https://cycling.data.tfl.gov.uk
- **Licence:** [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
- **Coverage:** 2012 – present, updated weekly
- **Format:** CSV (weekly files 2017+), zip (annual files 2012–2016)
- **Volume:** c. 140M rows across c. 500 files
- **Schema variants:** 9 different column-name schemas and 4 date formats across years


---

## Thanks for reading — and following along! ✨😌