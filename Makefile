SHELL := /bin/bash

# Santander Cycles Data Pipeline – Makefile
# ==========================================
# Prerequisites: gcloud CLI, terraform, bruin, uv
# Install uv: curl -LsSf https://astral.sh/uv/install.sh | sh

.PHONY: help setup install check-uv check-years \
        infra-plan infra-apply infra-destroy \
        bruin-ingest bruin-stg bruin-mrt bruin-run \
        stream-dash clean

TF_DIR := terraform

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Setup ─────────────────────────────────────────────────────────────────────
setup: check-uv install  ## Full local setup
	@cp -n .env.example .env 2>/dev/null && \
		echo "Created .env — fill in GCP_PROJECT, LOCATION, YEARS and TF_VAR_credentials" || \
		echo ".env already exists"

check-uv:
	@which uv > /dev/null 2>&1 || \
		(echo "uv not found. Install: curl -LsSf https://astral.sh/uv/install.sh | sh" && exit 1)

install:  ## Install all dependencies via uv
	uv sync

# ── Infrastructure ────────────────────────────────────────────────────────────
infra-plan:  ## Preview Terraform changes
	@set -a && source .env && set +a && \
		export TF_VAR_location=$$LOCATION && \
		export TF_VAR_project_id=$$GCP_PROJECT && \
		cd $(TF_DIR) && terraform init -upgrade && terraform plan

infra-apply:  ## Apply Terraform (create GCS bucket, BQ datasets, SA keys)
	@set -a && source .env && set +a && \
		export TF_VAR_location=$$LOCATION && \
		export TF_VAR_project_id=$$GCP_PROJECT && \
		cd $(TF_DIR) && terraform init -upgrade && terraform apply -auto-approve
	@echo ""
	@echo "Extracting service account key..."
	@mkdir -p keys
	cd $(TF_DIR) && terraform output -raw sa_key_b64 | base64 -d > ../keys/gcp-sa-key.json
	@echo "Key written to keys/gcp-sa-key.json"
	@echo ""
	@echo "Add this to your .env:"
	@cd $(TF_DIR) && echo "GCS_BKT=$$(terraform output -raw gcs_bkt)"

infra-destroy:  ## Destroy all GCP resources
	@set -a && source .env && set +a && \
		export TF_VAR_location=$$LOCATION && \
		export TF_VAR_project_id=$$GCP_PROJECT && \
		cd $(TF_DIR) && terraform destroy

# ── Bruin Pipeline ────────────────────────────────────────────────────────────
bruin-ingest:  ## Run raw ingestion asset (uses YEARS from .env)
	@set -a && source .env && set +a && \
		bruin run bruin/assets/raw_journeys.py --var years=$$YEARS

bruin-stg:  ## Run staging asset (includes quality checks)
	@set -a && source .env && set +a && \
		bruin run bruin/assets/stg_journeys.py

bruin-mrt:  ## Run all mart assets
	@set -a && source .env && set +a && \
		bruin run bruin/assets/mrt_dim_stations.py && \
		bruin run bruin/assets/mrt_fct_journeys.py && \
		bruin run bruin/assets/mrt_station_stats.py && \
		bruin run bruin/assets/mrt_bike_stats.py && \
		bruin run bruin/assets/mrt_kpis_monthly.py

bruin-bikepoints:  ## Fetch TfL station/borough data (run once, or to pick up new stations)
	@set -a && source .env && set +a && \
		bruin run bruin/assets/raw_bikepoints.py && \
		bruin run bruin/assets/stg_bikepoints.py

bruin-run:  ## Run full pipeline (ingest → stg → mrt)
	@set -a && source .env && set +a && \
		bruin run bruin/

# ── Dashboard ─────────────────────────────────────────────────────────────────
stream-dash:  ## Start Streamlit dashboard (opens at localhost:8501)
	@set -a && source .env && set +a && \
		uv run streamlit run streamlit/app.py

# ── Utilities ─────────────────────────────────────────────────────────────────
clean:  ## Remove venv and temp files
	rm -rf .venv /tmp/tfl
	find . -name "__pycache__" -type d -not -path "./bruin/*" -exec rm -rf {} + 2>/dev/null || true