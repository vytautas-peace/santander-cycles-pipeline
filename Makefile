SHELL := /bin/bash

# Santander Cycles Data Pipeline – Makefile
# ==========================================
# Prerequisites: gcloud CLI (install manually)
# Run `make setup` to install terraform, bruin, uv, and Python dependencies

.PHONY: help setup install install-uv install-terraform install-bruin check-years \
        infra-plan infra-apply infra-destroy \
        bruin-ingest bruin-stg bruin-mrt bruin-run \
        stream-dash clean

TF_DIR := terraform

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Setup ─────────────────────────────────────────────────────────────────────
setup: install-uv install-terraform install-bruin install  ## Full local setup
	@cp -n .env.init .env 2>/dev/null && \
		echo "Created .env — fill in GCP_PROJECT, LOCATION, TF_VAR_credentials" || \
		echo ".env already exists"
	@cp -n .bruin.yml.init .bruin.yml 2>/dev/null && \
		echo "Created .bruin.yml" || \
		echo ".bruin.yml already exists"

install-uv:  ## Install uv if not already installed
	@if which uv > /dev/null 2>&1; then \
		echo "uv already installed: $$(uv --version)"; \
	else \
		echo "Installing uv..." && \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
	fi

install-terraform:  ## Install Terraform if not already installed
	@if which terraform > /dev/null 2>&1; then \
		echo "terraform already installed: $$(terraform version | head -1)"; \
	elif [ "$$(uname)" = "Darwin" ]; then \
		brew install terraform; \
	else \
		sudo apt-get update && sudo apt-get install -y gnupg software-properties-common curl && \
		curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg && \
		echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $$(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list && \
		sudo apt-get update && sudo apt-get install -y terraform; \
	fi

install-bruin:  ## Install Bruin CLI if not already installed
	@if which bruin > /dev/null 2>&1; then \
		echo "bruin already installed: $$(bruin --version)"; \
	elif [ "$$(uname)" = "Darwin" ]; then \
		brew install bruin-data/tap/bruin; \
	else \
		curl -LsSf https://sh.bruin.run | sh; \
	fi

install:  ## Install Python dependencies via uv
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
bruin-ingest:  ## Run ingestion asset (uses YEARS from .env)
	@set -a && source .env && set +a && \
		bruin run bruin/assets/ing_journeys.py --var years=$$YEARS

bruin-stage:  ## Run staging asset (includes quality checks)
	@set -a && source .env && set +a && \
		bruin run bruin/assets/stg_journeys.py

bruin-marts:  ## Run all mart assets
	@set -a && source .env && set +a && \
		bruin run bruin/assets/mrt_dim_stations.py && \
		bruin run bruin/assets/mrt_fct_journeys.py && \
		bruin run bruin/assets/mrt_station_stats.py && \
		bruin run bruin/assets/mrt_bike_stats.py && \
		bruin run bruin/assets/mrt_kpis_monthly.py

bruin-run:  ## Run full pipeline (ingest → stg → mrt)
	@set -a && source .env && set +a && \
		bruin run bruin/ --var years=$$YEARS

# ── Dashboard ─────────────────────────────────────────────────────────────────
stream-dash:  ## Start Streamlit dashboard (opens at localhost:8501)
	@set -a && source .env && set +a && \
		uv run streamlit run streamlit/app.py

# ── Utilities ─────────────────────────────────────────────────────────────────
clean:  ## Remove venv and temp files
	rm -rf .venv /tmp/tfl
	find . -name "__pycache__" -type d -not -path "./bruin/*" -exec rm -rf {} + 2>/dev/null || true