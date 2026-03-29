# Santander Cycles Data Pipeline – Makefile
# ==========================================
# Prerequisites: gcloud CLI, terraform, uv, make
# Install uv: curl -LsSf https://astral.sh/uv/install.sh | sh

.PHONY: help setup install infra-plan infra-apply infra-destroy \
        run-pipeline run-pipeline-test register-schedule \
        dbt-deps dbt-run dbt-test dbt-docs dbt-ci \
        test lint clean all

PYTHON      := uv run python
DBT_DIR     := dbt
PREFECT_DIR := prefect
TF_DIR      := terraform

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Setup ─────────────────────────────────────────────────────────────────────
setup: check-uv install  ## Full local setup (install deps + copy env template)
	@cp -n .env.example .env 2>/dev/null && \
		echo "Created .env — fill in GCP_PROJECT and GCS_BUCKET" || \
		echo ".env already exists"

check-uv:  ## Verify uv is installed
	@which uv > /dev/null 2>&1 || \
		(echo "uv not found. Install it with:" && \
		 echo "  curl -LsSf https://astral.sh/uv/install.sh | sh" && \
		 echo "Then restart your terminal and retry." && exit 1)

install:  ## Create venv and install all dependencies via uv
	uv sync

# ── Infrastructure ────────────────────────────────────────────────────────────
infra-plan:  ## Preview Terraform changes
	@set -a && source .env && set +a && \
		export TF_VAR_location=$$LOCATION && \
		cd $(TF_DIR) && terraform init -upgrade && terraform plan

infra-apply:  ## Apply Terraform (create GCS bucket, BQ datasets, SA key)
	@set -a && source .env && set +a && \
		export TF_VAR_location=$$LOCATION && \
		cd $(TF_DIR) && terraform init -upgrade && terraform apply -auto-approve
	@echo ""
	@echo "Extracting service account key..."
	@mkdir -p keys
	cd $(TF_DIR) && terraform output -raw sa_key_b64 | base64 -d > ../keys/gcp-sa-key.json
	@echo "Key written to keys/gcp-sa-key.json"
	@echo ""
	@echo "Add this to your .env:"
	@cd $(TF_DIR) && echo "GCS_BUCKET=$$(terraform output -raw gcs_bucket_name)"

infra-destroy:  ## Destroy all GCP resources (CAUTION: deletes data)
	@set -a && source .env && set +a && \
		export TF_VAR_location=$$LOCATION && \
		cd $(TF_DIR) && terraform destroy

# ── Ingestion Pipeline ────────────────────────────────────────────────────────
run-pipeline:  ## Run full ingestion (all historical files)
	@set -a && source .env && set +a && \
		cd $(PREFECT_DIR) && uv run python pipeline.py

run-pipeline-test:  ## Run ingestion with 3 files only (smoke test)
	@set -a && source .env && set +a && \
		uv run python -c \
		"import sys; sys.path.insert(0,'prefect'); \
		from pipeline import ingestion_flow; ingestion_flow(file_limit=3)"

register-schedule:  ## Register weekly Prefect deployment
	@set -a && source .env && set +a && \
		cd $(PREFECT_DIR) && uv run python schedule.py

# ── dbt ───────────────────────────────────────────────────────────────────────
dbt-deps:  ## Install dbt packages
	cd $(DBT_DIR) && uv run dbt deps --profiles-dir .

dbt-run:  ## Run all dbt models
	@set -a && source .env && set +a && \
		cd $(DBT_DIR) && uv run dbt run --profiles-dir . --target dev

dbt-test:  ## Run all dbt tests
	@set -a && source .env && set +a && \
		cd $(DBT_DIR) && uv run dbt test --profiles-dir . --target dev

dbt-docs:  ## Generate and serve dbt docs
	@set -a && source .env && set +a && \
		cd $(DBT_DIR) && uv run dbt docs generate --profiles-dir . && \
		uv run dbt docs serve

dbt-ci:  ## Compile + test (for CI)
	@set -a && source .env && set +a && \
		cd $(DBT_DIR) && uv run dbt deps --profiles-dir . && \
		uv run dbt compile --profiles-dir . && \
		uv run dbt test --profiles-dir .

# ── Tests & Lint ──────────────────────────────────────────────────────────────
test:  ## Run Python unit tests
	uv run pytest scripts/test_pipeline.py -v --tb=short

lint:  ## Lint Python code with ruff
	uv run ruff check $(PREFECT_DIR)/

# ── Utilities ─────────────────────────────────────────────────────────────────
clean:  ## Remove venv, temp files, and dbt artefacts
	rm -rf .venv /tmp/tfl $(DBT_DIR)/target $(DBT_DIR)/dbt_packages
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true


# ── Dashboard ─────────────────────────────────────────────────────────────────
dashboard:  ## Start Streamlit dashboard (opens at localhost:8501)
	@set -a && source .env && set +a && \
		uv run streamlit run streamlit/app.py


all: infra-apply install dbt-deps run-pipeline-test dbt-run dbt-test  ## Full run from scratch
