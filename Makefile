# Santander Cycles Data Pipeline – Makefile
# ==========================================
# Prerequisites: gcloud CLI, terraform, uv, make
# Install uv: curl -LsSf https://astral.sh/uv/install.sh | sh

.PHONY: help setup install check-uv check-years \
        infra-plan infra-apply infra-destroy \
        run pref-ingest \
        dbt-deps dbt-run dbt-test dbt-docs \
        stream-dash test lint clean

PYTHON      := uv run python
DBT_DIR     := dbt
PREFECT_DIR := prefect
TF_DIR      := terraform

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Year validation ───────────────────────────────────────────────────────────
check-years:
	@set -a && source .env && set +a && \
		uv run python -c " \
import sys, re, os; \
years = os.environ.get('YEARS', '').strip(); \
if not years: \
    print('ERROR: YEARS not set in .env. Add e.g. YEARS=2022 or YEARS=all'); sys.exit(1); \
if years != 'all': \
    parts = years.split(); \
    invalid = [y for y in parts if not re.match(r'^20[0-9]{2}$$', y)]; \
    if invalid: \
        print(f'ERROR: Invalid years: {invalid}. Use 4-digit years or \"all\"'); sys.exit(1); \
    out_of_range = [y for y in parts if not (2012 <= int(y) <= 2030)]; \
    if out_of_range: \
        print(f'ERROR: Years out of range (2012-2030): {out_of_range}'); sys.exit(1); \
print(f'Years: {years}'); \
"

# ── Setup ─────────────────────────────────────────────────────────────────────
setup: check-uv install  ## Full local setup
	@cp -n .env.example .env 2>/dev/null && \
		echo "Created .env — fill in GCP_PROJECT, GCS_BKT, LOCATION" || \
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
	@cd $(TF_DIR) && echo "GCS_BKT=$$(terraform output -raw gcs_bkt)"

infra-destroy:  ## Destroy all GCP resources
	@set -a && source .env && set +a && \
		export TF_VAR_location=$$LOCATION && \
		cd $(TF_DIR) && terraform destroy


# ── Ingestion ─────────────────────────────────────────────────────────────────
pref-ingest:  ## Run Prefect ingestion (requires YEARS)
	@set -a && source .env && set +a && \
		uv run python prefect/pipeline.py

# ── Transformation ────────────────────────────────────────────────────────────
dbt-run:  ## Run dbt models (requires YEARS)
	@set -a && source .env && set +a && \
		[ -n "$$YEARS" ] || (echo "ERROR: YEARS not set. Use: make run" && exit 1) && \
		cd $(DBT_DIR) && uv run dbt run \
			--profiles-dir . \
			--vars "{\"years\": \"$$YEARS\"}" \
			--target dev

dbt-deps:  ## Install dbt packages
	cd $(DBT_DIR) && uv run dbt deps --profiles-dir .

dbt-test:  ## Run dbt tests (requires YEARS)
	@set -a && source .env && set +a && \
		[ -n "$$YEARS" ] || (echo "ERROR: YEARS not set. Use: make run" && exit 1) && \
		cd $(DBT_DIR) && uv run dbt test \
			--profiles-dir . \
			--vars "{\"years\": \"$$YEARS\"}" \
			--target dev

dbt-docs:  ## Generate and serve dbt docs
	@set -a && source .env && set +a && \
		cd $(DBT_DIR) && uv run dbt docs generate --profiles-dir . && \
		uv run dbt docs serve

# ── Dashboard ─────────────────────────────────────────────────────────────────
stream-dash:  ## Start Streamlit dashboard (opens at localhost:8501)
	@set -a && source .env && set +a && \
		uv run streamlit run dashboard/app.py

# ── Tests & Lint ──────────────────────────────────────────────────────────────
test:  ## Run Python unit tests
	uv run pytest scripts/test_pipeline.py -v --tb=short

lint:  ## Lint Python code
	uv run ruff check prefect/

# ── Utilities ─────────────────────────────────────────────────────────────────
clean:  ## Remove venv, temp files, dbt artefacts
	rm -rf .venv /tmp/tfl $(DBT_DIR)/target $(DBT_DIR)/dbt_packages
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true