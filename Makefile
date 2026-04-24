SHELL := /bin/bash

.PHONY: env infra-apply infra-destroy docker-up docker-down docker-down-v kestra-lets-flow

env:
	mv .env_example .env

infra-apply:
	source .env && cd terraform && terraform init && terraform apply

docker-up:
	source .env && \
	echo "SECRET_GCP_SERVICE_ACCOUNT=$$(base64 < secrets/gcp-sa-key.json | tr -d '\n')" > secrets/.env_encoded && \
	cd docker && docker compose up -d

kestra-lets-flow:
	source .env && \
	curl -v -u 'admin@kestra.io:Admin1234!' \
		"http://localhost:8080/api/v1/executions/prod/guide" \
		-F project="$${GCLOUD_PROJECT}" \
		-F location="$${LOCATION}" \
		-F bucket="$${GCS_BKT}" \
		-F start_date="$${START_DATE}" \
		-F end_date="$${END_DATE}"

docker-down:
	cd docker && docker compose down

docker-down-v:
	cd docker && docker compose down -v

infra-destroy:
	source .env && cd terraform && terraform destroy