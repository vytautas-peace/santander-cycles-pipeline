#!/bin/sh

# Export environment variables set in .env file
source .env

# Export service account encoded in base64 for Kestra
echo SECRET_GCP_SERVICE_ACCOUNT=$(base64 < secrets/gcp-sa-key.json | tr -d '\n') > secrets/.env_encoded

echo "Triggering Kestra guide flow..."

# Execute your specific curl command
curl -v -u 'admin@kestra.io:Admin1234!' \
    "http://localhost:8080/api/v1/executions/prod/guide" \
    -F project="${GCLOUD_PROJECT}" \
    -F location="${LOCATION}" \
    -F bucket="${GCS_BKT}" \
    -F start_date="${START_DATE}" \
    -F end_date="${END_DATE}"