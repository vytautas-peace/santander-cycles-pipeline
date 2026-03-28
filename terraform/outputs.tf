output "data_lake_bucket" {
  description = "GCS data lake bucket name"
  value       = google_storage_bucket.data_lake.name
}

output "raw_dataset" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "staging_dataset" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "marts_dataset" {
  description = "BigQuery marts dataset ID"
  value       = google_bigquery_dataset.mart.dataset_id
}

output "pipeline_sa_email" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline_sa.email
}
