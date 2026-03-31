terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.location
  credentials = file(var.credentials)
}

# GCS Data Lake
resource "google_storage_bucket" "gcs_bkt" {
  name                        = "${var.project_id}-bkt"
  location                    = var.location
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  versioning { enabled = true }

  labels = { project = "santander-cycles", env = var.environment }
}


# BigQuery Datasets
resource "google_bigquery_dataset" "ingestion" {
  dataset_id                 = "san_cycles_ing"
  friendly_name              = "Santander Cycles - Ingestion"
  description                = "Ingested CSV data from TfL"
  location                   = var.location
  delete_contents_on_destroy = true
  labels = { project = "santander-cycles", layer = "ing", env = var.environment }
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "san_cycles_stg"
  friendly_name              = "Santander Cycles - Staging"
  description                = "Cleaned and typed staging models"
  location                   = var.location
  delete_contents_on_destroy = true
  labels = { project = "santander-cycles", layer = "staging", env = var.environment }
}

resource "google_bigquery_dataset" "mart" {
  dataset_id                 = "san_cycles_mrt"
  friendly_name              = "Santander Cycles - Data Mart"
  description                = "Production fact and dimension tables"
  location                   = var.location
  delete_contents_on_destroy = true
  labels = { project = "santander-cycles", layer = "mart", env = var.environment }
}

# BigQuery Ingestion Table
resource "google_bigquery_table" "ing_journeys" {
  dataset_id          = google_bigquery_dataset.ingestion.dataset_id
  table_id            = "journeys"
  deletion_protection = false

  time_partitioning {
    type  = "MONTH"
    field = "start_date"
  }

  clustering = ["start_station_id"]

  schema = jsonencode([
    { name = "rental_id",        type = "INTEGER",    mode = "NULLABLE" },
    { name = "duration",         type = "INTEGER",   mode = "NULLABLE" },
    { name = "bike_id",          type = "INTEGER",    mode = "NULLABLE" },
    { name = "end_date",         type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "end_station_id",   type = "INTEGER",    mode = "NULLABLE" },
    { name = "end_station_name", type = "STRING",    mode = "NULLABLE" },
    { name = "start_date",       type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "start_station_id", type = "INTEGER",    mode = "NULLABLE" },
    { name = "start_station_name", type = "STRING",  mode = "NULLABLE" },
    { name = "end_station_priority_id", type = "INTEGER", mode = "NULLABLE" },
    { name = "_source_file",     type = "STRING",    mode = "NULLABLE" },
    { name = "_ingested_at",     type = "TIMESTAMP", mode = "NULLABLE" }
  ])

  labels = { project = "santander-cycles", layer = "ing" }
  depends_on = [google_bigquery_dataset.ingestion]
}


# Service Account
resource "google_service_account" "pipeline_sa" {
  account_id   = "santander-cycles-pipeline"
  display_name = "Santander Cycles Pipeline SA"
}

resource "google_project_iam_member" "sa_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_gcs_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}


output "gcs_bkt" {
  description = "GCS data lake bucket name"
  value       = google_storage_bucket.gcs_bkt.name
}

output "bq_ds_ing" {
  description = "BigQuery ingestion dataset ID"
  value       = google_bigquery_dataset.ingestion.dataset_id
}

output "bq_ds_stg" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id 
}

output "bq_ds_mrt" {
  description = "BigQuery marts dataset ID"
  value       = google_bigquery_dataset.mart.dataset_id 
}

output "pipeline_sa_email" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline_sa.email
}

output "sa_key_b64" {
  description = "Pipeline service account key"
  value       = google_service_account_key.pipeline_sa_key.private_key
  sensitive   = true 
}
