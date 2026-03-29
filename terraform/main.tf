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
}

# GCS Data Lake
resource "google_storage_bucket" "data_lake" {
  name                        = "${var.project_id}-santander-cycles-lake"
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

resource "google_storage_bucket_object" "raw_folder" {
  name    = "raw/"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "processed_folder" {
  name    = "processed/"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

# BigQuery Datasets
resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "santander_cycles_raw"
  friendly_name              = "Santander Cycles - Raw"
  description                = "Raw ingested CSV data from TfL"
  location                   = var.location
  delete_contents_on_destroy = true
  labels = { project = "santander-cycles", layer = "raw", env = var.environment }
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "santander_cycles_staging"
  friendly_name              = "Santander Cycles - Staging"
  description                = "Cleaned and typed dbt stg_ models"
  location                   = var.location
  delete_contents_on_destroy = true
  labels = { project = "santander-cycles", layer = "staging", env = var.environment }
}

resource "google_bigquery_dataset" "mart" {
  dataset_id                 = "santander_cycles_mart"
  friendly_name              = "Santander Cycles - Data Mart"
  description                = "Production fact and dimension tables"
  location                   = var.location
  delete_contents_on_destroy = true
  labels = { project = "santander-cycles", layer = "mart", env = var.environment }
}

# BigQuery Raw Table
resource "google_bigquery_table" "raw_rides" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_rides"
  deletion_protection = false

  time_partitioning {
    type  = "MONTH"
    field = "start_date"
  }

  clustering = ["start_station_id"]

  schema = jsonencode([
    { name = "rental_id",        type = "STRING",    mode = "NULLABLE" },
    { name = "duration",         type = "INTEGER",   mode = "NULLABLE" },
    { name = "bike_id",          type = "STRING",    mode = "NULLABLE" },
    { name = "end_date",         type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "end_station_id",   type = "STRING",    mode = "NULLABLE" },
    { name = "end_station_name", type = "STRING",    mode = "NULLABLE" },
    { name = "start_date",       type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "start_station_id", type = "STRING",    mode = "NULLABLE" },
    { name = "start_station_name", type = "STRING",  mode = "NULLABLE" },
    { name = "end_station_priority_id", type = "STRING", mode = "NULLABLE" },
    { name = "_source_file",     type = "STRING",    mode = "NULLABLE" },
    { name = "_ingested_at",     type = "TIMESTAMP", mode = "NULLABLE" }
  ])

  labels = { project = "santander-cycles", layer = "raw" }
  depends_on = [google_bigquery_dataset.raw]
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

output "gcs_bucket_name"  { value = google_storage_bucket.data_lake.name }
output "bq_raw_dataset"   { value = google_bigquery_dataset.raw.dataset_id }
output "bq_mart_dataset"  { value = google_bigquery_dataset.mart.dataset_id }
output "sa_email"         { value = google_service_account.pipeline_sa.email }
output "sa_key_b64"       { 
  value     = google_service_account_key.pipeline_sa_key.private_key
  sensitive = true 
}
