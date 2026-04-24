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


output "pipeline_sa_email" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline_sa.email
}

output "sa_key_b64" {
  description = "Pipeline service account key"
  value       = google_service_account_key.pipeline_sa_key.private_key
  sensitive   = true 
}
