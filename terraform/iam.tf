# ============================================================
# Data source - Project courant
# Utile pour Workload Identity ou références au project_number
# ============================================================
data "google_project" "current" {
  project_id = var.project_id
}

# ============================================================
# Service Account dédié au bucket raw data
# ============================================================
resource "google_service_account" "raw_bucket_sa" {
  account_id   = "bucket-raw-data-sa"
  display_name = "Service Account pour raw_data_bucket"
  project      = var.project_id
}

# ============================================================
# IAM - Permissions sur le bucket
# Rôle restreint à objectCreator (ingestion uniquement)
# Remplacer par objectAdmin si la suppression est nécessaire
# ============================================================
resource "google_storage_bucket_iam_member" "object_creator_access_raw_bucket_sa" {
  bucket = google_storage_bucket.raw_data_bucket.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.raw_bucket_sa.email}"
}
