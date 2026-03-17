# Data source pour récupérer le project number
data "google_project" "current" {
  project_id = var.project_id
}

#==========================================================================
# Service Account dédié à au bucket
#==========================================================================
resource "google_service_account" "raw_bucket_sa" {
  account_id   = "bucket-raw-data"
  display_name = "Service Account pour raw_data_bucket"
}

# Permission d'écrire dans le buckets de données
resource "google_storage_bucket_iam_member" "function_crypto_stream_bucket_access" {
  bucket = google_storage_bucket.raw_data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.raw_bucket_sa.email}"
}
