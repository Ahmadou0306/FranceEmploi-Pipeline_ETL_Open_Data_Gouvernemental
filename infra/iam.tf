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
# DROITS GCS POUR PUBLIER SUR LES TOPICS 
# (GCS a besoin de droits pour envoyer des notifs à Pub/Sub)
# ============================================================
data "google_storage_project_service_account" "gcs_account" {}
resource "google_pubsub_topic_iam_member" "gcs_publisher" {
  for_each = local.flux

  topic  = google_pubsub_topic.snowpipe_topics[each.key].name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}


# ============================================================
# Service Account dédié à Snowpipe
# ============================================================
resource "google_service_account" "snowpipe_sa" {
  account_id   = "snowpipe-sa"
  display_name = "Service Account pour Snowpipe"
  project      = var.project_id
}

# DROITS SNOWPIPE SA SUR LES SUBSCRIPTIONS
resource "google_pubsub_subscription_iam_member" "snowflake_subscriber" {
  for_each = local.flux

  subscription = google_pubsub_subscription.snowpipe_subs[each.key].name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.snowpipe_sa.email}"
}

# DROITS SNOWPIPE SA SUR LE BUCKET (lecture)
resource "google_storage_bucket_iam_member" "snowpipe_object_viewer" {
  bucket = google_storage_bucket.raw_data_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.snowpipe_sa.email}"
}

# ============================================================
# IAM - Service Account Snowflake (fourni par DESC INTEGRATION)
# Principal externe géré par Snowflake, pas créé par Terraform
# ============================================================

# Abonné Pub/Sub (lire et acquitter les messages)
resource "google_pubsub_subscription_iam_member" "snowflake_external_subscriber" {
  for_each = local.flux

  subscription = google_pubsub_subscription.snowpipe_subs[each.key].name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${var.gcp_pubsub_service_account}"
}

# Viewer Pub/Sub au niveau SUBSCRIPTION (accorde pubsub.subscriptions.get sans droits projet)
resource "google_pubsub_subscription_iam_member" "snowflake_external_viewer" {
  for_each = local.flux

  subscription = google_pubsub_subscription.snowpipe_subs[each.key].name
  role         = "roles/pubsub.viewer"
  member       = "serviceAccount:${var.gcp_pubsub_service_account}"
}

# Lecture des objets dans le bucket par le SA STORAGE (STORAGE_GCP_SERVICE_ACCOUNT de gcs_snowpipe_integration)
resource "google_storage_bucket_iam_member" "snowflake_external_object_viewer" {
  bucket = google_storage_bucket.raw_data_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.gcs_storage_service_account}"
}

# Monitoring Viewer au niveau PROJET
# Nécessaire pour que Snowflake surveille les métriques des subscriptions Pub/Sub
resource "google_project_iam_member" "snowflake_monitoring_viewer" {
  project = var.project_id
  role    = "roles/monitoring.viewer"
  member  = "serviceAccount:${var.gcp_pubsub_service_account}"
}