# ============================================================
# Bucket GCS - Stockage des données brutes
# ============================================================
resource "google_storage_bucket" "raw_data_bucket" {
  name          = "${var.project_name}-data"
  location      = var.region
  project       = var.project_id # Projet explicite (multi-projet)
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true

  versioning {
    enabled = var.environment == "prod"
  }

  # Lifecycle uniquement hors prod (évite suppression accidentelle en prod)
  dynamic "lifecycle_rule" {
    for_each = var.environment != "prod" ? [1] : []
    content {
      condition {
        age = 30
      }
      action {
        type = "Delete"
      }
    }
  }

  # Chiffrement CMEK (optionnel, activé si kms_key_name est fourni)
  dynamic "encryption" {
    for_each = var.kms_key_name != "" ? [1] : []
    content {
      default_kms_key_name = var.kms_key_name
    }
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ============================================================
# Création des "dossiers" GCS (placeholders .keep)
# Nécessaire pour que les préfixes existent avant la config Snowpipe
# Chemins créés :
#   raw/tranche_age/current/
#   raw/demandeurs_emploi_tranche_age/current/
#   raw/offres_emploi_france_travail/current/
#   raw/chomeurs_indemnises_departement/current/
# ============================================================
resource "google_storage_bucket_object" "raw_folder_placeholders" {
  for_each = local.flux

  bucket  = google_storage_bucket.raw_data_bucket.name
  name    = "${each.value.object_prefix}/.keep"
  content = "placeholder"

  depends_on = [google_storage_bucket.raw_data_bucket]
}

# ============================================================
# Output
# ============================================================
output "raw_data_bucket_output" {
  description = "Nom du bucket d'ingestion des données"
  value       = google_storage_bucket.raw_data_bucket.name
}

output "raw_folder_paths" {
  description = "Chemins GCS créés pour le Pub/Sub Snowpipe"
  value = {
    for k, obj in google_storage_bucket_object.raw_folder_placeholders :
    k => "gs://${google_storage_bucket.raw_data_bucket.name}/${obj.name}"
  }
}
