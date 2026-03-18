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
# Output
# ============================================================
output "raw_data_bucket_output" {
  description = "Nom du bucket d'ingestion des données"
  value       = google_storage_bucket.raw_data_bucket.name
}
