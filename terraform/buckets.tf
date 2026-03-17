# Creation du bucket pour Stockage des data
resource "google_storage_bucket" "raw_data_bucket" {
  name          = "${var.project_name}-data-${var.environment}"
  location      = var.region
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true

  versioning {
    enabled = var.environment == "prod"
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}



# Output
output "raw_data_bucket" {
  description = "Nom du bucket d'ingestion des données "
  value       = google_storage_bucket.crypto_stream_bucket.name
}
