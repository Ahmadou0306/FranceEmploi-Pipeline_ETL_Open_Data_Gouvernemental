terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
  }

  # Pour stocker le state 
  # Créer manuellement le bucket avec gsutil mb -l europe-west1 gs://france-emploi-datawarehouse-state
  backend "gcs" {
    bucket      = "france-emploi-datawarehouse-state"
    prefix      = "terraform/state"
    credentials = "config/service_account_key.json"
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  zone        = var.zone
  credentials = file(var.credentials_file)
}