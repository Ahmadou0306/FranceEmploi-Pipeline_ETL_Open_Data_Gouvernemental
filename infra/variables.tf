variable "project_id" {
  description = "id du projet sur GCP"
  type        = string
  default     = "training-gcp-484513" # à supprimer
}

variable "email_props" {
  description = "Adresse email du proprietaire de l'account"
  type        = string
  default     = "ahmadou.ndiaye030602@gmail.com" # A supprimer
}


variable "project_name" {
  description = "Nom du projet "
  type        = string
  default     = "france-emploi-datawarehouse"
}

variable "region" {
  description = "Région GCP par défaut"
  type        = string
  default     = "europe-west1"
}

variable "zone" {
  description = "Zone GCP par défaut"
  type        = string
  default     = "europe-west1-a"
}

variable "credentials_file" {
  description = "Chemin du credentials"
  type        = string
  default     = "config/service_account_key.json"
}

variable "environment" {
  description = "Environnement (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "kms_key_name" {
  description = "Nom complet de la clé KMS pour le chiffrement CMEK (laisser vide pour désactiver)"
  type        = string
  default     = ""
}

variable "gcp_pubsub_service_account" {
  description = "SA Snowflake pour Pub/Sub - fourni par DESC INTEGRATION notif_* (champ GCP_PUBSUB_SERVICE_ACCOUNT)"
  type        = string
  default     = "xpofmohbyo@awseuwest3-ba98.iam.gserviceaccount.com" # à remplacer
}

variable "gcs_storage_service_account" {
  description = "SA Snowflake pour GCS - fourni par DESC INTEGRATION gcs_snowpipe_integration (champ STORAGE_GCP_SERVICE_ACCOUNT)"
  type        = string
  default     = "xpofmohbyo@awseuwest3-ba98.iam.gserviceaccount.com" # à remplacer après DESC INTEGRATION gcs_snowpipe_integration
}

locals {
  flux = {
    chomeurs_indemnises = {
      topic_name    = "snowpipe-chomeurs-indemnises"
      object_prefix = "raw/chomeurs_indemnises_departement/current"
    }
    demandeurs_emploi_tranche_age = {
      topic_name    = "snowpipe-demandeurs-emploi-tranche-age"
      object_prefix = "raw/demandeurs_emploi_tranche_age/current"
    }
    offres_emploi_france_travail = {
      topic_name    = "snowpipe-offres-emploi-france-travail"
      object_prefix = "raw/offres_emploi_france_travail/current"
    }
    tranche_age = {
      topic_name    = "snowpipe-tranche-age"
      object_prefix = "raw/tranche_age/current"
    }
  }
}
