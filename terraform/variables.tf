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
  default     = "../config/gcp/airflow-gcp-key.json"
}

variable "environment" {
  description = "Environnement (dev, staging, prod)"
  type        = string
  default     = "dev"
}
