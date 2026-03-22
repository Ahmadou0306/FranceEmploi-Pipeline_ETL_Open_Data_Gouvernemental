-- ============================================================
-- INITIALISATION BASE DE DONNÉES ET SCHÉMAS
-- À exécuter une seule fois à la création du projet
-- ============================================================

CREATE DATABASE IF NOT EXISTS FRANCE_EMPLOI_DB;

USE DATABASE FRANCE_EMPLOI_DB;

-- Schémas de la chaîne de transformation : STAGING → INTERMEDIATE → SMART
CREATE SCHEMA IF NOT EXISTS STAGING;       -- Zone d'atterrissage : JSON brut + tables typées
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;  -- Données nettoyées et enrichies
CREATE SCHEMA IF NOT EXISTS SMART;         -- Agrégats prêts pour l'analyse

USE SCHEMA FRANCE_EMPLOI_DB.PUBLIC;


-- ============================================================
-- WAREHOUSE
-- X-SMALL : taille minimale, suffisante pour ce pipeline.
-- AUTO_SUSPEND = 60s : s'éteint après 1 minute d'inactivité.
-- AUTO_RESUME = TRUE : se rallume automatiquement à la demande.
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS FRANCE_EMPLOI_WH
    WAREHOUSE_SIZE  = 'X-SMALL'
    AUTO_SUSPEND    = 60
    AUTO_RESUME     = TRUE
    INITIALLY_SUSPENDED = TRUE;  -- Ne démarre pas tant qu'une requête n'est pas soumise

USE WAREHOUSE FRANCE_EMPLOI_WH;


-- ============================================================
-- STORAGE INTEGRATION (accès Snowflake → GCS)
--
-- Génère un STORAGE_GCP_SERVICE_ACCOUNT.
-- Après exécution, faire DESC pour récupérer ce SA et l'autoriser dans GCP :
--   IAM → Bucket france-emploi-datawarehouse-data → roles/storage.objectViewer
-- ============================================================

CREATE OR REPLACE STORAGE INTEGRATION gcs_snowpipe_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://france-emploi-datawarehouse-data/raw/');

-- Copier la valeur STORAGE_GCP_SERVICE_ACCOUNT → à ajouter dans Terraform (var.gcs_storage_service_account)
DESC INTEGRATION gcs_snowpipe_integration;


-- ============================================================
-- NOTIFICATION INTEGRATIONS (Pub/Sub GCS → Snowflake)
--
-- Génère un GCP_PUBSUB_SERVICE_ACCOUNT par intégration.
-- Ce SA doit avoir sur chaque subscription Pub/Sub :
--   - roles/pubsub.subscriber  (consommer les messages)
--   - roles/pubsub.viewer      (lire les métadonnées de la subscription)
-- Géré via la variable gcp_pubsub_service_account dans Terraform.
-- ============================================================

CREATE OR REPLACE NOTIFICATION INTEGRATION NOTIF_CHOMEURS_INDEMNISES
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    ENABLED = TRUE
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/training-gcp-484513/subscriptions/snowpipe-chomeurs-indemnises-sub';
DESC INTEGRATION NOTIF_CHOMEURS_INDEMNISES;

CREATE OR REPLACE NOTIFICATION INTEGRATION NOTIF_DEMANDEURS_EMPLOI_TRANCHE_AGE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    ENABLED = TRUE
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/training-gcp-484513/subscriptions/snowpipe-demandeurs-emploi-tranche-age-sub';
DESC INTEGRATION NOTIF_DEMANDEURS_EMPLOI_TRANCHE_AGE;

CREATE OR REPLACE NOTIFICATION INTEGRATION NOTIF_OFFRES_EMPLOI_FRANCE_TRAVAIL
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    ENABLED = TRUE
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/training-gcp-484513/subscriptions/snowpipe-offres-emploi-france-travail-sub';
DESC INTEGRATION NOTIF_OFFRES_EMPLOI_FRANCE_TRAVAIL;

CREATE OR REPLACE NOTIFICATION INTEGRATION NOTIF_TRANCHE_AGE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    ENABLED = TRUE
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/training-gcp-484513/subscriptions/snowpipe-tranche-age-sub';
DESC INTEGRATION NOTIF_TRANCHE_AGE;
