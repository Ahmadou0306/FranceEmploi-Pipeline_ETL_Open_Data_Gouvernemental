# Initialisation du pipeline Snowflake

Ce dossier contient les scripts SQL à exécuter **une seule fois** pour initialiser les objets Snowflake.
L'infrastructure GCP (bucket, topics, subscriptions, IAM) est gérée séparément par Terraform.

---

## Prérequis

- Terraform appliqué avec succès (`terraform apply` dans `terraform/`)
- SnowSQL installé et configuré (`snowsql -c <connexion>`)
- Accès ACCOUNTADMIN ou SYSADMIN sur le compte Snowflake

---

## Ordre d'exécution

### Étape 1 — `connexion-snowflake-GCS.sql`
Crée la base de données, les schémas, la Storage Integration et les Notification Integrations.

```bash
snowsql -f connexion-snowflake-GCS.sql
```

> **Action manuelle requise après cette étape :**
>
> 1. Récupérer le `STORAGE_GCP_SERVICE_ACCOUNT` via `DESC INTEGRATION gcs_snowpipe_integration`
> 2. L'ajouter à Terraform comme valeur de `var.gcs_storage_service_account` et relancer `terraform apply`
>    → accorde `roles/storage.objectViewer` sur le bucket GCS
>
> 2. Récupérer le `GCP_PUBSUB_SERVICE_ACCOUNT` via `DESC INTEGRATION NOTIF_CHOMEURS_INDEMNISES` (même SA pour toutes les intégrations)
> 3. L'ajouter à Terraform comme valeur de `var.gcp_pubsub_service_account` et relancer `terraform apply`
>    → accorde `roles/pubsub.subscriber` + `roles/pubsub.viewer` sur les subscriptions

---

### Étape 2 — `creation-stages-and-tables-raw.sql`
Crée le File Format JSON et les Stages externes pointant vers les dossiers GCS.

```bash
snowsql -f creation-stages-and-tables-raw.sql
```

---

### Étape 3 — `snowpipe.sql`
Crée les tables RAW (zone d'atterrissage JSON) et les Pipes Snowpipe avec auto-ingest.

```bash
snowsql -f snowpipe.sql
```

> Après création des pipes, vérifier leur statut :
> ```sql
> SELECT SYSTEM$PIPE_STATUS('FRANCE_EMPLOI_DB.PUBLIC.PIPE_CHOMEURS_INDEMNISES');
> ```

---

## Script automatisé

Le script `init.sh` enchaîne les étapes 1 à 3.
Il s'arrête avant l'étape 2 pour permettre la configuration manuelle des SA dans GCP/Terraform.

```bash
bash init.sh
```
