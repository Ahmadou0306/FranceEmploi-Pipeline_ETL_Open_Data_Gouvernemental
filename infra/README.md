# Infrastructure - France Emploi Pipeline

Terraform gérant les ressources GCP nécessaires à l'ingestion des données :
- **Bucket GCS** `france-emploi-datawarehouse-data` - stockage des fichiers raw
- **Topics & Subscriptions Pub/Sub** - 4 flux (un par source de données) déclenchant Snowpipe
- **IAM** - permissions entre GCS, Pub/Sub et les service accounts Snowflake

---

## Prérequis

| Outil | Version minimale |
|-------|-----------------|
| Terraform | >= 1.0 |
| gcloud CLI | toute version récente |
| Accès GCP | Owner ou Editor sur le projet |

---

## 1. Service Account Terraform - `config/service_account_key.json`

Ce fichier est la clé d'authentification utilisée **uniquement par Terraform** pour créer les ressources GCP. Il s'agit du premier des deux service accounts du projet, appliquant le principe du moindre privilège : ce compte admin ne quitte jamais le poste de déploiement et n'est jamais embarqué dans les conteneurs applicatifs.

La clé doit être placée dans **`infra/config/service_account_key.json`** avant de lancer `terraform init`. Ce chemin est listé dans `.gitignore` et ne doit jamais être commité.

### Créer le service account et télécharger la clé

```bash
# 1. Créer le service account
gcloud iam service-accounts create terraform-sa \
  --display-name="Terraform Service Account" \
  --project=<PROJECT_ID>

# 2. Télécharger la clé JSON dans le bon répertoire (infra/config/)
gcloud iam service-accounts keys create config/service_account_key.json \
  --iam-account=terraform-sa@<PROJECT_ID>.iam.gserviceaccount.com
```

### Attribuer les rôles requis - `setup_config.sh`

Le script `setup_config.sh` attribue les rôles IAM nécessaires au service account Terraform.

**Rôles attribués :**

| Rôle | Raison |
|------|--------|
| `roles/iam.roleAdmin` | Gérer les rôles personnalisés |
| `roles/iam.serviceAccountAdmin` | Créer/gérer les service accounts |
| `roles/monitoring.admin` | Accorder les droits monitoring à Snowflake |
| `roles/pubsub.admin` | Créer topics, subscriptions et leurs permissions |
| `roles/resourcemanager.projectIamAdmin` | Attribuer des rôles au niveau projet |
| `roles/storage.admin` | Créer et configurer le bucket GCS |

**Exécution :**

```bash
# Depuis le répertoire infra/
bash setup_config.sh
# → Saisir le project ID  (ex: training-gcp-484513)
# → Saisir l'email du SA  (ex: terraform-sa@training-gcp-484513.iam.gserviceaccount.com)
```

### Vérifier les rôles effectivement attribués

Après exécution de `setup_config.sh`, vérifier que les rôles ont bien été appliqués :

```bash
gcloud projects get-iam-policy <PROJECT_ID> \
  --flatten="bindings[].members" \
  --filter="bindings.members:terraform-sa@<PROJECT_ID>.iam.gserviceaccount.com" \
  --format="table(bindings.role)"
```

La sortie doit lister les six rôles du tableau ci-dessus. Cette commande est également utile pour auditer les permissions du service account Airflow (`airflow-gcp-key`) ou de tout autre SA du projet en substituant l'email correspondant.

---

## 2. Bucket Terraform State

Le state Terraform est stocké dans un bucket GCS. À créer **une seule fois** manuellement avant `terraform init` :

```bash
gsutil mb -l europe-west1 gs://france-emploi-datawarehouse-state
```

---

## 3. Variables Terraform

Copier `variables.tf` comme base et renseigner les valeurs dans un fichier `terraform.tfvars` (non commité) :

```hcl
# infra/terraform.tfvars  (à créer, ignoré par git via *.tfvars si ajouté au .gitignore)
project_id   = "ton-project-id"
email_props  = "ton-email@gmail.com"
environment  = "dev"

# Renseignés après la première étape Snowflake (voir Section 4)
gcs_storage_service_account = ""
gcp_pubsub_service_account  = ""
```

**Variables obligatoires sans défaut :**

| Variable | Description | Comment l'obtenir |
|----------|-------------|-------------------|
| `project_id` | ID du projet GCP | Console GCP |
| `email_props` | Email du propriétaire | Ton compte GCP |
| `gcs_storage_service_account` | SA Snowflake pour GCS | `DESC INTEGRATION gcs_snowpipe_integration` → champ `STORAGE_GCP_SERVICE_ACCOUNT` |
| `gcp_pubsub_service_account` | SA Snowflake pour Pub/Sub | `DESC INTEGRATION notif_*` → champ `GCP_PUBSUB_SERVICE_ACCOUNT` |

---

## 4. Déploiement - ordre des étapes

Le déploiement se fait en **deux passes Terraform** car les SA Snowflake ne sont connus qu'après la première intégration Snowflake.

### Étape 1 - Infrastructure de base

```bash
cd infra/

terraform init
terraform plan -var="project_id=<PROJECT_ID>" -var="email_props=<EMAIL>" \
               -var="gcs_storage_service_account=placeholder@x.iam.gserviceaccount.com" \
               -var="gcp_pubsub_service_account=placeholder@x.iam.gserviceaccount.com"

terraform apply ...  # mêmes variables
```

Cela crée : bucket GCS, topics Pub/Sub, subscriptions.

### Étape 2 - Récupérer les SA Snowflake

Dans Snowflake, exécuter :

```sql
-- SA pour l'intégration GCS Storage
DESC INTEGRATION gcs_snowpipe_integration;
-- → Copier STORAGE_GCP_SERVICE_ACCOUNT

-- SA pour les intégrations Pub/Sub
DESC INTEGRATION NOTIF_CHOMEURS_INDEMNISES;
-- → Copier GCP_PUBSUB_SERVICE_ACCOUNT
```

### Étape 3 - Appliquer les permissions IAM Snowflake

```bash
terraform apply \
  -var="project_id=<PROJECT_ID>" \
  -var="email_props=<EMAIL>" \
  -var="gcs_storage_service_account=<STORAGE_GCP_SERVICE_ACCOUNT>" \
  -var="gcp_pubsub_service_account=<GCP_PUBSUB_SERVICE_ACCOUNT>"
```

Cela attribue aux SA Snowflake les droits `pubsub.subscriber`, `pubsub.viewer`, `storage.objectViewer` et `monitoring.viewer`.

---

## 5. Ressources créées

```
GCS
└── france-emploi-datawarehouse-data/
    ├── raw/chomeurs_indemnises_departement/current/
    ├── raw/demandeurs_emploi_tranche_age/current/
    ├── raw/offres_emploi_france_travail/current/
    └── raw/tranche_age/current/

Pub/Sub Topics + Subscriptions (×4)
├── snowpipe-chomeurs-indemnises          → snowpipe-chomeurs-indemnises-sub
├── snowpipe-demandeurs-emploi-tranche-age → ...
├── snowpipe-offres-emploi-france-travail  → ...
└── snowpipe-tranche-age                   → ...
```

---

## 6. Destruction

```bash
# Hors prod uniquement (force_destroy = true en dev)
terraform destroy \
  -var="project_id=<PROJECT_ID>" \
  -var="email_props=<EMAIL>" \
  -var="gcs_storage_service_account=<SA>" \
  -var="gcp_pubsub_service_account=<SA>"
```
