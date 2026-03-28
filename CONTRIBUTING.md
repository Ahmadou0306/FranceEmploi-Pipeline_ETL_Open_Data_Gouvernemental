# Guide de contribution

Merci de l'intérêt porté au projet. Ce document explique comment proposer des modifications de manière cohérente avec l'architecture existante.

---

## Table des matières

1. [Prérequis](#1-prérequis)
2. [Mettre en place l'environnement local](#2-mettre-en-place-lenvironnement-local)
3. [Workflow Git](#3-workflow-git)
4. [Ajouter une nouvelle source de données](#4-ajouter-une-nouvelle-source-de-données)
5. [Ajouter un modèle dbt](#5-ajouter-un-modèle-dbt)
6. [Conventions de nommage](#6-conventions-de-nommage)
7. [Tests](#7-tests)
8. [Ouvrir une Pull Request](#8-ouvrir-une-pull-request)

---

## 1. Prérequis

- Docker Desktop ≥ 4.x et Docker Compose V2
- Python 3.11+
- dbt-snowflake 1.11+
- Terraform ≥ 1.5 (modifications infra uniquement)
- Accès en lecture au fichier `.env.exemple` pour créer votre `.env`

---

## 2. Mettre en place l'environnement local

```bash
# Cloner le dépôt
git clone <url-du-repo>
cd FranceEmploi-Pipeline_ETL_Open_Data_Gouvernemental

# Copier les variables d'environnement
cp .env.exemple .env
# Renseigner les valeurs manquantes dans .env

# Démarrer la stack Airflow
docker compose up -d

# Vérifier que les conteneurs tournent
docker compose ps
```

Airflow est disponible sur `http://localhost:8081` (identifiants par défaut : `airflow` / `airflow`).

---

## 3. Workflow Git

```
main           ← branche stable, protégée
  └── feat/<sujet>    ← nouvelle fonctionnalité
  └── fix/<sujet>     ← correction de bug
  └── docs/<sujet>    ← documentation uniquement
  └── infra/<sujet>   ← modifications Terraform
```

```bash
# Créer une branche à partir de main
git checkout main && git pull
git checkout -b feat/nom-de-la-feature

# Travailler, committer avec des messages clairs
git commit -m "feat(dag): ajouter collecte trimestrielle DARES"

# Pousser et ouvrir une PR
git push -u origin feat/nom-de-la-feature
```

**Format des messages de commit** (Conventional Commits) :

| Préfixe | Usage |
|---------|-------|
| `feat`  | Nouvelle fonctionnalité |
| `fix`   | Correction de bug |
| `docs`  | Documentation seule |
| `refactor` | Réécriture sans changement de comportement |
| `test`  | Ajout ou modification de tests |
| `infra` | Terraform / GCP / Snowflake |
| `chore` | Maintenance (deps, CI, etc.) |

---

## 4. Ajouter une nouvelle source de données

Chaque source suit le même patron à trois tâches : `archive_current → extract_data → upload_to_gcs`.

### 4a. Créer le DAG de collecte

Copier le fichier le plus proche de votre cas d'usage (ex. [dags/collected/mensuelle/emploi.py](dags/collected/mensuelle/emploi.py)) et modifier **uniquement** :

```python
COLLECTED_NAME = "nom_snake_case_de_la_source"   # identifiant unique
BASE_URL       = "https://..."                    # URL de l'API
DESCRIPTION    = "..."
SCHEDULING_CRONTAB = "0 0 1 * *"                 # adapter si besoin
```

Le `DAG_ID` sera automatiquement `france_emploi_<COLLECTED_NAME>`.

### 4b. Nommer le fichier

```
dags/collected/<fréquence>/<COLLECTED_NAME>.py
```

Exemples de fréquences valides : `mensuelle/`, `annuelle/`, `trimestrielle/`, `journalier/`.

### 4c. Initialiser Snowflake

Ajouter la table RAW et le stage GCS correspondants dans [init-snowflake-pipeline/creation-stages-and-tables-raw.sql](init-snowflake-pipeline/creation-stages-and-tables-raw.sql), puis la Snowpipe dans [init-snowflake-pipeline/snowpipe.sql](init-snowflake-pipeline/snowpipe.sql).

### 4d. Ajouter le topic Pub/Sub (si nécessaire)

Déclarer le nouveau topic dans [infra/pub_sub.tf](infra/pub_sub.tf) et les permissions dans [infra/iam.tf](infra/iam.tf).

---

## 5. Ajouter un modèle dbt

```
france_emploi_transformations/models/
  staging/      ← parsing JSON → types SQL
  intermediate/ ← jointures, calculs dérivés (vues)
  marts/        ← tables analytiques finales (tables matérialisées)
```

**Règles :**

- Staging : une source = un fichier `stg_<COLLECTED_NAME>.sql`. Ne faire que du parsing JSON et du cast de types, aucune logique métier.
- Intermediate : préfixe `int_`. Matérialisation en `view`.
- Marts : préfixe `mart_` ou `dim_`. Matérialisation en `table`, clustered sur `(departement, date_mois)` si pertinent.
- Ajouter les tests `not_null` et `unique` dans `schema.yml` pour toute nouvelle colonne clé.

Lancer le build localement avant d'ouvrir la PR :

```bash
cd france_emploi_transformations/
dbt build --select <nom_du_modele>+
```

---

## 6. Conventions de nommage

| Élément | Convention | Exemple |
|---------|-----------|---------|
| Fichier DAG | `snake_case`, nom de la source, sans faute | `emploi.py` |
| `COLLECTED_NAME` | `snake_case`, descriptif | `offres_emploi_france_travail` |
| Modèle dbt staging | `stg_<COLLECTED_NAME>` | `stg_offres_emploi_france_travail` |
| Modèle dbt mart | `mart_<sujet>` ou `dim_<sujet>` | `mart_territoire` |
| Variable Terraform | `snake_case` | `gcs_bucket_name` |
| Colonne SQL | `snake_case` | `taux_chomage` |

> Les noms avec une faute d'orthographe (ex. `emploie` au lieu de `emploi`) ne seront pas acceptés en PR.

---

## 7. Tests

### Tests dbt

Les tests de schéma sont définis dans `schema.yml`. Pour tout nouveau mart, ajouter au minimum :

```yaml
columns:
  - name: departement
    tests:
      - not_null
      - relationships:
          to: ref('dim_territoire')
          field: code_departement
```

Les tests singuliers (ex. `assert_taux_chomage_valide.sql`) se placent dans `france_emploi_transformations/tests/`.

### Tests Python (DAGs)

Les DAGs étant des scripts Airflow sans logique métier complexe, les tests se concentrent sur les fonctions utilitaires dans `dags/utils/utils.py`. Tout ajout à `utils.py` doit être couvert.

```bash
# Depuis la racine du projet
docker compose exec airflow-worker python -m pytest dags/tests/ -v
```

---

## 8. Ouvrir une Pull Request

1. S'assurer que `dbt build` passe sans erreur ni warning.
2. Vérifier qu'aucun secret (clé JSON, mot de passe, token) n'est commité — consulter `.gitignore`.
3. Remplir le template de PR :
   - **Contexte** : pourquoi ce changement ?
   - **Changements** : ce qui a été ajouté/modifié/supprimé.
   - **Tests effectués** : commandes exécutées et résultats.
4. Cibler la branche `main`.
5. Un reviewer doit approuver avant le merge.

---

Pour toute question, ouvrir une issue sur le dépôt.
