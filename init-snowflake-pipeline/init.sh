#!/bin/bash
# =============================================================
# Initialisation du pipeline Snowflake — France Emploi
# Exécute les scripts SQL dans l'ordre requis.
#
# Usage : bash init.sh
# Prérequis : snowsql installé, terraform apply déjà effectué
# =============================================================

set -e  # Arrêt immédiat en cas d'erreur

# --- Configuration connexion SnowSQL ---
SNOWSQL_ACCOUNT="${SNOWSQL_ACCOUNT:-}"       # ex: xy12345.eu-west-1
SNOWSQL_USER="${SNOWSQL_USER:-}"             # ex: admin
SNOWSQL_DATABASE="FRANCE_EMPLOI_DB"
SNOWSQL_WAREHOUSE="${SNOWSQL_WAREHOUSE:-COMPUTE_WH}"
SNOWSQL_ROLE="${SNOWSQL_ROLE:-SYSADMIN}"

if [[ -z "$SNOWSQL_ACCOUNT" || -z "$SNOWSQL_USER" ]]; then
    echo "ERREUR : Définir les variables SNOWSQL_ACCOUNT et SNOWSQL_USER avant d'exécuter ce script."
    echo "  export SNOWSQL_ACCOUNT=xy12345.eu-west-1"
    echo "  export SNOWSQL_USER=admin"
    exit 1
fi

SNOWSQL_CMD="snowsql -a $SNOWSQL_ACCOUNT -u $SNOWSQL_USER -r $SNOWSQL_ROLE -w $SNOWSQL_WAREHOUSE"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ""
echo "============================================================="
echo " ÉTAPE 1 — Initialisation DB, schémas et intégrations GCS"
echo "============================================================="
$SNOWSQL_CMD -f "$DIR/connexion-snowflake-GCS.sql"

echo ""
echo "============================================================="
echo " ACTION MANUELLE REQUISE"
echo "============================================================="
echo ""
echo " 1. Dans Snowflake, exécuter :"
echo "      DESC INTEGRATION gcs_snowpipe_integration;"
echo "    → Copier STORAGE_GCP_SERVICE_ACCOUNT dans var.gcs_storage_service_account"
echo ""
echo " 2. Dans Snowflake, exécuter :"
echo "      DESC INTEGRATION NOTIF_CHOMEURS_INDEMNISES;"
echo "    → Copier GCP_PUBSUB_SERVICE_ACCOUNT dans var.gcp_pubsub_service_account"
echo ""
echo " 3. Relancer : cd terraform && terraform apply"
echo ""
read -r -p " Appuyer sur [Entrée] une fois Terraform appliqué pour continuer..."

echo ""
echo "============================================================="
echo " ÉTAPE 2 — Création des stages externes GCS"
echo "============================================================="
$SNOWSQL_CMD -f "$DIR/creation-stages-and-tables-raw.sql"

echo ""
echo "============================================================="
echo " ÉTAPE 3 — Création des tables RAW et pipes Snowpipe"
echo "============================================================="
$SNOWSQL_CMD -f "$DIR/snowpipe.sql"

echo ""
echo "============================================================="
echo " Initialisation terminée."
echo " Vérifier le statut des pipes dans Snowflake :"
echo "   SELECT SYSTEM\$PIPE_STATUS('FRANCE_EMPLOI_DB.PUBLIC.PIPE_CHOMEURS_INDEMNISES');"
echo "============================================================="
