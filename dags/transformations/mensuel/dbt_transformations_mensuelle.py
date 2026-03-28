import logging
import os
import re
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

from config.config import PROJECT_NAME
from utils.utils import get_collected_tags

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
TRANSFORMATION_NAME = "dbt_mensuelle"
DESCRIPTION = (
    "Transformations dbt mensuelles : build des modèles staging, intermediate et marts "
    "après les collectes mensuelles (chômeurs indemnisés, offres d'emploi, demandeurs). "
    "Les tests sont exécutés par dbt build avant chaque modèle downstream."
)
SCHEDULING_CRONTAB = "0 3 1 * *"  # 1er de chaque mois à 3h00 - après les collectes de minuit

# Sources mensuelles + tous leurs descendants (intermediate + marts)
# stg_tranche_age (annuel) est exclu - il est géré par le DAG annuel
DBT_SELECT = (
    "stg_chomeurs_indemnises+ "
    "stg_offres_emploi_france_travail+ "
    "stg_demandeur_emploi_tranche_age+"
)

DBT_PROJECT_DIR  = os.getenv("DBT_PROJECT_DIR",  "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")


# ─────────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────────
def _run_dbt_build(select: str) -> None:
    """
    Exécute dbt build sur la sélection donnée.
    Logue chaque ligne de sortie dbt en ERROR/WARNING/INFO selon le contenu.
    Lève une AirflowException si dbt retourne un code non nul.
    """
    cmd = [
        "dbt", "build",
        "--select", select,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--no-use-colors",
    ]

    logger.info(f"[dbt build] Lancement : {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    failed_nodes = []
    error_pattern = re.compile(r"ERROR|Failure in|Database Error|Compilation Error", re.IGNORECASE)
    node_pattern  = re.compile(r"(model|test)\s+([\w.]+)", re.IGNORECASE)

    for line in result.stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if error_pattern.search(stripped):
            logger.error(f"[dbt build] {stripped}")
            match = node_pattern.search(stripped)
            if match and match.group(2) not in failed_nodes:
                failed_nodes.append(match.group(2))
        elif "WARN" in stripped:
            logger.warning(f"[dbt build] {stripped}")
        else:
            logger.info(f"[dbt build] {stripped}")

    for line in result.stderr.splitlines():
        if line.strip():
            logger.error(f"[dbt build] stderr : {line.strip()}")

    if result.returncode != 0:
        detail    = f"nœuds en échec : {failed_nodes}" if failed_nodes else "voir les logs ci-dessus"
        error_msg = f"dbt build a échoué (code={result.returncode}) - {detail}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

    logger.info(f"[dbt build] Terminé avec succès - sélection : {select!r}")


# ─────────────────────────────────────────────
# TASK
# ─────────────────────────────────────────────
def dbt_build(**kwargs) -> None:
    _run_dbt_build(DBT_SELECT)


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────
default_args = {
    "owner":             "ahmad",
    "depends_on_past":   False,
    "email":             ["ahmadou.ndiaye030602@gmail.com"],
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    f"{PROJECT_NAME}_{TRANSFORMATION_NAME}",
    default_args=default_args,
    description=DESCRIPTION,
    start_date=datetime(2026, 3, 1),
    schedule=SCHEDULING_CRONTAB,
    catchup=False,
    max_active_runs=1,
    tags=get_collected_tags(TRANSFORMATION_NAME, "monthly"),
) as dag:

    build_task = PythonOperator(
        task_id="dbt_build",
        python_callable=dbt_build,
    )
