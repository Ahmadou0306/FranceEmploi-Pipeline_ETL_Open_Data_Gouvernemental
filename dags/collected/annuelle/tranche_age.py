import json
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import storage

from config.config import GCS_BUCKET_NAME, PROJECT_NAME
from utils.utils import build_gcs_current_path, build_gcs_archive_path, get_collected_tags, fetch_xml_json_with_retry

logger = logging.getLogger(__name__)

COLLECTED_NAME = "tranche_age"
BASE_URL = "https://api.insee.fr/melodi/data/DS_RP_POPULATION_PRINC"
DESCRIPTION = (
    "Population par sexe, tranche d'âge (Y_LT15, Y15T24, Y25T54, Y55T64, Y_GE65) et département, issue du Recensement de la Population. "
    "Retourne des données Annuelles. "
    f"Source: {BASE_URL}"
)
SCHEDULING_CRONTAB = "0 0 1 1 *"





# ─────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────
def extract_data(ti,**kwargs):
    logger.info(f"Début extraction | URL: {BASE_URL}")

    all_observations = []
    current_page = 1
    is_last_page = False
    max_retries = 3
    maw_row_result = 10000

    while not is_last_page: 
        params = {"maxResult": maw_row_result, "page": current_page}
        page_data = fetch_xml_json_with_retry(BASE_URL, params, logger, max_retries=max_retries)

        # On extrait uniquement les observations, pas la réponse entière
        observations = page_data.get("observations", [])
        all_observations.extend(observations)
        is_last_page = page_data.get("paging", {}).get("isLast", True)

        logger.info(
            f"Page {current_page} récupérée - "
            f"{len(observations)} obs | isLast={is_last_page}"
        )
        current_page += 1

    nb_records = len(all_observations)
    logger.info(f"Extraction terminée : {nb_records} enregistrements au total")

    ti.xcom_push(key=f"{COLLECTED_NAME}_data", value=json.dumps(all_observations, ensure_ascii=False))
    ti.xcom_push(key=f"{COLLECTED_NAME}_nb_record", value=nb_records)
    return nb_records


# ─────────────────────────────────────────────
# MOVE CURRENT FILE TO ARCHIVE
# ─────────────────────────────────────────────

def archive_current_if_exists(ti, **kwargs) -> bool:
    
    dt = kwargs.get("logical_date") or datetime.now()

    current_path = build_gcs_current_path(COLLECTED_NAME, "json")
    archive_path = build_gcs_archive_path(COLLECTED_NAME, "json", "yearly", dt)
    
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        current_blob = bucket.blob(current_path)
        if not current_blob.exists():
            logger.info(f"[archive] Aucun fichier current trouvé : {current_path}")
            return False
        bucket.copy_blob(current_blob, bucket, archive_path)
        current_blob.delete()
        logger.info(f"[archive] {current_path} -> {archive_path}")
        return True
    except Exception as e:
        logger.error(f"Erreur upload GCS : {e}")
        raise

# ─────────────────────────────────────────────
# UPLOAD GCS
# ─────────────────────────────────────────────
def upload_to_gcs(ti, **kwargs):
    logger.info("Début upload vers GCS")

    json_string = ti.xcom_pull(key=f"{COLLECTED_NAME}_data", task_ids="extract_data")
    nb_records = ti.xcom_pull(key=f"{COLLECTED_NAME}_nb_record", task_ids="extract_data")

    if not json_string:
        raise ValueError("Aucune donnée récupérée depuis XCom")

    gcs_path = build_gcs_current_path(COLLECTED_NAME, "json")

    file_size_mb = len(json_string.encode("utf-8")) / (1024 * 1024)
    destination = f"gs://{GCS_BUCKET_NAME}/{gcs_path}"
    logger.info(f"Destination: {destination} | Taille: {file_size_mb:.2f} MB")

    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(json_string, content_type="application/json")
        logger.info(f"Upload réussi : {destination} | {nb_records} enregistrements | {file_size_mb:.2f} MB")
        return destination

    except Exception as e:
        logger.error(f"Erreur upload GCS : {e}")
        raise


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────
default_args = {
    "owner": "ahmad",
    "depends_on_past": False,
    "email": ["ahmadou.ndiaye030602@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    f"{PROJECT_NAME}_{COLLECTED_NAME}",
    default_args=default_args,
    description=DESCRIPTION,
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULING_CRONTAB,
    catchup=True,
    max_active_runs=1,
    tags=get_collected_tags(COLLECTED_NAME, "yearly"),
) as dag:

    archive_current_if_exists_task = PythonOperator(
        task_id="archive_current_if_exists",
        python_callable=archive_current_if_exists,
    )
    
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        trigger_rule="all_success",
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        trigger_rule="all_success",
    )

    archive_current_if_exists_task >> extract_task >> upload_task