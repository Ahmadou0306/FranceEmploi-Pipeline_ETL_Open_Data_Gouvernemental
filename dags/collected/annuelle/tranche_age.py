import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import storage

from config.config import GCS_BUCKET_NAME, PROJECT_NAME
from utils.utils import (
    build_gcs_current_path,
    build_gcs_archive_path,
    get_collected_tags,
    fetch_xml_json_with_retry,
)

logger = logging.getLogger(__name__)

COLLECTED_NAME = "tranche_age"
BASE_URL       = "https://api.insee.fr/melodi/data/DS_RP_POPULATION_PRINC"
DESCRIPTION    = (
    "Population par sexe, tranche d'âge (Y_LT15, Y15T24, Y25T54, Y55T64, Y_GE65) "
    "et département, issue du Recensement de la Population. "
    "Retourne des données annuelles. "
    f"Source: {BASE_URL}"
)
SCHEDULING_CRONTAB = "0 0 1 1 *"
MAX_PAGES          = 500   # Garde-fou contre une boucle infinie
MAX_ROW_RESULT     = 1000  # Taille de page - rester sous la limite de 10 000 résultats accessibles
START_YEAR         = 2020  # Début du recensement annuel tournant


# ==========================================================
# ARCHIVE
# ==========================================================
def archive_current_if_exists(ti, **kwargs) -> bool:
    """Déplace le fichier current vers l'archive avant une nouvelle extraction."""

    # Airflow 3 peut passer logical_date comme string ISO
    logical_date = kwargs.get("logical_date")
    if isinstance(logical_date, str):
        dt = datetime.fromisoformat(logical_date)
    else:
        dt = logical_date or datetime.now()

    current_path = build_gcs_current_path(COLLECTED_NAME, "json")
    archive_path = build_gcs_archive_path(COLLECTED_NAME, "json", "yearly", dt)

    try:
        client       = storage.Client()
        bucket       = client.bucket(GCS_BUCKET_NAME)
        current_blob = bucket.blob(current_path)

        if not current_blob.exists():
            logger.info(f"[archive] Aucun fichier current trouvé : {current_path}")
            return False

        bucket.copy_blob(current_blob, bucket, archive_path)
        current_blob.delete()
        logger.info(f"[archive] {current_path} -> {archive_path}")
        return True

    except Exception as e:
        logger.error(f"[archive] Erreur GCS : {e}")
        raise


# ==========================================================
# EXTRACT + UPLOAD GCS (streaming)
# ==========================================================
def extract_data(ti, **kwargs) -> int:
    """Extrait les données et les upload directement vers GCS pour éviter l'OOM."""
    logger.info(f"Début extraction | URL: {BASE_URL}")

    max_retries  = 3
    end_year     = datetime.now().year
    gcs_path     = build_gcs_current_path(COLLECTED_NAME, "json")
    destination  = f"gs://{GCS_BUCKET_NAME}/{gcs_path}"

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob   = bucket.blob(gcs_path)

    nb_records = 0

    with blob.open("w", content_type="application/json") as gcs_file:
        gcs_file.write("[")
        first_record = True

        for year in range(end_year, START_YEAR - 1, -1):
            if year < 2015:  # Petit stop pour test et éviter de tout télécharger
                break

            current_page = 1
            is_last_page = False

            while not is_last_page and current_page <= MAX_PAGES:
                params = {
                    "maxResult":   MAX_ROW_RESULT,
                    "page":        current_page,
                    "startPeriod": str(year),
                    "endPeriod":   str(year),
                }
                page_data    = fetch_xml_json_with_retry(BASE_URL, params, logger, max_retries=max_retries)
                observations = page_data.get("observations", [])
                is_last_page = page_data.get("paging", {}).get("isLast", True)

                for obs in observations:
                    if not first_record:
                        gcs_file.write(",")
                    gcs_file.write(json.dumps(obs, ensure_ascii=False))
                    first_record = False

                nb_records += len(observations)
                logger.info(
                    f"Année {year} | Page {current_page} - "
                    f"{len(observations)} obs | isLast={is_last_page}"
                )
                current_page += 1

            if current_page > MAX_PAGES:
                logger.warning(
                    f"[extract] Année {year} - limite de sécurité atteinte ({MAX_PAGES} pages). "
                    "Les données peuvent être incomplètes."
                )

            logger.info(f"Année {year} terminée")

        gcs_file.write("]")

    logger.info(f"Extraction + upload terminés : {nb_records} enregistrements → {destination}")

    ti.xcom_push(key=f"{COLLECTED_NAME}_nb_record", value=nb_records)
    ti.xcom_push(key=f"{COLLECTED_NAME}_gcs_path",  value=destination)
    return nb_records


# ==========================================================
# UPLOAD GCS
# ==========================================================
def upload_to_gcs(ti, **kwargs) -> str:
    """L'upload est désormais fait dans extract_data - cette tâche valide simplement le résultat."""
    destination = ti.xcom_pull(key=f"{COLLECTED_NAME}_gcs_path",  task_ids="extract_data")
    nb_records  = ti.xcom_pull(key=f"{COLLECTED_NAME}_nb_record", task_ids="extract_data")

    if not destination:
        raise ValueError("Chemin GCS introuvable dans XCom - l'extraction a peut-être échoué.")

    logger.info(f"Upload confirmé : {destination} | {nb_records} enregistrements")
    return destination


# ==========================================================
# DAG
# ==========================================================
default_args = {
    "owner":            "ahmad",
    "depends_on_past":  False,
    "email":            ["ahmadou.ndiaye030602@gmail.com"],
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    f"{PROJECT_NAME}_{COLLECTED_NAME}",
    default_args=default_args,
    description=DESCRIPTION,
    start_date=datetime(2026, 3, 1),
    schedule=SCHEDULING_CRONTAB,
    catchup=True,
    max_active_runs=1,
    tags=get_collected_tags(COLLECTED_NAME, "yearly"),
) as dag:

    archive_task = PythonOperator(
        task_id="archive_current_if_exists",
        python_callable=archive_current_if_exists,
    )

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        trigger_rule="all_done",
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        trigger_rule="all_success",
    )

    archive_task >> extract_task >> upload_task