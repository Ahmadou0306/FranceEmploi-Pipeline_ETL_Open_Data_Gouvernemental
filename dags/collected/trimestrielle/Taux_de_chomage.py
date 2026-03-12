import json
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta, datetime
import time
from google.cloud import storage
from airflow import DAG
import requests
from io import StringIO
from google.cloud import bigquery
import xml.etree.ElementTree as ET


import logging
logger = logging.getLogger(__name__)

from config.config import PROJECT_NAME, GCS_BUCKET_NAME
from utils.utils import get_collected_tags, build_gcs_path


COLLECTED_NAME="taux_de_chomage_insee"
BASE_URL = "https://api.insee.fr/series/BDM/V1/data/SERIES_BDM/001688370"
SCHEDULING_CRONTAB = '0 0 1 */3 *', # Toutes les Jours à 3 mois


def upload_to_gcs(ti, **kwargs):
    logger.info("DEBUT UPLOAD VERS GCS")
    logger.info(kwargs)

    data = ti.xcom_pull(key=f'{COLLECTED_NAME}_json', task_ids='convert_xml_to_json_task')
    nb_record = ti.xcom_pull(key=f'{COLLECTED_NAME}_nb_record', task_ids='convert_xml_to_json_task')
    
    logger.info(f"Données récupérées depuis XCom:")

    
    # construction du chemin GCS
    gcs_path = build_gcs_path(COLLECTED_NAME,"quarterly","json",datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info(f"Build du chemin dans GCS:")
    
    try:
        # Initialiser client GCS
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(gcs_path)

        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        file_size_mb = len(json_data) / (1024 * 1024)
        
        logger.info(f"Taille: {file_size_mb:.2f} MB")
        logger.info(f"Destination: gs://{GCS_BUCKET_NAME}/{gcs_path}")
        
        # Upload
        blob.upload_from_string(
            json_data,
            content_type='application/json'
        )
        
        logger.info(f"Upload réussi!")
        logger.info(f"gs://{GCS_BUCKET_NAME}/{gcs_path}")
        logger.info(f"{nb_record} enregistrements")
        logger.info(f"{file_size_mb:.2f} MB")

        return f"gs://{GCS_BUCKET_NAME}/{gcs_path}"
    except Exception as e:
        logger.error(f"Erreur lors de l'upload: {e}")
        raise



def convert_xml_to_json(ti):
    data = ti.xcom_pull(key=f'{COLLECTED_NAME}_data', task_ids='extract_data')
    if not data:
        logger.warning("Aucune donnée XML à transformer")
        return 0
    try:
        # Parser le XML
        root = ET.fromstring(data)

        # Namespaces présents dans le XML
        ns = {
            'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'ss':      'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific',
            'ns1':     'urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=FR1:SERIES_BDM(1.0):ObsLevelDim:TIME_PERIOD',
        }

        # Extraire les métadonnées de la série
        dataset = root.find('message:DataSet', ns)
        series  = dataset.find('ns1:Series', ns)

        metadata = {
            'idbank':       series.get('IDBANK'),
            'freq':         series.get('FREQ'),
            'title_fr':     series.get('TITLE_FR'),
            'title_en':     series.get('TITLE_EN'),
            'last_update':  series.get('LAST_UPDATE'),
            'unit_measure': series.get('UNIT_MEASURE'),
            'unit_mult':    series.get('UNIT_MULT'),
            'ref_area':     series.get('REF_AREA'),
            'decimals':     series.get('DECIMALS'),
        }

        # Extraire les observations
        observations = []
        for obs in series.findall('ns1:Obs', ns):
            observations.append({
                'time_period': obs.get('TIME_PERIOD'),
                'obs_value':   int(obs.get('OBS_VALUE')),
                'obs_status':  obs.get('OBS_STATUS'),
                'obs_qual':    obs.get('OBS_QUAL'),
                'obs_type':    obs.get('OBS_TYPE'),
            })

        # Structure JSON finale
        result = {
            'metadata':     metadata,
            'observations': observations,
            'nb_records':   len(observations)
        }


        nb_record = len(observations)
        logger.info(f"Transformation réussie: {nb_record} observations")
        # Stocker dans XCom
        ti.xcom_push(key=f'{COLLECTED_NAME}_json', value=json.dumps(result))
        ti.xcom_push(key=f'{COLLECTED_NAME}_nb_record', value=nb_record)

        return nb_record
    except ET.ParseError as e:
        logger.error(f"Erreur de parsing XML: {e}")
        raise Exception(f"Erreur de parsing XML: {e}")


def extract_data(ti,**kwargs):
    execution_date = kwargs.get('logical_date') or kwargs.get('execution_date')
    logger.info(f"Execution date: {execution_date}")
    logger.info(f"URL: {BASE_URL}")

    max_retries=3

    for attempt in range(max_retries):
        try:
            response = requests.get(BASE_URL)
            if response.status_code == 200:
                data = response.text
                logger.info(f"Données XML Récupéré")
                break
            else:
                raise requests.exceptions.RequestException(f"Erreur lors de l'appel de l'API: {response.status_code}-{response.text}")
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout (tentative {attempt + 1}/{max_retries})")
            if attempt == max_retries - 1:
                raise Exception(f"Timeout après {max_retries} tentatives")
            time.sleep(2 ** attempt)  # Backoff exponentiel
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur API: {e}")
            if attempt == max_retries - 1:
                raise Exception(f"Erreur API après {max_retries} tentatives: {e}")
            time.sleep(2 ** attempt)

    nb_record = len(data)
    if data and nb_record>0:
        logger.info(f"Extraction réussie: {nb_record} enregistrements")
    else:
        logger.warning(f"Attention, Réponse vide !!!")
    # Stocker dans XCom pour la tâche suivante
    ti.xcom_push(key=f'{COLLECTED_NAME}_data', value=data)
    return nb_record


# DÉFINITION DU DAG

default_args = {
    'owner': 'ahmad',
    'depends_on_past': False,
#    'start_date': datetime(2020, 1, 1),
    'email': ['ahmadou.ndiaye030602@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# On aurait pu utiliser ici
# from config.api_config import DAG_DEFAULT_ARGS
# default_args = DAG_DEFAULT_ARGS
# default_args["retry_delay"] = timedelta(minutes=5)
# default_args["execution_timeout"] = timedelta(hours=2)

with DAG(
    f"{PROJECT_NAME}_{COLLECTED_NAME}",
    default_args=default_args,
    description=(
        "Taux de chômage par région - INSEE"
        "Retourne les données trimestrielles du chômage BIT des hommes de moins de 25 ans en France métropolitaine, exprimées en milliers d'individus."
        f"Les données sont présentes ici: {BASE_URL}"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=SCHEDULING_CRONTAB,
    catchup=True,
    max_active_runs=1, # Nombre de worker

    tags=get_collected_tags(COLLECTED_NAME, "quarterly"),
) as dag:
   
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    convert_xml_to_json_task = PythonOperator(
        task_id='convert_xml_to_json',
        python_callable=convert_xml_to_json,
        trigger_rule="all_success",
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        trigger_rule="all_success",
    )

    extract_task >> convert_xml_to_json_task >> upload_to_gcs_task