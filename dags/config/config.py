from datetime import timedelta
import os

PROJECT_NAME = os.getenv('PROJECT_NAME')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')

GCP_CREDENTIALS_PATH = os.getenv(
    'GOOGLE_APPLICATION_CREDENTIALS',
    '/opt/airflow/config/gcp/airflow-gcp-key.json'
)