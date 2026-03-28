# Dockerfile
FROM apache/airflow:3.1.6

USER airflow

# Installer les packages Google Cloud
RUN pip install --no-cache-dir \
    google-cloud-storage==2.14.0

# Installer le provider Google avec une version spécifique
RUN pip install --no-cache-dir \
    apache-airflow-providers-google==10.22.0

# Installer dbt en dernier
RUN pip install --no-cache-dir \
    dbt-core==1.11.7 \
    dbt-snowflake==1.11.3

# Vérifications
RUN python -c "import airflow; print(f'Airflow: {airflow.__version__}')"
RUN python -c "import dbt.version; print(f'DBT: {dbt.version.__version__}')"
RUN dbt --version