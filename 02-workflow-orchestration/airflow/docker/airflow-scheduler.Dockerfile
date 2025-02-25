FROM apache/airflow:slim-2.10.5-python3.12

USER root

# Install PostgreSQL development headers and build dependencies
RUN sudo apt-get update && apt-get install -y \
    libpq-dev gcc python3-dev wget && \
    rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /dtc-de-zoomcap/airflw-scheduler

# Install Python packages required for the ingestion needs
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    apache-airflow[crypto] \
    apache-airflow[google] \
    apache-airflow-providers-postgres \
    apache-airflow-providers-amazon \
    apache-airflow-providers-google \
    psycopg2 requests pandas polars sqlalchemy boto3 s3fs

CMD [ "airflow", "scheduler" ]