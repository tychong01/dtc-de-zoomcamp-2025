import os
import logging
import requests
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID_DTC", default_var="certain-reducer-451402-d8")
GCP_LOCATION = "asia-southeast1"
GCS_BUCKET_NAME = "dtc-zoomcamp-tychong"
BQ_DATASET_NAME = "de_zoomcamp"
BQ_TABLE_NAME = "yellow_taxi_trips_raw"
GCS_PREFIX = "raw/yellow-taxi"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
default_args = {
    'start_date': datetime(2019, 1, 1),
    'retries': 0
}

with DAG(
    dag_id='etl_yellow_taxi_gcp',
    default_args=default_args,   
    tags=["GCP"],
    schedule_interval='@monthly',
    catchup=False,
    max_active_runs=1,
    concurrency=1
) as dag:
    def download_data(**kwargs):
        """Download and unzip the Yellow Taxi data file."""
        logical_date = kwargs['logical_date'].strftime('%Y-%m')
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{logical_date}.csv.gz"
        output_file = f"/opt/airflow/data/yellow-tripdata-{logical_date}.csv.gz"

        # Ensure directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        try:
            # Download the file
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for bad status codes
            with open(output_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"Downloaded {url} to {output_file}")
        except Exception as e:
            logger.error(f"Failed to download or unzip file: {e}")
            raise

    download_data = PythonOperator(
        task_id="download_data",
        python_callable=download_data,
        provide_context=True,
        dag=dag,
    )

    create_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_bucket",
        bucket_name=GCS_BUCKET_NAME,
        storage_class="STANDARD",
        location=GCP_LOCATION,
        project_id=GCP_PROJECT_ID,
        gcp_conn_id="google_cloud_default",
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/opt/airflow/data/yellow-tripdata-{{ logical_date.strftime('%Y-%m') }}.csv.gz",
        dst=f"{GCS_PREFIX}/{{{{ logical_date.strftime('%Y') }}}}/yellow-tripdata-{{{{ logical_date.strftime('%Y-%m') }}}}.csv.gz",
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id="google_cloud_default",
    
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=BQ_DATASET_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_table_yellow_taxi",
        dataset_id=BQ_DATASET_NAME,
        table_id=BQ_TABLE_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        schema_fields=[
            {"name": "unique_row_id", "type": "STRING"},
            {"name": "filename", "type": "STRING"},
            {"name": "VendorID", "type": "INTEGER"},
            {"name": "tpep_pickup_datetime", "type": "TIMESTAMP"},
            {"name": "tpep_dropoff_datetime", "type": "TIMESTAMP"},
            {"name": "passenger_count", "type": "INTEGER"},
            {"name": "trip_distance", "type": "FLOAT"},
            {"name": "RatecodeID", "type": "INTEGER"},
            {"name": "store_and_fwd_flag", "type": "BOOLEAN"},
            {"name": "PULocationID", "type": "INTEGER"},
            {"name": "DOLocationID", "type": "INTEGER"},
            {"name": "payment_type", "type": "INTEGER"},
            {"name": "fare_amount", "type": "FLOAT"},
            {"name": "extra", "type": "FLOAT"},
            {"name": "mta_tax", "type": "FLOAT"},
            {"name": "tip_amount", "type": "FLOAT"},
            {"name": "tolls_amount", "type": "FLOAT"},
            {"name": "improvement_surcharge", "type": "FLOAT"},
            {"name": "total_amount", "type": "FLOAT"},
            {"name": "congestion_surcharge", "type": "FLOAT"},
        ],
        gcp_conn_id="google_cloud_default",
    )

    create_ext_table = BigQueryInsertJobOperator(
        task_id="create_ext_table_yellow_taxi",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}_ext`
                    OPTIONS (
                        format = 'CSV',
                        uris = ['gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}/{{{{ logical_date.strftime('%Y') }}}}/yellow-tripdata-{{{{ logical_date.strftime('%Y-%m') }}}}.csv.gz']
                    );
                """,
                "useLegacySql": False,
            }
        },
        location=GCP_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    create_staging_table = BigQueryInsertJobOperator(
        task_id="create_bq_staging_table_yellow_taxi",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}_staging` AS
                    SELECT
                    TO_HEX(MD5(CONCAT(
                        COALESCE(CAST(VendorID AS STRING), ""),
                        COALESCE(CAST(tpep_pickup_datetime AS STRING), ""),
                        COALESCE(CAST(tpep_dropoff_datetime AS STRING), ""),
                        COALESCE(CAST(PULocationID AS STRING), ""),
                        COALESCE(CAST(DOLocationID AS STRING), "")
                        ))) AS unique_row_id,
                        "yellow-trip-data-{{ logical_date.strftime('%Y-%m') }}.csv" AS filename,
                        VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
                        PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount,
                        CAST(congestion_surcharge AS FLOAT64) as congestion_surcharge
                    FROM `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}_ext`""",
                "useLegacySql": False,
            }
        },
        location=GCP_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    merge_table = BigQueryInsertJobOperator(
        task_id="merge_table_yellow_taxi",
        configuration={
            "query": {
                "query": f"""
                    MERGE `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}` AS target
                    USING `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}_staging` AS source
                    ON target.unique_row_id = source.unique_row_id
                    WHEN NOT MATCHED THEN
                    INSERT (unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge)
                    VALUES (source.unique_row_id, source.filename, source.VendorID, source.tpep_pickup_datetime, source.tpep_dropoff_datetime, source.passenger_count, source.trip_distance, source.RatecodeID, source.store_and_fwd_flag, source.PULocationID, source.DOLocationID, source.payment_type, source.fare_amount, source.extra, source.mta_tax, source.tip_amount, source.tolls_amount, source.improvement_surcharge, source.total_amount, source.congestion_surcharge)
                    """,
                "useLegacySql": False,
            }
        },
        location=GCP_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

download_data >> create_bucket >> upload_to_gcs >> create_dataset >> create_table >> create_ext_table >> create_staging_table >> merge_table