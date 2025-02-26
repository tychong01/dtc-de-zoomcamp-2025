import os
import requests
import gzip
import psycopg2
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime 
import csv

# Steps
# 1. Download data from https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{yyyy}-{mm}.csv.gz and unzip
# 2. Create staging and production table in postgres
# 3. Copy data into staging table
# 4. Update table with unique ID and data filename (track lineage)
# 5. Merge staging table with production table

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
default_args = {
    'start_date': datetime(2019, 1, 1),
    'retries': 0
}


with DAG(
    dag_id='etl_yellow_taxi_postgres',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    concurrency=1
) as dag:
    def download_and_unzip(**kwargs):
        """Download and unzip the Yellow Taxi data file."""
        logical_date = kwargs['logical_date'].strftime('%Y-%m')
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{logical_date}.csv.gz"
        output_file = f"/opt/airflow/data/yellow_tripdata_{logical_date}.csv.gz"
        csv_file = f"/opt/airflow/data/yellow_tripdata_{logical_date}.csv"

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

            # Unzip the file
            with gzip.open(output_file, 'rb') as f_in:
                with open(csv_file, 'wb') as f_out:
                    f_out.write(f_in.read())
            logger.info(f"Unzipped {output_file} to {csv_file}")

            os.remove(output_file)
            logger.info(f"Removed {output_file}")
        except Exception as e:
            logger.error(f"Failed to download or unzip file: {e}")
            raise
    
    def create_table_sql(env='production'):
        """Create the SQL statement for creating the table."""
        if env == 'staging':
            return """
                CREATE TABLE IF NOT EXISTS yellow_taxi_trips_staging (
                    vendor_id INTEGER,
                    tpep_pickup_datetime TIMESTAMP,
                    tpep_dropoff_datetime TIMESTAMP,
                    passenger_count INTEGER,
                    trip_distance FLOAT,
                    rate_code_id INTEGER,
                    store_and_fwd_flag CHAR(1),
                    pu_location_id INTEGER,
                    do_location_id INTEGER,
                    payment_type INTEGER,
                    fare_amount FLOAT,
                    extra FLOAT,
                    mta_tax FLOAT,
                    tip_amount FLOAT,
                    tolls_amount FLOAT,
                    improvement_surcharge FLOAT,
                    total_amount FLOAT,
                    congestion_surcharge TEXT,
                    unique_id TEXT,
                    data_file_name TEXT,
                    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
        elif env == 'production':
            return """
                CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
                    unique_id TEXT,
                    vendor_id INTEGER,
                    tpep_pickup_datetime TIMESTAMP,
                    tpep_dropoff_datetime TIMESTAMP,
                    passenger_count INTEGER,
                    trip_distance FLOAT,
                    rate_code_id INTEGER,
                    store_and_fwd_flag CHAR(1),
                    pu_location_id INTEGER,
                    do_location_id INTEGER,
                    payment_type INTEGER,
                    fare_amount FLOAT,
                    extra FLOAT,
                    mta_tax FLOAT,
                    tip_amount FLOAT,
                    tolls_amount FLOAT,
                    improvement_surcharge FLOAT,
                    total_amount FLOAT,
                    congestion_surcharge TEXT,
                    data_file_name TEXT,
                    ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
        else:
            raise ValueError("Environment must be either 'staging' or 'production'")

    def copy_to_staging(**kwargs):
        """Copy data from CSV to staging table."""
        logical_date = kwargs['logical_date'].strftime('%Y-%m')
        csv_file = f"/opt/airflow/data/yellow_tripdata_{logical_date}.csv"
        pg_hook = PostgresHook(postgres_conn_id='postgres_default', database='nytaxi')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            with open(csv_file, 'r') as f:
                reader = csv.reader(f)
                header = next(reader)
                logger.info(f"CSV Header: {header}")

                # Map CSV column names to table column names
                csv_to_table_mapping = {
                    'VendorID': 'vendor_id',
                    'tpep_pickup_datetime': 'tpep_pickup_datetime',
                    'tpep_dropoff_datetime': 'tpep_dropoff_datetime',
                    'passenger_count': 'passenger_count',
                    'trip_distance': 'trip_distance',
                    'RatecodeID': 'rate_code_id',
                    'store_and_fwd_flag': 'store_and_fwd_flag',
                    'PULocationID': 'pu_location_id',
                    'DOLocationID': 'do_location_id',
                    'payment_type': 'payment_type',
                    'fare_amount': 'fare_amount',
                    'extra': 'extra',
                    'mta_tax': 'mta_tax',
                    'tip_amount': 'tip_amount',
                    'tolls_amount': 'tolls_amount',
                    'improvement_surcharge': 'improvement_surcharge',
                    'total_amount': 'total_amount',
                    'congestion_surcharge': 'congestion_surcharge'
                }

                cursor.copy_from(f, 'yellow_taxi_trips_staging', sep=',', columns=[
                    csv_to_table_mapping[col] for col in header if col in csv_to_table_mapping
                ])
            conn.commit()
            logger.info(f"Copied data from {csv_file} to staging table")
            cursor.execute("SELECT COUNT(*) FROM yellow_taxi_trips_staging")
            row_count = cursor.fetchone()[0]
            logger.info(f"Copied {row_count} rows from {csv_file} to staging table")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to copy data to staging table: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    download_data_task = PythonOperator(
        task_id='download_data',
        python_callable=download_and_unzip,
        provide_context=True
    )

    create_staging_table_task = SQLExecuteQueryOperator(
        task_id='create_staging_table',
        sql=create_table_sql('staging'),
        conn_id='postgres_default',
        hook_params={
            'database': "nytaxi"
        }
    )

    create_production_table_task = SQLExecuteQueryOperator(
        task_id='create_production_table',
        sql=create_table_sql('production'),
        conn_id='postgres_default',
        hook_params={
            'database': "nytaxi"
        }
    )

    truncate_staging_task = SQLExecuteQueryOperator(
        task_id='truncate_staging',
        sql="TRUNCATE TABLE yellow_taxi_trips_staging",
        conn_id='postgres_default',
        hook_params={
            'database': "nytaxi"
        }
    )

    import_staging_task = PythonOperator(
        task_id='import_staging',
        python_callable=copy_to_staging,
        provide_context=True
    )

    update_staging_task = SQLExecuteQueryOperator(
        task_id='update_staging',
        sql="""
            UPDATE yellow_taxi_trips_staging
            SET data_file_name = 'yellow_tripdata_{{ logical_date.strftime('%Y-%m') }}.csv',
                unique_id = md5(
                    COALESCE(CAST(vendor_id AS text), '') ||
                    COALESCE(CAST(tpep_pickup_datetime AS text), '') || 
                    COALESCE(CAST(tpep_dropoff_datetime AS text), '') || 
                    COALESCE(CAST(passenger_count AS text), '') || 
                    COALESCE(CAST(pu_location_id AS text), '') || 
                    COALESCE(CAST(do_location_id AS text), '') || 
                    COALESCE(CAST(fare_amount AS text), '') || 
                    COALESCE(CAST(trip_distance AS text), '')
                )
            WHERE data_file_name IS NULL
        """,
        conn_id='postgres_default',
        hook_params={
            'database': "nytaxi"
        }
    )

    merge_production_task = SQLExecuteQueryOperator(
        task_id='merge_production',
        sql="""
            MERGE INTO yellow_taxi_trips AS T
            USING yellow_taxi_trips_staging AS S
            ON T.unique_id = S.unique_id
            WHEN NOT MATCHED THEN
                INSERT (
                    vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
                    trip_distance, rate_code_id, store_and_fwd_flag, pu_location_id,
                    do_location_id, payment_type, fare_amount, extra, mta_tax,
                    tip_amount, tolls_amount, improvement_surcharge, total_amount,
                    congestion_surcharge, unique_id, data_file_name
                ) VALUES (
                    S.vendor_id, S.tpep_pickup_datetime, S.tpep_dropoff_datetime, S.passenger_count,
                    S.trip_distance, S.rate_code_id, S.store_and_fwd_flag, S.pu_location_id,
                    S.do_location_id, S.payment_type, S.fare_amount, S.extra, S.mta_tax,
                    S.tip_amount, S.tolls_amount, S.improvement_surcharge, S.total_amount,
                    S.congestion_surcharge, S.unique_id, S.data_file_name
                )
        """,
        conn_id='postgres_default',
        hook_params={
            'database': "nytaxi"
        }
    
    )

    download_data_task >> create_staging_table_task >> create_production_table_task >> truncate_staging_task >> import_staging_task >> update_staging_task >> merge_production_task
