import os
import logging
import requests
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# Import S3 and Redshift operators from the Amazon provider
from airflow.providers.amazon.aws.operators.s3_create_bucket import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

# Configuration variables
AWS_S3_BUCKET = Variable.get("AWS_S3_BUCKET", default_var="dtc-zoomcamp-tychong")
REDSHIFT_DATABASE = Variable.get("REDSHIFT_DATABASE", default_var="dev")
REDSHIFT_WORKGROUP = Variable.get("REDSHIFT_WORKGROUP", default_var="my-redshift-workgroup")
AWS_REGION = "ap-southeast-1"  # Set to your AWS region

# Table and schema names
TARGET_SCHEMA = "public"  # Where the final table lives
TARGET_TABLE = "yellow_taxi_trips"
EXTERNAL_TABLE = TARGET_TABLE + "_ext"
STAGING_TABLE = TARGET_TABLE + "_staging"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'start_date': datetime(2019, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='etl_yellow_taxi_aws',
    default_args=default_args,   
    tags=["AWS", "Redshift", "S3"],
    schedule_interval='@monthly',
    catchup=False,
    max_active_runs=1,
    concurrency=1
) as dag:

    def download_data(**kwargs):
        """
        Download and save the Yellow Taxi CSV file.
        The logical_date is used (formatted as YYYY-MM) to download the monthly file.
        """
        # Airflow supplies logical_date in the context
        logical_date = kwargs['logical_date'].strftime('%Y-%m')
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{logical_date}.csv.gz"
        output_file = f"/opt/airflow/data/yellow-tripdata-{logical_date}.csv.gz"

        # Ensure directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(output_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"Downloaded {url} to {output_file}")
        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            raise

    download_task = PythonOperator(
        task_id="download_data",
        python_callable=download_data,
        provide_context=True,
    )

    # Create S3 bucket (if it does not already exist)
    create_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket",
        bucket_name=AWS_S3_BUCKET,
        region=AWS_REGION,
        aws_conn_id="aws_default",
    )

    # Upload the file to S3; use templated file path based on logical_date
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        src="/opt/airflow/data/yellow-tripdata-{{ logical_date.strftime('%Y-%m') }}.csv.gz",
        dest="yellow-taxi/yellow-tripdata-{{ logical_date.strftime('%Y-%m') }}.csv.gz",
        bucket=AWS_S3_BUCKET,
        aws_conn_id="aws_default",
    )

    # Create target table in Redshift Serverless (if not exists)
    create_target_table = RedshiftDataOperator(
        task_id="create_redshift_target_table",
        sql="""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                unique_row_id VARCHAR(32),
                filename VARCHAR(256),
                VendorID INTEGER,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance FLOAT,
                RatecodeID INTEGER,
                store_and_fwd_flag VARCHAR(10),
                PULocationID INTEGER,
                DOLocationID INTEGER,
                payment_type INTEGER,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                improvement_surcharge FLOAT,
                total_amount FLOAT,
                congestion_surcharge VARCHAR(32)
            );
        """.format(schema=TARGET_SCHEMA, table=TARGET_TABLE),
        database=REDSHIFT_DATABASE,
        workgroup_name=REDSHIFT_WORKGROUP,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )

    # Create external table in Redshift (Spectrum external table)
    create_external_table = RedshiftDataOperator(
        task_id="create_redshift_external_table",
        sql="""
            DROP TABLE IF EXISTS spectrum.{ext_table};
            CREATE EXTERNAL TABLE spectrum.{ext_table} (
                VendorID INTEGER,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INTEGER,
                trip_distance FLOAT,
                RatecodeID INTEGER,
                store_and_fwd_flag VARCHAR(10),
                PULocationID INTEGER,
                DOLocationID INTEGER,
                payment_type INTEGER,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                improvement_surcharge FLOAT,
                total_amount FLOAT,
                congestion_surcharge FLOAT
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION 's3://{{ params.bucket }}/yellow-taxi/yellow-tripdata-{{ logical_date.strftime("%Y-%m") }}.csv.gz'
            CSV;
        """,
        database=REDSHIFT_DATABASE,
        workgroup_name=REDSHIFT_WORKGROUP,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        params={"bucket": AWS_S3_BUCKET},
    )

    # Create a staging table in Redshift using a CTAS query on the external table.
    create_staging_table = RedshiftDataOperator(
        task_id="create_redshift_staging_table",
        sql="""
            CREATE OR REPLACE TABLE {schema}.{staging} AS
            SELECT 
                md5(
                  COALESCE(CAST(VendorID AS VARCHAR), '') ||
                  COALESCE(CAST(tpep_pickup_datetime AS VARCHAR), '') ||
                  COALESCE(CAST(tpep_dropoff_datetime AS VARCHAR), '') ||
                  COALESCE(CAST(PULocationID AS VARCHAR), '') ||
                  COALESCE(CAST(DOLocationID AS VARCHAR), '')
                ) AS unique_row_id,
                'yellow-trip-data-{{ logical_date.strftime("%Y-%m") }}.csv' AS filename,
                *
            FROM spectrum.{ext_table};
        """.format(schema=TARGET_SCHEMA, staging=STAGING_TABLE, ext_table=EXTERNAL_TABLE),
        database=REDSHIFT_DATABASE,
        workgroup_name=REDSHIFT_WORKGROUP,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )

    # Merge the staging table into the target table.
    # (Redshift now supports MERGE statements; adjust syntax if needed.)
    merge_table = RedshiftDataOperator(
        task_id="merge_redshift_table",
        sql="""
            MERGE {schema}.{target} AS target
            USING {schema}.{staging} AS source
            ON target.unique_row_id = source.unique_row_id
            WHEN NOT MATCHED THEN
            INSERT (unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                    passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
                    DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                    improvement_surcharge, total_amount, congestion_surcharge)
            VALUES (source.unique_row_id, source.filename, source.VendorID, source.tpep_pickup_datetime,
                    source.tpep_dropoff_datetime, source.passenger_count, source.trip_distance, source.RatecodeID,
                    source.store_and_fwd_flag, source.PULocationID, source.DOLocationID, source.payment_type,
                    source.fare_amount, source.extra, source.mta_tax, source.tip_amount, source.tolls_amount,
                    source.improvement_surcharge, source.total_amount, source.congestion_surcharge);
        """.format(schema=TARGET_SCHEMA, target=TARGET_TABLE, staging=STAGING_TABLE),
        database=REDSHIFT_DATABASE,
        workgroup_name=REDSHIFT_WORKGROUP,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )

    # Define task dependencies
    download_task >> create_bucket >> upload_to_s3
    upload_to_s3 >> create_target_table >> create_external_table
    create_external_table >> create_staging_table >> merge_table
