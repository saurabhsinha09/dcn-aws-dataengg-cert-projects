from datetime import datetime, timedelta
import logging
import boto3

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# --- Environment & Infrastructure Configuration ---
AWS_REGION = 'ap-south-1'
S3_BUCKET = 'dcn-dataengg'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

# --- S3 Functions ---

def check_files_in_s3(prefix: str, bucket_name: str = S3_BUCKET) -> bool:
    """Checks if non-empty objects exist under a specific S3 prefix."""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    contents = response.get('Contents', [])

    for obj in contents:
        if obj.get('Size', 0) > 0:
            logging.info(f"Non-empty file found in {prefix}: {obj['Key']}")
            return True

    logging.info(f"No non-empty files found in {prefix}")
    return False

def check_all_files() -> str:
    """Branches DAG execution based on presence of files in input prefixes."""
    logging.info("Checking for files in required S3 prefixes...")
    user_streams = check_files_in_s3('spotify_data/user-streams/')
    songs = check_files_in_s3('spotify_data/songs/')
    users = check_files_in_s3('spotify_data/users/')
    
    logging.info(f"Found -> user_streams: {user_streams}, songs: {songs}, users: {users}")
    
    if user_streams and songs and users:
        logging.info("All S3 prefixes contain data. Proceeding to Glue ETL job.")
        return 'run_spark_etl_job'
    else:
        logging.info("One or more required prefixes are empty. Skipping downstream execution.")
        return 'skip_execution'

def move_files_to_archived(bucket_name: str = S3_BUCKET):
    """Archives processed stream files to the user-streams-archived directory while preserving the source folder."""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    source_prefix = 'spotify_data/user-streams/'
    dest_prefix = 'spotify_data/user-streams-archived/'

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
    contents = response.get('Contents', [])
    
    for obj in contents:
        source_key = obj['Key']

        # Ignore folder markers
        if source_key == source_prefix or source_key.endswith('/'):
            continue

        file_subpath = source_key[len(source_prefix):]
        dest_key = f"{dest_prefix}{file_subpath}"

        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Key=dest_key
        )
        s3.delete_object(Bucket=bucket_name, Key=source_key)
        logging.info(f"Archived file: {source_key} -> {dest_key}")

# --- DAG Definition ---

with DAG(
    dag_id='process-songs-metrics',
    default_args=default_args,
    description='Pipeline to process Spotify metrics via AWS Glue and archive source data.',
    schedule='@daily',
    catchup=False
) as dag:

    # Task 1: Check S3 inputs & branch
    check_files = BranchPythonOperator(
        task_id='check_files',
        python_callable=check_all_files
    )

    # Task 2a: Trigger & wait for Spark Glue Job
    run_spark_etl_job = GlueJobOperator(
        task_id='run_spark_etl_job',
        job_name='calculate_metrics_etl',
        aws_conn_id='aws_default',
        region_name=AWS_REGION,
        wait_for_completion=True,
        waiter_delay=30
    )

    # Task 2b: Trigger & wait for DynamoDB Loader Glue Job
    run_python_dynamo_job = GlueJobOperator(
        task_id='run_python_dynamo_job',
        job_name='insert_metrics_dynamo',
        aws_conn_id='aws_default',
        region_name=AWS_REGION,
        wait_for_completion=True,
        waiter_delay=30
    )

    # Task 3: Move processed source files
    move_files = PythonOperator(
        task_id='move_files',
        python_callable=move_files_to_archived
    )

    # Alternate Branch: No-op exit when no data is available
    skip_execution = EmptyOperator(
        task_id='skip_execution'
    )

    # Execution Graph
    check_files >> [run_spark_etl_job, skip_execution]
    run_spark_etl_job >> run_python_dynamo_job >> move_files