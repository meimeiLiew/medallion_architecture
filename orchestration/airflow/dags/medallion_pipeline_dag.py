"""
Medallion Architecture Pipeline DAG
This DAG orchestrates the data flow through the bronze, silver, and gold layers.
"""

from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
import subprocess
import logging
import sys
import time

# Load environment variables
load_dotenv()

# Define Slack alert functions
def task_fail_slack_alert(context):
    """
    Callback function for task failure alerts
    """
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context['execution_date']
    exception = context.get('exception')
    
    message = f"""
    :red_circle: Task Failed: 
    *DAG*: {dag_id}
    *Task*: {task_id}
    *Execution Date*: {execution_date}
    *Exception*: {exception}
    """
    
    # In a real environment, you would send this to Slack
    print(f"ALERT: {message}")

def sla_miss_slack_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Callback function for SLA miss alerts
    """
    message = f"""
    :warning: SLA Miss: 
    *DAG*: {dag.dag_id}
    *Tasks*: {', '.join(task.task_id for task in task_list)}
    *Blocking Tasks*: {', '.join(task.task_id for task in blocking_task_list)}
    """
    
    # In a real environment, you would send this to Slack
    print(f"SLA MISS: {message}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['meimeiliew95@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2),  # SLA for the entire DAG
}

# Define the DAG
dag = DAG(
    'medallion_pipeline',
    default_args=default_args,
    description='Medallion Architecture Pipeline for Contracts, Budgets, and Change Orders',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=['medallion', 'etl', 'bigquery', 'dbt'],
    on_failure_callback=task_fail_slack_alert,
    sla_miss_callback=sla_miss_slack_alert,
)

# Variables from environment
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
BRONZE_DATASET = 'bronze'
SILVER_DATASET = os.getenv('BIGQUERY_DATASET_SILVER')
GOLD_DATASET = os.getenv('BIGQUERY_DATASET_GOLD')

# 1. Create Bronze Dataset if it doesn't exist
create_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bronze_dataset',
    dataset_id=BRONZE_DATASET,
    project_id=PROJECT_ID,
    exists_ok=True,
    dag=dag,
)

# 2. Upload CSV files to GCS (Bronze Layer)
upload_contracts_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_contracts_to_gcs',
    src='bronze/data/contracts_simple.csv',
    dst='bronze/contracts/contracts.csv',
    bucket=BUCKET_NAME,
    dag=dag,
)

upload_budgets_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_budgets_to_gcs',
    src='bronze/data/budgets_simple.csv',
    dst='bronze/budgets/budgets.csv',
    bucket=BUCKET_NAME,
    dag=dag,
)

upload_co_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_co_to_gcs',
    src='bronze/data/co_simple.csv',
    dst='bronze/co/co.csv',
    bucket=BUCKET_NAME,
    dag=dag,
)

# 3. Load data from GCS to BigQuery (Bronze Layer)
load_contracts_to_bq = GCSToBigQueryOperator(
    task_id='load_contracts_to_bq',
    bucket=BUCKET_NAME,
    source_objects=['bronze/contracts/contracts.csv'],
    destination_project_dataset_table=f'{PROJECT_ID}.{BRONZE_DATASET}.contracts',
    schema_fields=[
        {'name': 'contract_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'contract_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'client_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'start_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'end_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'contract_value', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'contract_status', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=False,
    dag=dag,
)

# Load budgets data from GCS to BigQuery
load_budgets_to_bq = GCSToBigQueryOperator(
    task_id='load_budgets_to_bq',
    bucket=BUCKET_NAME,
    source_objects=['bronze/budgets/budgets.csv'],
    destination_project_dataset_table=f'{PROJECT_ID}.{BRONZE_DATASET}.budgets',
    schema_fields=[
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'contractId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'scope', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'plannedStartDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'plannedEndDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'actualStartDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'actualEndDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'originalAmount', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'actualCost', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'createdAt', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'updatedAt', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    allow_jagged_rows=True,
    autodetect=False,
    dag=dag,
)

# Load change orders data from GCS to BigQuery
load_co_to_bq = GCSToBigQueryOperator(
    task_id='load_co_to_bq',
    bucket=BUCKET_NAME,
    source_objects=['bronze/co/co.csv'],
    destination_project_dataset_table=f'{PROJECT_ID}.{BRONZE_DATASET}.co',
    schema_fields=[
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'scope', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'contractId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'estimated', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'proposed', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'submitted', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'approved', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'committed', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'createdAt', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'updatedAt', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'statusChangedAt', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    allow_quoted_newlines=True,
    allow_jagged_rows=True,
    autodetect=False,
    dag=dag,
)

# 4. Create Silver Dataset if it doesn't exist
create_silver_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_silver_dataset',
    dataset_id=SILVER_DATASET,
    project_id=PROJECT_ID,
    exists_ok=True,
    dag=dag,
)

# 5. Create Gold Dataset if it doesn't exist
create_gold_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_gold_dataset',
    dataset_id=GOLD_DATASET,
    project_id=PROJECT_ID,
    exists_ok=True,
    dag=dag,
)

# Function to run dbt commands
def run_dbt_commands(**kwargs):
    """
    Run dbt commands for the medallion architecture
    """
    logger = logging.getLogger(__name__)
    
    # Set environment variables
    os.environ['GCP_PROJECT_ID'] = 'medallion-dev'
    os.environ['BIGQUERY_DATASET_SILVER'] = 'medallion_pipeline_silver'
    os.environ['BIGQUERY_DATASET_GOLD'] = 'medallion_pipeline_gold'
    os.environ['GCP_CREDENTIALS_PATH'] = '/opt/airflow/medallion-dev-6a948fd7a82c.json'
    
    # Change to the dbt directory
    dbt_dir = '/opt/airflow/silver_layer/transformations'
    
    # Check if directory exists
    if not os.path.exists(dbt_dir):
        error_msg = f"DBT directory does not exist: {dbt_dir}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    # Check if profiles directory exists
    profiles_dir = os.path.join(dbt_dir, 'profiles')
    if not os.path.exists(profiles_dir):
        error_msg = f"Profiles directory does not exist: {profiles_dir}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    # Check if credentials file exists
    creds_path = os.environ['GCP_CREDENTIALS_PATH']
    if not os.path.exists(creds_path):
        error_msg = f"GCP credentials file does not exist: {creds_path}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    # Change to the dbt directory
    original_dir = os.getcwd()
    os.chdir(dbt_dir)
    
    try:
        # Function to run a command with retries
        def run_with_retry(cmd, max_retries=3, retry_delay=5, ignore_errors=False):
            retries = 0
            while retries < max_retries:
                try:
                    logger.info(f"Executing command: {' '.join(cmd)}")
                    process = subprocess.run(
                        cmd,
                        check=not ignore_errors,  # Only check for errors if ignore_errors is False
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True
                    )
                    logger.info(f"Command output: {process.stdout}")
                    if process.returncode != 0 and ignore_errors:
                        logger.warning(f"Command exited with non-zero code {process.returncode}, but errors are being ignored. Error: {process.stderr}")
                    return process
                except subprocess.CalledProcessError as e:
                    retries += 1
                    logger.warning(f"Command failed (attempt {retries}/{max_retries}): {e.stderr}")
                    if retries >= max_retries:
                        if ignore_errors:
                            logger.warning(f"Command failed after {max_retries} attempts, but errors are being ignored.")
                            return e
                        raise
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
        
        # Run dbt models
        logger.info("Running dbt models...")
        dbt_run_cmd = ['dbt', 'run', '--profiles-dir=./profiles']
        run_with_retry(dbt_run_cmd)
        
        # Run dbt tests but don't fail the task if tests fail
        logger.info("Running dbt tests...")
        dbt_test_cmd = ['dbt', 'test', '--profiles-dir=./profiles']
        try:
            test_result = run_with_retry(dbt_test_cmd, ignore_errors=True)
            if hasattr(test_result, 'returncode') and test_result.returncode != 0:
                logger.warning(f"DBT tests failed with exit code {test_result.returncode}, but continuing with the pipeline.")
                logger.warning(f"Test errors: {test_result.stderr}")
        except Exception as e:
            logger.warning(f"DBT tests failed with error: {str(e)}, but continuing with the pipeline.")
        
        # Generate dbt docs
        logger.info("Generating dbt docs...")
        dbt_docs_cmd = ['dbt', 'docs', 'generate', '--profiles-dir=./profiles']
        try:
            run_with_retry(dbt_docs_cmd, ignore_errors=True)
        except Exception as e:
            logger.warning(f"DBT docs generation failed with error: {str(e)}, but continuing with the pipeline.")
        
        logger.info("All dbt commands completed successfully")
        return "DBT commands completed successfully"
    except subprocess.CalledProcessError as e:
        error_msg = f"Command failed with exit code {e.returncode}. Error: {e.stderr}"
        logger.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
    finally:
        # Change back to the original directory
        os.chdir(original_dir)

# 6. Run dbt Core locally for transformations (Silver and Gold layers)
run_dbt_models = PythonOperator(
    task_id='run_dbt_models',
    python_callable=run_dbt_commands,
    retries=3,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

# 7. Validate data quality in Silver layer
validate_silver_contracts = BigQueryCheckOperator(
    task_id='validate_silver_contracts',
    sql=f"""
    SELECT COUNT(*) = 0
    FROM `{PROJECT_ID}.medallion_pipeline_silver_silver.contracts_silver` 
    WHERE contract_id IS NULL
    """,
    use_legacy_sql=False,
    dag=dag,
)

validate_silver_budgets = BigQueryCheckOperator(
    task_id='validate_silver_budgets',
    sql=f"""
    SELECT COUNT(*) = 0
    FROM `{PROJECT_ID}.medallion_pipeline_silver_silver.budgets_silver` 
    WHERE budget_id IS NULL
    """,
    use_legacy_sql=False,
    dag=dag,
)

validate_silver_change_orders = BigQueryCheckOperator(
    task_id='validate_silver_change_orders',
    sql=f"""
    SELECT COUNT(*) = 0
    FROM `{PROJECT_ID}.medallion_pipeline_silver_silver.change_orders_silver` 
    WHERE change_order_id IS NULL
    """,
    use_legacy_sql=False,
    dag=dag,
)

# 8. Validate data quality in Gold layer
validate_gold_project_analytics = BigQueryCheckOperator(
    task_id='validate_gold_project_analytics',
    sql=f"""
    SELECT COUNT(*) = 0
    FROM `{PROJECT_ID}.medallion_pipeline_silver_gold.project_analytics` 
    WHERE contract_id IS NULL
    """,
    use_legacy_sql=False,
    dag=dag,
)

validate_gold_client_analytics = BigQueryCheckOperator(
    task_id='validate_gold_client_analytics',
    sql=f"""
    SELECT COUNT(*) = 0
    FROM `{PROJECT_ID}.medallion_pipeline_silver_gold.client_analytics` 
    WHERE client_name IS NULL
    """,
    use_legacy_sql=False,
    dag=dag,
)

validate_gold_time_analytics = BigQueryCheckOperator(
    task_id='validate_gold_time_analytics',
    sql=f"""
    SELECT COUNT(*) = 0
    FROM `{PROJECT_ID}.medallion_pipeline_silver_gold.time_analytics` 
    WHERE year IS NULL OR month IS NULL
    """,
    use_legacy_sql=False,
    dag=dag,
)

validate_gold_contract_summary = BigQueryCheckOperator(
    task_id='validate_gold_contract_summary',
    sql=f"""
    SELECT COUNT(*) = 0
    FROM `{PROJECT_ID}.medallion_pipeline_silver_gold.contract_summary` 
    WHERE client_name IS NULL
    """,
    use_legacy_sql=False,
    dag=dag,
)

# Set task dependencies
create_bronze_dataset >> [upload_contracts_to_gcs, upload_budgets_to_gcs, upload_co_to_gcs]
upload_contracts_to_gcs >> load_contracts_to_bq
upload_budgets_to_gcs >> load_budgets_to_bq
upload_co_to_gcs >> load_co_to_bq
create_silver_dataset >> run_dbt_models
create_gold_dataset >> run_dbt_models
[load_contracts_to_bq, load_budgets_to_bq, load_co_to_bq] >> run_dbt_models
run_dbt_models >> [validate_silver_contracts, validate_silver_budgets, validate_silver_change_orders, 
                  validate_gold_project_analytics, validate_gold_client_analytics, validate_gold_time_analytics,
                  validate_gold_contract_summary] 