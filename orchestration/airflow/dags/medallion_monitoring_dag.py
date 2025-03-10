from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
import json
import os

# Environment variables
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'medallion-dev')
BRONZE_DATASET = os.environ.get('BIGQUERY_DATASET_BRONZE', 'bronze')
SILVER_DATASET = os.environ.get('BIGQUERY_DATASET_SILVER', 'medallion_pipeline_silver')
GOLD_DATASET = os.environ.get('BIGQUERY_DATASET_GOLD', 'medallion_pipeline_gold')

# Define the DAG
dag = DAG(
    'medallion_monitoring',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['meimeiliew95@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Monitoring DAG for Medallion Architecture Pipeline',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    start_date=datetime(2025, 3, 5),
    catchup=False,
    tags=['medallion', 'monitoring', 'data_quality'],
)

# Function to check data freshness
def check_data_freshness(**kwargs):
    bq_hook = BigQueryHook(use_legacy_sql=False)
    
    # Check freshness for each layer
    tables = [
        f"{PROJECT_ID}.{BRONZE_DATASET}.contracts",
        f"{PROJECT_ID}.{BRONZE_DATASET}.budgets",
        f"{PROJECT_ID}.{BRONZE_DATASET}.change_orders",
        f"{PROJECT_ID}.{SILVER_DATASET}_silver.contracts_silver",
        f"{PROJECT_ID}.{SILVER_DATASET}_silver.budgets_silver",
        f"{PROJECT_ID}.{SILVER_DATASET}_silver.change_orders_silver",
        f"{PROJECT_ID}.{GOLD_DATASET}_gold.contract_analytics",
        f"{PROJECT_ID}.{GOLD_DATASET}_gold.project_analytics",
    ]
    
    results = {}
    for table in tables:
        query = f"""
        SELECT 
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(CAST(processed_at AS TIMESTAMP)), HOUR) as hours_since_update,
            COUNT(*) as row_count
        FROM `{table}`
        """
        try:
            result = bq_hook.get_pandas_df(query)
            results[table] = {
                'hours_since_update': result['hours_since_update'].iloc[0] if not result.empty else None,
                'row_count': result['row_count'].iloc[0] if not result.empty else 0
            }
            
            # Alert if data is stale (more than 24 hours old)
            if results[table]['hours_since_update'] and results[table]['hours_since_update'] > 24:
                print(f"ALERT: Data in {table} is stale! Last update was {results[table]['hours_since_update']} hours ago.")
        except Exception as e:
            results[table] = {'error': str(e)}
            print(f"Error checking freshness for {table}: {e}")
    
    # Store results as XCom for downstream tasks
    kwargs['ti'].xcom_push(key='data_freshness', value=json.dumps(results))
    return results

# Function to check data volume trends
def check_data_volume_trends(**kwargs):
    bq_hook = BigQueryHook(use_legacy_sql=False)
    
    # Check volume trends for each layer
    tables = [
        f"{PROJECT_ID}.{BRONZE_DATASET}.contracts",
        f"{PROJECT_ID}.{BRONZE_DATASET}.budgets",
        f"{PROJECT_ID}.{BRONZE_DATASET}.change_orders",
        f"{PROJECT_ID}.{SILVER_DATASET}_silver.contracts_silver",
        f"{PROJECT_ID}.{SILVER_DATASET}_silver.budgets_silver",
        f"{PROJECT_ID}.{SILVER_DATASET}_silver.change_orders_silver",
        f"{PROJECT_ID}.{GOLD_DATASET}_gold.contract_analytics",
        f"{PROJECT_ID}.{GOLD_DATASET}_gold.project_analytics",
    ]
    
    results = {}
    for table in tables:
        query = f"""
        SELECT 
            DATE(CAST(processed_at AS TIMESTAMP)) as process_date,
            COUNT(*) as daily_count
        FROM `{table}`
        WHERE CAST(processed_at AS TIMESTAMP) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        GROUP BY process_date
        ORDER BY process_date
        """
        try:
            result = bq_hook.get_pandas_df(query)
            if not result.empty:
                # Calculate day-over-day change
                result['previous_count'] = result['daily_count'].shift(1)
                result['day_over_day_change_pct'] = (result['daily_count'] - result['previous_count']) / result['previous_count'] * 100
                
                # Alert on significant changes (more than 20% change)
                significant_changes = result[abs(result['day_over_day_change_pct']) > 20].dropna()
                if not significant_changes.empty:
                    for _, row in significant_changes.iterrows():
                        print(f"ALERT: Significant volume change in {table} on {row['process_date']}: {row['day_over_day_change_pct']:.2f}% change")
                
                results[table] = result.to_dict(orient='records')
            else:
                results[table] = []
        except Exception as e:
            results[table] = {'error': str(e)}
            print(f"Error checking volume trends for {table}: {e}")
    
    # Store results as XCom for downstream tasks
    kwargs['ti'].xcom_push(key='volume_trends', value=json.dumps(results))
    return results

# Function to generate monitoring report
def generate_monitoring_report(**kwargs):
    ti = kwargs['ti']
    freshness_data = json.loads(ti.xcom_pull(task_ids='check_data_freshness', key='data_freshness'))
    volume_data = json.loads(ti.xcom_pull(task_ids='check_data_volume_trends', key='volume_trends'))
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'data_freshness': freshness_data,
        'volume_trends': volume_data,
        'alerts': []
    }
    
    # Check for stale data
    for table, data in freshness_data.items():
        if 'hours_since_update' in data and data['hours_since_update'] and data['hours_since_update'] > 24:
            report['alerts'].append({
                'type': 'stale_data',
                'table': table,
                'hours_since_update': data['hours_since_update'],
                'message': f"Data in {table} is stale! Last update was {data['hours_since_update']} hours ago."
            })
    
    # Check for volume anomalies
    for table, data in volume_data.items():
        if isinstance(data, list):
            for day_data in data:
                if 'day_over_day_change_pct' in day_data and abs(day_data['day_over_day_change_pct']) > 20:
                    report['alerts'].append({
                        'type': 'volume_anomaly',
                        'table': table,
                        'date': day_data['process_date'],
                        'change_pct': day_data['day_over_day_change_pct'],
                        'message': f"Significant volume change in {table} on {day_data['process_date']}: {day_data['day_over_day_change_pct']:.2f}% change"
                    })
    
    # Print report summary
    print(f"Monitoring Report Generated: {datetime.now().isoformat()}")
    print(f"Total Alerts: {len(report['alerts'])}")
    for alert in report['alerts']:
        print(f"- {alert['message']}")
    
    # In a real environment, you would store this report in a database or send it via email/Slack
    return report

# Define tasks
check_data_freshness_task = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    provide_context=True,
    dag=dag,
)

check_data_volume_trends_task = PythonOperator(
    task_id='check_data_volume_trends',
    python_callable=check_data_volume_trends,
    provide_context=True,
    dag=dag,
)

generate_monitoring_report_task = PythonOperator(
    task_id='generate_monitoring_report',
    python_callable=generate_monitoring_report,
    provide_context=True,
    dag=dag,
)

# Check for null values in critical columns
check_null_values_task = BigQueryCheckOperator(
    task_id='check_null_values',
    sql=f"""
    SELECT 
        COUNTIF(contract_id IS NULL) +
        COUNTIF(contract_name IS NULL) +
        COUNTIF(start_date IS NULL) +
        COUNTIF(end_date IS NULL) +
        COUNTIF(contract_value IS NULL) +
        COUNTIF(contract_status IS NULL) as total_nulls
    FROM `{PROJECT_ID}.{SILVER_DATASET}_silver.contracts_silver`
    HAVING total_nulls > 0
    """,
    use_legacy_sql=False,
    dag=dag,
)

# Set task dependencies
[check_data_freshness_task, check_data_volume_trends_task] >> generate_monitoring_report_task
check_null_values_task 