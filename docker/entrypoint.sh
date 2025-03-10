#!/bin/bash
set -e

# Initialize the database
airflow db init

# Create a user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Ensure dbt profiles directory exists
mkdir -p /opt/airflow/silver_layer/transformations/profiles

# Create dbt profiles.yml if it doesn't exist
if [ ! -f "/opt/airflow/silver_layer/transformations/profiles/profiles.yml" ]; then
    cat > /opt/airflow/silver_layer/transformations/profiles/profiles.yml << EOF
medallion:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: \${GCP_PROJECT_ID}
      dataset: \${BIGQUERY_DATASET_SILVER}
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
EOF
    echo "Created dbt profiles.yml"
fi

# Ensure Great Expectations directories exist
mkdir -p /opt/airflow/tests/great_expectations/uncommitted/data_docs/local_site

# Start Airflow services
if [ "$1" = "webserver" ]; then
    exec airflow webserver
elif [ "$1" = "scheduler" ]; then
    exec airflow scheduler
else
    exec "$@"
fi 