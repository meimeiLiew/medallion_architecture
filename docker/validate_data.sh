#!/bin/bash

# Set error handling
set -e

echo "Running data quality validation..."

# Set environment variables
export GCP_PROJECT_ID=${GCP_PROJECT_ID:-medallion-dev}
export BIGQUERY_DATASET_SILVER=${BIGQUERY_DATASET_SILVER:-medallion_pipeline_silver}
export BIGQUERY_DATASET_GOLD=${BIGQUERY_DATASET_GOLD:-medallion_pipeline_gold}

# Create directories if they don't exist
mkdir -p /opt/airflow/tests/great_expectations/expectations
mkdir -p /opt/airflow/tests/great_expectations/validations
mkdir -p /opt/airflow/tests/great_expectations/checkpoints
mkdir -p /opt/airflow/tests/great_expectations/uncommitted/data_docs/local_site

# Run validation
cd /opt/airflow
python -m tests.validate_data_quality

# Check exit code
if [ $? -eq 0 ]; then
    echo "✅ Data quality validation passed!"
    exit 0
else
    echo "❌ Data quality validation failed!"
    exit 1
fi 