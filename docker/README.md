# Containerized Medallion Architecture

This directory contains Docker configuration files to run the medallion architecture pipeline in a containerized environment.

## Prerequisites

- Docker
- Docker Compose

## Getting Started

1. Make sure you have Docker and Docker Compose installed on your system.

2. Clone this repository and navigate to the project root directory.

3. Build and start the containers:

```bash
docker-compose up -d
```

This will start the following services:
- PostgreSQL database for Airflow metadata
- Airflow webserver
- Airflow scheduler

4. Access the Airflow web interface at http://localhost:8080
   - Username: admin
   - Password: admin

## Running the Pipeline

1. In the Airflow web interface, enable the `medallion_pipeline` DAG.

2. Trigger the DAG manually or wait for the scheduled run.

## Validating Data Quality

The containerized solution includes a robust data quality validation framework using Great Expectations. This ensures that data meets the defined quality standards at each layer of the medallion architecture.

### Running Data Validation

To run data quality validation:

```bash
docker-compose exec airflow-webserver /opt/airflow/docker/validate_data.sh
```

### Understanding Validation Results

The validation script will output results for each layer:
- Bronze layer validation: Checks raw data for completeness and basic structure
- Silver layer validation: Verifies transformed data meets business rules
- Gold layer validation: Ensures aggregated data is accurate for reporting

### Validation Components

The validation framework consists of:
- `validate_data.sh`: Shell script to execute the validation
- `validate_data_quality.py`: Python script that runs the validation logic
- Expectation JSON files: Define data quality rules for each layer

### Adding Custom Validations

To add custom validations:
1. Create a new JSON file in the appropriate directory under `tests/expectations/`
2. Define your expectations following the Great Expectations format
3. Update the `validate_data_quality.py` script to include validation for the new expectations

## Dependency Management

The Docker container includes all necessary dependencies to run the pipeline and validation:
- Apache Airflow 2.6.3
- dbt Core 1.5.0
- Great Expectations 0.17.15
- Required providers for Google Cloud and dbt Cloud

All dependencies are specified in the Dockerfile to ensure consistent execution across environments.

## Stopping the Services

To stop the services:

```bash
docker-compose down
```

To stop the services and remove volumes:

```bash
docker-compose down -v
```

## Troubleshooting

If you encounter any issues:

1. Check the logs:

```bash
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
```

2. Restart the services:

```bash
docker-compose restart
```

3. Rebuild the containers:

```bash
docker-compose build --no-cache
docker-compose up -d
```

4. Validation-specific issues:
   - Verify that the data files exist in the expected locations
   - Check that the expectation JSON files are properly formatted
   - Ensure the validation script has execute permissions
   - Review the validation logs for specific error messages 