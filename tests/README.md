# Data Quality Testing

This directory contains the data quality validation framework for the medallion architecture. It uses Great Expectations to validate data at each layer of the architecture.

## Directory Structure

- `expectations/`: Contains JSON files defining expectations for each layer
  - `bronze/`: Expectations for the bronze layer
  - `silver/`: Expectations for the silver layer
- `great_expectations/`: Great Expectations configuration
  - `expectations/`: Great Expectations expectation suites
  - `checkpoints/`: Checkpoint configurations for running validations
  - `uncommitted/`: Configuration variables and data docs
- `validate_data_quality.py`: Python script to validate data against expectations
- `unit_tests/`: Unit tests for individual components
- `integration_tests/`: Integration tests for the entire pipeline

## Running Data Quality Validation

### Local Execution

To run data quality validation locally:

```bash
# Activate your virtual environment
source venv/bin/activate

# Run the validation script
python tests/validate_data_quality.py
```

After running the validation, you can view the data docs at:
```
tests/great_expectations/uncommitted/data_docs/local_site/index.html
```

### Docker Execution

To run data quality validation in Docker:

```bash
# Make sure Docker is running
docker-compose up -d

# Execute the validation script in the container
docker exec -it medallion_architecture_airflow-webserver_1 /opt/airflow/docker/validate_data.sh
```

## Great Expectations Configuration

The Great Expectations configuration is set up to validate data in both the bronze and silver layers:

1. **Datasources**: 
   - `bronze_data`: Points to the bronze/data directory
   - `silver_data`: Points to the silver/data directory

2. **Expectation Suites**:
   - `bronze.contracts`: Validates contracts data in the bronze layer
   - `bronze.budgets`: Validates budgets data in the bronze layer
   - `silver.contracts_silver`: Validates contracts data in the silver layer

3. **Checkpoints**:
   - `bronze_checkpoint`: Runs validations for the bronze layer
   - `silver_checkpoint`: Runs validations for the silver layer

## Adding New Expectations

1. Create a new JSON file in the appropriate directory under `expectations/`
2. Define your expectations following the Great Expectations format
3. Add the expectation suite to `great_expectations/expectations/`
4. Update the checkpoint configuration in `great_expectations/checkpoints/`
5. Update the `validate_data_quality.py` script if needed

## Expectations Format

Expectations are defined in JSON format following the Great Expectations schema. Example:

```json
{
  "data_asset_type": "Dataset",
  "expectation_suite_name": "bronze.contracts",
  "expectations": [
    {
      "expectation_type": "expect_table_columns_to_match_ordered_list",
      "kwargs": {
        "column_list": ["column1", "column2"]
      },
      "meta": {}
    }
  ],
  "meta": {
    "great_expectations_version": "0.17.15"
  }
}
```

## Integration with CI/CD

The data quality validation is integrated into the CI/CD pipeline through GitHub Actions. The workflow runs the validation script as part of the testing phase to ensure data quality before deployment.

The workflow also generates data docs that can be viewed as artifacts in the GitHub Actions run.

## Troubleshooting

If you encounter issues with the validation:

1. Check that the data files exist in the expected locations
2. Verify that the expectations JSON files are properly formatted
3. Ensure that Great Expectations is properly installed (version 0.17.15)
4. Check the logs for specific error messages
5. Verify that the Great Expectations configuration is correct
6. Check that the checkpoint configurations are properly set up 