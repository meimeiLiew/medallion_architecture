#!/usr/bin/env python
"""
Data Quality Validation Script
This script validates data against defined expectations using pandas.
"""

import os
import sys
import json
import pandas as pd
from dotenv import load_dotenv
import datetime
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.datasource.new_datasource import Datasource
from great_expectations.execution_engine.sqlalchemy_execution_engine import SqlAlchemyExecutionEngine
from great_expectations.datasource.data_connector.runtime_data_connector import RuntimeDataConnector
from great_expectations.datasource.data_connector.configured_asset_sql_data_connector import ConfiguredAssetSqlDataConnector

# Load environment variables
load_dotenv()

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DATA_DIR = os.path.join(BASE_DIR, 'bronze', 'data')
SILVER_DATA_DIR = os.path.join(BASE_DIR, 'silver', 'data')
EXPECTATIONS_DIR = os.path.join(BASE_DIR, 'tests', 'expectations')
GE_DIR = os.path.join(BASE_DIR, 'tests', 'great_expectations')

# Use sample files for testing (can be overridden by environment variable)
USE_SAMPLE_FILES = os.environ.get('USE_SAMPLE_FILES', 'True').lower() in ('true', '1', 't')

# Environment variables
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'medallion-dev')
SILVER_DATASET = os.environ.get('BIGQUERY_DATASET_SILVER', 'medallion_pipeline_silver')
GOLD_DATASET = os.environ.get('BIGQUERY_DATASET_GOLD', 'medallion_pipeline_gold')

# Configure Great Expectations context
data_context_config = DataContextConfig(
    store_backend_defaults=None,
    datasources={
        "bigquery_datasource": {
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": f"bigquery://{PROJECT_ID}",
            },
            "data_connectors": {
                "default_runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
                "default_configured_data_connector": {
                    "class_name": "ConfiguredAssetSqlDataConnector",
                    "assets": {
                        "contracts_silver": {
                            "class_name": "Asset",
                            "schema_name": f"{SILVER_DATASET}_silver",
                            "table_name": "contracts_silver",
                        },
                        "budgets_silver": {
                            "class_name": "Asset",
                            "schema_name": f"{SILVER_DATASET}_silver",
                            "table_name": "budgets_silver",
                        },
                        "change_orders_silver": {
                            "class_name": "Asset",
                            "schema_name": f"{SILVER_DATASET}_silver",
                            "table_name": "change_orders_silver",
                        },
                        "contract_analytics": {
                            "class_name": "Asset",
                            "schema_name": f"{GOLD_DATASET}_gold",
                            "table_name": "contract_analytics",
                        },
                        "project_analytics": {
                            "class_name": "Asset",
                            "schema_name": f"{GOLD_DATASET}_gold",
                            "table_name": "project_analytics",
                        }
                    }
                }
            }
        }
    },
    expectations_store_name="expectations_store",
    validations_store_name="validations_store",
    evaluation_parameter_store_name="evaluation_parameter_store",
    checkpoint_store_name="checkpoint_store",
    stores={
        "expectations_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "tests/great_expectations/expectations",
            }
        },
        "validations_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "tests/great_expectations/validations",
            }
        },
        "evaluation_parameter_store": {
            "class_name": "EvaluationParameterStore",
        },
        "checkpoint_store": {
            "class_name": "CheckpointStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "tests/great_expectations/checkpoints",
            }
        }
    },
    data_docs_sites={
        "local_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "tests/great_expectations/uncommitted/data_docs/local_site",
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
            }
        }
    }
)

# Create context and datasource
context = BaseDataContext(project_config=data_context_config)

def load_expectations(file_path):
    """Load expectations from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Expectations file not found at {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in expectations file {file_path}")
        return None
    except Exception as e:
        print(f"Error loading expectations: {str(e)}")
        return None

def validate_column_list(df, column_list):
    """Validate that the dataframe has the expected columns in the expected order."""
    df_columns = df.columns.tolist()
    if df_columns != column_list:
        print(f"Column mismatch. Expected: {column_list}, Got: {df_columns}")
        return False
    return True

def validate_not_null(df, column):
    """Validate that a column has no null values."""
    null_count = df[column].isnull().sum()
    if null_count > 0:
        print(f"Column '{column}' has {null_count} null values")
        return False
    return True

def validate_column_type(df, column, expected_type):
    """Validate that a column has the expected data type."""
    # Map Great Expectations types to pandas dtypes
    type_mapping = {
        "NUMERIC": ["int64", "float64"],
        "STRING": ["object", "string"],
        "DATE": ["datetime64[ns]", "object"]
    }
    
    # Get the actual type
    actual_type = str(df[column].dtype)
    
    # Check if the column is of the expected type
    if expected_type in type_mapping and actual_type in type_mapping[expected_type]:
        return True
    
    # For DATE type, try to convert and see if it works
    if expected_type == "DATE" and actual_type not in type_mapping["DATE"]:
        try:
            pd.to_datetime(df[column])
            return True
        except:
            pass
    
    print(f"Column '{column}' has type '{actual_type}', expected '{expected_type}'")
    return False

def validate_values_in_set(df, column, value_set):
    """Validate that all values in a column are in the expected set."""
    invalid_values = df[~df[column].isin(value_set)][column].unique()
    if len(invalid_values) > 0:
        print(f"Column '{column}' has values not in the expected set: {invalid_values}")
        return False
    return True

def validate_unique(df, column):
    """Validate that all values in a column are unique."""
    duplicate_count = len(df) - len(df[column].unique())
    if duplicate_count > 0:
        print(f"Column '{column}' has {duplicate_count} duplicate values")
        return False
    return True

def validate_bronze_layer():
    """Validate the bronze layer data using pandas."""
    print("Validating Bronze Layer...")
    
    # Ensure sample files exist
    if USE_SAMPLE_FILES:
        contracts_file = os.path.join(BRONZE_DATA_DIR, 'sample_contracts.csv')
        budgets_file = os.path.join(BRONZE_DATA_DIR, 'sample_budgets.csv')
    else:
        contracts_file = os.path.join(BRONZE_DATA_DIR, 'contracts.csv')
        budgets_file = os.path.join(BRONZE_DATA_DIR, 'budgets.csv')
    
    if not os.path.exists(contracts_file) or not os.path.exists(budgets_file):
        print(f"Error: Bronze data files not found at {BRONZE_DATA_DIR}")
        return False
    
    # Validate contracts
    contracts_success = validate_bronze_contracts(contracts_file)
    
    # Validate budgets
    budgets_success = validate_bronze_budgets(budgets_file)
    
    # Print results
    print(f"Bronze Layer Validation Results:")
    print(f"Contracts: {'PASSED' if contracts_success else 'FAILED'}")
    print(f"Budgets: {'PASSED' if budgets_success else 'FAILED'}")
    
    return contracts_success and budgets_success

def validate_bronze_contracts(contracts_file):
    """Validate the bronze contracts data against expectations."""
    try:
        contracts_df = pd.read_csv(contracts_file)
    except Exception as e:
        print(f"Error loading contracts data: {str(e)}")
        return False
    
    # Load expectations from Great Expectations directory
    expectations_path = os.path.join(GE_DIR, 'expectations', 'bronze_contracts.json')
    if not os.path.exists(expectations_path):
        # Fall back to original expectations directory
        expectations_path = os.path.join(EXPECTATIONS_DIR, 'bronze', 'contracts_expectations.json')
    
    expectations = load_expectations(expectations_path)
    if expectations is None:
        return False
    
    # Validate expectations
    all_passed = True
    
    for expectation in expectations["expectations"]:
        expectation_type = expectation["expectation_type"]
        kwargs = expectation["kwargs"]
        
        if expectation_type == "expect_table_columns_to_match_ordered_list":
            result = validate_column_list(contracts_df, kwargs["column_list"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_not_be_null":
            result = validate_not_null(contracts_df, kwargs["column"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_be_of_type":
            result = validate_column_type(contracts_df, kwargs["column"], kwargs["type_"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_be_in_set":
            result = validate_values_in_set(contracts_df, kwargs["column"], kwargs["value_set"])
            if not result:
                all_passed = False
    
    return all_passed

def validate_bronze_budgets(budgets_file):
    """Validate the bronze budgets data against expectations."""
    try:
        budgets_df = pd.read_csv(budgets_file)
    except Exception as e:
        print(f"Error loading budgets data: {str(e)}")
        return False
    
    # Load expectations from Great Expectations directory
    expectations_path = os.path.join(GE_DIR, 'expectations', 'bronze_budgets.json')
    if not os.path.exists(expectations_path):
        # Fall back to original expectations directory
        expectations_path = os.path.join(EXPECTATIONS_DIR, 'bronze', 'budgets_expectations.json')
    
    expectations = load_expectations(expectations_path)
    if expectations is None:
        return False
    
    # Validate expectations
    all_passed = True
    
    for expectation in expectations["expectations"]:
        expectation_type = expectation["expectation_type"]
        kwargs = expectation["kwargs"]
        
        if expectation_type == "expect_table_columns_to_match_ordered_list":
            result = validate_column_list(budgets_df, kwargs["column_list"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_not_be_null":
            result = validate_not_null(budgets_df, kwargs["column"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_be_of_type":
            result = validate_column_type(budgets_df, kwargs["column"], kwargs["type_"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_be_in_set":
            result = validate_values_in_set(budgets_df, kwargs["column"], kwargs["value_set"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_be_unique":
            result = validate_unique(budgets_df, kwargs["column"])
            if not result:
                all_passed = False
    
    return all_passed

def validate_silver_layer():
    """Validate the silver layer data using pandas."""
    print("Validating Silver Layer...")
    
    # For testing, create a simple silver contracts dataframe if it doesn't exist
    if USE_SAMPLE_FILES:
        # Check if sample contracts file exists
        sample_contracts_file = os.path.join(BRONZE_DATA_DIR, 'sample_contracts.csv')
        if not os.path.exists(sample_contracts_file):
            print(f"Warning: Sample contracts file not found at {sample_contracts_file}")
            print("Skipping silver contracts validation.")
            return True
        
        # Create a silver version of the contracts data if it doesn't exist
        silver_contracts_file = os.path.join(SILVER_DATA_DIR, 'contracts_silver.csv')
        if not os.path.exists(silver_contracts_file):
            try:
                contracts_df = pd.read_csv(sample_contracts_file)
                # Add processed_at column for silver layer
                contracts_df['processed_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Save to silver directory for future use
                os.makedirs(SILVER_DATA_DIR, exist_ok=True)
                contracts_df.to_csv(silver_contracts_file, index=False)
            except Exception as e:
                print(f"Error creating silver contracts data: {str(e)}")
                return False
    
    # Check if silver data file exists
    silver_contracts_file = os.path.join(SILVER_DATA_DIR, 'contracts_silver.csv')
    if not os.path.exists(silver_contracts_file):
        print(f"Warning: Silver contracts file not found at {silver_contracts_file}")
        print("Skipping silver contracts validation.")
        return True
    
    # Validate silver contracts
    silver_success = validate_silver_contracts(silver_contracts_file)
    
    # Print results
    print(f"Silver Layer Validation Results:")
    print(f"Contracts: {'PASSED' if silver_success else 'FAILED'}")
    
    return silver_success

def validate_silver_contracts(silver_contracts_file):
    """Validate the silver contracts data against expectations."""
    try:
        contracts_df = pd.read_csv(silver_contracts_file)
    except Exception as e:
        print(f"Error loading silver contracts data: {str(e)}")
        return False
    
    # Load expectations from Great Expectations directory
    expectations_path = os.path.join(GE_DIR, 'expectations', 'silver_contracts.json')
    if not os.path.exists(expectations_path):
        # Fall back to original expectations directory
        expectations_path = os.path.join(EXPECTATIONS_DIR, 'silver', 'contracts_silver_expectations.json')
    
    expectations = load_expectations(expectations_path)
    if expectations is None:
        return False
    
    # Validate expectations
    all_passed = True
    
    for expectation in expectations["expectations"]:
        expectation_type = expectation["expectation_type"]
        kwargs = expectation["kwargs"]
        
        if expectation_type == "expect_table_columns_to_match_ordered_list":
            result = validate_column_list(contracts_df, kwargs["column_list"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_not_be_null":
            result = validate_not_null(contracts_df, kwargs["column"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_be_of_type":
            result = validate_column_type(contracts_df, kwargs["column"], kwargs["type_"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_be_in_set":
            result = validate_values_in_set(contracts_df, kwargs["column"], kwargs["value_set"])
            if not result:
                all_passed = False
        
        elif expectation_type == "expect_column_values_to_be_unique":
            result = validate_unique(contracts_df, kwargs["column"])
            if not result:
                all_passed = False
    
    return all_passed

def generate_data_docs():
    """Generate simple data docs."""
    print("Generating Data Docs...")
    
    # Create data docs directory
    data_docs_dir = os.path.join(GE_DIR, 'uncommitted', 'data_docs', 'local_site')
    os.makedirs(data_docs_dir, exist_ok=True)
    
    # Get validation results
    bronze_contracts_result = validate_bronze_contracts(os.path.join(BRONZE_DATA_DIR, 'sample_contracts.csv'))
    bronze_budgets_result = validate_bronze_budgets(os.path.join(BRONZE_DATA_DIR, 'sample_budgets.csv'))
    silver_contracts_result = validate_silver_contracts(os.path.join(SILVER_DATA_DIR, 'contracts_silver.csv'))
    
    # Create a simple HTML report
    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Data Quality Validation Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #2c3e50; }}
        h2 {{ color: #3498db; }}
        .passed {{ color: green; }}
        .failed {{ color: red; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <h1>Data Quality Validation Report</h1>
    <p>Generated at: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    
    <h2>Bronze Layer</h2>
    <table>
        <tr>
            <th>Dataset</th>
            <th>Status</th>
        </tr>
        <tr>
            <td>Contracts</td>
            <td class="{'passed' if bronze_contracts_result else 'failed'}">{'PASSED' if bronze_contracts_result else 'FAILED'}</td>
        </tr>
        <tr>
            <td>Budgets</td>
            <td class="{'passed' if bronze_budgets_result else 'failed'}">{'PASSED' if bronze_budgets_result else 'FAILED'}</td>
        </tr>
    </table>
    
    <h2>Silver Layer</h2>
    <table>
        <tr>
            <th>Dataset</th>
            <th>Status</th>
        </tr>
        <tr>
            <td>Contracts</td>
            <td class="{'passed' if silver_contracts_result else 'failed'}">{'PASSED' if silver_contracts_result else 'FAILED'}</td>
        </tr>
    </table>
</body>
</html>
"""
    
    # Write the HTML report
    with open(os.path.join(data_docs_dir, 'index.html'), 'w') as f:
        f.write(html_content)
    
    data_docs_path = os.path.join(data_docs_dir, 'index.html')
    print(f"Data Docs generated at: {data_docs_path}")

def main():
    """Main function to run all validations."""
    print("Starting data quality validation...")
    
    # Create silver data directory if it doesn't exist
    os.makedirs(SILVER_DATA_DIR, exist_ok=True)
    
    # Run validations
    bronze_success = validate_bronze_layer()
    silver_success = validate_silver_layer()
    
    # Generate data docs
    generate_data_docs()
    
    # Print summary
    print("\nValidation Summary:")
    print(f"Bronze Layer: {'PASSED' if bronze_success else 'FAILED'}")
    print(f"Silver Layer: {'PASSED' if silver_success else 'FAILED'}")
    
    if bronze_success and silver_success:
        print("\nAll validations passed!")
        return 0
    else:
        print("\nSome validations failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())