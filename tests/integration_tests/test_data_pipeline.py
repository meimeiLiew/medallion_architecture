import os
import sys
import pandas as pd
import pytest

# Add parent directory to path to import validate_data_quality
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from validate_data_quality import (
    validate_bronze_layer,
    validate_silver_layer
)

def test_bronze_to_silver_pipeline():
    """
    Integration test to verify the data flow from bronze to silver layer.
    
    This test:
    1. Validates that bronze layer data meets quality expectations
    2. Validates that silver layer data meets quality expectations
    3. Verifies that the transformation from bronze to silver is correct
    """
    # Set up test environment
    os.environ['USE_SAMPLE_FILES'] = 'True'
    
    # Step 1: Validate bronze layer
    bronze_success = validate_bronze_layer()
    assert bronze_success, "Bronze layer validation failed"
    
    # Step 2: Validate silver layer
    # This implicitly tests the transformation from bronze to silver
    silver_success = validate_silver_layer()
    assert silver_success, "Silver layer validation failed"
    
    # Step 3: Verify specific transformations
    # Load bronze and silver data
    bronze_contracts = pd.read_csv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 
                                  'bronze', 'data', 'sample_contracts.csv'))
    
    silver_contracts = pd.read_csv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 
                                  'silver_layer', 'data', 'contracts_silver.csv'))
    
    # Verify that all bronze contracts are in silver
    assert len(bronze_contracts) <= len(silver_contracts), "Some contracts are missing in silver layer"
    
    # Verify that contract IDs are preserved
    bronze_ids = set(bronze_contracts['contract_id'])
    silver_ids = set(silver_contracts['contract_id'])
    assert bronze_ids.issubset(silver_ids), "Some contract IDs are missing in silver layer"
    
    # Verify that silver has the processed_at column
    assert 'processed_at' in silver_contracts.columns, "Silver contracts missing processed_at column" 