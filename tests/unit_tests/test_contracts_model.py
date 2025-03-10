import pytest
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from great_expectations.dataset import PandasDataset

# Load environment variables
load_dotenv()

# Sample test data
sample_contracts = [
    {
        "contract_id": "C001",
        "contract_name": "Project Alpha",
        "client_name": "Acme Corp",
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "contract_value": 100000.00,
        "contract_status": "Active"
    },
    {
        "contract_id": "C002",
        "contract_name": "Project Beta",
        "client_name": "XYZ Inc",
        "start_date": "2023-02-15",
        "end_date": "2023-08-15",
        "contract_value": 75000.00,
        "contract_status": "Active"
    },
    {
        "contract_id": "C003",
        "contract_name": "Project Gamma",
        "client_name": "Acme Corp",
        "start_date": "2022-10-01",
        "end_date": "2023-03-31",
        "contract_value": 50000.00,
        "contract_status": "Completed"
    }
]

@pytest.fixture
def contracts_df():
    """Create a sample contracts DataFrame for testing."""
    df = pd.DataFrame(sample_contracts)
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    return df

def test_contracts_data_structure(contracts_df):
    """Test that the contracts data has the expected structure."""
    expected_columns = [
        "contract_id", "contract_name", "client_name", 
        "start_date", "end_date", "contract_value", "contract_status"
    ]
    
    # Check that all expected columns are present
    assert all(col in contracts_df.columns for col in expected_columns)
    
    # Check data types
    assert contracts_df['contract_id'].dtype == 'object'
    assert contracts_df['contract_value'].dtype == 'float64'
    assert pd.api.types.is_datetime64_dtype(contracts_df['start_date'])
    assert pd.api.types.is_datetime64_dtype(contracts_df['end_date'])

def test_contracts_data_quality(contracts_df):
    """Test the data quality of the contracts data."""
    # Convert to Great Expectations dataset for validation
    ge_df = PandasDataset(contracts_df)
    
    # Test for no null values in required fields
    assert ge_df.expect_column_values_to_not_be_null("contract_id").success
    assert ge_df.expect_column_values_to_not_be_null("contract_name").success
    assert ge_df.expect_column_values_to_not_be_null("client_name").success
    
    # Test that contract values are positive
    assert ge_df.expect_column_values_to_be_between(
        "contract_value", min_value=0, strict_min=False
    ).success
    
    # Test that contract status is in the expected set
    assert ge_df.expect_column_values_to_be_in_set(
        "contract_status", 
        ["Active", "Completed", "Pending", "Cancelled"]
    ).success
    
    # Test that end dates are after start dates
    for _, row in contracts_df.iterrows():
        assert row['end_date'] >= row['start_date']

def test_contracts_aggregation():
    """Test the contract aggregation logic."""
    df = pd.DataFrame(sample_contracts)
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    df['contract_value'] = df['contract_value'].astype(float)
    
    # Aggregate by client
    client_agg = df.groupby('client_name').agg(
        total_contracts=('contract_id', 'count'),
        total_value=('contract_value', 'sum')
    ).reset_index()
    
    # Check Acme Corp aggregations
    acme_row = client_agg[client_agg['client_name'] == 'Acme Corp'].iloc[0]
    assert acme_row['total_contracts'] == 2
    assert acme_row['total_value'] == 150000.00
    
    # Check XYZ Inc aggregations
    xyz_row = client_agg[client_agg['client_name'] == 'XYZ Inc'].iloc[0]
    assert xyz_row['total_contracts'] == 1
    assert xyz_row['total_value'] == 75000.00 